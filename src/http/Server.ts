import { NodeHttpServer } from "@effect/platform-node"
import { Cause, Clock, DateTime, Effect, Layer, Option, Redacted, Result, Schedule, Stream } from "effect"
import { HttpRouter, HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiError, HttpApiScalar } from "effect/unstable/httpapi"
import { createHash, timingSafeEqual } from "node:crypto"
import { createServer } from "node:http"
import { BridgeConfig } from "../config.ts"
import { Events } from "../engine/Events.ts"
import { Health } from "../engine/Health.ts"
import { MediaStore, MediaStoreError } from "../engine/Media.ts"
import { Outbound } from "../engine/Outbound.ts"
import type { Outgoing } from "../domain/outbound.ts"
import { TurnRegistry } from "../engine/Turns.ts"
import { WhatsApp } from "../whatsapp/WhatsApp.ts"
import { EVENT_SCHEMA_VERSION, type BridgeEvent } from "../domain/events.ts"
import { BridgeApi, Unhealthy } from "./Api.ts"

const status = Effect.gen(function* () {
  const health = yield* Health
  const registry = yield* TurnRegistry
  const snapshot = yield* health.get
  const openTurns = yield* registry.openCount
  return { ...snapshot, openTurns }
})

export const HealthGroupLive = HttpApiBuilder.group(BridgeApi, "health", (handlers) =>
  handlers
    .handle("check", () =>
      Effect.gen(function* () {
        const s = yield* status
        if (s.state !== "ok") return yield* new Unhealthy({ status: s })
        return s
      }),
    )
    .handle("status", () => status),
)

/** Sent every 25 seconds so a dead SSE connection is noticed on either side. */
const HEARTBEAT_EVERY = "25 seconds"

const marker = (tag: "Ready" | "Heartbeat", seq: number) =>
  Effect.map(DateTime.now, (at): BridgeEvent => ({ _tag: tag, v: EVENT_SCHEMA_VERSION, seq, at }))

export const EventsGroupLive = HttpApiBuilder.group(BridgeApi, "events", (handlers) =>
  handlers.handle("stream", ({ query }) =>
    Events.use((events) =>
      Effect.gen(function* () {
        const latest = yield* events.latestSeq
        // `Ready` goes out immediately: it flushes the response headers and tells Intro where the cursor is.
        const ready = yield* marker("Ready", latest)
        const live = events.since(query.since ?? latest)
        const heartbeats = Stream.fromSchedule(Schedule.spaced(HEARTBEAT_EVERY)).pipe(
          Stream.mapEffect(() => Effect.flatMap(events.latestSeq, (seq) => marker("Heartbeat", seq))),
        )
        return Stream.concat(Stream.make(ready), Stream.merge(live, heartbeats))
      }),
    ),
  ),
)

export const ChatsGroupLive = HttpApiBuilder.group(BridgeApi, "chats", (handlers) =>
  handlers
    .handle("send", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        const messageId = yield* whatsapp.sendText(params.chatId, payload.text)
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("sendMedia", ({ params, payload }) =>
      Effect.gen(function* () {
        const outbound = yield* Outbound
        const whatsapp = yield* WhatsApp
        const media = yield* outbound.load(payload)
        const messageId = yield* whatsapp.sendMedia(params.chatId, media)
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("react", ({ params, payload }) => WhatsApp.use((w) => w.react(params.chatId, params.messageId, payload.emoji)))
    .handle("unreact", ({ params }) => WhatsApp.use((w) => w.unreact(params.chatId, params.messageId)))
    .handle("typing", ({ params, payload }) => WhatsApp.use((w) => w.setTyping(params.chatId, payload.composing)))
    .handle("media", ({ params }) => MediaStore.use((m) => m.get(params.chatId, params.messageId)))
    .handle("mediaContent", ({ params }) =>
      Effect.gen(function* () {
        const store = yield* MediaStore
        const file = yield* store.get(params.chatId, params.messageId)
        return yield* HttpServerResponse.file(file.path, { contentType: file.mimetype }).pipe(
          Effect.mapError((e) => new MediaStoreError({ operation: "serve", detail: e.message })),
        )
      }),
    ),
)

export const TurnsGroupLive = HttpApiBuilder.group(BridgeApi, "turns", (handlers) =>
  handlers
    .handle("list", () => TurnRegistry.use((r) => r.list))
    .handle("get", ({ params }) => TurnRegistry.use((r) => r.get(params.turnId)))
    .handle("reply", ({ params, payload }) =>
      Effect.gen(function* () {
        const registry = yield* TurnRegistry
        const outbound = yield* Outbound
        // Files are read and checked before the turn is touched, so a bad path is a 4xx and the turn stays open.
        yield* registry.get(params.turnId)
        const files = yield* Effect.forEach(payload.attachments ?? [], outbound.load)
        const replies: ReadonlyArray<Outgoing> = [...(payload.replies ?? []), ...(payload.text ? [payload.text] : []), ...files]
        const sentMessageIds = yield* registry.reply(params.turnId, replies)
        return { turnId: params.turnId, sentMessageIds }
      }),
    ),
)

/**
 * Client mistakes get a JSON body an agent can read, instead of the framework's
 * empty 400 / 404: a request that fails its schema (params, query, payload) and
 * an unknown route. This has to be router middleware, not server middleware:
 * the server turns a failed handler into a response and sends it before any
 * outer middleware sees the cause.
 */
const CLIENT_KINDS: ReadonlySet<string> = new Set(["Params", "Query", "Headers", "Payload"])

export const ClientErrorsLive = HttpRouter.middleware(
  (app) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest
      return yield* app.pipe(
        Effect.catchCause((cause) => {
          const failure = Option.getOrUndefined(Cause.findErrorOption(cause)) ?? Result.getOrUndefined(Cause.findDefect(cause))
          // `Body` / `ResponseHeaders` are the server failing to encode its own answer: a bug, not the client's fault.
          if (HttpApiError.HttpApiSchemaError.is(failure) && CLIENT_KINDS.has(failure.kind)) {
            return Effect.succeed(
              HttpServerResponse.jsonUnsafe(
                { _tag: "InvalidRequest", kind: failure.kind, message: failure.cause.message, method: request.method, path: request.url },
                { status: 400 },
              ),
            )
          }
          if (HttpServerError.isHttpServerError(failure) && failure.reason._tag === "RouteNotFound") {
            return Effect.succeed(
              HttpServerResponse.jsonUnsafe({ _tag: "RouteNotFound", method: request.method, path: request.url }, { status: 404 }),
            )
          }
          return Effect.failCause(cause)
        }),
      )
    }),
  { global: true },
)

/** All groups mounted, plus the OpenAPI document and the Scalar UI. */
export const ApiLive = Layer.mergeAll(
  HttpApiBuilder.layer(BridgeApi, { openapiPath: "/openapi.json" }).pipe(
    Layer.provide([HealthGroupLive, EventsGroupLive, ChatsGroupLive, TurnsGroupLive]),
  ),
  HttpApiScalar.layer(BridgeApi, { path: "/docs" }),
  ClientErrorsLive,
)

/** Public paths that never need the bearer token. */
const OPEN_PATHS = ["/health", "/docs", "/openapi.json"]

/** Constant-time comparison; hashing first equalises lengths so nothing leaks through timing. */
const sameToken = (presented: string, expected: string): boolean =>
  timingSafeEqual(createHash("sha256").update(presented).digest(), createHash("sha256").update(expected).digest())

/** When `BRIDGE_API_TOKEN` is set, everything but `/health` and the docs requires it. */
export const bearerAuth =
  (token: Option.Option<Redacted.Redacted<string>>) =>
  <E, R>(app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>) =>
    Option.match(token, {
      onNone: () => app,
      onSome: (secret) =>
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const path = request.url.split("?")[0] ?? request.url
          if (OPEN_PATHS.some((p) => path === p || path.startsWith(`${p}/`))) return yield* app
          const header = request.headers.authorization ?? ""
          const presented = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : ""
          if (presented.length > 0 && sameToken(presented, Redacted.value(secret))) return yield* app
          return HttpServerResponse.jsonUnsafe({ _tag: "Unauthorized", message: "missing or wrong bearer token" }, { status: 401 })
        }),
    })

/** One Info line per request; a client that hangs up (an SSE consumer leaving) is Debug, not an error. */
export const requestLog = <E, R>(app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const started = yield* Clock.currentTimeMillis
    const fields = (extra: Record<string, unknown>) =>
      Effect.map(Clock.currentTimeMillis, (now) => ({ method: request.method, path: request.url, ms: now - started, ...extra }))
    return yield* app.pipe(
      Effect.tap((response) => Effect.flatMap(fields({ status: response.status }), (f) => Effect.logInfo("http").pipe(Effect.annotateLogs(f)))),
      Effect.tapCause((cause) =>
        Effect.flatMap(fields({}), (f) =>
          Cause.hasInterrupts(cause)
            ? Effect.logDebug("http client went away").pipe(Effect.annotateLogs(f))
            : Effect.logWarning("http request failed", cause).pipe(Effect.annotateLogs(f)),
        ),
      ),
    )
  })

/** The listening server. Binds `BRIDGE_HOST:BRIDGE_PORT` (loopback by default). */
export const HttpServerLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const auth = bearerAuth(config.apiToken)
    return HttpRouter.serve(ApiLive, { disableLogger: true, middleware: (app) => requestLog(auth(app)) }).pipe(
      Layer.provide(
        // Open SSE connections are cut after the deadline so Ctrl-C never hangs on a subscriber.
        NodeHttpServer.layer(createServer, { host: config.host, port: config.port, gracefulShutdownTimeout: "3 seconds" }),
      ),
    )
  }),
)
