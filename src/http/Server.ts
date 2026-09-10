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
import { Outbound, OutboundInvalid } from "../engine/Outbound.ts"
import type { Outgoing } from "../domain/outbound.ts"
import type { MessageId } from "../domain/ids.ts"
import { TurnNotOpen, TurnRegistry } from "../engine/Turns.ts"
import {
  ContextNotAllowed,
  OwnerContext,
  lineChats,
  lineSearch,
  primaryChat,
  primaryChats,
  primaryContacts,
  primaryMedia,
  primaryMessages,
  primarySearch,
  type TurnAccess,
} from "../engine/Context.ts"
import type { TurnId } from "../domain/ids.ts"
import { Scheduler } from "../store/Scheduled.ts"
import { Store } from "../store/Store.ts"
import { NotAllowlisted, SendFailed, WhatsApp } from "../whatsapp/WhatsApp.ts"
import type { ChatId } from "../domain/ids.ts"
import type { HistoryRow } from "../domain/history.ts"
import { EVENT_SCHEMA_VERSION, type BridgeEvent } from "../domain/events.ts"
import { BridgeApi, Unhealthy } from "./Api.ts"

const status = Effect.gen(function* () {
  const health = yield* Health
  const registry = yield* TurnRegistry
  const store = yield* Store
  const config = yield* BridgeConfig
  const snapshot = yield* health.get
  const openTurns = yield* registry.openCount
  const owned = Option.isSome(config.owner)
  return {
    ...snapshot,
    openTurns,
    databasePath: store.databasePath,
    history: store.historyEnabled,
    access: owned ? ("owner" as const) : ("open" as const),
    ...(owned ? { observerUrl: config.observerUrl } : {}),
  }
})

/** The access rule, applied to a chat the agent wants to reach. */
const ensureAllowed = (chatId: ChatId) =>
  WhatsApp.use((w) => w.chatAllowed(chatId)).pipe(Effect.flatMap((ok) => (ok ? Effect.void : Effect.fail(new NotAllowlisted({ chatId })))))

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
        yield* ensureAllowed(params.chatId)
        if (payload.sendAt !== undefined) {
          const scheduler = yield* Scheduler
          const held = yield* scheduler.schedule(
            { kind: "text", chatId: params.chatId, payload: { text: payload.text, ...(payload.quoteMessageId === undefined ? {} : { quoteMessageId: payload.quoteMessageId }) } },
            payload.sendAt,
            payload.deliverWhenOnline === true,
          )
          return { scheduledId: held.scheduledId, sendAt: held.sendAt, deliverWhenOnline: held.deliverWhenOnline }
        }
        const messageId = yield* whatsapp.sendText(params.chatId, payload.text, { quoteMessageId: payload.quoteMessageId, mentions: payload.mentions })
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("sendLocation", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        yield* ensureAllowed(params.chatId)
        const { quoteMessageId, ...location } = payload
        const messageId = yield* whatsapp.sendLocation(params.chatId, location, { quoteMessageId })
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("sendContacts", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        yield* ensureAllowed(params.chatId)
        const messageId = yield* whatsapp.sendContacts(params.chatId, payload.contacts, { quoteMessageId: payload.quoteMessageId })
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("sendPoll", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        yield* ensureAllowed(params.chatId)
        const messageId = yield* whatsapp.sendPoll(
          params.chatId,
          { question: payload.question, options: payload.options, multiple: payload.multiple === true },
          { quoteMessageId: payload.quoteMessageId },
        )
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("sendAlbum", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        const outbound = yield* Outbound
        yield* ensureAllowed(params.chatId)
        const items = yield* Effect.forEach(payload.items, outbound.load)
        const odd = items.find((i) => i.kind !== "image" && i.kind !== "video")
        if (odd !== undefined) return yield* new OutboundInvalid({ detail: `an album takes images and videos only, not ${odd.kind}` })
        const messageIds = yield* whatsapp.sendAlbum(params.chatId, items, { quoteMessageId: payload.quoteMessageId })
        return { chatId: params.chatId, messageIds }
      }),
    )
    .handle("presence", ({ params }) => WhatsApp.use((w) => w.watchPresence(params.chatId)))
    .handle("forward", ({ params, payload }) =>
      Effect.gen(function* () {
        const whatsapp = yield* WhatsApp
        yield* ensureAllowed(params.chatId)
        const messageId = yield* whatsapp.forward(params.chatId, payload.fromChatId, payload.messageId)
        return { chatId: params.chatId, messageId }
      }),
    )
    .handle("edit", ({ params, payload }) => WhatsApp.use((w) => w.editText(params.chatId, params.messageId, payload.text)))
    .handle("deleteMessage", ({ params }) => WhatsApp.use((w) => w.deleteMessage(params.chatId, params.messageId)))
    .handle("sendMedia", ({ params, payload }) =>
      Effect.gen(function* () {
        const outbound = yield* Outbound
        const whatsapp = yield* WhatsApp
        yield* ensureAllowed(params.chatId)
        const { sendAt, deliverWhenOnline, ...file } = payload
        // The file is checked now, even for a later send, so a bad path is a 4xx today rather than a failure tonight.
        const media = yield* outbound.load(file)
        if (sendAt !== undefined) {
          const scheduler = yield* Scheduler
          const held = yield* scheduler.schedule({ kind: "media", chatId: params.chatId, payload: file }, sendAt, deliverWhenOnline === true)
          return { scheduledId: held.scheduledId, sendAt: held.sendAt, deliverWhenOnline: held.deliverWhenOnline }
        }
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

/** Any row that still has a file on disk counts as used when it is returned. */
const touchMedia = (rows: ReadonlyArray<HistoryRow>) =>
  Effect.forEach(
    rows.filter((r) => r.mediaPath !== undefined && r.mediaEvictedAt === undefined),
    (r) => MediaStore.use((m) => m.touch(r.chatId, r.messageId)),
    { discard: true },
  )

export const ContactsGroupLive = HttpApiBuilder.group(BridgeApi, "contacts", (handlers) =>
  handlers.handle("contact", ({ params }) => WhatsApp.use((w) => w.contact(params.jid))),
)

/** The turn's capability, or why there is none. */
const accessOf = (turnId: TurnId) => TurnRegistry.use((r) => r.access(turnId))

/** Owner turns only: everything under `primary`, and the line across chats. */
const ownerOnly = (turnId: TurnId) =>
  Effect.flatMap(accessOf(turnId), (a) =>
    a.kind === "owner"
      ? Effect.succeed(a.context)
      : Effect.fail(new ContextNotAllowed({ turnId, access: a.kind, detail: "only a turn in the owner's DM may read this" })),
  )

/** A chat on the line: any chat for an owner turn, only its own for a chat turn. */
const chatScoped = (turnId: TurnId, chatId: ChatId) =>
  Effect.flatMap(accessOf(turnId), (a): Effect.Effect<TurnAccess, ContextNotAllowed> => {
    if (a.kind === "owner" || a.context.chatId === chatId) return Effect.succeed(a)
    return Effect.fail(new ContextNotAllowed({ turnId, access: a.kind, detail: `this turn may read ${a.context.chatId} only` }))
  })

const withOwner = <A, E, R>(turnId: TurnId, read: Effect.Effect<A, E, R | OwnerContext>) =>
  Effect.flatMap(ownerOnly(turnId), (context) => read.pipe(Effect.provideService(OwnerContext, context)))

const logRead = (turnId: TurnId, what: string, extra: Record<string, unknown> = {}) =>
  Effect.logInfo("context read").pipe(Effect.annotateLogs({ turnId, what, ...extra }))

export const ContextGroupLive = HttpApiBuilder.group(BridgeApi, "context", (handlers) =>
  handlers
    .handle("summary", ({ params }) =>
      Effect.gen(function* () {
        const registry = yield* TurnRegistry
        const turn = yield* registry.get(params.turnId)
        const access = yield* registry.access(params.turnId)
        const config = yield* BridgeConfig
        return { turnId: params.turnId, chatId: turn.chatId, access: access.kind, primary: Option.isSome(config.owner) }
      }),
    )
    .handle("lineChats", ({ params }) => withOwner(params.turnId, lineChats).pipe(Effect.tap(() => logRead(params.turnId, "line.chats"))))
    .handle("lineChat", ({ params }) =>
      Effect.gen(function* () {
        const a = yield* chatScoped(params.turnId, params.chatId)
        yield* logRead(params.turnId, "line.chat", { chatId: params.chatId })
        return yield* a.kind === "owner" ? a.context.line.chat(params.chatId) : a.context.chat
      }),
    )
    .handle("lineMessages", ({ params, query }) =>
      Effect.gen(function* () {
        const a = yield* chatScoped(params.turnId, params.chatId)
        const limit = query.limit ?? 50
        const searching = query.q !== undefined && query.q.trim().length > 0
        const rows: ReadonlyArray<HistoryRow> =
          a.kind === "owner"
            ? searching
              ? yield* a.context.line.search(query.q ?? "", { chatId: params.chatId, before: query.before, limit })
              : yield* a.context.line.messages(params.chatId, { before: query.before, since: query.since, limit })
            : searching
              ? yield* a.context.search(query.q ?? "", { before: query.before, limit })
              : yield* a.context.messages({ before: query.before, since: query.since, limit })
        yield* touchMedia(rows)
        yield* logRead(params.turnId, searching ? "line.search" : "line.messages", { chatId: params.chatId, rows: rows.length })
        return rows
      }),
    )
    .handle("lineSearch", ({ params, query }) =>
      withOwner(params.turnId, lineSearch(query.q, { before: query.before, limit: query.limit ?? 50 })).pipe(
        Effect.tap((rows) => Effect.andThen(touchMedia(rows), logRead(params.turnId, "line.search", { rows: rows.length }))),
        Effect.map((rows): ReadonlyArray<HistoryRow> => rows),
      ),
    )
    .handle("primaryChats", ({ params }) => withOwner(params.turnId, primaryChats).pipe(Effect.tap(() => logRead(params.turnId, "primary.chats"))))
    .handle("primaryChat", ({ params }) =>
      withOwner(params.turnId, primaryChat(params.chatId)).pipe(Effect.tap(() => logRead(params.turnId, "primary.chat", { chatId: params.chatId }))),
    )
    .handle("primaryMessages", ({ params, query }) =>
      Effect.gen(function* () {
        const limit = query.limit ?? 50
        const searching = query.q !== undefined && query.q.trim().length > 0
        const rows: ReadonlyArray<HistoryRow> = searching
          ? yield* withOwner(params.turnId, primarySearch(query.q ?? "", { chatId: params.chatId, before: query.before, limit }))
          : yield* withOwner(params.turnId, primaryMessages(params.chatId, { before: query.before, since: query.since, limit }))
        yield* logRead(params.turnId, searching ? "primary.search" : "primary.messages", { chatId: params.chatId, rows: rows.length })
        return rows
      }),
    )
    .handle("primarySearch", ({ params, query }) =>
      withOwner(params.turnId, primarySearch(query.q, { before: query.before, limit: query.limit ?? 50 })).pipe(
        Effect.tap((rows) => logRead(params.turnId, "primary.search", { rows: rows.length })),
        Effect.map((rows): ReadonlyArray<HistoryRow> => rows),
      ),
    )
    .handle("primaryContacts", ({ params, query }) =>
      withOwner(params.turnId, primaryContacts(query.q, query.limit ?? 50)).pipe(Effect.tap((rows) => logRead(params.turnId, "primary.contacts", { rows: rows.length }))),
    )
    .handle("primaryMedia", ({ params }) =>
      withOwner(params.turnId, primaryMedia(params.chatId, params.messageId)).pipe(
        Effect.tap(() => logRead(params.turnId, "primary.media", { chatId: params.chatId, messageId: params.messageId })),
      ),
    ),
)

export const ScheduledGroupLive = HttpApiBuilder.group(BridgeApi, "scheduled", (handlers) =>
  handlers.handle("list", () => Scheduler.use((s) => s.list)).handle("cancel", ({ params }) => Scheduler.use((s) => s.cancel(params.scheduledId))),
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
        const bubbles: Array<Outgoing> = [
          ...(payload.replies ?? []).map((item): Outgoing => (typeof item === "string" ? { text: item } : item)),
          ...(payload.text ? [{ text: payload.text }] : []),
        ]
        // The reply-level quote goes on the first bubble unless that bubble brought its own.
        const first = bubbles[0]
        if (payload.quoteMessageId !== undefined && first !== undefined && !("bytes" in first) && first.quoteMessageId === undefined) {
          bubbles[0] = { ...first, quoteMessageId: payload.quoteMessageId }
        }
        const sentMessageIds = yield* registry.reply(params.turnId, [...bubbles, ...files], {
          replaceProgress: payload.replaceProgress,
          album: payload.album,
        })
        return { turnId: params.turnId, sentMessageIds }
      }),
    )
    .handle("progress", ({ params, payload }) =>
      Effect.gen(function* () {
        const registry = yield* TurnRegistry
        const whatsapp = yield* WhatsApp
        const turn = yield* registry.get(params.turnId)
        if (turn.state !== "responding" && turn.state !== "awaiting") return yield* new TurnNotOpen({ turnId: params.turnId, state: turn.state })
        let progressMessageId: MessageId | undefined
        if (payload.text !== undefined) {
          progressMessageId = yield* whatsapp
            .sendText(turn.chatId, payload.text)
            .pipe(Effect.catchTag("MessageUnknown", (e) => new SendFailed({ operation: "progress", chatId: turn.chatId, detail: e.message })))
        }
        const result = yield* registry.progress(params.turnId, { extendBy: payload.extendBy, progressMessageId })
        return {
          turnId: params.turnId,
          deadline: result.deadline,
          typingUntil: result.typingUntil,
          ...(progressMessageId === undefined ? {} : { progressMessageId }),
        }
      }),
    )
    .handle("abandon", ({ params, payload }) => TurnRegistry.use((r) => r.abandon(params.turnId, payload.reason, payload.text))),
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
    Layer.provide([HealthGroupLive, EventsGroupLive, ChatsGroupLive, ContactsGroupLive, ContextGroupLive, ScheduledGroupLive, TurnsGroupLive]),
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
