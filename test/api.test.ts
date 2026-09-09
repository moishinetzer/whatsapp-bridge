import { describe, expect, it } from "@effect/vitest"
import { NodeHttpServer } from "@effect/platform-node"
import { Effect, Fiber, Layer, Option, Queue, Redacted, Ref, Stream } from "effect"
import { HttpBody, HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { toChatId } from "../src/domain/ids.ts"
import { Events } from "../src/engine/Events.ts"
import { Health } from "../src/engine/Health.ts"
import { TurnId } from "../src/domain/ids.ts"
import { BridgeApi } from "../src/http/Api.ts"
import { ApiLive, bearerAuth } from "../src/http/Server.ts"
import { EngineTestLayer, FakeWhatsApp, TestResponder, chat, external, jpeg, message, settle, takeSoon } from "./support/fakes.ts"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const TOKEN = "s3cret"

const ServerTestLayer = Layer.unwrap(
  Effect.gen(function* () {
    const engine = EngineTestLayer({ apiToken: Option.some(Redacted.make(TOKEN)) })
    const server = HttpRouter.serve(ApiLive, { middleware: bearerAuth(Option.some(Redacted.make(TOKEN))) }).pipe(
      Layer.provide(engine),
      Layer.provideMerge(NodeHttpServer.layerTest),
    )
    return Layer.mergeAll(engine, server)
  }),
)

const client = (withToken: boolean) =>
  HttpApiClient.make(BridgeApi, {
    transformClient: (c) => (withToken ? c.pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(TOKEN))) : c),
  })

describe("HTTP API", () => {
  it.effect("health is 503 until the transport reports ok, then 200", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const health = yield* Health

      const unhealthy = yield* api.health.check().pipe(Effect.flip)
      expect(unhealthy._tag).toBe("Unhealthy")
      expect(unhealthy._tag === "Unhealthy" && unhealthy.status.state).toBe("starting")

      const status = yield* api.health.status()
      expect(status.state).toBe("starting")
      expect(status.openTurns).toBe(0)
      expect(status.responder).toBe("manual")

      yield* health.setState("ok")
      const ok = yield* api.health.check()
      expect(ok.state).toBe("ok")
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("requires the bearer token everywhere except /health", () =>
    Effect.gen(function* () {
      const anonymous = yield* client(false)
      const status = yield* anonymous.health.status().pipe(Effect.flip)
      expect(String(status)).toContain("401")
      const health = yield* anonymous.health.check().pipe(Effect.flip)
      expect(health._tag).toBe("Unhealthy")
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("sends text, reacts and unreacts through the transport", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const chatId = toChatId("+44 7700 900123")

      const sent = yield* api.chats.send({ params: { chatId }, payload: { text: "Open Grok Bot → approve the Link spend" } })
      expect(sent.chatId).toBe("447700900123@s.whatsapp.net")
      expect((yield* takeSoon(wa.sent)).text).toBe("Open Grok Bot → approve the Link spend")

      const messageId = message("x").messageId
      yield* api.chats.react({ params: { chatId, messageId }, payload: { emoji: "👀" } })
      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId, messageId, emoji: "👀" })
      yield* api.chats.unreact({ params: { chatId, messageId } })
      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId, messageId, emoji: "" })

      yield* api.chats.typing({ params: { chatId }, payload: { composing: true } })
      expect(yield* Ref.get(wa.typing)).toEqual([{ chatId, composing: true }])
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("streams events over SSE and lets Intro answer a turn", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      const stream = yield* api.events.stream({ query: {} })
      const eventsFiber = yield* stream.pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped)

      const m1 = message("via http")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle

      const open = yield* api.turns.list()
      expect(open.map((t) => t.turnId)).toEqual([call.turnId])
      expect((yield* api.turns.get({ params: { turnId: call.turnId } })).state).toBe("awaiting")

      const missing = yield* api.turns.reply({ params: { turnId: TurnId.make("turn_nope") }, payload: { text: "?" } }).pipe(Effect.flip)
      expect(missing._tag).toBe("TurnNotFound")

      const replyFiber = yield* api.turns.reply({ params: { turnId: call.turnId }, payload: { text: "answered" } }).pipe(Effect.forkScoped)
      expect((yield* takeSoon(wa.sent)).text).toBe("answered")
      const result = yield* Fiber.join(replyFiber)
      expect(result.turnId).toBe(call.turnId)
      expect(result.sentMessageIds.length).toBe(1)

      const seen = Array.from(yield* Fiber.join(eventsFiber))
      expect(seen.map((e) => e._tag)).toEqual(["Ready", "TurnOpened", "TurnCompleted"])
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("publishes connection changes on the event stream", () =>
    Effect.gen(function* () {
      const events = yield* Events
      const api = yield* client(true)
      yield* events.publish({ _tag: "ConnectionChanged", state: "ok" })
      const stream = yield* api.events.stream({ query: { since: 0 } })
      const fiber = yield* stream.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)
      const [ready, first] = Array.from(yield* Fiber.join(fiber))
      expect(ready?._tag).toBe("Ready")
      expect(ready?.seq).toBe(1)
      expect(first?._tag).toBe("ConnectionChanged")
      expect(first?.seq).toBe(1)
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})

describe("HTTP API validation", () => {
  it.effect("rejects an invalid payload with 400 instead of touching the transport", () =>
    Effect.gen(function* () {
      // The typed client validates before sending, so go raw to exercise the server's own check.
      const raw = yield* HttpClient.HttpClient
      const wa = yield* FakeWhatsApp
      const response = yield* raw.post("/chats/447700900123@s.whatsapp.net/messages", {
        headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
        body: HttpBody.jsonUnsafe({ text: "" }),
      })
      expect(response.status).toBe(400)
      const body = (yield* response.json) as { _tag: string; kind: string; message: string }
      expect(body._tag).toBe("InvalidRequest")
      expect(body.kind).toBe("Payload")
      expect(body.message).toContain("length")
      const badCursor = yield* raw.get("/events?since=abc", { headers: { authorization: `Bearer ${TOKEN}` } })
      expect(badCursor.status).toBe(400)
      expect(((yield* badCursor.json) as { _tag: string; kind: string }).kind).toBe("Query")
      const unknown = yield* raw.get("/nope", { headers: { authorization: `Bearer ${TOKEN}` } })
      expect(unknown.status).toBe(404)
      expect(((yield* unknown.json) as { _tag: string })._tag).toBe("RouteNotFound")
      const malformed = yield* raw.post("/chats/447700900123@s.whatsapp.net/messages", {
        headers: { authorization: `Bearer ${TOKEN}` },
        body: HttpBody.text("{not json", "application/json"),
      })
      expect(malformed.status).toBe(400)
      const wrongType = yield* raw.post("/chats/447700900123@s.whatsapp.net/messages", {
        headers: { authorization: `Bearer ${TOKEN}` },
        body: HttpBody.text("text=hi", "text/plain"),
      })
      expect(wrongType.status).toBe(415)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})

describe("HTTP API media", () => {
  it.effect("returns the local file record and streams the bytes", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const raw = yield* HttpClient.HttpClient
      const wa = yield* FakeWhatsApp
      const m = message("photo", { kind: "image", mimetype: "image/jpeg" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg(100)]]))

      const file = yield* api.chats.media({ params: { chatId: chat, messageId: m.messageId } })
      expect(file.size).toBe(100)
      expect(file.mimetype).toBe("image/jpeg")

      const response = yield* raw.get(`/chats/${chat}/messages/${m.messageId}/media/content`, {
        headers: { authorization: `Bearer ${TOKEN}` },
      })
      expect(response.status).toBe(200)
      expect(response.headers["content-type"]).toContain("image/jpeg")
      const bytes = yield* response.arrayBuffer
      expect(new Uint8Array(bytes)).toEqual(jpeg(100).bytes)

      const missing = yield* api.chats.media({ params: { chatId: chat, messageId: message("x").messageId } }).pipe(Effect.flip)
      expect(missing._tag).toBe("MediaUnavailable")
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})

describe("HTTP API outbound media", () => {
  it.effect("sends a file from raw bytes or a path, and reports what it could not send", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp

      const fromBytes = yield* api.chats.sendMedia({
        params: { chatId: chat },
        payload: { data: new Uint8Array([1, 2, 3, 4]), mimetype: "image/png", caption: "a chart" },
      })
      const sentBytes = yield* takeSoon(wa.sentMedia)
      expect(sentBytes.messageId).toBe(fromBytes.messageId)
      expect(sentBytes.media).toMatchObject({ kind: "image", mimetype: "image/png", caption: "a chart" })
      expect(Array.from(sentBytes.media.bytes)).toEqual([1, 2, 3, 4])

      const dir = mkdtempSync(join(tmpdir(), "bridge-api-"))
      const file = join(dir, "notes.pdf")
      writeFileSync(file, new Uint8Array(16))
      yield* api.chats.sendMedia({ params: { chatId: chat }, payload: { path: file } })
      const sentFile = yield* takeSoon(wa.sentMedia)
      expect(sentFile.media).toMatchObject({ kind: "document", mimetype: "application/pdf", fileName: "notes.pdf" })

      const missing = yield* api.chats.sendMedia({ params: { chatId: chat }, payload: { path: join(dir, "gone.png") } }).pipe(Effect.flip)
      expect(missing._tag).toBe("OutboundFileUnreadable")
      const noType = yield* api.chats.sendMedia({ params: { chatId: chat }, payload: { data: new Uint8Array([1]) } }).pipe(Effect.flip)
      expect(noType._tag).toBe("OutboundInvalid")
      yield* Ref.set(wa.failNextSends, 1)
      const failed = yield* api.chats.sendMedia({ params: { chatId: chat }, payload: { data: new Uint8Array([1]), mimetype: "image/png" } }).pipe(Effect.flip)
      expect(failed._tag).toBe("SendFailed")
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("a reply can carry attachments, sent after the text and before 👀 clears", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      yield* wa.deliver(message("send me the chart"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle

      const bad = yield* api.turns
        .reply({ params: { turnId: call.turnId }, payload: { text: "here", attachments: [{ path: "/nowhere/chart.png" }] } })
        .pipe(Effect.flip)
      expect(bad._tag).toBe("OutboundFileUnreadable")
      expect((yield* api.turns.get({ params: { turnId: call.turnId } })).state).toBe("awaiting")

      const result = yield* api.turns.reply({
        params: { turnId: call.turnId },
        payload: { text: "here you go", attachments: [{ data: new Uint8Array([9, 9]), mimetype: "image/jpeg", caption: "Q3" }] },
      })
      expect(result.sentMessageIds.length).toBe(2)
      expect((yield* takeSoon(wa.sent)).text).toBe("here you go")
      const file = yield* takeSoon(wa.sentMedia)
      expect(file.media).toMatchObject({ kind: "image", mimetype: "image/jpeg", caption: "Q3" })
      expect(file.messageId).toBe(result.sentMessageIds[1])
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("")
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})
