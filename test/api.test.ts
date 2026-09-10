import { describe, expect, it } from "@effect/vitest"
import { NodeHttpServer } from "@effect/platform-node"
import { DateTime, Duration, Effect, Fiber, Layer, Option, Queue, Redacted, Ref, Stream } from "effect"
import { HttpBody, HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { ChatId, MessageId, toChatId } from "../src/domain/ids.ts"
import { Store } from "../src/store/Store.ts"
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

/** Narrows a send result to the immediate case; scheduled sends are asserted separately. */
const asSent = <A extends object>(r: A): Extract<A, { messageId: unknown }> => {
  if (!("messageId" in r)) throw new Error("expected an immediate send, got a scheduled one")
  return r as Extract<A, { messageId: unknown }>
}

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

      const sent = asSent(yield* api.chats.send({ params: { chatId }, payload: { text: "Open Grok Bot → approve the Link spend" } }))
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

      const fromBytes = asSent(
        yield* api.chats.sendMedia({
          params: { chatId: chat },
          payload: { data: new Uint8Array([1, 2, 3, 4]), mimetype: "image/png", caption: "a chart" },
        }),
      )
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

describe("HTTP API v0.4", () => {
  it.effect("quotes, edits and deletes the bridge's own messages", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const quoted = asSent(yield* api.chats.send({ params: { chatId: chat }, payload: { text: "re: that", quoteMessageId: MessageId.make("in_42") } }))
      expect(yield* takeSoon(wa.sent)).toMatchObject({ messageId: quoted.messageId, quoteMessageId: "in_42" })
      yield* Ref.set(wa.unknownQuotes, new Set([MessageId.make("gone")]))
      const unknown = yield* api.chats.send({ params: { chatId: chat }, payload: { text: "x", quoteMessageId: MessageId.make("gone") } }).pipe(Effect.flip)
      expect(unknown._tag).toBe("MessageUnknown")
      yield* api.chats.edit({ params: { chatId: chat, messageId: quoted.messageId }, payload: { text: "re: this" } })
      expect(yield* takeSoon(wa.edits)).toEqual({ chatId: chat, messageId: quoted.messageId, text: "re: this" })
      yield* api.chats.deleteMessage({ params: { chatId: chat, messageId: quoted.messageId } })
      expect(yield* takeSoon(wa.deletes)).toEqual({ chatId: chat, messageId: quoted.messageId })
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("progress sends an interim bubble the reply can replace; abandon fails the turn", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const events = yield* Events
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      const m1 = message("find the invoice")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle

      const progress = yield* api.turns.progress({ params: { turnId: call.turnId }, payload: { text: "Looking…", extendBy: Duration.minutes(20) } })
      const interim = yield* takeSoon(wa.sent)
      expect(interim.text).toBe("Looking…")
      expect(progress.progressMessageId).toBe(interim.messageId)
      expect(progress.turnId).toBe(call.turnId)

      const result = yield* api.turns.reply({
        params: { turnId: call.turnId },
        payload: { replies: [{ text: "Here it is.", quoteMessageId: m1.messageId }], text: "Paid in March.", replaceProgress: true },
      })
      expect(yield* takeSoon(wa.edits)).toEqual({ chatId: chat, messageId: interim.messageId, text: "Here it is." })
      expect(yield* takeSoon(wa.sent)).toMatchObject({ text: "Paid in March." })
      expect(result.sentMessageIds[0]).toBe(interim.messageId)

      const closed = yield* api.turns.progress({ params: { turnId: call.turnId }, payload: {} }).pipe(Effect.flip)
      expect(closed._tag).toBe("TurnNotOpen")

      const m2 = message("and the receipt")
      yield* wa.deliver(m2)
      yield* takeSoon(wa.reactions)
      yield* takeSoon(wa.reactions)
      const second = yield* takeSoon(responder.calls)
      yield* settle
      const failedEvent = yield* events.stream.pipe(Stream.filter((e) => e._tag === "TurnFailed"), Stream.take(1), Stream.runCollect, Effect.forkScoped)
      yield* api.turns.abandon({ params: { turnId: second.turnId }, payload: { reason: "cannot find it", text: "" } })
      const [failed] = Array.from(yield* Fiber.join(failedEvent))
      expect(failed).toMatchObject({ _tag: "TurnFailed", turnId: second.turnId, reason: "abandoned: cannot find it" })
      expect((yield* api.turns.get({ params: { turnId: second.turnId } })).state).toBe("failed")
      yield* settle
      // An empty abandon text sends nothing.
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})

const HistoryServerLayer = Layer.unwrap(
  Effect.gen(function* () {
    const engine = EngineTestLayer({ apiToken: Option.some(Redacted.make(TOKEN)), history: true })
    const server = HttpRouter.serve(ApiLive, { middleware: bearerAuth(Option.some(Redacted.make(TOKEN))) }).pipe(
      Layer.provide(engine),
      Layer.provideMerge(NodeHttpServer.layerTest),
    )
    return Layer.mergeAll(engine, server)
  }),
)

describe("HTTP API v0.5", () => {
  it.effect("status names the database, history answers 409 when off", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const status = yield* api.health.status()
      expect(status.databasePath).toBe(":memory:")
      expect(status.history).toBe(false)
      const off = yield* api.history.chats().pipe(Effect.flip)
      expect(off._tag).toBe("HistoryDisabled")
      const info = yield* api.history.chat({ params: { chatId: chat } })
      expect(info).toEqual({ chatId: chat, isGroup: false })
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("records the conversation and serves it back with paging, search and reactions", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      const m1 = message("what time is the dentist?")
      // The fake transport never writes history itself (the real one does before the engine sees a message); do it here.
      const store = yield* Store
      yield* store.recordInbound(m1, new Uint8Array([1]))
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* api.turns.reply({ params: { turnId: call.turnId }, payload: { text: "Ten tomorrow." } })
      const out = yield* takeSoon(wa.sent)
      yield* store.recordOutbound({
        chatId: chat,
        messageId: out.messageId,
        kind: "text",
        text: out.text,
        turnId: call.turnId,
        at: DateTime.makeUnsafe(DateTime.toEpochMillis(m1.timestamp) + 1),
        raw: new Uint8Array([2]),
      })
      yield* wa.inject({ _tag: "Reaction", chatId: chat, messageId: out.messageId, senderId: chat, emoji: "👍", onOurMessage: true })
      yield* store.recordReaction(chat, out.messageId, chat, "👍")

      const chats = yield* api.history.chats()
      expect(chats.map((c) => [c.chatId, c.messageCount])).toEqual([[chat, 2]])
      const page = yield* api.history.chatMessages({ params: { chatId: chat }, query: { limit: 1 } })
      expect(page.map((r) => r.messageId)).toEqual([out.messageId])
      expect(page[0]).toMatchObject({ direction: "out", turnId: call.turnId, reactions: [{ emoji: "👍" }] })
      const older = yield* api.history.chatMessages({ params: { chatId: chat }, query: { before: out.messageId } })
      expect(older.map((r) => r.messageId)).toEqual([m1.messageId])
      expect(older[0]?.turnId).toBe(call.turnId)
      const hits = yield* api.history.search({ query: { q: "dent" } })
      expect(hits.length).toBe(1)
      expect(hits[0]?.snippet).toContain("[dentist]")
      const scoped = yield* api.history.chatMessages({ params: { chatId: chat }, query: { q: "tomorrow" } })
      expect(scoped.map((r) => r.messageId)).toEqual([out.messageId])
      const info = yield* api.history.chat({ params: { chatId: chat } })
      expect(info).toMatchObject({ chatId: chat, isGroup: false, name: "Moishi", messageCount: 2 })
      const status = yield* api.health.status()
      expect(status.history).toBe(true)
    }).pipe(Effect.provide(HistoryServerLayer)),
  )

  it.effect("schedules, lists and cancels sends; forwards and looks contacts up", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const at = DateTime.addDuration(yield* DateTime.now, Duration.hours(2))
      const held = yield* api.chats.send({ params: { chatId: chat }, payload: { text: "later", sendAt: at, deliverWhenOnline: true } })
      expect("scheduledId" in held).toBe(true)
      if (!("scheduledId" in held)) return
      expect(held.deliverWhenOnline).toBe(true)
      const media = yield* api.chats.sendMedia({ params: { chatId: chat }, payload: { data: new Uint8Array([1, 2]), mimetype: "image/png", sendAt: at } })
      expect("scheduledId" in media).toBe(true)
      const listed = yield* api.scheduled.list()
      expect(listed.map((s) => s.kind).sort()).toEqual(["media", "text"])
      yield* api.scheduled.cancel({ params: { scheduledId: held.scheduledId } })
      const gone = yield* api.scheduled.cancel({ params: { scheduledId: held.scheduledId } }).pipe(Effect.flip)
      expect(gone._tag).toBe("ScheduledNotFound")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)

      const forwarded = yield* api.chats.forward({ params: { chatId: chat }, payload: { fromChatId: ChatId.make("447700900456@s.whatsapp.net"), messageId: MessageId.make("in_77") } })
      expect(yield* takeSoon(wa.forwards)).toMatchObject({ toChatId: chat, fromChatId: "447700900456@s.whatsapp.net", messageId: "in_77", sentAs: forwarded.messageId })
      yield* Ref.set(wa.unknownQuotes, new Set([MessageId.make("gone")]))
      const unknown = yield* api.chats.forward({ params: { chatId: chat }, payload: { fromChatId: chat, messageId: MessageId.make("gone") } }).pipe(Effect.flip)
      expect(unknown._tag).toBe("MessageUnknown")

      yield* Ref.set(wa.contacts, new Map([[chat, { jid: chat, phone: "447700900123", name: "Moishi", onWhatsApp: true }]]))
      const known = yield* api.history.contact({ params: { jid: chat } })
      expect(known).toMatchObject({ name: "Moishi", onWhatsApp: true })
      const nobody = yield* api.history.contact({ params: { jid: "1@s.whatsapp.net" } })
      expect(nobody.onWhatsApp).toBe(false)

      const group = ChatId.make("123-456@g.us")
      yield* Ref.set(wa.groups, new Map([[group, { chatId: group, subject: "Family", participants: [{ jid: chat, admin: true }] }]]))
      const info = yield* api.history.chat({ params: { chatId: group } })
      expect(info).toMatchObject({ isGroup: true, name: "Family", group: { subject: "Family" } })
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})

describe("HTTP API v0.6", () => {
  it.effect("sends locations, contacts, polls and albums; refuses what WhatsApp cannot carry", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const raw = yield* HttpClient.HttpClient

      const pin = yield* api.chats.sendLocation({ params: { chatId: chat }, payload: { latitude: 51.5, longitude: -0.12, name: "Office" } })
      expect(yield* takeSoon(wa.locations)).toEqual({ chatId: chat, location: { latitude: 51.5, longitude: -0.12, name: "Office" }, messageId: pin.messageId })

      const card = yield* api.chats.sendContacts({ params: { chatId: chat }, payload: { contacts: [{ name: "Dana", phone: "447700900555" }] } })
      expect(yield* takeSoon(wa.contactsSent)).toEqual({ chatId: chat, contacts: [{ name: "Dana", phone: "447700900555" }], messageId: card.messageId })

      const poll = yield* api.chats.sendPoll({ params: { chatId: chat }, payload: { question: "Lunch?", options: ["Pizza", "Sushi", "Salad"], multiple: true } })
      expect(yield* takeSoon(wa.polls)).toEqual({ chatId: chat, poll: { question: "Lunch?", options: ["Pizza", "Sushi", "Salad"], multiple: true }, messageId: poll.messageId })
      const onlyOne = yield* raw.post(`/chats/${chat}/polls`, {
        headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
        body: HttpBody.jsonUnsafe({ question: "Lunch?", options: ["Pizza"] }),
      })
      expect(onlyOne.status).toBe(400)

      const album = yield* api.chats.sendAlbum({
        params: { chatId: chat },
        payload: { items: [{ data: new Uint8Array([1]), mimetype: "image/png" }, { data: new Uint8Array([2]), mimetype: "video/mp4", caption: "clip" }] },
      })
      expect(album.messageIds.length).toBe(2)
      const sentAlbum = yield* takeSoon(wa.albums)
      expect(sentAlbum.items.map((i) => i.kind)).toEqual(["image", "video"])
      expect(sentAlbum.messageIds).toEqual(album.messageIds)
      const mixed = yield* api.chats
        .sendAlbum({
          params: { chatId: chat },
          payload: { items: [{ data: new Uint8Array([1]), mimetype: "image/png" }, { data: new Uint8Array([2]), mimetype: "application/pdf", fileName: "a.pdf" }] },
        })
        .pipe(Effect.flip)
      expect(mixed._tag).toBe("OutboundInvalid")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.albums))).toBe(true)
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("mentions ride along with a text; presence subscribes the chat; a reply can send an album", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const other = ChatId.make("447700900456@s.whatsapp.net")

      const sent = asSent(yield* api.chats.send({ params: { chatId: chat }, payload: { text: "@447700900456 over to you", mentions: [other] } }))
      yield* takeSoon(wa.sent)
      expect((yield* Ref.get(wa.mentions)).get(sent.messageId)).toEqual([other])

      yield* Ref.set(wa.presences, new Map([[chat, "available" as const]]))
      const presence = yield* api.chats.presence({ params: { chatId: chat } })
      expect(presence).toMatchObject({ chatId: chat, state: "available" })
      expect((yield* Ref.get(wa.presenceWatches)).has(chat)).toBe(true)

      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* wa.deliver(message("both charts please"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      const result = yield* api.turns.reply({
        params: { turnId: call.turnId },
        payload: {
          text: "here",
          attachments: [{ data: new Uint8Array([1]), mimetype: "image/png" }, { data: new Uint8Array([2]), mimetype: "image/png" }],
          album: true,
        },
      })
      expect(result.sentMessageIds.length).toBe(3)
      expect((yield* takeSoon(wa.sent)).text).toBe("here")
      expect((yield* takeSoon(wa.albums)).messageIds).toEqual(result.sentMessageIds.slice(1))
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})
