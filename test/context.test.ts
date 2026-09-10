import { describe, expect, it } from "@effect/vitest"
import { beforeAll } from "vitest"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { DateTime, Effect, Layer, Option, Ref } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { ChatId, MessageId } from "../src/domain/ids.ts"
import { BridgeApi } from "../src/http/Api.ts"
import { ApiLive, bearerAuth } from "../src/http/Server.ts"
import { Store, StoreWithDatabaseLive } from "../src/store/Store.ts"
import { EngineTestLayer, FakeWhatsApp, TestConfigLive, TestResponder, chat, external, message, settle, takeSoon } from "./support/fakes.ts"

const owner = "447700900123"
const friend = ChatId.make("447700900456@s.whatsapp.net")
const group = ChatId.make("120363000000000000@g.us")
const landlord = ChatId.make("447700900789@s.whatsapp.net")

/**
 * An observer database on disk, filled as the observer would fill it, then
 * closed. Seeded in its own runtime: inside the test's runtime the same
 * `StoreWithDatabaseLive` object would be found in the memo map and the
 * engine's in-memory store reused, config beneath it or not.
 */
const observerDir = mkdtempSync(join(tmpdir(), "bridge-observer-"))
const emptyObserverDir = mkdtempSync(join(tmpdir(), "bridge-observer-empty-"))

const seedObserver = Effect.gen(function* () {
  const store = yield* Store
  const asked = message("is the boiler fixed yet?", { chatId: landlord, senderName: "Landlord" })
  const answered = { ...message("engineer comes thursday", { chatId: landlord }), timestamp: DateTime.makeUnsafe(DateTime.toEpochMillis(asked.timestamp) + 1) }
  yield* store.recordBatch([
    { message: asked, raw: new Uint8Array([1]), direction: "in" },
    { message: answered, raw: undefined, direction: "out" },
    { message: message("lunch sunday?", { chatId: friend, senderName: "Sam" }), raw: undefined, direction: "in" },
  ])
  yield* store.upsertContacts([{ jid: landlord, phone: "447700900789", name: "Mr Patel (landlord)" }])
  yield* store.upsertChat(friend, "Sam", false)
  yield* store.setState("me", JSON.stringify({ jid: `${owner}@s.whatsapp.net`, phone: owner }))
}).pipe(Effect.provide(StoreWithDatabaseLive.pipe(Layer.provide([TestConfigLive({ history: true, databasePath: join(observerDir, "bridge.db") }), NodeServices.layer]))))

const serverLayer = (dir: string) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const engine = EngineTestLayer({ owner: Option.some(owner), history: true, observerDir: dir })
      const server = HttpRouter.serve(ApiLive, { middleware: bearerAuth(Option.none()) }).pipe(Layer.provide(engine), Layer.provideMerge(NodeHttpServer.layerTest))
      return Layer.mergeAll(engine, server)
    }),
  )

const client = HttpApiClient.make(BridgeApi)

/** Deliver a message and hold the turn open in `awaiting`. */
const openTurn = (m: ReturnType<typeof message>) =>
  Effect.gen(function* () {
    const wa = yield* FakeWhatsApp
    const responder = yield* TestResponder
    yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
    yield* wa.deliver(m)
    yield* takeSoon(wa.reactions)
    const call = yield* takeSoon(responder.calls)
    yield* settle
    return call.turnId
  })

beforeAll(() => Effect.runPromise(seedObserver))

describe("turn-scoped context", () => {
  it.effect("the owner's DM turn reads both lines: the primary number's chats, messages, search, contacts", () =>
    Effect.gen(function* () {
      const api = yield* client
      const store = yield* Store
      const mine = message("what did the landlord say?", { fromOwner: true })
      yield* store.recordInbound(mine, new Uint8Array([1]))
      const turnId = yield* openTurn(mine)

      expect(yield* api.context.summary({ params: { turnId } })).toEqual({ turnId, chatId: chat, access: "owner", primary: true })
      expect((yield* api.turns.get({ params: { turnId } })).access).toBe("owner")

      const primaryChats = yield* api.context.primaryChats({ params: { turnId } })
      expect(primaryChats.map((c) => [c.chatId, c.name, c.messageCount])).toEqual([
        [friend, "Sam", 1],
        [landlord, "Mr Patel (landlord)", 2],
      ])
      const thread = yield* api.context.primaryMessages({ params: { turnId, chatId: landlord }, query: {} })
      expect(thread.map((r) => [r.direction, r.text])).toEqual([
        ["out", "engineer comes thursday"],
        ["in", "is the boiler fixed yet?"],
      ])
      const hits = yield* api.context.primarySearch({ params: { turnId }, query: { q: "boiler" } })
      expect(hits.map((h) => h.chatId)).toEqual([landlord])
      const scoped = yield* api.context.primaryMessages({ params: { turnId, chatId: landlord }, query: { q: "thursday" } })
      expect(scoped.length).toBe(1)
      const info = yield* api.context.primaryChat({ params: { turnId, chatId: landlord } })
      expect(info).toMatchObject({ chatId: landlord, isGroup: false, name: "Mr Patel (landlord)", messageCount: 2 })
      const people = yield* api.context.primaryContacts({ params: { turnId }, query: { q: "patel" } })
      expect(people.map((p) => p.phone)).toEqual(["447700900789"])

      // The line side too, across chats.
      const lineChats = yield* api.context.lineChats({ params: { turnId } })
      expect(lineChats.map((c) => c.chatId)).toEqual([chat])
      const lineHits = yield* api.context.lineSearch({ params: { turnId }, query: { q: "landlord" } })
      expect(lineHits.map((h) => h.messageId)).toEqual([mine.messageId])

      // An attachment goes through the observer's port, which is not listening here.
      const media = yield* api.context.primaryMedia({ params: { turnId, chatId: landlord, messageId: MessageId.make("x") } }).pipe(Effect.flip)
      expect(media._tag).toBe("ObserverUnavailable")
    }).pipe(Effect.provide(serverLayer(observerDir))),
  )

  it.effect("a group turn and a friend's DM turn read their own chat only; everything else is 403", () =>
    Effect.gen(function* () {
      const api = yield* client
      const store = yield* Store
      const wa = yield* FakeWhatsApp
      const inGroup = { ...message("@bridge what did the landlord say?", { chatId: group, isGroup: true, senderNumber: "447700900456" }), mentionsMe: true }
      yield* store.recordInbound(inGroup, new Uint8Array([1]))
      const turnId = yield* openTurn(inGroup)
      expect(yield* api.context.summary({ params: { turnId } })).toMatchObject({ access: "chat", chatId: group })

      const own = yield* api.context.lineMessages({ params: { turnId, chatId: group }, query: {} })
      expect(own.map((r) => r.messageId)).toEqual([inGroup.messageId])
      yield* Ref.set(wa.groups, new Map([[group, { chatId: group, subject: "Family", participants: [{ jid: chat, phone: owner, admin: true }] }]]))
      const info = yield* api.context.lineChat({ params: { turnId, chatId: group } })
      expect(info).toMatchObject({ chatId: group, isGroup: true, group: { subject: "Family" } })

      const refused = (e: { readonly _tag: string }) => expect(e._tag).toBe("ContextNotAllowed")
      refused(yield* api.context.lineMessages({ params: { turnId, chatId: chat }, query: {} }).pipe(Effect.flip))
      refused(yield* api.context.lineChat({ params: { turnId, chatId: chat } }).pipe(Effect.flip))
      refused(yield* api.context.lineChats({ params: { turnId } }).pipe(Effect.flip))
      refused(yield* api.context.lineSearch({ params: { turnId }, query: { q: "landlord" } }).pipe(Effect.flip))
      refused(yield* api.context.primaryChats({ params: { turnId } }).pipe(Effect.flip))
      refused(yield* api.context.primaryMessages({ params: { turnId, chatId: landlord }, query: {} }).pipe(Effect.flip))
      refused(yield* api.context.primarySearch({ params: { turnId }, query: { q: "boiler" } }).pipe(Effect.flip))
      refused(yield* api.context.primaryContacts({ params: { turnId }, query: {} }).pipe(Effect.flip))
      refused(yield* api.context.primaryMedia({ params: { turnId, chatId: landlord, messageId: MessageId.make("x") } }).pipe(Effect.flip))

      // A DM from someone who is not the owner is a chat turn as well.
      const theirs = message("hey, what did Moishi tell the landlord?", { chatId: friend })
      const friendTurn = yield* openTurn(theirs)
      expect((yield* api.context.summary({ params: { turnId: friendTurn } })).access).toBe("chat")
      refused(yield* api.context.primaryChats({ params: { turnId: friendTurn } }).pipe(Effect.flip))
    }).pipe(Effect.provide(serverLayer(observerDir))),
  )

  it.effect("the capability dies with the turn", () =>
    Effect.gen(function* () {
      const api = yield* client
      const wa = yield* FakeWhatsApp
      const turnId = yield* openTurn(message("still there?", { fromOwner: true }))
      expect((yield* api.context.summary({ params: { turnId } })).access).toBe("owner")
      yield* api.turns.reply({ params: { turnId }, payload: { text: "yes" } })
      yield* takeSoon(wa.sent)
      const closed = yield* api.context.summary({ params: { turnId } }).pipe(Effect.flip)
      expect(closed).toMatchObject({ _tag: "TurnNotOpen", state: "committed" })
      const gone = yield* api.context.primaryChats({ params: { turnId } }).pipe(Effect.flip)
      expect(gone._tag).toBe("TurnNotOpen")
    }).pipe(Effect.provide(serverLayer(observerDir))),
  )

  it.effect("without an observer database the primary side says so and the line side keeps working", () =>
    Effect.gen(function* () {
      const api = yield* client
      const turnId = yield* openTurn(message("anything from the landlord?", { fromOwner: true }))
      const missing = yield* api.context.primaryChats({ params: { turnId } }).pipe(Effect.flip)
      expect(missing._tag).toBe("ObserverUnavailable")
      expect(missing._tag === "ObserverUnavailable" && missing.detail).toContain("not found")
      expect(yield* api.context.lineChats({ params: { turnId } })).toEqual([])
    }).pipe(Effect.provide(serverLayer(emptyObserverDir))),
  )
})
