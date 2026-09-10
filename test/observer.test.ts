import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { DateTime, Effect, Layer, Option } from "effect"
import { proto } from "@whiskeysockets/baileys"
import { ChatId, MessageId } from "../src/domain/ids.ts"
import { Store, StoreWithDatabaseLive } from "../src/store/Store.ts"
import { chunkWanted, contactOf } from "../src/whatsapp/Observer.ts"
import { TestConfigLive, chat, message } from "./support/fakes.ts"

const DAY = 24 * 60 * 60 * 1000
const now = 1_800_000_000_000
const BOOTSTRAP = proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP
const RECENT = proto.HistorySync.HistorySyncType.RECENT
const FULL = proto.HistorySync.HistorySyncType.FULL

describe("observer: which history chunks are downloaded", () => {
  const state = (accepted = 0, backfillDays = 30) => ({ backfillDays, accepted, nowMs: now })
  it("takes the bootstrap chunk whatever its oldest message, and recent chunks inside the window", () => {
    expect(chunkWanted({ syncType: BOOTSTRAP, fileLength: 1000, oldestMs: now - 400 * DAY }, state())).toBe(true)
    expect(chunkWanted({ syncType: RECENT, fileLength: 1000, oldestMs: now - 10 * DAY }, state())).toBe(true)
    expect(chunkWanted({ syncType: RECENT, fileLength: 1000, oldestMs: undefined }, state())).toBe(true)
  })
  it("refuses recent chunks older than the window, the full archive, and everything when the window is zero", () => {
    expect(chunkWanted({ syncType: RECENT, fileLength: 1000, oldestMs: now - 31 * DAY }, state())).toBe(false)
    expect(chunkWanted({ syncType: FULL, fileLength: 1000, oldestMs: now - DAY }, state())).toBe(false)
    expect(chunkWanted({ syncType: BOOTSTRAP, fileLength: 1000, oldestMs: undefined }, state(0, 0))).toBe(false)
  })
  it("caps the count and the size", () => {
    expect(chunkWanted({ syncType: RECENT, fileLength: 1000, oldestMs: now - DAY }, state(20))).toBe(false)
    expect(chunkWanted({ syncType: RECENT, fileLength: 9 * 1024 * 1024, oldestMs: now - DAY }, state())).toBe(false)
    expect(chunkWanted({ syncType: RECENT, fileLength: 8 * 1024 * 1024, oldestMs: now - DAY }, state(19))).toBe(true)
  })
})

describe("observer: contacts from the phone", () => {
  it("keeps the saved name, the push name, the lid and the phone digits", () => {
    expect(contactOf({ id: "447700900555@s.whatsapp.net", name: "Dana Levi", notify: "Dana", lid: "123@lid" })).toEqual({
      jid: "447700900555@s.whatsapp.net",
      lid: "123@lid",
      phone: "447700900555",
      name: "Dana Levi",
      pushName: "Dana",
    })
    expect(contactOf({ id: "123@lid", phoneNumber: "447700900555@s.whatsapp.net" })).toEqual({ jid: "123@lid", lid: "123@lid", phone: "447700900555" })
    expect(contactOf({ name: "nobody" })).toBeUndefined()
  })
})

const layer = StoreWithDatabaseLive.pipe(Layer.provide([TestConfigLive({ history: true }), NodeServices.layer]))
const other = ChatId.make("447700900456@s.whatsapp.net")

describe("observer: the store side", () => {
  it.effect("a batch counts only new rows, indexes their text, and keeps the account's own messages as `out`", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const m1 = message("dinner at eight?")
      const m2 = message("yes, bringing wine", { chatId: other })
      const first = yield* store.recordBatch([
        { message: m1, raw: new Uint8Array([1]), direction: "in" },
        { message: m2, raw: undefined, direction: "out" },
      ])
      expect(first).toBe(2)
      const again = yield* store.recordBatch([{ message: m1, raw: new Uint8Array([1]), direction: "in" }])
      expect(again).toBe(0)
      const mine = yield* store.messages(other, { limit: 10 })
      expect(mine.map((m) => [m.direction, m.senderId])).toEqual([["out", "me"]])
      const hits = yield* store.search("wine", { limit: 10 })
      expect(hits.map((h) => h.messageId)).toEqual([m2.messageId])
    }).pipe(Effect.provide(layer)),
  )

  it.effect("a call is a row whose text follows the call, and it is searchable", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const at = DateTime.makeUnsafe(1_700_000_500_000)
      const call = { callId: "c1", chatId: chat, fromId: chat, direction: "in" as const, at, status: "offer", isVideo: false, isGroup: false }
      yield* store.recordCall(call)
      yield* store.recordCall({ ...call, status: "timeout" })
      const rows = yield* store.messages(chat, { limit: 10 })
      expect(rows.map((r) => [r.kind, r.text])).toEqual([["call", "voice call: timeout"]])
      expect((yield* store.search("call", { limit: 10 })).length).toBe(1)
    }).pipe(Effect.provide(layer)),
  )

  it.effect("contacts merge across sources and are searchable by name, push name or digits", () =>
    Effect.gen(function* () {
      const store = yield* Store
      yield* store.upsertContacts([{ jid: "447700900555@s.whatsapp.net", phone: "447700900555", pushName: "Dana" }])
      yield* store.upsertContacts([{ jid: "447700900555@s.whatsapp.net", name: "Dana Levi", lid: "555@lid" }])
      const found = yield* store.contacts("dana", 10)
      expect(found.map((c) => [c.name, c.pushName, c.lid, c.phone])).toEqual([["Dana Levi", "Dana", "555@lid", "447700900555"]])
      expect((yield* store.contacts("900555", 10)).length).toBe(1)
      expect(Option.map(yield* store.contactByLid("555@lid"), (c) => c.jid)).toEqual(Option.some("447700900555@s.whatsapp.net"))
      expect(Option.map(yield* store.contactByPhone("447700900555"), (c) => c.name)).toEqual(Option.some("Dana Levi"))
    }).pipe(Effect.provide(layer)),
  )

  it.effect("a chat's name prefers WhatsApp's own, then the address book, then the last push name", () =>
    Effect.gen(function* () {
      const store = yield* Store
      yield* store.recordInbound(message("hi", { chatId: other, senderName: "pushy" }), new Uint8Array([1]))
      expect(yield* store.chatName(other)).toEqual(Option.some("pushy"))
      yield* store.upsertContacts([{ jid: other, name: "Saved Name" }])
      expect(yield* store.chatName(other)).toEqual(Option.some("Saved Name"))
      yield* store.upsertChat(other, "Chat Name", false)
      expect(yield* store.chatName(other)).toEqual(Option.some("Chat Name"))
      const summary = (yield* store.chats).find((c) => c.chatId === other)
      expect(summary?.name).toBe("Chat Name")
    }).pipe(Effect.provide(layer)),
  )

  it.effect("state survives in the database and purge honours a cutoff", () =>
    Effect.gen(function* () {
      const store = yield* Store
      yield* store.setState("me", JSON.stringify({ jid: "447700900123@s.whatsapp.net" }))
      expect(yield* store.getState("me")).toEqual(Option.some(JSON.stringify({ jid: "447700900123@s.whatsapp.net" })))
      const old = message("old")
      const fresh = message("fresh")
      yield* store.recordBatch([
        { message: { ...old, timestamp: DateTime.makeUnsafe(1_000) }, raw: undefined, direction: "in" },
        { message: fresh, raw: undefined, direction: "in" },
      ])
      const removed = yield* store.purgeBefore(DateTime.makeUnsafe(2_000))
      expect(removed).toBe(1)
      expect((yield* store.messages(chat, { limit: 10 })).map((m) => m.messageId)).toEqual([fresh.messageId])
      expect((yield* store.search("old", { limit: 10 })).length).toBe(0)
      expect(MessageId.make("x")).toBe("x")
    }).pipe(Effect.provide(layer)),
  )
})
