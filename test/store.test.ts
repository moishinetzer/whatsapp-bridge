import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { DateTime, Effect, Layer, Option } from "effect"
import { ChatId, MessageId, TurnId } from "../src/domain/ids.ts"
import { Store, StoreWithDatabaseLive, ftsQuery } from "../src/store/Store.ts"
import { TestConfigLive, chat, message } from "./support/fakes.ts"

const layer = (history: boolean) => StoreWithDatabaseLive.pipe(Layer.provide([TestConfigLive({ history }), NodeServices.layer]))
const bytes = (n: number) => new Uint8Array([n, n + 1, n + 2])
const other = ChatId.make("447700900456@s.whatsapp.net")

describe("Store", () => {
  it.effect("remembers what the bridge sent, always, history or not", () =>
    Effect.gen(function* () {
      const store = yield* Store
      expect(store.historyEnabled).toBe(false)
      const now = yield* DateTime.now
      yield* store.rememberSent(MessageId.make("out_1"), { chatId: chat, sentAt: now, raw: bytes(7), turnId: Option.some(TurnId.make("turn_a")) })
      const found = yield* store.sent(MessageId.make("out_1"))
      expect(Option.isSome(found)).toBe(true)
      if (Option.isSome(found)) {
        expect(found.value.chatId).toBe(chat)
        expect(Array.from(found.value.raw)).toEqual([7, 8, 9])
        expect(Option.getOrUndefined(found.value.turnId)).toBe("turn_a")
      }
      yield* store.forgetSent(MessageId.make("out_1"))
      expect(Option.isNone(yield* store.sent(MessageId.make("out_1")))).toBe(true)
      // History writes are no-ops and reads say why.
      yield* store.recordInbound(message("hidden"), bytes(1))
      const disabled = yield* store.chats.pipe(Effect.flip)
      expect(disabled._tag).toBe("HistoryDisabled")
      expect(Option.isNone(yield* store.raw(chat, MessageId.make("in_1")))).toBe(true)
    }).pipe(Effect.provide(layer(false))),
  )

  it.effect("records both directions and pages newest first with a cursor and a since", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const m1 = message("the dentist is tomorrow at ten")
      const m2 = message("and lunch with sam at one", { chatId: other })
      const m3 = message("thanks!")
      yield* store.recordInbound(m1, bytes(1))
      yield* store.recordInbound(m2, bytes(2))
      yield* store.recordOutbound({
        chatId: chat,
        messageId: MessageId.make("out_1"),
        kind: "text",
        text: "Noted: dentist at ten.",
        quotedMessageId: m1.messageId,
        turnId: TurnId.make("turn_1"),
        at: DateTime.makeUnsafe(DateTime.toEpochMillis(m1.timestamp) + 1),
        raw: bytes(9),
      })
      yield* store.recordInbound(m3, bytes(3))
      yield* store.setTurn(chat, [m1.messageId], TurnId.make("turn_1"))

      const chats = yield* store.chats
      expect(chats.map((c) => [c.chatId, c.messageCount])).toEqual([
        [chat, 3],
        [other, 1],
      ])
      expect(chats[0]?.name).toBe("Moishi")

      const page = yield* store.messages(chat, { limit: 2 })
      expect(page.map((m) => m.messageId)).toEqual([m3.messageId, "out_1"])
      expect(page[1]).toMatchObject({ direction: "out", status: "sent", quotedMessageId: m1.messageId, turnId: "turn_1" })
      const older = yield* store.messages(chat, { limit: 10, before: MessageId.make("out_1") })
      expect(older.map((m) => m.messageId)).toEqual([m1.messageId])
      expect(older[0]?.turnId).toBe("turn_1")
      const since = yield* store.messages(chat, { limit: 10, since: m1.timestamp })
      expect(since.map((m) => m.messageId)).toEqual([m3.messageId, "out_1"])

      const raw = yield* store.raw(chat, m1.messageId)
      expect(Option.isSome(raw) && Array.from(raw.value)).toEqual([1, 2, 3])
    }).pipe(Effect.provide(layer(true))),
  )

  it.effect("edits, tombstones, reactions and delivery state show on the rows", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const m1 = message("teh calendar")
      yield* store.recordInbound(m1, bytes(1))
      yield* store.markEdited(chat, m1.messageId, "the calendar")
      yield* store.recordReaction(chat, m1.messageId, "447700900999@s.whatsapp.net", "👍")
      yield* store.recordOutbound({ chatId: chat, messageId: MessageId.make("out_1"), kind: "text", text: "ok", at: yield* DateTime.now, raw: bytes(2) })
      yield* store.updateStatus(chat, MessageId.make("out_1"), "read")
      yield* store.updateStatus(chat, MessageId.make("out_1"), "delivered") // never goes backwards

      const rows = yield* store.messages(chat, { limit: 10 })
      const inbound = rows.find((r) => r.messageId === m1.messageId)
      expect(inbound).toMatchObject({ text: "teh calendar", editedText: "the calendar" })
      expect(inbound?.reactions).toEqual([{ senderId: "447700900999@s.whatsapp.net", emoji: "👍", at: expect.anything() }])
      expect(rows.find((r) => r.messageId === "out_1")?.status).toBe("read")

      yield* store.recordReaction(chat, m1.messageId, "447700900999@s.whatsapp.net", "")
      yield* store.markDeleted(chat, m1.messageId)
      const after = (yield* store.messages(chat, { limit: 10 })).find((r) => r.messageId === m1.messageId)
      expect(after?.text).toBeUndefined()
      expect(after?.editedText).toBeUndefined()
      expect(after?.deletedAt).toBeDefined()
      expect(after?.reactions).toEqual([])
      expect(Option.isNone(yield* store.raw(chat, m1.messageId))).toBe(true)
    }).pipe(Effect.provide(layer(true))),
  )

  it.effect("searches with prefixes and phrases across chats and within one", () =>
    Effect.gen(function* () {
      const store = yield* Store
      yield* store.recordInbound(message("dentist appointment tomorrow"), bytes(1))
      yield* store.recordInbound(message("the dentist moved it", { chatId: other }), bytes(2))
      yield* store.recordInbound(message("lunch with sam"), bytes(3))
      const edited = message("typo hre")
      yield* store.recordInbound(edited, bytes(4))
      yield* store.markEdited(chat, edited.messageId, "typo here fixed")

      const all = yield* store.search("dent", { limit: 10 })
      expect(all.length).toBe(2)
      expect(all[0]?.snippet).toContain("[dentist]")
      const mine = yield* store.search("dentist", { chatId: chat, limit: 10 })
      expect(mine.map((h) => h.chatId)).toEqual([chat])
      const phrase = yield* store.search('"lunch with"', { limit: 10 })
      expect(phrase.length).toBe(1)
      const fixed = yield* store.search("fixed", { limit: 10 })
      expect(fixed.length).toBe(1)
      const gone = yield* store.search("hre", { limit: 10 })
      expect(gone.length).toBe(0)
      // Query syntax that would break FTS5 is neutralised.
      const weird = yield* store.search('den"tist (', { limit: 10 })
      expect(Array.isArray(weird)).toBe(true)
      expect(ftsQuery("a b")).toBe('"a"* "b"*')
      expect(ftsQuery('"exact phrase"')).toBe('"exact phrase"')
    }).pipe(Effect.provide(layer(true))),
  )

  it.effect("purges history before an instant and records turns", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const old = message("old")
      yield* store.recordInbound(old, bytes(1))
      const recent = message("recent")
      yield* store.recordInbound(recent, bytes(2))
      const removed = yield* store.purgeBefore(DateTime.makeUnsafe(DateTime.toEpochMillis(recent.timestamp)))
      expect(removed).toBe(1)
      expect((yield* store.messages(chat, { limit: 10 })).map((m) => m.messageId)).toEqual([recent.messageId])
      expect((yield* store.search("old", { limit: 10 })).length).toBe(0)
      yield* store.recordTurn({ turnId: TurnId.make("turn_z"), chatId: chat, isGroup: false, state: "committed", messages: [recent], openedAt: recent.timestamp, sentMessageIds: [MessageId.make("out_1")] })
    }).pipe(Effect.provide(layer(true))),
  )
})
