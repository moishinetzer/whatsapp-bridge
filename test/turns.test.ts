import { describe, expect, it } from "@effect/vitest"
import { DateTime, Deferred, Duration, Effect, Fiber, Latch, Option, Queue, Ref, Stream } from "effect"
import { TestClock } from "effect/testing"
import type { BridgeEvent } from "../src/domain/events.ts"
import { outgoing, text } from "../src/domain/outbound.ts"
import { MessageId } from "../src/domain/ids.ts"
import { Events } from "../src/engine/Events.ts"
import { HandlerFailed } from "../src/engine/Responder.ts"
import { MediaStore } from "../src/engine/Media.ts"
import { TurnRegistry } from "../src/engine/Turns.ts"
import { ChatId } from "../src/domain/ids.ts"
import { EngineTestLayer, FakeWhatsApp, TestResponder, chat, external, jpeg, message, png, replies, settle, takeSoon } from "./support/fakes.ts"

/** Collect the next `n` events without missing any published before we subscribe. */
const collectEvents = (n: number) =>
  Effect.gen(function* () {
    const events = yield* Events
    const ready = yield* Deferred.make<void>()
    const fiber = yield* events.stream.pipe(
      Stream.onStart(Deferred.succeed(ready, undefined)),
      Stream.take(n),
      Stream.runCollect,
      Effect.forkScoped,
    )
    yield* Deferred.await(ready)
    return Fiber.join(fiber).pipe(Effect.map((c) => Array.from(c) as ReadonlyArray<BridgeEvent>))
  })

describe("TurnEngine", () => {
  it.effect("acknowledges with 👀, sends the reply, then clears 👀", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)

      const m1 = message("hi")
      yield* wa.deliver(m1)

      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId: chat, messageId: m1.messageId, emoji: "👀" })
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.messageId)).toEqual([m1.messageId])
      expect(call.chatId).toBe(chat)

      const sent = yield* takeSoon(wa.sent)
      expect(sent).toMatchObject({ chatId: chat, text: "pong" })
      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId: chat, messageId: m1.messageId, emoji: "" })

      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnCompleted"])
      const turn = yield* registry.get(call.turnId)
      expect(turn.state).toBe("committed")
      expect(turn.sentMessageIds).toEqual([sent.messageId])
      expect(yield* registry.openCount).toBe(0)

      // typing went on and off
      const typing = yield* Ref.get(wa.typing)
      expect(typing.map((t) => t.composing)).toEqual([true, false])
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("batches messages that arrive inside the batch window into one turn", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder

      const m1 = message("first")
      const m2 = message("second")
      yield* wa.deliver(m1)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "👀" })
      expect(yield* takeSoon(wa.reads)).toMatchObject({ messageId: m1.messageId })
      yield* wa.deliver(m2)
      // The eye moves: off the first message, onto the newest.
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "👀" })
      expect(yield* takeSoon(wa.reads)).toMatchObject({ messageId: m2.messageId })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)

      yield* TestClock.adjust("1 second")

      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.text)).toEqual(["first", "second"])
      expect(call.supersedes).toBeUndefined()

      yield* takeSoon(wa.sent)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "" })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.reactions))).toBe(true)
    }).pipe(Effect.provide(EngineTestLayer({ batchWindow: Duration.seconds(1) }))),
  )

  it.effect("interrupts an in-flight turn when newer messages arrive and answers all of them once", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(4)
      const gate = yield* Deferred.make<void>()
      yield* Ref.set(responder.behaviour, () => Deferred.await(gate).pipe(Effect.as(replies("late answer"))))

      const m1 = message("do the thing")
      yield* wa.deliver(m1)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("👀")
      const first = yield* takeSoon(responder.calls)
      expect(first.messages.map((m) => m.text)).toEqual(["do the thing"])

      const m2 = message("actually, the other thing")
      yield* wa.deliver(m2)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "👀" })

      const second = yield* takeSoon(responder.calls)
      expect(second.turnId).not.toBe(first.turnId)
      expect(second.supersedes).toBe(first.turnId)
      expect(second.messages.map((m) => m.text)).toEqual(["do the thing", "actually, the other thing"])
      expect(yield* Ref.get(responder.interrupted)).toBe(1)
      expect((yield* registry.get(first.turnId)).state).toBe("superseded")
      expect((yield* registry.get(first.turnId)).supersededBy).toBe(second.turnId)

      yield* Deferred.succeed(gate, undefined)

      const sent = yield* takeSoon(wa.sent)
      expect(sent.text).toBe("late answer")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "" })

      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnSuperseded", "TurnOpened", "TurnCompleted"])
      expect((yield* registry.get(second.turnId)).state).toBe("committed")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("never abandons a reply that is already being sent; the newer message starts a fresh turn", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const events = yield* collectEvents(4)
      const latch = yield* Latch.make(false)
      yield* Ref.set(wa.sendGate, Option.some(latch))

      const m1 = message("one")
      yield* wa.deliver(m1)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("👀")
      const first = yield* takeSoon(responder.calls)
      expect((yield* takeSoon(wa.sending)).text).toBe("pong")

      const m2 = message("two")
      yield* wa.deliver(m2)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "👀" })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)

      yield* Ref.set(wa.sendGate, Option.none())
      yield* latch.open

      expect((yield* takeSoon(wa.sent)).text).toBe("pong")

      const second = yield* takeSoon(responder.calls)
      expect(second.turnId).not.toBe(first.turnId)
      expect(second.supersedes).toBeUndefined()
      expect(second.messages.map((m) => m.text)).toEqual(["two"])
      expect((yield* takeSoon(wa.sent)).text).toBe("pong")
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m2.messageId, emoji: "" })
      expect(yield* Ref.get(responder.interrupted)).toBe(0)

      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnCompleted", "TurnOpened", "TurnCompleted"])
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("a failed responder fails the turn, takes the eye off, sends the failure text, and the next message starts clean", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)
      yield* Ref.set(responder.behaviour, (turn) => Effect.fail(new HandlerFailed({ turnId: turn.turnId, detail: "boom" })))

      const m1 = message("hello?")
      yield* wa.deliver(m1)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("👀")
      const call = yield* takeSoon(responder.calls)

      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnFailed"])
      const failed = seen[1]
      expect(failed?._tag === "TurnFailed" && failed.reason).toBe("responder failed: boom")
      expect((yield* registry.get(call.turnId)).state).toBe("failed")
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      const apology = yield* takeSoon(wa.sent)
      expect(apology).toMatchObject({ text: "Something went wrong.", quoteMessageId: m1.messageId })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.reactions))).toBe(true)
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)

      yield* Ref.set(responder.behaviour, () => Effect.succeed(replies("recovered")))
      const m2 = message("still there?")
      yield* wa.deliver(m2)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("👀")
      const next = yield* takeSoon(responder.calls)
      expect(next.messages.map((m) => m.messageId)).toEqual([m2.messageId])
      expect((yield* takeSoon(wa.sent)).text).toBe("recovered")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("retries a flaky send before giving up", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(wa.failNextSends, 2)

      yield* wa.deliver(message("flaky"))
      yield* takeSoon(wa.reactions)
      yield* takeSoon(responder.calls)
      // two failures, each followed by a backoff sleep on the test clock
      yield* settle
      yield* TestClock.adjust("1 second")
      yield* settle
      yield* TestClock.adjust("2 seconds")
      expect((yield* takeSoon(wa.sent)).text).toBe("pong")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("manual mode: Intro answers through the registry and gets the sent ids back", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      const m1 = message("question")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      const open = yield* registry.list
      expect(open.map((t) => [t.turnId, t.state])).toEqual([[call.turnId, "awaiting"]])

      const replyFiber = yield* registry.reply(call.turnId, ["answer 1", "answer 2"]).pipe(Effect.forkScoped)
      expect((yield* takeSoon(wa.sent)).text).toBe("answer 1")
      expect((yield* takeSoon(wa.sent)).text).toBe("answer 2")
      const sentIds = yield* Fiber.join(replyFiber)
      expect(sentIds).toEqual(["out_1", "out_2"])
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("")

      const again = yield* registry.reply(call.turnId, ["again"]).pipe(Effect.flip)
      expect(again._tag).toBe("TurnNotAwaitingReply")
      const missing = yield* registry.reply(call.turnId.replace("turn_", "turn_x") as typeof call.turnId, ["?"]).pipe(Effect.flip)
      expect(missing._tag).toBe("TurnNotFound")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("sends a responder's files after its text, in order, and clears 👀 once", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const chart = png()
      yield* Ref.set(responder.behaviour, () => Effect.succeed({ _tag: "Replies", replies: outgoing(["here is the chart", chart, "and a note"]) } as const))

      const m1 = message("chart please")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)

      expect((yield* takeSoon(wa.sent)).text).toBe("here is the chart")
      const file = yield* takeSoon(wa.sentMedia)
      expect(file.media).toBe(chart)
      expect(file.chatId).toBe(chat)
      expect((yield* takeSoon(wa.sent)).text).toBe("and a note")
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("")
      yield* settle
      const turn = yield* registry.get(call.turnId)
      expect(turn.state).toBe("committed")
      expect(turn.sentMessageIds?.length).toBe(3)
      expect(turn.sentMessageIds?.[1]).toBe(file.messageId)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("manual mode: a reply to a superseded turn is refused and names the current turn", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      yield* wa.deliver(message("first"))
      yield* takeSoon(wa.reactions)
      const first = yield* takeSoon(responder.calls)
      yield* wa.deliver(message("second"))
      yield* takeSoon(wa.reactions)
      const second = yield* takeSoon(responder.calls)
      yield* settle

      const refused = yield* registry.reply(first.turnId, ["stale"]).pipe(Effect.flip)
      expect(refused._tag).toBe("TurnSuperseded")
      expect(refused._tag === "TurnSuperseded" && refused.currentTurnId).toBe(second.turnId)

      const ids = yield* registry.reply(second.turnId, ["fresh"])
      expect(ids.length).toBe(1)
      expect((yield* takeSoon(wa.sent)).text).toBe("fresh")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("manual mode: a turn nobody answers times out and fails", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      yield* wa.deliver(message("anyone?"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* TestClock.adjust("1 minute")

      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnFailed"])
      expect((yield* registry.get(call.turnId)).failure).toContain("no reply within")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("keeps chats independent: two chats are answered concurrently", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const other = "447700900456@s.whatsapp.net" as typeof chat
      const gate = yield* Deferred.make<void>()
      yield* Ref.set(responder.behaviour, (turn) =>
        turn.chatId === chat ? Deferred.await(gate).pipe(Effect.as(replies("slow"))) : Effect.succeed(replies("fast")),
      )

      yield* wa.deliver(message("slow chat"))
      yield* wa.deliver(message("fast chat", { chatId: other }))
      yield* takeSoon(wa.reactions)
      yield* takeSoon(wa.reactions)
      yield* takeSoon(responder.calls)
      yield* takeSoon(responder.calls)

      expect((yield* takeSoon(wa.sent)).text).toBe("fast")
      yield* Deferred.succeed(gate, undefined)
      expect((yield* takeSoon(wa.sent)).text).toBe("slow")
    }).pipe(Effect.provide(EngineTestLayer())),
  )
})

describe("TurnEngine hardening", () => {
  it.effect("survives a responder that throws: the turn fails, the worker lives on", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)
      yield* Ref.set(responder.behaviour, () => Effect.die(new Error("kaboom")))

      const m1 = message("explode")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "TurnFailed"])
      expect((yield* registry.get(call.turnId)).failure).toContain("crashed")
      // typing was switched off again
      const typing = yield* Ref.get(wa.typing)
      expect(typing.at(-1)?.composing).toBe(false)
      expect((yield* takeSoon(wa.sent)).text).toBe("Something went wrong.")

      yield* Ref.set(responder.behaviour, () => Effect.succeed(replies("alive")))
      yield* wa.deliver(message("still there?"))
      yield* takeSoon(wa.reactions)
      yield* takeSoon(responder.calls)
      expect((yield* takeSoon(wa.sent)).text).toBe("alive")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("handles a re-delivered message id exactly once", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const m1 = message("once")
      yield* wa.deliver(m1)
      yield* wa.deliver(m1)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("👀")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.length).toBe(1)
      yield* takeSoon(wa.sent)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.reactions))).toBe(true)
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("opens a turn after the max wait even when messages keep trickling in", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder

      yield* wa.deliver(message("one"))
      yield* takeSoon(wa.reactions)
      yield* settle
      yield* TestClock.adjust("900 millis")
      yield* wa.deliver(message("two"))
      yield* takeSoon(wa.reactions)
      yield* settle
      yield* TestClock.adjust("900 millis")
      yield* wa.deliver(message("three"))
      yield* takeSoon(wa.reactions)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)
      // the window alone would wait until t=2.8s; the max wait of 2s opens the turn at t=2.0s
      yield* TestClock.adjust("900 millis")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.text)).toEqual(["one", "two", "three"])
    }).pipe(Effect.provide(EngineTestLayer({ batchWindow: Duration.seconds(1), batchMaxWait: Duration.seconds(2) }))),
  )
})

describe("TurnEngine sending", () => {
  it.effect("fixes the message id before retrying so a flaky send cannot double-deliver", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(wa.failNextSends, 1)
      yield* wa.deliver(message("once please"))
      yield* takeSoon(wa.reactions)
      yield* takeSoon(responder.calls)
      yield* settle
      yield* TestClock.adjust("1 second")
      const sent = yield* takeSoon(wa.sent)
      // the id was minted before the first attempt (out_1); the retry reused it instead of minting out_2
      expect(sent.messageId).toBe("out_1")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("splits a long reply into several messages and drops empty ones", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const long = `${"first paragraph ".repeat(150)}\n\n${"second paragraph ".repeat(150)}`
      yield* Ref.set(responder.behaviour, () => Effect.succeed(replies("", long, "   ", "tail")))
      yield* wa.deliver(message("essay please"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      const a = yield* takeSoon(wa.sent)
      const b = yield* takeSoon(wa.sent)
      const c = yield* takeSoon(wa.sent)
      expect(a.text.length).toBeLessThanOrEqual(4_000)
      expect(b.text.length).toBeLessThanOrEqual(4_000)
      expect(c.text).toBe("tail")
      yield* takeSoon(wa.reactions)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect((yield* registry.get(call.turnId)).sentMessageIds?.length).toBe(3)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("an empty answer completes the turn and clears 👀 without sending", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(replies()))
      const m1 = message("no need to answer")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect((yield* registry.get(call.turnId)).state).toBe("committed")
    }).pipe(Effect.provide(EngineTestLayer())),
  )
})

describe("TurnEngine media", () => {
  it.effect("opens the turn with the downloaded image attached", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const events = yield* collectEvents(1)
      const m = message("what is this?", { kind: "image", mimetype: "image/jpeg" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg(300)]]))
      yield* wa.deliver(m)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      const media = call.messages[0]?.media
      expect(media?.mimetype).toBe("image/jpeg")
      expect(media?.size).toBe(300)
      expect(media?.path.endsWith(`${m.messageId}.jpeg`)).toBe(true)
      const [opened] = yield* events
      expect(opened?._tag === "TurnOpened" && opened.messages[0]?.media?.path).toBe(media?.path)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("opens the turn without the attachment when the download outlasts the media wait", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const m = message("slow photo", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg()]]))
      const latch = yield* Latch.make(false)
      yield* Ref.set(wa.mediaGate, Option.some(latch))
      yield* wa.deliver(m)
      yield* takeSoon(wa.reactions)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)
      yield* TestClock.adjust("8 seconds")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages[0]?.media).toBeUndefined()
      // the download still completes in the background and is served on demand afterwards
      yield* latch.open
      const store = yield* MediaStore
      expect((yield* store.get(chat, m.messageId)).size).toBe(64)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("respects the policy: no automatic download for documents under `images`, none at all under `none`", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const doc = message("contract", { kind: "document", mimetype: "application/pdf" })
      yield* Ref.set(wa.media, new Map([[doc.messageId, { ...jpeg(), mimetype: "application/pdf", extension: "pdf", kind: "document" }]]))
      yield* wa.deliver(doc)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      expect(call.messages[0]?.media).toBeUndefined()
      expect(yield* Ref.get(wa.downloads)).toBe(0)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("policy none skips images too", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const m = message("photo", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg()]]))
      yield* wa.deliver(m)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      expect(call.messages[0]?.media).toBeUndefined()
      expect(yield* Ref.get(wa.downloads)).toBe(0)
    }).pipe(Effect.provide(EngineTestLayer({ mediaAuto: "none" }))),
  )
})

describe("TurnEngine: edits, deletes, reactions", () => {
  it.effect("an edit while batching reaches the responder as the corrected text", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const events = yield* collectEvents(1)
      const m1 = message("teh calendar")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      yield* wa.inject({ _tag: "Edited", chatId: chat, messageId: m1.messageId, text: "the calendar" })
      expect((yield* events).map((e) => e._tag)).toEqual(["MessageEdited"])
      yield* TestClock.adjust("1 second")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.text)).toEqual(["the calendar"])
      yield* takeSoon(wa.sent)
    }).pipe(Effect.provide(EngineTestLayer({ batchWindow: Duration.seconds(1) }))),
  )

  it.effect("an edit while awaiting supersedes the turn with the corrected text", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      const m1 = message("book tuesday")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const first = yield* takeSoon(responder.calls)
      yield* settle
      yield* wa.inject({ _tag: "Edited", chatId: chat, messageId: m1.messageId, text: "book thursday" })
      const second = yield* takeSoon(responder.calls)
      expect(second.supersedes).toBe(first.turnId)
      expect(second.messages.map((m) => m.text)).toEqual(["book thursday"])
      expect((yield* registry.get(first.turnId)).state).toBe("superseded")
      yield* settle
      const refused = yield* registry.reply(first.turnId, ["tuesday it is"]).pipe(Effect.flip)
      expect(refused._tag).toBe("TurnSuperseded")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("deleting the only message of an open turn cancels it without a failure text", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(3)
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      const m1 = message("never mind")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* wa.inject({ _tag: "Deleted", chatId: chat, messageId: m1.messageId })
      const seen = yield* events
      expect(seen.map((e) => e._tag)).toEqual(["TurnOpened", "MessageDeleted", "TurnCancelled"])
      expect((yield* registry.get(call.turnId)).state).toBe("cancelled")
      expect(yield* registry.openCount).toBe(0)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect(Option.isNone(yield* Queue.poll(wa.reactions))).toBe(true)
      const late = yield* registry.reply(call.turnId, ["too late"]).pipe(Effect.flip)
      expect(late._tag).toBe("TurnNotAwaitingReply")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("deleting the newest message while batching moves the eye back and folds the rest", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const m1 = message("first")
      const m2 = message("second")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      yield* wa.deliver(m2)
      yield* takeSoon(wa.reactions)
      yield* takeSoon(wa.reactions)
      yield* wa.inject({ _tag: "Deleted", chatId: chat, messageId: m2.messageId })
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "👀" })
      yield* TestClock.adjust("1 second")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.text)).toEqual(["first"])
      yield* takeSoon(wa.sent)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
    }).pipe(Effect.provide(EngineTestLayer({ batchWindow: Duration.seconds(1) }))),
  )

  it.effect("a reaction becomes an event and opens nothing", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(1)
      yield* wa.inject({ _tag: "Reaction", chatId: chat, messageId: MessageId.make("out_9"), senderId: chat, emoji: "👍", onOurMessage: true })
      const seen = yield* events
      expect(seen[0]).toMatchObject({ _tag: "ReactionReceived", emoji: "👍", onOurMessage: true, messageId: "out_9" })
      yield* settle
      expect(Option.isNone(yield* Queue.poll(responder.calls))).toBe(true)
      expect(yield* registry.openCount).toBe(0)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("read receipts can be switched off", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* wa.deliver(message("hi"))
      yield* takeSoon(wa.reactions)
      yield* takeSoon(responder.calls)
      yield* takeSoon(wa.sent)
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.reads))).toBe(true)
    }).pipe(Effect.provide(EngineTestLayer({ readReceipts: false }))),
  )
})

describe("TurnEngine: progress, abandon, quoting", () => {
  it.effect("abandon fails the turn with the reason and the agent's own text", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      const m1 = message("do something impossible")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* registry.abandon(call.turnId, "needs the app", "This needs the app.")
      const seen = yield* events
      expect(seen[1]).toMatchObject({ _tag: "TurnFailed", reason: "abandoned: needs the app" })
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
      expect(yield* takeSoon(wa.sent)).toMatchObject({ text: "This needs the app.", quoteMessageId: m1.messageId })
      expect((yield* registry.get(call.turnId)).state).toBe("failed")
      const again = yield* registry.abandon(call.turnId, "twice").pipe(Effect.flip)
      expect(again._tag).toBe("TurnNotOpen")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("progress pushes the reply deadline out, but never past an hour", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      const events = yield* collectEvents(2)
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* wa.deliver(message("slow one"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      // The turn timeout is one minute in tests. At 50 s the agent reports progress for another minute.
      yield* TestClock.adjust("50 seconds")
      const before = yield* registry.progress(call.turnId, { extendBy: Duration.minutes(1) })
      yield* TestClock.adjust("40 seconds")
      expect((yield* registry.get(call.turnId)).state).toBe("awaiting")
      // A request for a day is capped at one hour after opening.
      const capped = yield* registry.progress(call.turnId, { extendBy: Duration.days(1) })
      expect(DateTime.toEpochMillis(capped.deadline) - DateTime.toEpochMillis(before.deadline)).toBeLessThanOrEqual(Duration.toMillis(Duration.hours(1)))
      yield* TestClock.adjust("61 minutes")
      const seen = yield* events
      expect(seen[1]).toMatchObject({ _tag: "TurnFailed" })
      expect(seen[1]?._tag === "TurnFailed" && seen[1].reason).toContain("no reply within")
      yield* takeSoon(wa.reactions)
      yield* takeSoon(wa.sent)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("replaceProgress edits the interim bubble into the first reply bubble", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      const m1 = message("look this up")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* registry.progress(call.turnId, { progressMessageId: MessageId.make("progress_1") })
      const ids = yield* registry.reply(call.turnId, [text("Found it: Tuesday."), text("Anything else?")], { replaceProgress: true })
      expect(yield* takeSoon(wa.edits)).toEqual({ chatId: chat, messageId: "progress_1", text: "Found it: Tuesday." })
      expect((yield* takeSoon(wa.sent)).text).toBe("Anything else?")
      expect(ids[0]).toBe("progress_1")
      expect(ids.length).toBe(2)
      expect(yield* takeSoon(wa.reactions)).toMatchObject({ messageId: m1.messageId, emoji: "" })
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("keeps the typing indicator alive every 8 seconds until the turn ends", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* wa.deliver(message("thinking time"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      const composing = () => Effect.map(Ref.get(wa.typing), (t) => t.filter((e) => e.composing).length)
      expect(yield* composing()).toBe(1)
      yield* TestClock.adjust("20 seconds")
      expect(yield* composing()).toBe(3)
      yield* registry.reply(call.turnId, ["done"])
      yield* takeSoon(wa.sent)
      yield* settle
      const typing = yield* Ref.get(wa.typing)
      expect(typing.at(-1)?.composing).toBe(false)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("a reply can quote a message; an unknown quote is dropped rather than failing the turn", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* Ref.set(wa.unknownQuotes, new Set([MessageId.make("gone")]))
      const m1 = message("which one?")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      const ids = yield* registry.reply(call.turnId, [text("this one", m1.messageId), text("not that", MessageId.make("gone"))])
      expect(yield* takeSoon(wa.sent)).toMatchObject({ text: "this one", quoteMessageId: m1.messageId })
      const second = yield* takeSoon(wa.sent)
      expect(second.text).toBe("not that")
      expect(second.quoteMessageId).toBeUndefined()
      expect(ids.length).toBe(2)
    }).pipe(Effect.provide(EngineTestLayer())),
  )
})

describe("TurnEngine: groups, typing, albums", () => {
  const group = ChatId.make("120363000000000000@g.us")

  it.effect("a group turn carries the last ten messages the bridge saw but did not answer", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const earlier: Array<ReturnType<typeof message>> = []
      for (let i = 0; i < 12; i++) {
        const m = message(`chatter ${i}`, { chatId: group, isGroup: true })
        earlier.push(m)
        yield* wa.observe(m)
      }
      yield* settle
      const asked = message("@bridge which one?", { chatId: group, isGroup: true })
      yield* wa.deliver(asked)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      expect(call.isGroup).toBe(true)
      expect(call.messages.map((m) => m.messageId)).toEqual([asked.messageId])
      expect(call.context?.map((m) => m.text)).toEqual(earlier.slice(-10).map((m) => m.text))
      yield* takeSoon(wa.sent)

      // Answered messages join the ring too, so the next turn sees what was just asked.
      const again = message("and this?", { chatId: group, isGroup: true })
      yield* wa.deliver(again)
      const second = yield* takeSoon(responder.calls)
      expect(second.context?.at(-1)?.messageId).toBe(asked.messageId)
      expect(second.context?.length).toBe(10)
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("a DM turn carries no context", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* wa.observe(message("noise"))
      yield* settle
      yield* wa.deliver(message("hi"))
      const call = yield* takeSoon(responder.calls)
      expect(call.context).toBeUndefined()
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  const typingConfig = { batchWindow: Duration.seconds(2), batchMaxWait: Duration.seconds(5), batchTypingWait: Duration.seconds(15) }

  it.effect("someone still typing holds the batch window past the max wait; the turn opens when they stop", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const m1 = message("so")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      yield* wa.reportPresence({ chatId: chat, state: "composing" })
      yield* settle
      const noCall = () => Effect.map(Queue.poll(responder.calls), Option.isNone)

      yield* TestClock.adjust("2 seconds")
      yield* settle
      expect(yield* noCall()).toBe(true)
      yield* TestClock.adjust("2 seconds")
      yield* wa.reportPresence({ chatId: chat, state: "composing" })
      yield* settle
      yield* TestClock.adjust("2 seconds")
      yield* settle
      // Six seconds in, past batchMaxWait, and still waiting because they type.
      expect(yield* noCall()).toBe(true)

      yield* wa.reportPresence({ chatId: chat, state: "paused" })
      yield* settle
      yield* TestClock.adjust("2 seconds")
      const call = yield* takeSoon(responder.calls)
      expect(call.messages.map((m) => m.messageId)).toEqual([m1.messageId])
    }).pipe(Effect.provide(EngineTestLayer(typingConfig))),
  )

  it.effect("typing cannot hold the window past the typing wait", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* wa.deliver(message("so"))
      yield* takeSoon(wa.reactions)
      for (let i = 0; i < 8; i++) {
        yield* wa.reportPresence({ chatId: chat, state: "composing" })
        yield* settle
        yield* TestClock.adjust("2 seconds")
        yield* settle
      }
      expect(Option.isSome(yield* Queue.poll(responder.calls))).toBe(true)
    }).pipe(Effect.provide(EngineTestLayer(typingConfig))),
  )

  it.effect("with album, adjacent images go out as one album and the rest as they are", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* wa.deliver(message("photos please"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      const ids = yield* registry.reply(call.turnId, [text("here"), png(), png(), text("and one more"), png()], { album: true })
      expect(ids.length).toBe(5)
      expect((yield* takeSoon(wa.sent)).text).toBe("here")
      const album = yield* takeSoon(wa.albums)
      expect(album.items.length).toBe(2)
      expect(album.messageIds).toEqual(ids.slice(1, 3))
      expect((yield* takeSoon(wa.sent)).text).toBe("and one more")
      expect((yield* takeSoon(wa.sentMedia)).messageId).toBe(ids[4])
      expect(Option.isNone(yield* Queue.poll(wa.albums))).toBe(true)
      expect((yield* takeSoon(wa.reactions)).emoji).toBe("")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("without album, images go out one by one", () =>
    Effect.gen(function* () {
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      const registry = yield* TurnRegistry
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))
      yield* wa.deliver(message("photos please"))
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle
      yield* registry.reply(call.turnId, [png(), png()])
      yield* takeSoon(wa.sentMedia)
      yield* takeSoon(wa.sentMedia)
      expect(Option.isNone(yield* Queue.poll(wa.albums))).toBe(true)
    }).pipe(Effect.provide(EngineTestLayer())),
  )
})
