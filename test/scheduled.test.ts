import { describe, expect, it } from "@effect/vitest"
import { DateTime, Deferred, Duration, Effect, Fiber, Option, Queue, Ref, Stream } from "effect"
import { TestClock } from "effect/testing"
import type { BridgeEvent } from "../src/domain/events.ts"
import { Events } from "../src/engine/Events.ts"
import { Scheduler } from "../src/store/Scheduled.ts"
import { EngineTestLayer, FakeWhatsApp, chat, settle, takeSoon } from "./support/fakes.ts"

const nextEvent = (tag: BridgeEvent["_tag"]) =>
  Effect.gen(function* () {
    const events = yield* Events
    const ready = yield* Deferred.make<void>()
    const fiber = yield* events.stream.pipe(
      Stream.onStart(Deferred.succeed(ready, undefined)),
      Stream.filter((e) => e._tag === tag),
      Stream.take(1),
      Stream.runCollect,
      Effect.forkScoped,
    )
    yield* Deferred.await(ready)
    return Fiber.join(fiber).pipe(Effect.map((c) => Array.from(c)[0]))
  })

describe("Scheduler", () => {
  it.effect("holds a text until its time, then sends it and says so", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduler
      const wa = yield* FakeWhatsApp
      const sent = yield* nextEvent("ScheduledSent")
      const now = yield* DateTime.now
      const held = yield* scheduler.schedule(
        { kind: "text", chatId: chat, payload: { text: "Reminder: dentist at ten." } },
        DateTime.addDuration(now, Duration.minutes(30)),
        false,
      )
      expect(held.status).toBe("pending")
      yield* TestClock.adjust("29 minutes")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect((yield* scheduler.list).map((s) => [s.scheduledId, s.status])).toEqual([[held.scheduledId, "pending"]])
      yield* TestClock.adjust("2 minutes")
      const out = yield* takeSoon(wa.sent)
      expect(out.text).toBe("Reminder: dentist at ten.")
      const event = yield* sent
      expect(event).toMatchObject({ _tag: "ScheduledSent", scheduledId: held.scheduledId, messageId: out.messageId })
      yield* settle
      expect((yield* scheduler.list)[0]).toMatchObject({ scheduledId: held.scheduledId, status: "sent", messageId: out.messageId })
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("can be cancelled while pending, and only then", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduler
      const wa = yield* FakeWhatsApp
      const now = yield* DateTime.now
      const held = yield* scheduler.schedule({ kind: "text", chatId: chat, payload: { text: "later" } }, DateTime.addDuration(now, Duration.hours(1)), false)
      yield* scheduler.cancel(held.scheduledId)
      const again = yield* scheduler.cancel(held.scheduledId).pipe(Effect.flip)
      expect(again._tag).toBe("ScheduledNotFound")
      yield* TestClock.adjust("2 hours")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect((yield* scheduler.list)[0]?.status).toBe("cancelled")
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("waits for the recipient to be online when asked, and gives up after a day", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduler
      const wa = yield* FakeWhatsApp
      const skipped = yield* nextEvent("ScheduledSkipped")
      const now = yield* DateTime.now
      const held = yield* scheduler.schedule({ kind: "text", chatId: chat, payload: { text: "when you're around" } }, now, true)
      yield* TestClock.adjust("5 minutes")
      yield* settle
      expect(Option.isNone(yield* Queue.poll(wa.sent))).toBe(true)
      expect(Option.isSome(yield* Queue.poll(wa.presenceAsks))).toBe(true)
      // The recipient shows up: the next check sends it.
      yield* Ref.set(wa.presences, new Map([[chat, "available" as const]]))
      yield* TestClock.adjust("1 minute")
      expect((yield* takeSoon(wa.sent)).text).toBe("when you're around")
      // Another one nobody comes online for is dropped after 24 hours.
      yield* Ref.set(wa.presences, new Map())
      const lonely = yield* scheduler.schedule({ kind: "text", chatId: chat, payload: { text: "hello?" } }, yield* DateTime.now, true)
      yield* TestClock.adjust("25 hours")
      const event = yield* skipped
      expect(event).toMatchObject({ _tag: "ScheduledSkipped", scheduledId: lonely.scheduledId })
      expect(event?._tag === "ScheduledSkipped" && event.reason).toContain("online")
      void held
    }).pipe(Effect.provide(EngineTestLayer())),
  )

  it.effect("a transport failure marks the send failed with the reason", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduler
      const wa = yield* FakeWhatsApp
      const failed = yield* nextEvent("ScheduledFailed")
      yield* Ref.set(wa.failNextSends, 1)
      const held = yield* scheduler.schedule({ kind: "text", chatId: chat, payload: { text: "doomed" } }, yield* DateTime.now, false)
      yield* TestClock.adjust("1 second")
      const event = yield* failed
      expect(event).toMatchObject({ _tag: "ScheduledFailed", scheduledId: held.scheduledId })
      expect((yield* scheduler.list)[0]).toMatchObject({ status: "failed" })
    }).pipe(Effect.provide(EngineTestLayer())),
  )
})
