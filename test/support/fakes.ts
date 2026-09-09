import { Context, DateTime, Duration, Effect, Latch, Layer, Option, Queue, Ref, Schedule, Stream } from "effect"
import { BridgeConfig, type BridgeConfigShape } from "../../src/config.ts"
import { ChatId, MessageId } from "../../src/domain/ids.ts"
import { InboundMessage } from "../../src/domain/message.ts"
import { EventsLive } from "../../src/engine/Events.ts"
import { HealthLive } from "../../src/engine/Health.ts"
import { HandlerFailed, Responder, type ResponderReply, type TurnPayload } from "../../src/engine/Responder.ts"
import { TurnEngineLive, TurnRegistryLive } from "../../src/engine/Turns.ts"
import { SendFailed, WhatsApp } from "../../src/whatsapp/WhatsApp.ts"

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export const testConfig = (overrides: Partial<BridgeConfigShape> = {}): BridgeConfigShape => ({
  dataDir: "./data-test",
  authDir: "./data-test/auth",
  host: "127.0.0.1",
  port: 0,
  apiToken: Option.none(),
  responder: "manual",
  handlerUrl: Option.none(),
  handlerTimeout: Duration.seconds(5),
  batchWindow: Duration.zero,
  turnTimeout: Duration.minutes(1),
  seenEmoji: "👀",
  seenFallbackText: Option.none(),
  groups: false,
  allowedSenders: [],
  offlineWindow: Duration.zero,
  typing: true,
  logBodies: false,
  baileysLogLevel: "silent",
  browserName: "test",
  harnessSlow: Duration.zero,
  ...overrides,
})

export const TestConfigLive = (overrides: Partial<BridgeConfigShape> = {}) =>
  Layer.succeed(BridgeConfig, BridgeConfig.of(testConfig(overrides)))

// ---------------------------------------------------------------------------
// Fake WhatsApp transport
// ---------------------------------------------------------------------------

export interface Outbound {
  readonly chatId: ChatId
  readonly text: string
  readonly messageId: MessageId
}

export interface Reaction {
  readonly chatId: ChatId
  readonly messageId: MessageId
  readonly emoji: string
}

export interface FakeWhatsAppShape {
  readonly deliver: (message: InboundMessage) => Effect.Effect<void>
  /** Every text the bridge sent, in order. */
  readonly sent: Queue.Queue<Outbound>
  /** Every reaction call; `emoji: ""` is a removal. */
  readonly reactions: Queue.Queue<Reaction>
  /** Announced right before a send waits on `sendGate` (when one is set). */
  readonly sending: Queue.Queue<Outbound>
  readonly typing: Ref.Ref<ReadonlyArray<{ readonly chatId: ChatId; readonly composing: boolean }>>
  /** How many upcoming `sendText` calls fail. */
  readonly failNextSends: Ref.Ref<number>
  /** When set, `sendText` blocks until the latch opens. */
  readonly sendGate: Ref.Ref<Option.Option<Latch.Latch>>
}

export class FakeWhatsApp extends Context.Service<FakeWhatsApp, FakeWhatsAppShape>()("test/FakeWhatsApp") {}

export const FakeWhatsAppLive = Layer.effectContext(
  Effect.gen(function* () {
    const inbound = yield* Queue.unbounded<InboundMessage>()
    const sent = yield* Queue.unbounded<Outbound>()
    const sending = yield* Queue.unbounded<Outbound>()
    const reactions = yield* Queue.unbounded<Reaction>()
    const typing = yield* Ref.make<ReadonlyArray<{ readonly chatId: ChatId; readonly composing: boolean }>>([])
    const failNextSends = yield* Ref.make(0)
    const sendGate = yield* Ref.make<Option.Option<Latch.Latch>>(Option.none())
    const counter = yield* Ref.make(0)

    const sendText = (chatId: ChatId, text: string) =>
      Effect.gen(function* () {
        const failing = yield* Ref.modify(failNextSends, (n) => [n > 0, Math.max(0, n - 1)] as const)
        if (failing) return yield* new SendFailed({ operation: "sendText", chatId, detail: "simulated failure" })
        const n = yield* Ref.updateAndGet(counter, (c) => c + 1)
        const out: Outbound = { chatId, text, messageId: MessageId.make(`out_${n}`) }
        const gate = yield* Ref.get(sendGate)
        if (Option.isSome(gate)) {
          yield* Queue.offer(sending, out)
          yield* gate.value.await
        }
        yield* Queue.offer(sent, out)
        return out.messageId
      })

    const service = WhatsApp.of({
      inbound: Stream.fromQueue(inbound),
      linked: Effect.succeed({ jid: "bridge@s.whatsapp.net", name: "Intro" }),
      sendText,
      react: (chatId, messageId, emoji) => Queue.offer(reactions, { chatId, messageId, emoji }).pipe(Effect.asVoid),
      unreact: (chatId, messageId) => Queue.offer(reactions, { chatId, messageId, emoji: "" }).pipe(Effect.asVoid),
      setTyping: (chatId, composing) => Ref.update(typing, (t) => [...t, { chatId, composing }]),
    })

    const control = FakeWhatsApp.of({
      deliver: (m) => Queue.offer(inbound, m).pipe(Effect.asVoid),
      sent,
      sending,
      reactions,
      typing,
      failNextSends,
      sendGate,
    })

    return Context.make(WhatsApp, service).pipe(Context.add(FakeWhatsApp, control))
  }),
)

// ---------------------------------------------------------------------------
// Scriptable responder
// ---------------------------------------------------------------------------

export type Behaviour = (turn: TurnPayload) => Effect.Effect<ResponderReply, HandlerFailed>

export interface TestResponderShape {
  /** Every call the engine made, in order. */
  readonly calls: Queue.Queue<TurnPayload>
  readonly behaviour: Ref.Ref<Behaviour>
  /** How many calls were interrupted by the engine. */
  readonly interrupted: Ref.Ref<number>
}

export class TestResponder extends Context.Service<TestResponder, TestResponderShape>()("test/Responder") {}

export const replies = (...texts: ReadonlyArray<string>): ResponderReply => ({ _tag: "Replies", replies: texts })
export const external: ResponderReply = { _tag: "External" }

export const TestResponderLive = Layer.effectContext(
  Effect.gen(function* () {
    const calls = yield* Queue.unbounded<TurnPayload>()
    const behaviour = yield* Ref.make<Behaviour>(() => Effect.succeed(replies("pong")))
    const interrupted = yield* Ref.make(0)

    const respond = (turn: TurnPayload) =>
      Effect.gen(function* () {
        yield* Queue.offer(calls, turn)
        const f = yield* Ref.get(behaviour)
        return yield* f(turn)
      }).pipe(Effect.onInterrupt(() => Ref.update(interrupted, (n) => n + 1)))

    return Context.make(Responder, Responder.of({ kind: "pong", respond })).pipe(
      Context.add(TestResponder, TestResponder.of({ calls, behaviour, interrupted })),
    )
  }),
)

// ---------------------------------------------------------------------------
// The engine under test, wired to the fakes
// ---------------------------------------------------------------------------

export const EngineTestLayer = (overrides: Partial<BridgeConfigShape> = {}) => {
  const Config = TestConfigLive(overrides)
  const Health = HealthLive.pipe(Layer.provide(Config))
  const Events = EventsLive.pipe(Layer.provide(Health))
  const Registry = TurnRegistryLive
  const Engine = TurnEngineLive.pipe(Layer.provide([Config, FakeWhatsAppLive, Events, Registry, TestResponderLive]))
  return Layer.mergeAll(Config, Health, Events, FakeWhatsAppLive, Registry, TestResponderLive, Engine)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let counter = 0

export const chat = ChatId.make("447700900123@s.whatsapp.net")

export const message = (text: string, options: { readonly chatId?: ChatId; readonly isGroup?: boolean } = {}): InboundMessage =>
  InboundMessage.make({
    chatId: options.chatId ?? chat,
    messageId: MessageId.make(`in_${++counter}`),
    senderId: options.chatId ?? chat,
    senderNumber: "447700900123",
    senderName: "Moishi",
    isGroup: options.isGroup ?? false,
    timestamp: DateTime.makeUnsafe(1_700_000_000_000 + counter),
    kind: "text",
    text,
    hasMedia: false,
  })

/** Let every scheduled fiber run a few times (forks start on the next tick). */
export const settle = Effect.yieldNow.pipe(Effect.repeat(Schedule.recurs(10)), Effect.asVoid)

/** Named for readability at call sites; vitest's own timeout catches a test that never gets its element. */
export const takeSoon = <A>(queue: Queue.Queue<A>): Effect.Effect<A> => Queue.take(queue)
