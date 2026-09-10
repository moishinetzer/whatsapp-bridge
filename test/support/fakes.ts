import { NodeServices } from "@effect/platform-node"
import { Context, DateTime, Duration, Effect, Latch, Layer, Option, Queue, Ref, Schedule, Stream } from "effect"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { BridgeConfig, type BridgeConfigShape } from "../../src/config.ts"
import { ChatId, MessageId } from "../../src/domain/ids.ts"
import { InboundMessage, type MessageKind } from "../../src/domain/message.ts"
import { EventsLive } from "../../src/engine/Events.ts"
import { MediaStoreLive } from "../../src/engine/Media.ts"
import { OutboundLive } from "../../src/engine/Outbound.ts"
import { outgoing, type OutboundMedia } from "../../src/domain/outbound.ts"
import { HealthLive } from "../../src/engine/Health.ts"
import { HandlerFailed, Responder, type ResponderReply, type TurnPayload } from "../../src/engine/Responder.ts"
import { TurnEngineLive, TurnRegistryLive } from "../../src/engine/Turns.ts"
import {
  MediaTooLarge,
  MediaUnavailable,
  MessageUnknown,
  SendFailed,
  WhatsApp,
  type DownloadedMedia,
  type InboundUpdate,
} from "../../src/whatsapp/WhatsApp.ts"

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/** One scratch directory per test process for media files. */
export const testMediaDir = mkdtempSync(join(tmpdir(), "bridge-media-"))

export const testConfig = (overrides: Partial<BridgeConfigShape> = {}): BridgeConfigShape => ({
  dataDir: "./data-test",
  authDir: "./data-test/auth",
  mediaDir: testMediaDir,
  mediaAuto: "images",
  mediaMaxBytes: 25 * 1024 * 1024,
  mediaWait: Duration.seconds(8),
  mediaRetention: Duration.days(7),
  mediaSweepEvery: Duration.hours(1),
  host: "127.0.0.1",
  port: 0,
  apiToken: Option.none(),
  responder: "manual",
  handlerUrl: Option.none(),
  handlerTimeout: Duration.seconds(5),
  batchWindow: Duration.zero,
  batchMaxWait: Duration.seconds(5),
  turnTimeout: Duration.minutes(1),
  seenEmoji: "👀",
  seenFallbackText: Option.none(),
  readReceipts: true,
  failureText: Option.some("Something went wrong."),
  typingMax: Duration.minutes(3),
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
  readonly quoteMessageId?: MessageId | undefined
}

export interface Edit {
  readonly chatId: ChatId
  readonly messageId: MessageId
  readonly text: string
}

export interface OutboundFile {
  readonly chatId: ChatId
  readonly media: OutboundMedia
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
  /** Every file the bridge sent, in order. */
  readonly sentMedia: Queue.Queue<OutboundFile>
  /** Texts and files interleaved as they went out (`text` or `media`), for ordering assertions. */
  readonly order: Queue.Queue<"text" | "media">
  /** Every reaction call; `emoji: ""` is a removal. */
  readonly reactions: Queue.Queue<Reaction>
  /** Every `markRead` call, one entry per message id. */
  readonly reads: Queue.Queue<{ readonly chatId: ChatId; readonly messageId: MessageId }>
  /** Every edit of one of the bridge's own messages. */
  readonly edits: Queue.Queue<Edit>
  /** Every delete of one of the bridge's own messages. */
  readonly deletes: Queue.Queue<{ readonly chatId: ChatId; readonly messageId: MessageId }>
  /** Push an edit, delete or reaction into the bridge as if WhatsApp reported it. */
  readonly inject: (update: InboundUpdate) => Effect.Effect<void>
  /** Message ids the fake refuses to quote (everything else is accepted). */
  readonly unknownQuotes: Ref.Ref<ReadonlySet<MessageId>>
  /** Announced right before a send waits on `sendGate` (when one is set). */
  readonly sending: Queue.Queue<Outbound>
  readonly typing: Ref.Ref<ReadonlyArray<{ readonly chatId: ChatId; readonly composing: boolean }>>
  /** How many upcoming `sendText` calls fail. */
  readonly failNextSends: Ref.Ref<number>
  /** When set, `sendText` blocks until the latch opens. */
  readonly sendGate: Ref.Ref<Option.Option<Latch.Latch>>
  /** Attachments the fake transport can "download", by message id. */
  readonly media: Ref.Ref<ReadonlyMap<MessageId, DownloadedMedia>>
  /** When set, `downloadMedia` blocks until the latch opens. */
  readonly mediaGate: Ref.Ref<Option.Option<Latch.Latch>>
  /** How many times the transport was asked to download anything. */
  readonly downloads: Ref.Ref<number>
}

export class FakeWhatsApp extends Context.Service<FakeWhatsApp, FakeWhatsAppShape>()("test/FakeWhatsApp") {}

export const FakeWhatsAppLive = Layer.effectContext(
  Effect.gen(function* () {
    const inbound = yield* Queue.unbounded<InboundMessage>()
    const sent = yield* Queue.unbounded<Outbound>()
    const sentMedia = yield* Queue.unbounded<OutboundFile>()
    const order = yield* Queue.unbounded<"text" | "media">()
    const sending = yield* Queue.unbounded<Outbound>()
    const reactions = yield* Queue.unbounded<Reaction>()
    const reads = yield* Queue.unbounded<{ readonly chatId: ChatId; readonly messageId: MessageId }>()
    const edits = yield* Queue.unbounded<Edit>()
    const deletes = yield* Queue.unbounded<{ readonly chatId: ChatId; readonly messageId: MessageId }>()
    const updates = yield* Queue.unbounded<InboundUpdate>()
    const unknownQuotes = yield* Ref.make<ReadonlySet<MessageId>>(new Set())
    const typing = yield* Ref.make<ReadonlyArray<{ readonly chatId: ChatId; readonly composing: boolean }>>([])
    const failNextSends = yield* Ref.make(0)
    const sendGate = yield* Ref.make<Option.Option<Latch.Latch>>(Option.none())
    const media = yield* Ref.make<ReadonlyMap<MessageId, DownloadedMedia>>(new Map())
    const mediaGate = yield* Ref.make<Option.Option<Latch.Latch>>(Option.none())
    const downloads = yield* Ref.make(0)
    const counter = yield* Ref.make(0)

    const downloadMedia = (chatId: ChatId, messageId: MessageId, options: { readonly maxBytes: number }) =>
      Effect.gen(function* () {
        yield* Ref.update(downloads, (n) => n + 1)
        const gate = yield* Ref.get(mediaGate)
        if (Option.isSome(gate)) yield* gate.value.await
        const found = (yield* Ref.get(media)).get(messageId)
        if (found === undefined) return yield* new MediaUnavailable({ chatId, messageId, reason: "not in the fake" })
        if (found.bytes.byteLength > options.maxBytes) {
          return yield* new MediaTooLarge({ chatId, messageId, size: found.bytes.byteLength, maxBytes: options.maxBytes })
        }
        return found
      })

    const newOutboundId = Effect.map(Ref.updateAndGet(counter, (c) => c + 1), (n) => MessageId.make(`out_${n}`))

    const checkQuote = (chatId: ChatId, quoteMessageId: MessageId | undefined) =>
      Effect.gen(function* () {
        if (quoteMessageId === undefined) return
        if ((yield* Ref.get(unknownQuotes)).has(quoteMessageId)) {
          return yield* new MessageUnknown({ chatId, messageId: quoteMessageId, reason: "not in the fake" })
        }
      })

    const sendText = (chatId: ChatId, text: string, options?: { readonly messageId?: MessageId | undefined; readonly quoteMessageId?: MessageId | undefined }) =>
      Effect.gen(function* () {
        yield* checkQuote(chatId, options?.quoteMessageId)
        const failing = yield* Ref.modify(failNextSends, (n) => [n > 0, Math.max(0, n - 1)] as const)
        if (failing) return yield* new SendFailed({ operation: "sendText", chatId, detail: "simulated failure" })
        const messageId = options?.messageId ?? (yield* newOutboundId)
        const out: Outbound = { chatId, text, messageId, quoteMessageId: options?.quoteMessageId }
        const gate = yield* Ref.get(sendGate)
        if (Option.isSome(gate)) {
          yield* Queue.offer(sending, out)
          yield* gate.value.await
        }
        yield* Queue.offer(sent, out)
        yield* Queue.offer(order, "text")
        return out.messageId
      })

    const sendMedia = (chatId: ChatId, media: OutboundMedia, options?: { readonly messageId?: MessageId | undefined; readonly quoteMessageId?: MessageId | undefined }) =>
      Effect.gen(function* () {
        yield* checkQuote(chatId, options?.quoteMessageId ?? media.quoteMessageId)
        const failing = yield* Ref.modify(failNextSends, (n) => [n > 0, Math.max(0, n - 1)] as const)
        if (failing) return yield* new SendFailed({ operation: `send ${media.kind}`, chatId, detail: "simulated failure" })
        const messageId = options?.messageId ?? (yield* newOutboundId)
        yield* Queue.offer(sentMedia, { chatId, media, messageId })
        yield* Queue.offer(order, "media")
        return messageId
      })

    const service = WhatsApp.of({
      inbound: Stream.fromQueue(inbound),
      linked: Effect.succeed({ jid: "bridge@s.whatsapp.net", name: "Intro" }),
      newOutboundId,
      sendText,
      sendMedia,
      editText: (chatId, messageId, text) => Queue.offer(edits, { chatId, messageId, text }).pipe(Effect.asVoid),
      deleteMessage: (chatId, messageId) => Queue.offer(deletes, { chatId, messageId }).pipe(Effect.asVoid),
      markRead: (chatId, messageIds) => Effect.forEach(messageIds, (messageId) => Queue.offer(reads, { chatId, messageId }), { discard: true }),
      updates: Stream.fromQueue(updates),
      react: (chatId, messageId, emoji) => Queue.offer(reactions, { chatId, messageId, emoji }).pipe(Effect.asVoid),
      unreact: (chatId, messageId) => Queue.offer(reactions, { chatId, messageId, emoji: "" }).pipe(Effect.asVoid),
      setTyping: (chatId, composing) => Ref.update(typing, (t) => [...t, { chatId, composing }]),
      downloadMedia,
    })

    const control = FakeWhatsApp.of({
      deliver: (m) => Queue.offer(inbound, m).pipe(Effect.asVoid),
      sent,
      sentMedia,
      order,
      sending,
      reactions,
      reads,
      edits,
      deletes,
      inject: (u) => Queue.offer(updates, u).pipe(Effect.asVoid),
      unknownQuotes,
      typing,
      failNextSends,
      sendGate,
      media,
      mediaGate,
      downloads,
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

export const replies = (...texts: ReadonlyArray<string>): ResponderReply => ({ _tag: "Replies", replies: outgoing(texts) })
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
  const Registry = TurnRegistryLive.pipe(Layer.provide(Config))
  const Media = MediaStoreLive.pipe(Layer.provide([Config, FakeWhatsAppLive, NodeServices.layer]))
  const Outbound = OutboundLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const Engine = TurnEngineLive.pipe(Layer.provide([Config, FakeWhatsAppLive, Events, Registry, TestResponderLive, Media]))
  return Layer.mergeAll(Config, Health, Events, FakeWhatsAppLive, Registry, TestResponderLive, Media, Outbound, Engine)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let counter = 0

export const chat = ChatId.make("447700900123@s.whatsapp.net")

export const message = (
  text: string,
  options: { readonly chatId?: ChatId; readonly isGroup?: boolean; readonly kind?: MessageKind; readonly mimetype?: string } = {},
): InboundMessage => {
  const kind = options.kind ?? "text"
  const hasMedia = kind === "image" || kind === "sticker" || kind === "video" || kind === "audio" || kind === "document"
  return InboundMessage.make({
    chatId: options.chatId ?? chat,
    messageId: MessageId.make(`in_${++counter}`),
    senderId: options.chatId ?? chat,
    senderNumber: "447700900123",
    senderName: "Moishi",
    isGroup: options.isGroup ?? false,
    timestamp: DateTime.makeUnsafe(1_700_000_000_000 + counter),
    kind,
    text,
    hasMedia,
    ...(options.mimetype === undefined ? {} : { mimetype: options.mimetype }),
  })
}

/** A small PNG-ish payload to send outbound; the fake transport never inspects it. */
export const png = (bytes = 32): OutboundMedia => ({
  kind: "image",
  bytes: new Uint8Array(bytes).map((_, i) => (i * 7) % 256),
  mimetype: "image/png",
})

export const jpeg = (bytes = 64): DownloadedMedia => ({
  bytes: new Uint8Array(bytes).map((_, i) => i % 256),
  mimetype: "image/jpeg",
  extension: "jpeg",
  kind: "image",
})

/** Let every scheduled fiber run a few times (forks start on the next tick). */
export const settle = Effect.yieldNow.pipe(Effect.repeat(Schedule.recurs(10)), Effect.asVoid)

/** Named for readability at call sites; vitest's own timeout catches a test that never gets its element. */
export const takeSoon = <A>(queue: Queue.Queue<A>): Effect.Effect<A> => Queue.take(queue)
