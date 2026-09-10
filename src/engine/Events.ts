import { Context, DateTime, Effect, Layer, PubSub, Ref, Stream } from "effect"
import { EVENT_SCHEMA_VERSION, type BridgeEvent, type BridgeEventBody } from "../domain/events.ts"
import { Health } from "./Health.ts"

/** How many recent events a new subscriber can ask to have replayed. */
export const REPLAY = 200

export interface EventsShape {
  /** Stamp the envelope (`v`, `seq`, `at`) and broadcast. Every subscriber sees every event. */
  readonly publish: (body: BridgeEventBody) => Effect.Effect<BridgeEvent>
  /**
   * Events with `seq` greater than `since`: the last `REPLAY` are replayed from
   * memory, then live ones follow with no gap. Omit `since` for live events only.
   */
  readonly since: (seq: number) => Stream.Stream<BridgeEvent>
  /** Live events from the moment the stream starts. */
  readonly stream: Stream.Stream<BridgeEvent>
  /** The `seq` of the last event published (0 before the first). */
  readonly latestSeq: Effect.Effect<number>
}

export class Events extends Context.Service<Events, EventsShape>()("bridge/Events") {}

/** Log-safe summary of an event: ids and tags only, never bodies. */
const describe = (event: BridgeEvent): Record<string, unknown> => {
  switch (event._tag) {
    case "Ready":
    case "Heartbeat":
      return {}
    case "MessageReceived":
      return {
        chatId: event.message.chatId,
        messageId: event.message.messageId,
        kind: event.message.kind,
        isGroup: event.message.isGroup,
      }
    case "TurnOpened":
      return { turnId: event.turnId, chatId: event.chatId, messages: event.messages.length, supersedes: event.supersedes }
    case "TurnSuperseded":
      return { turnId: event.turnId, chatId: event.chatId, by: event.by }
    case "TurnCompleted":
      return { turnId: event.turnId, chatId: event.chatId, sent: event.sentMessageIds.length }
    case "TurnFailed":
      return { turnId: event.turnId, chatId: event.chatId, reason: event.reason }
    case "ConnectionChanged":
      return { state: event.state, detail: event.detail }
    case "TurnCancelled":
      return { turnId: event.turnId, chatId: event.chatId, reason: event.reason }
    case "ReactionReceived":
      return { chatId: event.chatId, messageId: event.messageId, senderId: event.senderId, emoji: event.emoji, onOurMessage: event.onOurMessage }
    case "MessageEdited":
    case "MessageDeleted":
      return { chatId: event.chatId, messageId: event.messageId }
    case "MessageDelivered":
    case "MessageRead":
      return { chatId: event.chatId, messageId: event.messageId, participantId: event.participantId }
    case "ScheduledSent":
      return { scheduledId: event.scheduledId, chatId: event.chatId, messageId: event.messageId }
    case "ScheduledSkipped":
    case "ScheduledFailed":
      return { scheduledId: event.scheduledId, chatId: event.chatId, reason: event.reason }
    case "PollVote":
      return { chatId: event.chatId, pollMessageId: event.pollMessageId, voterId: event.voterId, selected: event.selected.length }
    case "PresenceChanged":
      return { chatId: event.chatId, state: event.state }
  }
}

export const EventsLive = Layer.effect(
  Events,
  Effect.gen(function* () {
    const health = yield* Health
    // Sliding: a stalled SSE client drops its oldest buffered events instead of growing memory; it recovers with `?since=`.
    const pubsub = yield* PubSub.sliding<BridgeEvent>({ capacity: 2_048, replay: REPLAY })
    const seq = yield* Ref.make(0)

    const publish = Effect.fn("Events.publish")(function* (body: BridgeEventBody) {
      const at = yield* DateTime.now
      const n = yield* Ref.updateAndGet(seq, (s) => s + 1)
      const event = { ...body, v: EVENT_SCHEMA_VERSION, seq: n, at } as BridgeEvent
      yield* PubSub.publish(pubsub, event)
      yield* health.touch("event")
      yield* Effect.logDebug(`event ${event._tag}`).pipe(Effect.annotateLogs({ seq: n, ...describe(event) }))
      return event
    })

    const since = (n: number) => Stream.fromPubSub(pubsub).pipe(Stream.filter((e) => e.seq > n))
    const stream = Stream.unwrap(Effect.map(Ref.get(seq), since))

    return Events.of({ publish, since, stream, latestSeq: Ref.get(seq) })
  }),
)
