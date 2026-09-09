import { Schema } from "effect"
import { ChatId, MessageId, TurnId } from "./ids.ts"
import { InboundMessage } from "./message.ts"
import { BridgeState } from "./status.ts"

/** Bump when the shape of any event changes incompatibly. Intro pins on this. */
export const EVENT_SCHEMA_VERSION = 1 as const

const envelope = {
  v: Schema.Literal(EVENT_SCHEMA_VERSION),
  /** Monotonic per daemon run. Gaps mean you missed events (reconnect and list `/turns`). */
  seq: Schema.Number,
  at: Schema.DateTimeUtcFromString,
}

/**
 * The versioned event stream Intro consumes from `GET /events` (server-sent
 * events, one JSON object per `data:` line).
 */
export const BridgeEvent = Schema.TaggedUnion({
  /**
   * First event on every `GET /events` connection. Its `seq` is the last real
   * event's number: pass it back as `?since=` after a reconnect to replay what
   * you missed (up to the last 200 events).
   */
  Ready: { ...envelope },
  /** Sent every 25 seconds while nothing else happens, so a dead connection is noticed. `seq` is the latest real seq. */
  Heartbeat: { ...envelope },
  /** A message the bridge accepted for processing. 👀 is being applied. */
  MessageReceived: { ...envelope, message: InboundMessage },
  /** The batch window closed: these messages form one turn and want one reply. */
  TurnOpened: {
    ...envelope,
    turnId: TurnId,
    chatId: ChatId,
    isGroup: Schema.Boolean,
    messages: Schema.Array(InboundMessage),
    /** Set when this turn replaces one that was interrupted by newer messages. */
    supersedes: Schema.optionalKey(TurnId),
  },
  /** Newer messages arrived mid-turn. Stop working on `turnId`; `by` carries everything. */
  TurnSuperseded: { ...envelope, turnId: TurnId, chatId: ChatId, by: TurnId },
  /** The reply went out and the 👀 reactions were cleared. */
  TurnCompleted: { ...envelope, turnId: TurnId, chatId: ChatId, sentMessageIds: Schema.Array(MessageId) },
  /** The turn could not be answered (responder failed, timed out, or sending failed). 👀 stays. */
  TurnFailed: { ...envelope, turnId: TurnId, chatId: ChatId, reason: Schema.String },
  /** Connection state changed; mirrors `GET /health`. */
  ConnectionChanged: { ...envelope, state: BridgeState, detail: Schema.optionalKey(Schema.String) },
})
export type BridgeEvent = typeof BridgeEvent.Type

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never

/** An event minus the envelope the `Events` service stamps on. */
export type BridgeEventBody = DistributiveOmit<BridgeEvent, "v" | "seq" | "at">
