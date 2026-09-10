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
    /** Groups: the last messages in the chat before this turn, oldest first, so the agent sees what was being discussed. */
    context: Schema.optionalKey(Schema.Array(InboundMessage)),
  },
  /** Newer messages arrived mid-turn. Stop working on `turnId`; `by` carries everything. */
  TurnSuperseded: { ...envelope, turnId: TurnId, chatId: ChatId, by: TurnId },
  /** The reply went out and the 👀 reactions were cleared. */
  TurnCompleted: { ...envelope, turnId: TurnId, chatId: ChatId, sentMessageIds: Schema.Array(MessageId) },
  /** The turn could not be answered (responder failed, timed out, sending failed, or the agent abandoned it). The eye comes off and the failure text goes out. */
  TurnFailed: { ...envelope, turnId: TurnId, chatId: ChatId, reason: Schema.String },
  /** Every message of the turn was deleted by its sender before it was answered. Nothing is sent. */
  TurnCancelled: { ...envelope, turnId: TurnId, chatId: ChatId, reason: Schema.String },
  /** Someone reacted to a message in a handled chat. Empty `emoji` means the reaction was removed. Never opens a turn. */
  ReactionReceived: {
    ...envelope,
    chatId: ChatId,
    messageId: MessageId,
    senderId: Schema.String,
    emoji: Schema.String,
    /** True when the reacted-to message is one the bridge sent. */
    onOurMessage: Schema.Boolean,
  },
  /** The sender edited a message the bridge had accepted. `text` is the new body. */
  MessageEdited: { ...envelope, chatId: ChatId, messageId: MessageId, text: Schema.String },
  /** The sender deleted a message the bridge had accepted ("delete for everyone"). */
  MessageDeleted: { ...envelope, chatId: ChatId, messageId: MessageId },
  /** A message the bridge sent reached the recipient's device. In groups, one per participant. */
  MessageDelivered: { ...envelope, chatId: ChatId, messageId: MessageId, participantId: Schema.optionalKey(Schema.String) },
  /** A message the bridge sent was read (or a voice note played). In groups, one per participant. */
  MessageRead: { ...envelope, chatId: ChatId, messageId: MessageId, participantId: Schema.optionalKey(Schema.String) },
  /** A scheduled send went out. */
  ScheduledSent: { ...envelope, scheduledId: Schema.String, chatId: ChatId, messageId: MessageId },
  /** A scheduled send was dropped: too late after downtime, or its hold for the recipient to come online expired. */
  ScheduledSkipped: { ...envelope, scheduledId: Schema.String, chatId: ChatId, reason: Schema.String },
  /** A scheduled send failed at the transport; the item is kept as `failed` with the reason. */
  ScheduledFailed: { ...envelope, scheduledId: Schema.String, chatId: ChatId, reason: Schema.String },
  /** Someone voted on a poll the bridge sent. `selected` is the full current selection of that voter. */
  PollVote: { ...envelope, chatId: ChatId, pollMessageId: MessageId, voterId: Schema.String, selected: Schema.Array(Schema.String) },
  /** Presence changed in a chat the agent asked about through `GET /chats/{id}/presence`. */
  PresenceChanged: {
    ...envelope,
    chatId: ChatId,
    state: Schema.Literals(["available", "unavailable", "composing", "recording", "paused", "unknown"]),
    lastSeenAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  },
  /** Connection state changed; mirrors `GET /health`. */
  ConnectionChanged: { ...envelope, state: BridgeState, detail: Schema.optionalKey(Schema.String) },
})
export type BridgeEvent = typeof BridgeEvent.Type

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never

/** An event minus the envelope the `Events` service stamps on. */
export type BridgeEventBody = DistributiveOmit<BridgeEvent, "v" | "seq" | "at">
