import { Schema, type DateTime } from "effect"
import { ChatId, MessageId, TurnId } from "./ids.ts"
import { MessageKind } from "./message.ts"

export const Direction = Schema.Literals(["in", "out"])
export type Direction = typeof Direction.Type

/** Delivery state of a message the bridge sent, as far as WhatsApp reported it. */
export const DeliveryStatus = Schema.Literals(["sent", "delivered", "read"])
export type DeliveryStatus = typeof DeliveryStatus.Type

export const StoredReaction = Schema.Struct({
  senderId: Schema.String,
  emoji: Schema.String,
  at: Schema.DateTimeUtcFromString,
})

/** One row of the history: a message in either direction, with what happened to it since. */
export const StoredMessage = Schema.Struct({
  chatId: ChatId,
  messageId: MessageId,
  direction: Direction,
  senderId: Schema.String,
  senderName: Schema.optionalKey(Schema.String),
  isGroup: Schema.Boolean,
  timestamp: Schema.DateTimeUtcFromString,
  kind: MessageKind,
  /** The body as first seen (or the caption). Absent on a tombstone. */
  text: Schema.optionalKey(Schema.String),
  /** The latest body after the sender edited it. */
  editedText: Schema.optionalKey(Schema.String),
  mimetype: Schema.optionalKey(Schema.String),
  /** Where the attachment is on disk; `mediaEvictedAt` says when the cache let it go. */
  mediaPath: Schema.optionalKey(Schema.String),
  mediaEvictedAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  quotedMessageId: Schema.optionalKey(MessageId),
  /** Outbound only. */
  status: Schema.optionalKey(DeliveryStatus),
  /** Set when the sender deleted it for everyone; the text is gone. */
  deletedAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  /** The turn this message belonged to (inbound) or answered (outbound). */
  turnId: Schema.optionalKey(TurnId),
  reactions: Schema.Array(StoredReaction),
})
export type StoredMessage = typeof StoredMessage.Type

export const SearchHit = StoredMessage.pipe(Schema.fieldsAssign({ snippet: Schema.String }))
export type SearchHit = typeof SearchHit.Type

/** What the history endpoints return: a stored message, with a `snippet` when it came from a search. */
export const HistoryRow = StoredMessage.pipe(Schema.fieldsAssign({ snippet: Schema.optionalKey(Schema.String) }))
export type HistoryRow = typeof HistoryRow.Type

export const ChatSummary = Schema.Struct({
  chatId: ChatId,
  name: Schema.optionalKey(Schema.String),
  isGroup: Schema.Boolean,
  lastMessageAt: Schema.DateTimeUtcFromString,
  messageCount: Schema.Number,
})
export type ChatSummary = typeof ChatSummary.Type

/** One entry of the address book synced from the phone, or learned from traffic. */
export const ContactRecord = Schema.Struct({
  jid: Schema.String,
  lid: Schema.optionalKey(Schema.String),
  phone: Schema.optionalKey(Schema.String),
  /** The name saved in the phone's address book. */
  name: Schema.optionalKey(Schema.String),
  /** The name the contact set for themselves. */
  pushName: Schema.optionalKey(Schema.String),
  updatedAt: Schema.DateTimeUtcFromString,
})
export type ContactRecord = typeof ContactRecord.Type

/** A call, stored as a `call` row in the history so "they called twice" is context too. */
export interface CallRecord {
  readonly callId: string
  readonly chatId: ChatId
  readonly fromId: string
  readonly direction: Direction
  readonly at: DateTime.Utc
  readonly status: string
  readonly isVideo: boolean
  readonly isGroup: boolean
}
