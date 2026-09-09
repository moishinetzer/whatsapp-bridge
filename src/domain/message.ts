import { Schema } from "effect"
import { ChatId, MessageId } from "./ids.ts"

export const MessageKind = Schema.Literals([
  "text",
  "image",
  "video",
  "audio",
  "document",
  "sticker",
  "location",
  "contact",
  "other",
])
export type MessageKind = typeof MessageKind.Type

/**
 * One inbound WhatsApp message as Intro sees it. This is the versioned wire
 * shape (see `BridgeEvent` for the envelope). Media bytes are not delivered in
 * v1; `hasMedia` / `mimetype` tell Intro that something was attached.
 */
export const InboundMessage = Schema.Struct({
  chatId: ChatId,
  messageId: MessageId,
  /** Jid of the sender: the chat itself for DMs, the participant for groups. */
  senderId: Schema.String,
  /** Digits of the sender's phone number when WhatsApp exposed it. */
  senderNumber: Schema.optionalKey(Schema.String),
  /** The sender's WhatsApp display name (push name) when present. */
  senderName: Schema.optionalKey(Schema.String),
  isGroup: Schema.Boolean,
  /** When WhatsApp says the message was sent (ISO-8601 on the wire). */
  timestamp: Schema.DateTimeUtcFromString,
  kind: MessageKind,
  /** Body text, or the caption of a media message. Absent for media without caption. */
  text: Schema.optionalKey(Schema.String),
  hasMedia: Schema.Boolean,
  mimetype: Schema.optionalKey(Schema.String),
  /** Id of the message this one quotes/replies to, when any. */
  quotedMessageId: Schema.optionalKey(MessageId),
})
export type InboundMessage = typeof InboundMessage.Type
