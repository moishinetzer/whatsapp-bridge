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

/** A media attachment the bridge downloaded to local disk. */
export const MediaFile = Schema.Struct({
  /** Absolute path on the bridge machine; readable by the user the daemon runs as. */
  path: Schema.String,
  mimetype: Schema.String,
  /** Bytes on disk. */
  size: Schema.Number,
  kind: MessageKind,
  downloadedAt: Schema.DateTimeUtcFromString,
})
export type MediaFile = typeof MediaFile.Type

/**
 * One inbound WhatsApp message as Intro sees it. This is the versioned wire
 * shape (see `BridgeEvent` for the envelope). `hasMedia` / `mimetype` say
 * something was attached; `media` carries the local file once the bridge has
 * downloaded it (automatically for images, on demand for the rest).
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
  /** Present on `TurnOpened` when the attachment was downloaded in time; otherwise fetch it via the media endpoint. */
  media: Schema.optionalKey(MediaFile),
})
export type InboundMessage = typeof InboundMessage.Type
