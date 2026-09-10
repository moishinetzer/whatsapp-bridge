import { Context, Effect, Schema, Stream } from "effect"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import type { InboundMessage, MessageKind } from "../domain/message.ts"
import type { OutboundMedia } from "../domain/outbound.ts"
import { BridgeState, LinkedIdentity } from "../domain/status.ts"

/** No live socket. Retry later; `state` says whether waiting will help. */
export class NotConnected extends Schema.TaggedError<NotConnected>()(
  "NotConnected",
  { state: BridgeState, detail: Schema.optionalKey(Schema.String) },
  { httpApiStatus: 503 },
) {
  override get message() {
    return `not connected (${this.state})${this.detail === undefined ? "" : `: ${this.detail}`}`
  }
}

/** The socket was up but WhatsApp rejected or timed out the operation. */
export class SendFailed extends Schema.TaggedError<SendFailed>()(
  "SendFailed",
  { operation: Schema.String, chatId: ChatId, detail: Schema.String },
  { httpApiStatus: 502 },
) {
  override get message() {
    return `${this.operation} to ${this.chatId} failed: ${this.detail}`
  }
}

/** The bridge no longer holds the message (it restarted) or the message carried no media. */
export class MediaUnavailable extends Schema.TaggedError<MediaUnavailable>()(
  "MediaUnavailable",
  { chatId: ChatId, messageId: MessageId, reason: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message() {
    return `media for ${this.messageId} unavailable: ${this.reason}`
  }
}

export class MediaTooLarge extends Schema.TaggedError<MediaTooLarge>()(
  "MediaTooLarge",
  { chatId: ChatId, messageId: MessageId, size: Schema.Number, maxBytes: Schema.Number },
  { httpApiStatus: 413 },
) {
  override get message() {
    return `media for ${this.messageId} is ${this.size} bytes, over the ${this.maxBytes} byte limit`
  }
}

export class MediaDownloadFailed extends Schema.TaggedError<MediaDownloadFailed>()(
  "MediaDownloadFailed",
  { chatId: ChatId, messageId: MessageId, detail: Schema.String },
  { httpApiStatus: 502 },
) {
  override get message() {
    return `downloading media for ${this.messageId} failed: ${this.detail}`
  }
}

/** The message to quote, edit, delete or mark read is not one the bridge saw or sent this run. */
export class MessageUnknown extends Schema.TaggedError<MessageUnknown>()(
  "MessageUnknown",
  { chatId: ChatId, messageId: MessageId, reason: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message() {
    return `message ${this.messageId} unknown: ${this.reason}`
  }
}

/** WhatsApp only allows edits for 15 minutes and deletes for everyone for about 2 days. */
export class MessageTooOld extends Schema.TaggedError<MessageTooOld>()(
  "MessageTooOld",
  { chatId: ChatId, messageId: MessageId, operation: Schema.String, limitMs: Schema.Number, ageMs: Schema.Number },
  { httpApiStatus: 409 },
) {
  override get message() {
    return `cannot ${this.operation} ${this.messageId}: sent ${Math.round(this.ageMs / 1000)} s ago, WhatsApp allows ${Math.round(this.limitMs / 1000)} s`
  }
}

export type WhatsAppError = NotConnected | SendFailed
/** Sends that quote, edits and deletes can also fail because the target message is unknown or too old. */
export type MessageError = MessageUnknown | MessageTooOld

/** Something happened to a message the bridge had accepted, or someone reacted in a handled chat. */
export type InboundUpdate =
  | { readonly _tag: "Edited"; readonly chatId: ChatId; readonly messageId: MessageId; readonly text: string }
  | { readonly _tag: "Deleted"; readonly chatId: ChatId; readonly messageId: MessageId }
  | {
      readonly _tag: "Reaction"
      readonly chatId: ChatId
      readonly messageId: MessageId
      readonly senderId: string
      readonly emoji: string
      readonly onOurMessage: boolean
    }
export type MediaTransportError = MediaUnavailable | MediaTooLarge | MediaDownloadFailed

export interface DownloadedMedia {
  readonly bytes: Uint8Array
  readonly mimetype: string
  /** File extension without the dot, derived from the mimetype (`jpeg`, `webp`, `ogg`, `pdf`, ...). */
  readonly extension: string
  readonly kind: MessageKind
}

/** What `GET /contacts/{jid}` answers. Names are push names: what the person set on WhatsApp, not your address book. */
export const ContactInfo = Schema.Struct({
  jid: Schema.String,
  phone: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
  /** Expires after a while; fetch again when needed. */
  pictureUrl: Schema.optionalKey(Schema.String),
  onWhatsApp: Schema.Boolean,
})
export type ContactInfo = typeof ContactInfo.Type

export const GroupParticipantInfo = Schema.Struct({
  jid: Schema.String,
  phone: Schema.optionalKey(Schema.String),
  admin: Schema.Boolean,
})

export const GroupInfo = Schema.Struct({
  chatId: ChatId,
  subject: Schema.String,
  description: Schema.optionalKey(Schema.String),
  owner: Schema.optionalKey(Schema.String),
  participants: Schema.Array(GroupParticipantInfo),
  createdAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
})
export type GroupInfo = typeof GroupInfo.Type

export const PresenceState = Schema.Literals(["available", "unavailable", "composing", "recording", "paused", "unknown"])
export type PresenceState = typeof PresenceState.Type

/** The last thing WhatsApp said about someone's presence. `lastSeenAt` is hidden by most privacy settings. */
export const PresenceInfo = Schema.Struct({
  chatId: ChatId,
  state: PresenceState,
  at: Schema.optionalKey(Schema.DateTimeUtcFromString),
  lastSeenAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
})
export type PresenceInfo = typeof PresenceInfo.Type

/** WhatsApp refused a lookup, or the id is not something it knows. */
export class LookupFailed extends Schema.TaggedError<LookupFailed>()(
  "LookupFailed",
  { subject: Schema.String, detail: Schema.String },
  { httpApiStatus: 502 },
) {
  override get message() {
    return `lookup of ${this.subject} failed: ${this.detail}`
  }
}

export interface SendOptions {
  /**
   * The id the message will carry. Generate it once with `newOutboundId` and
   * pass the same id on every retry: WhatsApp de-duplicates by id, so a send
   * that timed out but actually went through is never delivered twice.
   */
  readonly messageId?: MessageId | undefined
  /** Send as a reply to this message. `MessageUnknown` when the bridge never saw or sent it. */
  readonly quoteMessageId?: MessageId | undefined
  /** The turn this send answers, for the history. */
  readonly turnId?: TurnId | undefined
}

export interface WhatsAppShape {
  /** Decoded inbound messages, echoes and noise already filtered out. */
  readonly inbound: Stream.Stream<InboundMessage>
  /** Resolves once the socket is open and the linked identity is known. */
  readonly linked: Effect.Effect<LinkedIdentity>
  /** A fresh outbound message id in WhatsApp's format. */
  readonly newOutboundId: Effect.Effect<MessageId>
  readonly sendText: (chatId: ChatId, text: string, options?: SendOptions) => Effect.Effect<MessageId, WhatsAppError | MessageUnknown>
  /** Send a file (image, video, audio or document) with the bytes already in memory; see `Outbound.load`. */
  readonly sendMedia: (chatId: ChatId, media: OutboundMedia, options?: SendOptions) => Effect.Effect<MessageId, WhatsAppError | MessageUnknown>
  /** Replace the text of a message the bridge sent this run (WhatsApp: within 15 minutes). */
  readonly editText: (chatId: ChatId, messageId: MessageId, text: string) => Effect.Effect<void, WhatsAppError | MessageError>
  /** Delete for everyone a message the bridge sent this run (WhatsApp: within about 2 days). */
  readonly deleteMessage: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void, WhatsAppError | MessageError>
  /** Mark accepted inbound messages as read (blue ticks). Unknown ids are skipped. */
  readonly markRead: (chatId: ChatId, messageIds: ReadonlyArray<MessageId>) => Effect.Effect<void, WhatsAppError>
  /** Forward a message the bridge saw or sent to another chat. */
  readonly forward: (toChatId: ChatId, fromChatId: ChatId, messageId: MessageId, options?: SendOptions) => Effect.Effect<MessageId, WhatsAppError | MessageUnknown>
  readonly contact: (jid: string) => Effect.Effect<ContactInfo, NotConnected | LookupFailed>
  readonly groupInfo: (chatId: ChatId) => Effect.Effect<GroupInfo, NotConnected | LookupFailed>
  /** Subscribes to the chat's presence (once) and returns the last known state. */
  readonly presence: (chatId: ChatId) => Effect.Effect<PresenceInfo, NotConnected>
  /** Edits, deletes and reactions on messages in handled chats, after the bridge accepted them. */
  readonly updates: Stream.Stream<InboundUpdate>
  readonly react: (chatId: ChatId, messageId: MessageId, emoji: string) => Effect.Effect<void, WhatsAppError>
  readonly unreact: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void, WhatsAppError>
  readonly setTyping: (chatId: ChatId, composing: boolean) => Effect.Effect<void, WhatsAppError>
  /**
   * Fetch and decrypt the attachment of a message seen this run. Refuses
   * anything bigger than `maxBytes` before downloading. Persisting the bytes is
   * the `MediaStore`'s job.
   */
  readonly downloadMedia: (
    chatId: ChatId,
    messageId: MessageId,
    options: { readonly maxBytes: number },
  ) => Effect.Effect<DownloadedMedia, MediaTransportError>
}

/**
 * The transport boundary. Everything above this line is protocol-agnostic;
 * `Baileys.ts` is the one live implementation and `test/support/FakeWhatsApp.ts`
 * the in-memory one.
 */
export class WhatsApp extends Context.Service<WhatsApp, WhatsAppShape>()("bridge/WhatsApp") {}

export { ChatId, MessageId }
