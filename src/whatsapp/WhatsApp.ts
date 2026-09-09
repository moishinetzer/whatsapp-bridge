import { Context, Effect, Schema, Stream } from "effect"
import { ChatId, MessageId } from "../domain/ids.ts"
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

export type WhatsAppError = NotConnected | SendFailed
export type MediaTransportError = MediaUnavailable | MediaTooLarge | MediaDownloadFailed

export interface DownloadedMedia {
  readonly bytes: Uint8Array
  readonly mimetype: string
  /** File extension without the dot, derived from the mimetype (`jpeg`, `webp`, `ogg`, `pdf`, ...). */
  readonly extension: string
  readonly kind: MessageKind
}

export interface SendOptions {
  /**
   * The id the message will carry. Generate it once with `newOutboundId` and
   * pass the same id on every retry: WhatsApp de-duplicates by id, so a send
   * that timed out but actually went through is never delivered twice.
   */
  readonly messageId?: MessageId | undefined
}

export interface WhatsAppShape {
  /** Decoded inbound messages, echoes and noise already filtered out. */
  readonly inbound: Stream.Stream<InboundMessage>
  /** Resolves once the socket is open and the linked identity is known. */
  readonly linked: Effect.Effect<LinkedIdentity>
  /** A fresh outbound message id in WhatsApp's format. */
  readonly newOutboundId: Effect.Effect<MessageId>
  readonly sendText: (chatId: ChatId, text: string, options?: SendOptions) => Effect.Effect<MessageId, WhatsAppError>
  /** Send a file (image, video, audio or document) with the bytes already in memory; see `Outbound.load`. */
  readonly sendMedia: (chatId: ChatId, media: OutboundMedia, options?: SendOptions) => Effect.Effect<MessageId, WhatsAppError>
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
