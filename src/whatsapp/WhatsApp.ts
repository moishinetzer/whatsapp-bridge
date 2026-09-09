import { Context, Effect, Schema, Stream } from "effect"
import { ChatId, MessageId } from "../domain/ids.ts"
import type { InboundMessage } from "../domain/message.ts"
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

export type WhatsAppError = NotConnected | SendFailed

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
  readonly react: (chatId: ChatId, messageId: MessageId, emoji: string) => Effect.Effect<void, WhatsAppError>
  readonly unreact: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void, WhatsAppError>
  readonly setTyping: (chatId: ChatId, composing: boolean) => Effect.Effect<void, WhatsAppError>
}

/**
 * The transport boundary. Everything above this line is protocol-agnostic;
 * `Baileys.ts` is the one live implementation and `test/support/FakeWhatsApp.ts`
 * the in-memory one.
 */
export class WhatsApp extends Context.Service<WhatsApp, WhatsAppShape>()("bridge/WhatsApp") {}

export { ChatId, MessageId }
