import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { VERSION } from "../version.ts"
import { BridgeEvent } from "../domain/events.ts"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import { MediaFile } from "../domain/message.ts"
import { BridgeStatus } from "../domain/status.ts"
import { MediaStoreError } from "../engine/Media.ts"
import { OutboundFileUnreadable, OutboundInvalid, OutboundTooLarge } from "../engine/Outbound.ts"
import { OutboundMediaPayload } from "../domain/outbound.ts"
import { TurnFailedError, TurnNotAwaitingReply, TurnNotFound, TurnSummary, TurnSuperseded } from "../engine/Turns.ts"
import { MediaDownloadFailed, MediaTooLarge, MediaUnavailable, NotConnected, SendFailed } from "../whatsapp/WhatsApp.ts"

/** `GET /health` answers 503 with the same body when the bridge is not `ok`. */
export class Unhealthy extends Schema.TaggedError<Unhealthy>()("Unhealthy", { status: BridgeStatus }, { httpApiStatus: 503 }) {
  override get message() {
    return `bridge is ${this.status.state}${this.status.detail === undefined ? "" : `: ${this.status.detail}`}`
  }
}

export const SendTextPayload = Schema.Struct({ text: Schema.String.pipe(Schema.check(Schema.isNonEmpty())) })
export const SentMessage = Schema.Struct({ chatId: ChatId, messageId: MessageId })
export const ReactionPayload = Schema.Struct({ emoji: Schema.String.pipe(Schema.check(Schema.isNonEmpty())) })
export const TypingPayload = Schema.Struct({ composing: Schema.Boolean })
export const ReplyPayload = Schema.Struct({
  text: Schema.optionalKey(Schema.String),
  replies: Schema.optionalKey(Schema.Array(Schema.String)),
  /** Files to send after the text bubbles, in order. */
  attachments: Schema.optionalKey(Schema.Array(OutboundMediaPayload)),
})
export const ReplyResult = Schema.Struct({ turnId: TurnId, sentMessageIds: Schema.Array(MessageId) })

const chatParams = { chatId: ChatId }
const messageParams = { chatId: ChatId, messageId: MessageId }
const turnParams = { turnId: TurnId }

export const HealthGroup = HttpApiGroup.make("health").add(
  HttpApiEndpoint.get("check", "/health", { success: BridgeStatus, error: Unhealthy }),
  HttpApiEndpoint.get("status", "/status", { success: BridgeStatus }),
)

export const EventsQuery = Schema.Struct({
  /**
   * Replay buffered events with `seq` greater than this before going live.
   * Default: live only. Must be a non-negative integer: `NumberFromString`
   * alone turns "abc" into NaN, which would silently filter out every event.
   */
  since: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)))),
})

export const EventsGroup = HttpApiGroup.make("events").add(
  HttpApiEndpoint.get("stream", "/events", { query: EventsQuery, success: HttpApiSchema.StreamSse({ data: BridgeEvent }) }),
)

export const ChatsGroup = HttpApiGroup.make("chats").add(
  HttpApiEndpoint.post("send", "/chats/:chatId/messages", {
    params: chatParams,
    payload: SendTextPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.post("sendMedia", "/chats/:chatId/media", {
    params: chatParams,
    payload: OutboundMediaPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed, OutboundInvalid, OutboundFileUnreadable, OutboundTooLarge],
  }),
  HttpApiEndpoint.post("react", "/chats/:chatId/messages/:messageId/reactions", {
    params: messageParams,
    payload: ReactionPayload,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.delete("unreact", "/chats/:chatId/messages/:messageId/reactions", {
    params: messageParams,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.post("typing", "/chats/:chatId/typing", {
    params: chatParams,
    payload: TypingPayload,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.get("media", "/chats/:chatId/messages/:messageId/media", {
    params: messageParams,
    success: MediaFile,
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
  HttpApiEndpoint.get("mediaContent", "/chats/:chatId/messages/:messageId/media/content", {
    params: messageParams,
    success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
)

export const TurnsGroup = HttpApiGroup.make("turns").add(
  HttpApiEndpoint.get("list", "/turns", { success: Schema.Array(TurnSummary) }),
  HttpApiEndpoint.get("get", "/turns/:turnId", { params: turnParams, success: TurnSummary, error: TurnNotFound }),
  HttpApiEndpoint.post("reply", "/turns/:turnId/reply", {
    params: turnParams,
    payload: ReplyPayload,
    success: ReplyResult,
    error: [TurnNotFound, TurnSuperseded, TurnNotAwaitingReply, TurnFailedError, OutboundInvalid, OutboundFileUnreadable, OutboundTooLarge],
  }),
)

/**
 * The daemon's local API. `HttpApiBuilder` derives the router from this and
 * `HttpApiClient` derives the typed client the CLI uses; `/openapi.json` and
 * `/docs` are generated from the same definition.
 */
export const BridgeApi = HttpApi.make("whatsapp-bridge")
  .add(HealthGroup, EventsGroup, ChatsGroup, TurnsGroup)
  .annotateMerge(
    OpenApi.annotations({
      title: "whatsapp-bridge",
      version: VERSION,
      description: "Local API of the WhatsApp ↔ Intro bridge. Events stream over SSE from /events; replies go to /turns/{id}/reply.",
    }),
  )
