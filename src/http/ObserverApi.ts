import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { VERSION } from "../version.ts"
import { ChatId, MessageId } from "../domain/ids.ts"
import { MediaFile } from "../domain/message.ts"
import { HealthSnapshot } from "../domain/status.ts"
import { MediaStoreError } from "../engine/Media.ts"
import { MediaDownloadFailed, MediaTooLarge, MediaUnavailable } from "../whatsapp/WhatsApp.ts"

/** How far the link-time backfill got. */
export const BackfillStatus = Schema.Struct({
  chunksAccepted: Schema.Number,
  chunksRefused: Schema.Number,
  messages: Schema.Number,
  mediaQueued: Schema.Number,
  complete: Schema.Boolean,
})

/** The body of the observer's `GET /health` and `GET /status`. */
export const ObserverStatus = HealthSnapshot.pipe(
  Schema.fieldsAssign({
    observer: Schema.Literal(true),
    databasePath: Schema.String,
    mediaDir: Schema.String,
    backfill: BackfillStatus,
    /** Messages the observer holds. */
    messages: Schema.Number,
    retentionDays: Schema.Number,
  }),
)
export type ObserverStatus = typeof ObserverStatus.Type

export class ObserverUnhealthy extends Schema.TaggedError<ObserverUnhealthy>()("ObserverUnhealthy", { status: ObserverStatus }, { httpApiStatus: 503 }) {
  override get message() {
    return `observer is ${this.status.state}${this.status.detail === undefined ? "" : `: ${this.status.detail}`}`
  }
}

const messageParams = { chatId: ChatId, messageId: MessageId }

export const ObserverHealthGroup = HttpApiGroup.make("health").add(
  HttpApiEndpoint.get("check", "/health", { success: ObserverStatus, error: ObserverUnhealthy }),
  HttpApiEndpoint.get("status", "/status", { success: ObserverStatus }),
)

/** The one thing the personal line asks the observer's port for: an attachment on disk. */
export const ObserverMediaGroup = HttpApiGroup.make("media").add(
  HttpApiEndpoint.get("get", "/chats/:chatId/messages/:messageId/media", {
    params: messageParams,
    success: MediaFile,
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
  HttpApiEndpoint.get("content", "/chats/:chatId/messages/:messageId/media/content", {
    params: messageParams,
    success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
)

/**
 * The observer daemon's API: health, and attachments on demand. There are no
 * send endpoints and no history endpoints; history is read from the database
 * file by the personal line and the CLI.
 */
export const ObserverApi = HttpApi.make("whatsapp-bridge-observer")
  .add(ObserverHealthGroup, ObserverMediaGroup)
  .annotateMerge(
    OpenApi.annotations({
      title: "whatsapp-bridge observer",
      version: VERSION,
      description: "The read-only linked device on the primary number. Health and attachments only.",
    }),
  )
