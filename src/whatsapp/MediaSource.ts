import { Context, Effect, Layer } from "effect"
import type { ChatId, MessageId } from "../domain/ids.ts"
import { WhatsApp, type DownloadedMedia, type MediaTransportError } from "./WhatsApp.ts"

export interface MediaSourceShape {
  /**
   * Fetch and decrypt the attachment of a message the transport knows.
   * Refuses anything bigger than `maxBytes` before downloading. Persisting the
   * bytes is the `MediaStore`'s job.
   */
  readonly downloadMedia: (
    chatId: ChatId,
    messageId: MessageId,
    options: { readonly maxBytes: number },
  ) => Effect.Effect<DownloadedMedia, MediaTransportError>
}

/**
 * The one thing the media store needs from a transport. The personal line's
 * transport and the observer both provide it; the observer provides nothing
 * else, so a media download is the only thing it can be asked to do.
 */
export class MediaSource extends Context.Service<MediaSource, MediaSourceShape>()("bridge/MediaSource") {}

/** The personal line's transport as a media source. */
export const MediaSourceFromWhatsApp = Layer.effect(
  MediaSource,
  Effect.map(WhatsApp, (whatsapp) => MediaSource.of({ downloadMedia: whatsapp.downloadMedia })),
)
