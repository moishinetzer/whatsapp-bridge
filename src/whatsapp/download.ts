import { downloadMediaMessage, extensionForMediaMessage, getContentType, normalizeMessageContent, type WAMessage, type WASocket } from "@whiskeysockets/baileys"
import type { ILogger } from "@whiskeysockets/baileys/lib/Utils/logger.js"
import { Effect } from "effect"
import type { InboundMessage } from "../domain/message.ts"
import { MediaDownloadFailed, MediaTooLarge, MediaUnavailable, type DownloadedMedia } from "./WhatsApp.ts"

/** protobuf's 64-bit numbers arrive as `Long` objects; only `toNumber` is needed here. */
interface LongLike {
  toNumber(): number
}

export const toNumber = (n: number | LongLike | null | undefined): number | undefined => {
  if (n === null || n === undefined) return undefined
  if (typeof n === "number") return n
  return typeof n === "object" && "toNumber" in n && typeof n.toNumber === "function" ? n.toNumber() : undefined
}

const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

/**
 * Fetch and decrypt the attachment carried by a raw message. Shared by the
 * personal line and the observer: the only difference between them is where
 * the raw message comes from.
 */
export const downloadFromRaw = (
  raw: WAMessage,
  message: InboundMessage,
  options: { readonly maxBytes: number; readonly socket: WASocket | undefined; readonly logger: ILogger },
) =>
  Effect.gen(function* () {
    const { chatId, messageId } = message
    const content = normalizeMessageContent(raw.message)
    const contentType = content === undefined ? undefined : getContentType(content)
    const media = content && contentType ? (content[contentType] as { mimetype?: string | null; fileLength?: number | LongLike | null } | null) : null
    if (!media || typeof media !== "object") {
      return yield* new MediaUnavailable({ chatId, messageId, reason: "message carries no downloadable media" })
    }
    const declared = toNumber(media.fileLength)
    if (declared !== undefined && declared > options.maxBytes) {
      return yield* new MediaTooLarge({ chatId, messageId, size: declared, maxBytes: options.maxBytes })
    }
    const context = options.socket === undefined ? undefined : { reuploadRequest: (m: WAMessage) => options.socket!.updateMediaMessage(m), logger: options.logger }
    const bytes = yield* Effect.tryPromise({
      try: () => downloadMediaMessage(raw, "buffer", {}, context),
      catch: (e) => new MediaDownloadFailed({ chatId, messageId, detail: messageOf(e) }),
    }).pipe(
      Effect.timeout("60 seconds"),
      Effect.catchTag("TimeoutError", () => new MediaDownloadFailed({ chatId, messageId, detail: "timed out after 60 seconds" })),
    )
    if (bytes.byteLength > options.maxBytes) {
      return yield* new MediaTooLarge({ chatId, messageId, size: bytes.byteLength, maxBytes: options.maxBytes })
    }
    const mimetype = (media.mimetype ?? message.mimetype ?? "application/octet-stream").split(";")[0] ?? "application/octet-stream"
    const extension = (content ? extensionForMediaMessage(content) : undefined) ?? mimetype.split("/")[1] ?? "bin"
    yield* Effect.logInfo("media downloaded").pipe(Effect.annotateLogs({ chatId, messageId, bytes: bytes.byteLength, mimetype }))
    const result: DownloadedMedia = {
      bytes: new Uint8Array(bytes),
      mimetype,
      extension: extension.replace(/[^a-z0-9]/gi, "").toLowerCase() || "bin",
      kind: message.kind,
    }
    return result
  })
