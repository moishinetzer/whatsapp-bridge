import { Context, Effect, FileSystem, Layer, Path, Schema } from "effect"
import { BridgeConfig } from "../config.ts"
import { type OutboundKind, type OutboundMedia, type OutboundMediaPayload } from "../domain/outbound.ts"

/** The request cannot be turned into a file to send (no mimetype for raw data, an empty file, ...). */
export class OutboundInvalid extends Schema.TaggedError<OutboundInvalid>()(
  "OutboundInvalid",
  { detail: Schema.String },
  { httpApiStatus: 400 },
) {
  override get message() {
    return `cannot send this file: ${this.detail}`
  }
}

/** `path` does not exist or cannot be read by the daemon. */
export class OutboundFileUnreadable extends Schema.TaggedError<OutboundFileUnreadable>()(
  "OutboundFileUnreadable",
  { path: Schema.String, detail: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message() {
    return `cannot read ${this.path}: ${this.detail}`
  }
}

export class OutboundTooLarge extends Schema.TaggedError<OutboundTooLarge>()(
  "OutboundTooLarge",
  { size: Schema.Number, maxBytes: Schema.Number },
  { httpApiStatus: 413 },
) {
  override get message() {
    return `file is ${this.size} bytes, over the ${this.maxBytes} byte limit`
  }
}

export type OutboundError = OutboundInvalid | OutboundFileUnreadable | OutboundTooLarge

export interface OutboundShape {
  /** Reads the file (or decodes the bytes), checks the size, resolves mimetype and kind. */
  readonly load: (payload: OutboundMediaPayload) => Effect.Effect<OutboundMedia, OutboundError>
}

/** Turns an `OutboundMediaPayload` into bytes the transport can send. */
export class Outbound extends Context.Service<Outbound, OutboundShape>()("bridge/Outbound") {}

/** Enough for what an assistant sends; anything else goes out as `application/octet-stream`, a document. */
const MIMETYPES: Readonly<Record<string, string>> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  gif: "image/gif",
  webp: "image/webp",
  mp4: "video/mp4",
  mov: "video/quicktime",
  "3gp": "video/3gpp",
  mp3: "audio/mpeg",
  m4a: "audio/mp4",
  aac: "audio/aac",
  wav: "audio/wav",
  ogg: "audio/ogg; codecs=opus",
  oga: "audio/ogg; codecs=opus",
  opus: "audio/ogg; codecs=opus",
  pdf: "application/pdf",
  txt: "text/plain",
  md: "text/markdown",
  csv: "text/csv",
  json: "application/json",
  zip: "application/zip",
  doc: "application/msword",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xls: "application/vnd.ms-excel",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ppt: "application/vnd.ms-powerpoint",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}

export const mimetypeForExtension = (extension: string): string | undefined => MIMETYPES[extension.toLowerCase()]

/** `image/*` → image, `video/*` → video, `audio/*` → audio, anything else a document. */
export const kindForMimetype = (mimetype: string): OutboundKind => {
  const major = mimetype.split("/")[0]?.toLowerCase()
  return major === "image" || major === "video" || major === "audio" ? major : "document"
}

const extensionForMimetype = (mimetype: string): string => {
  const bare = mimetype.split(";")[0]?.trim().toLowerCase() ?? ""
  const known = Object.entries(MIMETYPES).find(([, m]) => m.split(";")[0] === bare)
  return known?.[0] ?? (bare.split("/")[1] ?? "bin").replace(/[^a-z0-9]/g, "") ?? "bin"
}

export const OutboundLive = Layer.effect(
  Outbound,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path

    const unreadable = (file: string) => (cause: unknown) =>
      new OutboundFileUnreadable({ path: file, detail: cause instanceof Error ? cause.message : String(cause) })

    const fromPath = (file: string) =>
      Effect.gen(function* () {
        const resolved = path.resolve(file)
        const info = yield* fs.stat(resolved).pipe(Effect.mapError(unreadable(resolved)))
        if (info.type !== "File") return yield* new OutboundFileUnreadable({ path: resolved, detail: `not a regular file (${info.type})` })
        const size = Number(info.size)
        if (size > config.mediaMaxBytes) return yield* new OutboundTooLarge({ size, maxBytes: config.mediaMaxBytes })
        const bytes = yield* fs.readFile(resolved).pipe(Effect.mapError(unreadable(resolved)))
        const extension = path.extname(resolved).replace(/^\./, "")
        return { bytes, extension, baseName: path.basename(resolved) }
      })

    const load = Effect.fn("Outbound.load")(function* (payload: OutboundMediaPayload) {
      const source =
        payload.path !== undefined
          ? yield* fromPath(payload.path)
          : { bytes: payload.data ?? new Uint8Array(), extension: undefined, baseName: undefined }
      if (source.bytes.byteLength === 0) return yield* new OutboundInvalid({ detail: "the file is empty" })
      if (source.bytes.byteLength > config.mediaMaxBytes) {
        return yield* new OutboundTooLarge({ size: source.bytes.byteLength, maxBytes: config.mediaMaxBytes })
      }
      const mimetype =
        payload.mimetype ??
        (source.extension === undefined ? undefined : mimetypeForExtension(source.extension)) ??
        (payload.path === undefined ? undefined : "application/octet-stream")
      if (mimetype === undefined) return yield* new OutboundInvalid({ detail: "mimetype is required when sending raw data" })
      const kind = payload.kind ?? kindForMimetype(mimetype)
      const fileName = payload.fileName ?? source.baseName ?? `file.${extensionForMimetype(mimetype)}`
      const media: OutboundMedia = {
        kind,
        bytes: source.bytes,
        mimetype,
        ...(payload.caption === undefined || payload.caption.length === 0 ? {} : { caption: payload.caption }),
        ...(kind === "document" ? { fileName } : {}),
        ...(kind === "audio" && payload.voiceNote === true ? { voiceNote: true } : {}),
      }
      return media
    })

    return Outbound.of({ load })
  }),
)
