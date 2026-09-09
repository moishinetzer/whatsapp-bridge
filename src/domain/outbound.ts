import { Schema } from "effect"

/** How WhatsApp presents an outgoing file. `document` sends anything as a file, full quality, no preview. */
export const OutboundKind = Schema.Literals(["image", "video", "audio", "document"])
export type OutboundKind = typeof OutboundKind.Type

/**
 * What Intro hands the bridge to send a file: a local `path` on the bridge
 * machine, or base64 `data` with its `mimetype`. Exactly one of the two.
 * Everything else is optional and inferred: `mimetype` from the extension,
 * `kind` from the mimetype, `fileName` from the path.
 */
export const OutboundMediaPayload = Schema.Struct({
  /** Absolute path of a file readable by the user the daemon runs as. */
  path: Schema.optionalKey(Schema.String.pipe(Schema.check(Schema.isNonEmpty()))),
  /** The file's bytes, base64 (standard alphabet, with padding). Requires `mimetype`. */
  data: Schema.optionalKey(Schema.Uint8ArrayFromBase64),
  mimetype: Schema.optionalKey(Schema.String.pipe(Schema.check(Schema.isNonEmpty()))),
  kind: Schema.optionalKey(OutboundKind),
  /** Text under an image, video or document. Ignored for audio. */
  caption: Schema.optionalKey(Schema.String),
  /** Shown by WhatsApp for documents. Defaults to the path's basename. */
  fileName: Schema.optionalKey(Schema.String),
  /** Audio only: send as a voice note (needs `audio/ogg; codecs=opus`) instead of an audio file. */
  voiceNote: Schema.optionalKey(Schema.Boolean),
}).check(
  Schema.makeFilter((o) =>
    (o.path === undefined) === (o.data === undefined) ? { path: [], issue: "give exactly one of path or data" } : true,
  ),
)
export type OutboundMediaPayload = typeof OutboundMediaPayload.Type

/** A file ready to send: bytes in memory, kind and mimetype resolved. */
export interface OutboundMedia {
  readonly kind: OutboundKind
  readonly bytes: Uint8Array
  readonly mimetype: string
  readonly caption?: string
  readonly fileName?: string
  readonly voiceNote?: boolean
}

/** One item of a reply: a text bubble or a file. */
export type Outgoing = string | OutboundMedia

export const isMedia = (item: Outgoing): item is OutboundMedia => typeof item !== "string"
