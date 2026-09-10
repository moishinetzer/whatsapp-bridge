import { Schema } from "effect"
import { MessageId } from "./ids.ts"

/** How WhatsApp presents an outgoing file. `document` sends anything as a file, full quality, no preview. */
export const OutboundKind = Schema.Literals(["image", "video", "audio", "document"])
export type OutboundKind = typeof OutboundKind.Type

/**
 * What Intro hands the bridge to send a file: a local `path` on the bridge
 * machine, or base64 `data` with its `mimetype`. Exactly one of the two.
 * Everything else is optional and inferred: `mimetype` from the extension,
 * `kind` from the mimetype, `fileName` from the path.
 */
/** Exactly one of `path` and `data`. Shared with the schemas that extend the payload. */
export const exactlyOneSource = Schema.makeFilter((o: { readonly path?: string | undefined; readonly data?: Uint8Array | undefined }) =>
  (o.path === undefined) === (o.data === undefined) ? { path: [], issue: "give exactly one of path or data" } : true,
)

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
  /** Send as a reply to this message (a message the bridge saw or sent). */
  quoteMessageId: Schema.optionalKey(MessageId),
}).check(exactlyOneSource)
export type OutboundMediaPayload = typeof OutboundMediaPayload.Type

/** A file ready to send: bytes in memory, kind and mimetype resolved. */
export interface OutboundMedia {
  readonly kind: OutboundKind
  readonly bytes: Uint8Array
  readonly mimetype: string
  readonly caption?: string
  readonly fileName?: string
  readonly voiceNote?: boolean
  readonly quoteMessageId?: MessageId
}

/** A text bubble, optionally as a reply to a message. */
export interface OutgoingText {
  readonly text: string
  readonly quoteMessageId?: MessageId
}

/** One item of a reply: a text bubble or a file. */
export type Outgoing = OutgoingText | OutboundMedia

export const isMedia = (item: Outgoing): item is OutboundMedia => "bytes" in item

/** A plain text bubble. */
export const text = (body: string, quoteMessageId?: MessageId): OutgoingText =>
  quoteMessageId === undefined ? { text: body } : { text: body, quoteMessageId }

/** Accepts the loose shapes callers use (plain strings included) and returns proper items. */
export const outgoing = (items: ReadonlyArray<string | Outgoing>): ReadonlyArray<Outgoing> =>
  items.map((item) => (typeof item === "string" ? { text: item } : item))

/**
 * One entry of `replies` on the wire: a string, or an object when the bubble
 * should quote a message.
 */
export const ReplyItem = Schema.Union([
  Schema.String,
  Schema.Struct({ text: Schema.String, quoteMessageId: Schema.optionalKey(MessageId) }),
])
export type ReplyItem = typeof ReplyItem.Type
