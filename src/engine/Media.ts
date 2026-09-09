import { Cache, Context, DateTime, Duration, Effect, Exit, FileSystem, Layer, Path, Schedule, Schema } from "effect"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId } from "../domain/ids.ts"
import { MediaFile, type MessageKind } from "../domain/message.ts"
import { WhatsApp, type MediaTransportError } from "../whatsapp/WhatsApp.ts"

/** The disk under the media directory misbehaved. */
export class MediaStoreError extends Schema.TaggedError<MediaStoreError>()(
  "MediaStoreError",
  { operation: Schema.String, detail: Schema.String },
  { httpApiStatus: 500 },
) {
  override get message() {
    return `media store ${this.operation}: ${this.detail}`
  }
}

export type MediaError = MediaTransportError | MediaStoreError

export interface MediaStoreShape {
  /** Absolute directory the files live in. */
  readonly dir: string
  /**
   * The local file for a message's attachment: downloaded once, then served
   * from disk (also across restarts, via a sidecar `.json` next to the file).
   * Concurrent calls for the same message share one download; failures are
   * not remembered, so a later call retries.
   */
  readonly get: (chatId: ChatId, messageId: MessageId) => Effect.Effect<MediaFile, MediaError>
  /** Which kinds are downloaded automatically before a turn opens. */
  readonly autoKinds: ReadonlySet<MessageKind>
  /** Delete files older than the retention. Runs every `BRIDGE_MEDIA_SWEEP_EVERY` on its own; exposed for tests. */
  readonly sweep: Effect.Effect<number, MediaStoreError>
}

/**
 * Owns `<dataDir>/media`: attachments Intro can open by path. Files are 0600
 * in 0700 directories, named `<chat>/<messageId>.<ext>` with a sidecar
 * `<messageId>.json` holding the `MediaFile` record.
 */
export class MediaStore extends Context.Service<MediaStore, MediaStoreShape>()("bridge/MediaStore") {}

const safe = (segment: string): string => segment.replace(/[^A-Za-z0-9@._-]/g, "_")

const kindsFor = (policy: "images" | "all" | "none"): ReadonlySet<MessageKind> => {
  switch (policy) {
    case "images":
      return new Set<MessageKind>(["image", "sticker"])
    case "all":
      return new Set<MessageKind>(["image", "sticker", "video", "audio", "document"])
    case "none":
      return new Set()
  }
}

const SEPARATOR = "|"

export const MediaStoreLive = Layer.effect(
  MediaStore,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const whatsapp = yield* WhatsApp
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const dir = path.resolve(config.mediaDir)

    const storeError = (operation: string) => (cause: unknown) =>
      new MediaStoreError({ operation, detail: cause instanceof Error ? cause.message : String(cause) })

    const sidecarPath = (chatId: ChatId, messageId: MessageId) => path.join(dir, safe(chatId), `${safe(messageId)}.json`)

    const readSidecar = (chatId: ChatId, messageId: MessageId) =>
      Effect.gen(function* () {
        const sidecar = sidecarPath(chatId, messageId)
        if (!(yield* fs.exists(sidecar))) return undefined
        const raw = yield* fs.readFileString(sidecar)
        const record = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(MediaFile))(raw)
        // The file itself must still be there; a half-deleted pair is treated as missing.
        return (yield* fs.exists(record.path)) ? record : undefined
      }).pipe(
        Effect.catchTag("SchemaError", () => Effect.succeed(undefined)),
        Effect.mapError(storeError("read")),
      )

    const download = (chatId: ChatId, messageId: MessageId) =>
      Effect.gen(function* () {
        const media = yield* whatsapp.downloadMedia(chatId, messageId, { maxBytes: config.mediaMaxBytes })
        const chatDir = path.join(dir, safe(chatId))
        const file = path.join(chatDir, `${safe(messageId)}.${media.extension}`)
        const downloadedAt = yield* DateTime.now
        const record = MediaFile.make({
          path: file,
          mimetype: media.mimetype,
          size: media.bytes.byteLength,
          kind: media.kind,
          downloadedAt,
        })
        yield* Effect.gen(function* () {
          yield* fs.makeDirectory(chatDir, { recursive: true })
          yield* fs.chmod(dir, 0o700)
          yield* fs.chmod(chatDir, 0o700)
          yield* fs.writeFile(file, media.bytes, { mode: 0o600 })
          yield* fs.writeFileString(sidecarPath(chatId, messageId), JSON.stringify(Schema.encodeSync(MediaFile)(record)), { mode: 0o600 })
        }).pipe(Effect.mapError(storeError("write")))
        yield* Effect.logInfo("media stored").pipe(Effect.annotateLogs({ chatId, messageId, path: file, bytes: record.size }))
        return record
      })

    const lookup = (key: string) => {
      const at = key.lastIndexOf(SEPARATOR)
      const chatId = ChatId.make(key.slice(0, at))
      const messageId = MessageId.make(key.slice(at + 1))
      return Effect.gen(function* () {
        const onDisk = yield* readSidecar(chatId, messageId)
        return onDisk ?? (yield* download(chatId, messageId))
      })
    }

    // Successes live forever (the file is on disk); failures are not cached so a retry can succeed.
    const cache = yield* Cache.makeWith(lookup, {
      capacity: 2_000,
      timeToLive: (exit: Exit.Exit<MediaFile, MediaError>) => (Exit.isSuccess(exit) ? Duration.infinity : Duration.zero),
    })

    const get = Effect.fn("MediaStore.get")(function* (chatId: ChatId, messageId: MessageId) {
      return yield* Cache.get(cache, `${chatId}${SEPARATOR}${messageId}`)
    })

    const sweep = Effect.gen(function* () {
      if (!(yield* fs.exists(dir))) return 0
      const now = yield* DateTime.now
      const cutoff = DateTime.toEpochMillis(now) - Duration.toMillis(config.mediaRetention)
      let removed = 0
      const chats = yield* fs.readDirectory(dir)
      for (const chat of chats) {
        const chatDir = path.join(dir, chat)
        const entries = yield* fs.readDirectory(chatDir).pipe(Effect.orElseSucceed(() => [] as Array<string>))
        for (const entry of entries.filter((e) => e.endsWith(".json"))) {
          const sidecar = path.join(chatDir, entry)
          const record = yield* fs.readFileString(sidecar).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(MediaFile))),
            Effect.option,
          )
          if (record._tag === "None") continue
          if (DateTime.toEpochMillis(record.value.downloadedAt) >= cutoff) continue
          yield* fs.remove(record.value.path, { force: true })
          yield* fs.remove(sidecar, { force: true })
          yield* Cache.invalidate(cache, `${ChatId.make(chat)}${SEPARATOR}${MessageId.make(entry.slice(0, -".json".length))}`)
          removed += 1
        }
      }
      if (removed > 0) yield* Effect.logInfo("media sweep").pipe(Effect.annotateLogs({ removed, retentionMs: Duration.toMillis(config.mediaRetention) }))
      return removed
    }).pipe(Effect.mapError(storeError("sweep")))

    yield* sweep.pipe(
      Effect.catch((e) => Effect.logWarning("media sweep failed").pipe(Effect.annotateLogs({ reason: e.detail }))),
      Effect.repeat(Schedule.spaced(config.mediaSweepEvery)),
      Effect.forkScoped,
    )

    return MediaStore.of({ dir, get, autoKinds: kindsFor(config.mediaAuto), sweep })
  }),
)
