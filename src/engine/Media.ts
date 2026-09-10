import { Cache, Context, DateTime, Duration, Effect, Exit, FileSystem, Layer, Option, Path, Schedule, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId } from "../domain/ids.ts"
import { MediaFile, type MessageKind } from "../domain/message.ts"
import { Store } from "../store/Store.ts"
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
   * from disk (also across restarts, through the index in the database).
   * Concurrent calls for the same message share one download; failures are
   * not remembered, so a later call retries. Every call counts as a use.
   */
  readonly get: (chatId: ChatId, messageId: MessageId) => Effect.Effect<MediaFile, MediaError>
  /** Mark a cached file as used now, so the budget keeps it longer. Unknown files are ignored. */
  readonly touch: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void>
  /** Which kinds are downloaded automatically before a turn opens. */
  readonly autoKinds: ReadonlySet<MessageKind>
  /** Evict least recently used files past `BRIDGE_MEDIA_MAX_DISK` (and past `BRIDGE_MEDIA_RETENTION` when set). Returns how many. */
  readonly sweep: Effect.Effect<number, MediaStoreError>
  readonly usage: Effect.Effect<{ readonly bytes: number; readonly files: number }, MediaStoreError>
}

/**
 * Owns `<dataDir>/media`: attachments Intro can open by path. Files are 0600
 * in 0700 directories, named `<chat>/<messageId>.<ext>` with a sidecar
 * `<messageId>.json` holding the `MediaFile` record; the `media_cache` table
 * is the index and the LRU clock.
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

interface CacheRow {
  readonly chat_id: string
  readonly message_id: string
  readonly path: string
  readonly size: number
  readonly mimetype: string
  readonly kind: string
  readonly downloaded_at: number
  readonly last_used_at: number
}

export const MediaStoreLive = Layer.effect(
  MediaStore,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const whatsapp = yield* WhatsApp
    const store = yield* Store
    const sql = yield* SqlClient.SqlClient
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const dir = path.resolve(config.mediaDir)

    const storeError = (operation: string) => (cause: unknown) =>
      new MediaStoreError({ operation, detail: cause instanceof Error ? cause.message : String(cause) })

    const sidecarPath = (chatId: ChatId, messageId: MessageId) => path.join(dir, safe(chatId), `${safe(messageId)}.json`)
    const now = Effect.map(DateTime.now, DateTime.toEpochMillis)

    const toFile = (row: CacheRow): MediaFile => ({
      path: row.path,
      mimetype: row.mimetype,
      size: row.size,
      kind: row.kind as MessageKind,
      downloadedAt: DateTime.makeUnsafe(row.downloaded_at),
    })

    const index = (chatId: ChatId, messageId: MessageId, file: MediaFile) =>
      Effect.gen(function* () {
        const used = yield* now
        yield* sql`INSERT INTO media_cache (chat_id, message_id, path, size, mimetype, kind, downloaded_at, last_used_at)
          VALUES (${chatId}, ${messageId}, ${file.path}, ${file.size}, ${file.mimetype}, ${file.kind}, ${DateTime.toEpochMillis(file.downloadedAt)}, ${used})
          ON CONFLICT (chat_id, message_id) DO UPDATE SET path = excluded.path, size = excluded.size, last_used_at = excluded.last_used_at`
      }).pipe(Effect.mapError(storeError("index")))

    const touch = (chatId: ChatId, messageId: MessageId) =>
      Effect.flatMap(now, (used) => sql`UPDATE media_cache SET last_used_at = ${used} WHERE chat_id = ${chatId} AND message_id = ${messageId}`).pipe(
        Effect.catch((e) => Effect.logDebug("media touch failed").pipe(Effect.annotateLogs({ reason: e.message }))),
        Effect.asVoid,
      )

    /** The indexed file, when its bytes are still on disk. A row without a file is dropped. */
    const indexed = (chatId: ChatId, messageId: MessageId) =>
      Effect.gen(function* () {
        const rows = yield* sql<CacheRow>`SELECT * FROM media_cache WHERE chat_id = ${chatId} AND message_id = ${messageId}`
        const row = rows[0]
        if (row === undefined) return undefined
        if (yield* fs.exists(row.path)) return toFile(row)
        yield* sql`DELETE FROM media_cache WHERE chat_id = ${chatId} AND message_id = ${messageId}`
        return undefined
      }).pipe(Effect.mapError(storeError("lookup")))

    /** Files from before the index existed: their sidecar is the record. Imported on first sight. */
    const fromSidecar = (chatId: ChatId, messageId: MessageId) =>
      Effect.gen(function* () {
        const sidecar = sidecarPath(chatId, messageId)
        if (!(yield* fs.exists(sidecar))) return undefined
        const raw = yield* fs.readFileString(sidecar)
        const record = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(MediaFile))(raw)
        if (!(yield* fs.exists(record.path))) return undefined
        yield* index(chatId, messageId, record)
        return record
      }).pipe(
        Effect.catch((e) =>
          Effect.logWarning("media sidecar unreadable; will download again").pipe(
            Effect.annotateLogs({ chatId, messageId, reason: e instanceof Error ? e.message : String(e) }),
            Effect.as(undefined),
          ),
        ),
      )

    const download = (chatId: ChatId, messageId: MessageId) =>
      Effect.gen(function* () {
        const media = yield* whatsapp.downloadMedia(chatId, messageId, { maxBytes: config.mediaMaxBytes })
        const chatDir = path.join(dir, safe(chatId))
        yield* fs.makeDirectory(chatDir, { recursive: true, mode: 0o700 }).pipe(Effect.mapError(storeError("mkdir")))
        const file = path.join(chatDir, `${safe(messageId)}.${media.extension}`)
        yield* fs.writeFile(file, media.bytes, { mode: 0o600 }).pipe(Effect.mapError(storeError("write")))
        const record: MediaFile = {
          path: file,
          mimetype: media.mimetype,
          size: media.bytes.byteLength,
          kind: media.kind,
          downloadedAt: yield* DateTime.now,
        }
        yield* fs
          .writeFileString(sidecarPath(chatId, messageId), Schema.encodeSync(Schema.fromJsonString(MediaFile))(record), { mode: 0o600 })
          .pipe(Effect.mapError(storeError("sidecar")))
        yield* index(chatId, messageId, record)
        yield* store.attachMedia(chatId, messageId, file).pipe(Effect.ignore)
        yield* Effect.logInfo("media stored").pipe(Effect.annotateLogs({ chatId, messageId, path: file, bytes: record.size }))
        return record
      })

    const lookup = (key: string) =>
      Effect.gen(function* () {
        const [chat, id] = key.split(SEPARATOR)
        const chatId = ChatId.make(chat ?? "")
        const messageId = MessageId.make(id ?? "")
        const known = (yield* indexed(chatId, messageId)) ?? (yield* fromSidecar(chatId, messageId))
        if (known !== undefined) {
          yield* touch(chatId, messageId)
          return known
        }
        return yield* download(chatId, messageId)
      })

    // Successes stay for the run (a use touches the index); failures leave at once so the next call retries.
    const cache = yield* Cache.makeWith(lookup, {
      capacity: 10_000,
      timeToLive: (exit: Exit.Exit<MediaFile, MediaError>) => (Exit.isSuccess(exit) ? Duration.infinity : Duration.zero),
    })

    const get = Effect.fn("MediaStore.get")(function* (chatId: ChatId, messageId: MessageId) {
      const file = yield* Cache.get(cache, `${chatId}${SEPARATOR}${messageId}`)
      yield* touch(chatId, messageId)
      return file
    })

    const usage = Effect.map(
      sql<{ bytes: number | null; files: number }>`SELECT SUM(size) AS bytes, COUNT(*) AS files FROM media_cache`,
      (rows) => ({ bytes: rows[0]?.bytes ?? 0, files: rows[0]?.files ?? 0 }),
    ).pipe(Effect.mapError(storeError("usage")))

    const evict = (row: CacheRow) =>
      Effect.gen(function* () {
        yield* fs.remove(row.path, { force: true })
        yield* fs.remove(sidecarPath(ChatId.make(row.chat_id), MessageId.make(row.message_id)), { force: true })
        yield* sql`DELETE FROM media_cache WHERE chat_id = ${row.chat_id} AND message_id = ${row.message_id}`
        yield* Cache.invalidate(cache, `${row.chat_id}${SEPARATOR}${row.message_id}`)
        yield* store.markMediaEvicted(row.path).pipe(Effect.ignore)
      })

    const sweep = Effect.gen(function* () {
      let removed = 0
      const current = yield* now
      if (Option.isSome(config.mediaRetention)) {
        const cutoff = current - Duration.toMillis(config.mediaRetention.value)
        const stale = yield* sql<CacheRow>`SELECT * FROM media_cache WHERE downloaded_at < ${cutoff}`
        for (const row of stale) {
          yield* evict(row)
          removed += 1
        }
      }
      let { bytes } = yield* usage
      if (bytes > config.mediaMaxDisk) {
        const lru = yield* sql<CacheRow>`SELECT * FROM media_cache ORDER BY last_used_at ASC, downloaded_at ASC`
        for (const row of lru) {
          if (bytes <= config.mediaMaxDisk) break
          yield* evict(row)
          bytes -= row.size
          removed += 1
        }
      }
      if (removed > 0) {
        yield* Effect.logInfo("media sweep").pipe(Effect.annotateLogs({ removed, bytesNow: bytes, budget: config.mediaMaxDisk }))
      }
      return removed
    }).pipe(Effect.mapError(storeError("sweep")))

    yield* sweep.pipe(
      Effect.catch((e) => Effect.logWarning("media sweep failed").pipe(Effect.annotateLogs({ reason: e.detail }))),
      Effect.repeat(Schedule.spaced(config.mediaSweepEvery)),
      Effect.forkScoped,
    )

    return MediaStore.of({ dir, get, touch, autoKinds: kindsFor(config.mediaAuto), sweep, usage })
  }),
)
