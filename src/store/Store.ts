import { Context, DateTime, Effect, Layer, Option, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import type { ChatSummary, DeliveryStatus, SearchHit, StoredMessage } from "../domain/history.ts"
import type { InboundMessage, MessageKind } from "../domain/message.ts"
import type { TurnSummary } from "../engine/Turns.ts"
import { DatabaseLive, MigrationsLive } from "./Database.ts"

/** The database misbehaved. */
export class StoreError extends Schema.TaggedError<StoreError>()(
  "StoreError",
  { operation: Schema.String, detail: Schema.String },
  { httpApiStatus: 500 },
) {
  override get message() {
    return `store ${this.operation}: ${this.detail}`
  }
}

/** History is off; reads have nothing to read. */
export class HistoryDisabled extends Schema.TaggedError<HistoryDisabled>()("HistoryDisabled", {}, { httpApiStatus: 409 }) {
  override get message() {
    return "history is off: set BRIDGE_HISTORY=true and run `bridge history enable`"
  }
}

export interface SentEntry {
  readonly chatId: ChatId
  readonly sentAt: DateTime.Utc
  /** The message as WhatsApp's protobuf bytes; decode with `proto.WebMessageInfo.decode`. */
  readonly raw: Uint8Array
  readonly turnId: Option.Option<TurnId>
}

export interface OutboundRecord {
  readonly chatId: ChatId
  readonly messageId: MessageId
  readonly kind: MessageKind
  readonly text?: string | undefined
  readonly mimetype?: string | undefined
  readonly mediaPath?: string | undefined
  readonly quotedMessageId?: MessageId | undefined
  readonly turnId?: TurnId | undefined
  readonly at: DateTime.Utc
  readonly raw: Uint8Array
}

export interface PageOptions {
  /** Only messages older than this one. */
  readonly before?: MessageId | undefined
  /** Only messages newer than this instant. */
  readonly since?: DateTime.Utc | undefined
  readonly limit: number
}

export interface StoreShape {
  readonly historyEnabled: boolean
  readonly databasePath: string

  // Always on: what the bridge sent, so edits, deletes, quotes and resends survive a restart.
  readonly rememberSent: (messageId: MessageId, entry: SentEntry) => Effect.Effect<void, StoreError>
  readonly sent: (messageId: MessageId) => Effect.Effect<Option.Option<SentEntry>, StoreError>
  readonly forgetSent: (messageId: MessageId) => Effect.Effect<void, StoreError>
  /** Upsert a turn's summary; the outcome of every turn is kept. */
  readonly recordTurn: (summary: TurnSummary) => Effect.Effect<void, StoreError>

  // History: writes are no-ops when disabled, reads fail with `HistoryDisabled`.
  readonly recordInbound: (message: InboundMessage, raw: Uint8Array) => Effect.Effect<void, StoreError>
  readonly recordOutbound: (record: OutboundRecord) => Effect.Effect<void, StoreError>
  readonly markEdited: (chatId: ChatId, messageId: MessageId, text: string) => Effect.Effect<void, StoreError>
  /** Tombstone: the text, media path and raw bytes go; id and timestamps stay. */
  readonly markDeleted: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void, StoreError>
  /** An empty emoji removes the sender's reaction. */
  readonly recordReaction: (chatId: ChatId, messageId: MessageId, senderId: string, emoji: string) => Effect.Effect<void, StoreError>
  readonly updateStatus: (chatId: ChatId, messageId: MessageId, status: DeliveryStatus) => Effect.Effect<void, StoreError>
  readonly attachMedia: (chatId: ChatId, messageId: MessageId, path: string) => Effect.Effect<void, StoreError>
  readonly markMediaEvicted: (path: string) => Effect.Effect<void, StoreError>
  readonly setTurn: (chatId: ChatId, messageIds: ReadonlyArray<MessageId>, turnId: TurnId) => Effect.Effect<void, StoreError>

  readonly chats: Effect.Effect<ReadonlyArray<ChatSummary>, StoreError | HistoryDisabled>
  readonly messages: (chatId: ChatId, options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, StoreError | HistoryDisabled>
  readonly search: (
    query: string,
    options: { readonly chatId?: ChatId | undefined; readonly before?: MessageId | undefined; readonly limit: number },
  ) => Effect.Effect<ReadonlyArray<SearchHit>, StoreError | HistoryDisabled>
  /** The raw protobuf of an inbound message, when history kept it. */
  readonly raw: (chatId: ChatId, messageId: MessageId) => Effect.Effect<Option.Option<Uint8Array>, StoreError>
  /** Delete history rows older than the instant; returns how many. */
  readonly purgeBefore: (before: DateTime.Utc) => Effect.Effect<number, StoreError | HistoryDisabled>
}

export class Store extends Context.Service<Store, StoreShape>()("bridge/Store") {}

interface MessageRow {
  readonly chat_id: string
  readonly message_id: string
  readonly direction: string
  readonly sender_id: string
  readonly sender_name: string | null
  readonly is_group: number
  readonly timestamp: number
  readonly kind: string
  readonly text: string | null
  readonly edited_text: string | null
  readonly mimetype: string | null
  readonly media_path: string | null
  readonly media_evicted_at: number | null
  readonly quoted_message_id: string | null
  readonly status: string | null
  readonly deleted_at: number | null
  readonly turn_id: string | null
}

interface ReactionRow {
  readonly chat_id: string
  readonly message_id: string
  readonly sender_id: string
  readonly emoji: string
  readonly at: number
}

/**
 * FTS5 has its own query language and throws on unbalanced quotes. The agent's
 * words become quoted prefix terms (implicit AND); a query wrapped in quotes
 * becomes one phrase.
 */
export const ftsQuery = (q: string): string => {
  const trimmed = q.trim()
  if (trimmed.length === 0) return '""'
  const escape = (s: string) => s.replace(/"/g, '""')
  if (trimmed.length > 2 && trimmed.startsWith('"') && trimmed.endsWith('"')) return `"${escape(trimmed.slice(1, -1))}"`
  return trimmed
    .split(/\s+/)
    .map((term) => `"${escape(term)}"*`)
    .join(" ")
}

const millis = (d: DateTime.Utc): number => DateTime.toEpochMillis(d)
const at = (n: number): DateTime.Utc => DateTime.makeUnsafe(n)
const maybe = <T>(v: T | null | undefined): T | undefined => (v === null || v === undefined ? undefined : v)

export const StoreLive = Layer.effect(
  Store,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const sql = yield* SqlClient.SqlClient
    const enabled = config.history

    const storeError = (operation: string) => (cause: unknown) =>
      new StoreError({ operation, detail: cause instanceof Error ? cause.message : String(cause) })

    const run = <A, E>(operation: string, effect: Effect.Effect<A, E>): Effect.Effect<A, StoreError> =>
      effect.pipe(Effect.mapError(storeError(operation)))

    const whenEnabled = (operation: string, effect: Effect.Effect<unknown, unknown>): Effect.Effect<void, StoreError> =>
      enabled ? run(operation, effect).pipe(Effect.asVoid) : Effect.void

    const requireEnabled = <A>(operation: string, effect: Effect.Effect<A, unknown>): Effect.Effect<A, StoreError | HistoryDisabled> =>
      enabled ? run(operation, effect) : Effect.fail(new HistoryDisabled())

    // ---- always on -------------------------------------------------------

    const rememberSent = (messageId: MessageId, entry: SentEntry) =>
      run(
        "rememberSent",
        sql`INSERT INTO sent_messages (message_id, chat_id, sent_at, raw, turn_id)
            VALUES (${messageId}, ${entry.chatId}, ${millis(entry.sentAt)}, ${entry.raw}, ${Option.getOrNull(entry.turnId)})
            ON CONFLICT (message_id) DO UPDATE SET raw = excluded.raw, turn_id = COALESCE(excluded.turn_id, sent_messages.turn_id)`,
      ).pipe(Effect.asVoid)

    const sent = (messageId: MessageId) =>
      run(
        "sent",
        sql<{ chat_id: string; sent_at: number; raw: Uint8Array; turn_id: string | null }>`
          SELECT chat_id, sent_at, raw, turn_id FROM sent_messages WHERE message_id = ${messageId}`,
      ).pipe(
        Effect.map((rows) => {
          const row = rows[0]
          if (row === undefined) return Option.none<SentEntry>()
          return Option.some<SentEntry>({
            chatId: ChatId.make(row.chat_id),
            sentAt: at(row.sent_at),
            raw: row.raw,
            turnId: Option.fromNullishOr(row.turn_id).pipe(Option.map(TurnId.make)),
          })
        }),
      )

    const forgetSent = (messageId: MessageId) =>
      run("forgetSent", sql`DELETE FROM sent_messages WHERE message_id = ${messageId}`).pipe(Effect.asVoid)

    const recordTurn = (summary: TurnSummary) =>
      run(
        "recordTurn",
        sql`INSERT INTO turns (turn_id, chat_id, state, opened_at, closed_at, summary)
            VALUES (${summary.turnId}, ${summary.chatId}, ${summary.state}, ${millis(summary.openedAt)},
                    ${summary.state === "batching" || summary.state === "responding" || summary.state === "awaiting" ? null : Date.now()},
                    ${JSON.stringify({ ...summary, messages: summary.messages.map((m) => m.messageId) })})
            ON CONFLICT (turn_id) DO UPDATE SET state = excluded.state, closed_at = excluded.closed_at, summary = excluded.summary`,
      ).pipe(Effect.asVoid)

    // ---- history ---------------------------------------------------------

    const indexText = (chatId: ChatId, messageId: MessageId, text: string | undefined) =>
      text === undefined || text.trim().length === 0
        ? Effect.void
        : sql`INSERT INTO messages_fts (text, chat_id, message_id) VALUES (${text}, ${chatId}, ${messageId})`

    const unindex = (chatId: ChatId, messageId: MessageId) =>
      sql`DELETE FROM messages_fts WHERE chat_id = ${chatId} AND message_id = ${messageId}`

    const recordInbound = (m: InboundMessage, raw: Uint8Array) =>
      whenEnabled(
        "recordInbound",
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO messages (chat_id, message_id, direction, sender_id, sender_name, is_group, timestamp, kind, text, mimetype, media_path, quoted_message_id, raw)
              VALUES (${m.chatId}, ${m.messageId}, 'in', ${m.senderId}, ${m.senderName ?? null}, ${m.isGroup ? 1 : 0}, ${millis(m.timestamp)}, ${m.kind},
                      ${m.text ?? null}, ${m.mimetype ?? null}, ${m.media?.path ?? null}, ${m.quotedMessageId ?? null}, ${raw})
              ON CONFLICT (chat_id, message_id) DO NOTHING`
            yield* indexText(m.chatId, m.messageId, m.text)
          }),
        ),
      )

    const recordOutbound = (r: OutboundRecord) =>
      whenEnabled(
        "recordOutbound",
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO messages (chat_id, message_id, direction, sender_id, is_group, timestamp, kind, text, mimetype, media_path, quoted_message_id, status, turn_id, raw)
              VALUES (${r.chatId}, ${r.messageId}, 'out', 'me', ${r.chatId.endsWith("@g.us") ? 1 : 0}, ${millis(r.at)}, ${r.kind},
                      ${r.text ?? null}, ${r.mimetype ?? null}, ${r.mediaPath ?? null}, ${r.quotedMessageId ?? null}, 'sent', ${r.turnId ?? null}, ${r.raw})
              ON CONFLICT (chat_id, message_id) DO NOTHING`
            yield* indexText(r.chatId, r.messageId, r.text)
          }),
        ),
      )

    const markEdited = (chatId: ChatId, messageId: MessageId, text: string) =>
      whenEnabled(
        "markEdited",
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE messages SET edited_text = ${text} WHERE chat_id = ${chatId} AND message_id = ${messageId}`
            yield* unindex(chatId, messageId)
            yield* indexText(chatId, messageId, text)
          }),
        ),
      )

    const markDeleted = (chatId: ChatId, messageId: MessageId) =>
      whenEnabled(
        "markDeleted",
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE messages SET text = NULL, edited_text = NULL, media_path = NULL, raw = NULL, deleted_at = ${Date.now()}
                       WHERE chat_id = ${chatId} AND message_id = ${messageId}`
            yield* unindex(chatId, messageId)
          }),
        ),
      )

    const recordReaction = (chatId: ChatId, messageId: MessageId, senderId: string, emoji: string) =>
      whenEnabled(
        "recordReaction",
        emoji === ""
          ? sql`DELETE FROM reactions WHERE chat_id = ${chatId} AND message_id = ${messageId} AND sender_id = ${senderId}`
          : sql`INSERT INTO reactions (chat_id, message_id, sender_id, emoji, at) VALUES (${chatId}, ${messageId}, ${senderId}, ${emoji}, ${Date.now()})
                ON CONFLICT (chat_id, message_id, sender_id) DO UPDATE SET emoji = excluded.emoji, at = excluded.at`,
      )

    const rank: Record<DeliveryStatus, number> = { sent: 0, delivered: 1, read: 2 }
    const updateStatus = (chatId: ChatId, messageId: MessageId, status: DeliveryStatus) =>
      whenEnabled(
        "updateStatus",
        sql`UPDATE messages SET status = ${status} WHERE chat_id = ${chatId} AND message_id = ${messageId}
            AND (status IS NULL OR CASE status WHEN 'sent' THEN 0 WHEN 'delivered' THEN 1 ELSE 2 END < ${rank[status]})`,
      )

    const attachMedia = (chatId: ChatId, messageId: MessageId, path: string) =>
      whenEnabled(
        "attachMedia",
        sql`UPDATE messages SET media_path = ${path}, media_evicted_at = NULL WHERE chat_id = ${chatId} AND message_id = ${messageId}`,
      )

    const markMediaEvicted = (path: string) =>
      whenEnabled("markMediaEvicted", sql`UPDATE messages SET media_evicted_at = ${Date.now()} WHERE media_path = ${path}`)

    const setTurn = (chatId: ChatId, messageIds: ReadonlyArray<MessageId>, turnId: TurnId) =>
      messageIds.length === 0
        ? Effect.void
        : whenEnabled(
            "setTurn",
            sql`UPDATE messages SET turn_id = ${turnId} WHERE chat_id = ${chatId} AND ${sql.in("message_id", messageIds)}`,
          )

    const reactionsFor = (rows: ReadonlyArray<MessageRow>) =>
      rows.length === 0
        ? Effect.succeed(new Map<string, Array<{ senderId: string; emoji: string; at: DateTime.Utc }>>())
        : Effect.map(
            sql<ReactionRow>`SELECT chat_id, message_id, sender_id, emoji, at FROM reactions
              WHERE ${sql.in("message_id", rows.map((r) => r.message_id))} ORDER BY at`,
            (reactions) => {
              const byMessage = new Map<string, Array<{ senderId: string; emoji: string; at: DateTime.Utc }>>()
              for (const r of reactions) {
                const key = `${r.chat_id}|${r.message_id}`
                const list = byMessage.get(key) ?? []
                list.push({ senderId: r.sender_id, emoji: r.emoji, at: at(r.at) })
                byMessage.set(key, list)
              }
              return byMessage
            },
          )

    const toStored = (row: MessageRow, reactions: ReadonlyArray<{ senderId: string; emoji: string; at: DateTime.Utc }>): StoredMessage => ({
      chatId: ChatId.make(row.chat_id),
      messageId: MessageId.make(row.message_id),
      direction: row.direction === "out" ? "out" : "in",
      senderId: row.sender_id,
      ...(row.sender_name === null ? {} : { senderName: row.sender_name }),
      isGroup: row.is_group === 1,
      timestamp: at(row.timestamp),
      kind: row.kind as MessageKind,
      ...(row.text === null ? {} : { text: row.text }),
      ...(row.edited_text === null ? {} : { editedText: row.edited_text }),
      ...(row.mimetype === null ? {} : { mimetype: row.mimetype }),
      ...(row.media_path === null ? {} : { mediaPath: row.media_path }),
      ...(row.media_evicted_at === null ? {} : { mediaEvictedAt: at(row.media_evicted_at) }),
      ...(row.quoted_message_id === null ? {} : { quotedMessageId: MessageId.make(row.quoted_message_id) }),
      ...(row.status === null ? {} : { status: row.status as DeliveryStatus }),
      ...(row.deleted_at === null ? {} : { deletedAt: at(row.deleted_at) }),
      ...(row.turn_id === null ? {} : { turnId: TurnId.make(row.turn_id) }),
      reactions,
    })

    const hydrate = (rows: ReadonlyArray<MessageRow>) =>
      Effect.map(reactionsFor(rows), (byMessage) => rows.map((row) => toStored(row, byMessage.get(`${row.chat_id}|${row.message_id}`) ?? [])))

    const columns = sql.literal(
      "chat_id, message_id, direction, sender_id, sender_name, is_group, timestamp, kind, text, edited_text, mimetype, media_path, media_evicted_at, quoted_message_id, status, deleted_at, turn_id",
    )

    /** The timestamp of a message used as a cursor, or `undefined` when it is unknown. */
    const timestampOf = (chatId: ChatId | undefined, messageId: MessageId) =>
      Effect.map(
        chatId === undefined
          ? sql<{ timestamp: number }>`SELECT timestamp FROM messages WHERE message_id = ${messageId} LIMIT 1`
          : sql<{ timestamp: number }>`SELECT timestamp FROM messages WHERE chat_id = ${chatId} AND message_id = ${messageId}`,
        (rows) => maybe(rows[0]?.timestamp),
      )

    const messages = (chatId: ChatId, options: PageOptions) =>
      requireEnabled(
        "messages",
        Effect.gen(function* () {
          const limit = Math.max(1, Math.min(options.limit, 500))
          const before = options.before === undefined ? undefined : yield* timestampOf(chatId, options.before)
          const since = options.since === undefined ? undefined : millis(options.since)
          const rows = yield* sql<MessageRow>`SELECT ${columns} FROM messages WHERE chat_id = ${chatId}
            ${before === undefined ? sql.literal("") : sql`AND timestamp < ${before}`}
            ${since === undefined ? sql.literal("") : sql`AND timestamp > ${since}`}
            ORDER BY timestamp DESC, message_id DESC LIMIT ${limit}`
          return yield* hydrate(rows)
        }),
      )

    const search = (query: string, options: { readonly chatId?: ChatId | undefined; readonly before?: MessageId | undefined; readonly limit: number }) =>
      requireEnabled(
        "search",
        Effect.gen(function* () {
          const limit = Math.max(1, Math.min(options.limit, 500))
          const before = options.before === undefined ? undefined : yield* timestampOf(options.chatId, options.before)
          const rows = yield* sql<MessageRow & { snippet: string }>`
            SELECT m.chat_id, m.message_id, m.direction, m.sender_id, m.sender_name, m.is_group, m.timestamp, m.kind, m.text, m.edited_text,
                   m.mimetype, m.media_path, m.media_evicted_at, m.quoted_message_id, m.status, m.deleted_at, m.turn_id,
                   snippet(messages_fts, 0, '[', ']', '…', 12) AS snippet
            FROM messages_fts
            JOIN messages m ON m.chat_id = messages_fts.chat_id AND m.message_id = messages_fts.message_id
            WHERE messages_fts MATCH ${ftsQuery(query)}
            ${options.chatId === undefined ? sql.literal("") : sql`AND m.chat_id = ${options.chatId}`}
            ${before === undefined ? sql.literal("") : sql`AND m.timestamp < ${before}`}
            ORDER BY m.timestamp DESC LIMIT ${limit}`
          const stored = yield* hydrate(rows)
          return stored.map((s, i): SearchHit => ({ ...s, snippet: rows[i]?.snippet ?? "" }))
        }),
      )

    const chats = requireEnabled(
      "chats",
      Effect.map(
        sql<{ chat_id: string; is_group: number; last_message_at: number; message_count: number; name: string | null }>`
          SELECT m.chat_id, MAX(m.is_group) AS is_group, MAX(m.timestamp) AS last_message_at, COUNT(*) AS message_count,
                 (SELECT sender_name FROM messages n WHERE n.chat_id = m.chat_id AND n.direction = 'in' AND n.sender_name IS NOT NULL
                  ORDER BY n.timestamp DESC LIMIT 1) AS name
          FROM messages m GROUP BY m.chat_id ORDER BY last_message_at DESC`,
        (rows) =>
          rows.map(
            (row): ChatSummary => ({
              chatId: ChatId.make(row.chat_id),
              ...(row.name === null ? {} : { name: row.name }),
              isGroup: row.is_group === 1,
              lastMessageAt: at(row.last_message_at),
              messageCount: row.message_count,
            }),
          ),
      ),
    )

    const raw = (chatId: ChatId, messageId: MessageId) =>
      enabled
        ? run(
            "raw",
            Effect.map(
              sql<{ raw: Uint8Array | null }>`SELECT raw FROM messages WHERE chat_id = ${chatId} AND message_id = ${messageId}`,
              (rows) => Option.fromNullishOr(rows[0]?.raw),
            ),
          )
        : Effect.succeed(Option.none<Uint8Array>())

    const purgeBefore = (before: DateTime.Utc) =>
      requireEnabled(
        "purgeBefore",
        sql.withTransaction(
          Effect.gen(function* () {
            const doomed = yield* sql<{ chat_id: string; message_id: string }>`SELECT chat_id, message_id FROM messages WHERE timestamp < ${millis(before)}`
            for (const d of doomed) {
              yield* sql`DELETE FROM messages_fts WHERE chat_id = ${d.chat_id} AND message_id = ${d.message_id}`
              yield* sql`DELETE FROM reactions WHERE chat_id = ${d.chat_id} AND message_id = ${d.message_id}`
            }
            yield* sql`DELETE FROM messages WHERE timestamp < ${millis(before)}`
            return doomed.length
          }),
        ),
      )

    yield* Effect.logInfo("store ready").pipe(Effect.annotateLogs({ database: config.databasePath, history: enabled }))

    return Store.of({
      historyEnabled: enabled,
      databasePath: config.databasePath,
      rememberSent,
      sent,
      forgetSent,
      recordTurn,
      recordInbound,
      recordOutbound,
      markEdited,
      markDeleted,
      recordReaction,
      updateStatus,
      attachMedia,
      markMediaEvicted,
      setTurn,
      chats,
      messages,
      search,
      raw,
      purgeBefore,
    })
  }),
).pipe(Layer.provide(MigrationsLive))

/** The store on its own database: client, migrations, service. Needs `BridgeConfig` and the Node services. */
export const StoreWithDatabaseLive = StoreLive.pipe(Layer.provideMerge(DatabaseLive))
