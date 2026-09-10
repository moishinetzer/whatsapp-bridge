import { DateTime, Effect, Option } from "effect"
import type { SqlClient, SqlError } from "effect/unstable/sql"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import type { ChatSummary, ContactRecord, DeliveryStatus, SearchHit, StoredMessage } from "../domain/history.ts"
import type { MessageKind } from "../domain/message.ts"

export interface PageOptions {
  /** Only messages older than this one. */
  readonly before?: MessageId | undefined
  /** Only messages newer than this instant. */
  readonly since?: DateTime.Utc | undefined
  readonly limit: number
}

export interface SearchOptions {
  readonly chatId?: ChatId | undefined
  readonly before?: MessageId | undefined
  readonly limit: number
}

/**
 * The read side of a history database. Built over any `SqlClient`, so the
 * personal line reads its own file and, read-only, the observer's.
 */
export interface HistoryReads {
  readonly chats: Effect.Effect<ReadonlyArray<ChatSummary>, SqlError.SqlError>
  readonly chatName: (chatId: ChatId) => Effect.Effect<Option.Option<string>, SqlError.SqlError>
  readonly messages: (chatId: ChatId, options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, SqlError.SqlError>
  readonly search: (query: string, options: SearchOptions) => Effect.Effect<ReadonlyArray<SearchHit>, SqlError.SqlError>
  readonly raw: (chatId: ChatId, messageId: MessageId) => Effect.Effect<Option.Option<Uint8Array>, SqlError.SqlError>
  readonly contacts: (query: string | undefined, limit: number) => Effect.Effect<ReadonlyArray<ContactRecord>, SqlError.SqlError>
  readonly contactByPhone: (phone: string) => Effect.Effect<Option.Option<ContactRecord>, SqlError.SqlError>
  readonly contactByLid: (lid: string) => Effect.Effect<Option.Option<ContactRecord>, SqlError.SqlError>
  readonly state: (key: string) => Effect.Effect<Option.Option<string>, SqlError.SqlError>
}

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

interface ContactRow {
  readonly jid: string
  readonly lid: string | null
  readonly phone: string | null
  readonly name: string | null
  readonly push_name: string | null
  readonly updated_at: number
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

export const millis = (d: DateTime.Utc): number => DateTime.toEpochMillis(d)
export const at = (n: number): DateTime.Utc => DateTime.makeUnsafe(n)
const maybe = <T>(v: T | null | undefined): T | undefined => (v === null || v === undefined ? undefined : v)

/** The page size ceiling for every history read. */
export const MAX_PAGE = 500

const toContact = (row: ContactRow): ContactRecord => ({
  jid: row.jid,
  ...(row.lid === null ? {} : { lid: row.lid }),
  ...(row.phone === null ? {} : { phone: row.phone }),
  ...(row.name === null ? {} : { name: row.name }),
  ...(row.push_name === null ? {} : { pushName: row.push_name }),
  updatedAt: at(row.updated_at),
})

export const makeHistoryReads = (sql: SqlClient.SqlClient): HistoryReads => {
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
    Effect.gen(function* () {
      const limit = Math.max(1, Math.min(options.limit, MAX_PAGE))
      const before = options.before === undefined ? undefined : yield* timestampOf(chatId, options.before)
      const since = options.since === undefined ? undefined : millis(options.since)
      const rows = yield* sql<MessageRow>`SELECT ${columns} FROM messages WHERE chat_id = ${chatId}
        ${before === undefined ? sql.literal("") : sql`AND timestamp < ${before}`}
        ${since === undefined ? sql.literal("") : sql`AND timestamp > ${since}`}
        ORDER BY timestamp DESC, message_id DESC LIMIT ${limit}`
      return yield* hydrate(rows)
    })

  const search = (query: string, options: SearchOptions) =>
    Effect.gen(function* () {
      const limit = Math.max(1, Math.min(options.limit, MAX_PAGE))
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
    })

  // A chat's name: WhatsApp's own (group subject, saved name), else the address book, else the last push name seen.
  const nameExpression = sql.literal(`COALESCE(MAX(c.name), MAX(k.name), MAX(k.push_name),
    (SELECT sender_name FROM messages n WHERE n.chat_id = m.chat_id AND n.direction = 'in' AND n.sender_name IS NOT NULL
     ORDER BY n.timestamp DESC LIMIT 1))`)

  const chats = Effect.map(
    sql<{ chat_id: string; is_group: number; last_message_at: number; message_count: number; name: string | null }>`
      SELECT m.chat_id, MAX(m.is_group) AS is_group, MAX(m.timestamp) AS last_message_at, COUNT(*) AS message_count, ${nameExpression} AS name
      FROM messages m
      LEFT JOIN chats c ON c.chat_id = m.chat_id
      LEFT JOIN contacts k ON k.jid = m.chat_id OR k.lid = m.chat_id
      GROUP BY m.chat_id ORDER BY last_message_at DESC`,
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
  )

  const chatName = (chatId: ChatId) =>
    Effect.map(
      sql<{ name: string | null }>`
        SELECT COALESCE(MAX(c.name), MAX(k.name), MAX(k.push_name),
          (SELECT sender_name FROM messages n WHERE n.chat_id = ${chatId} AND n.direction = 'in' AND n.sender_name IS NOT NULL
           ORDER BY n.timestamp DESC LIMIT 1)) AS name
        FROM (SELECT ${chatId} AS chat_id) x
        LEFT JOIN chats c ON c.chat_id = x.chat_id
        LEFT JOIN contacts k ON k.jid = x.chat_id OR k.lid = x.chat_id`,
      (rows) => Option.fromNullishOr(rows[0]?.name),
    )

  const raw = (chatId: ChatId, messageId: MessageId) =>
    Effect.map(
      sql<{ raw: Uint8Array | null }>`SELECT raw FROM messages WHERE chat_id = ${chatId} AND message_id = ${messageId}`,
      (rows) => Option.fromNullishOr(rows[0]?.raw),
    )

  const contacts = (query: string | undefined, limit: number) => {
    const capped = Math.max(1, Math.min(limit, MAX_PAGE))
    const q = query?.trim() ?? ""
    const like = `%${q.replace(/[%_]/g, "")}%`
    return Effect.map(
      q.length === 0
        ? sql<ContactRow>`SELECT * FROM contacts WHERE name IS NOT NULL OR push_name IS NOT NULL ORDER BY name, push_name LIMIT ${capped}`
        : sql<ContactRow>`SELECT * FROM contacts WHERE name LIKE ${like} OR push_name LIKE ${like} OR phone LIKE ${like} OR jid LIKE ${like}
            ORDER BY name, push_name LIMIT ${capped}`,
      (rows) => rows.map(toContact),
    )
  }

  const contactByPhone = (phone: string) =>
    Effect.map(sql<ContactRow>`SELECT * FROM contacts WHERE phone = ${phone} LIMIT 1`, (rows) => Option.fromNullishOr(rows[0]).pipe(Option.map(toContact)))

  const contactByLid = (lid: string) =>
    Effect.map(sql<ContactRow>`SELECT * FROM contacts WHERE lid = ${lid} OR jid = ${lid} LIMIT 1`, (rows) =>
      Option.fromNullishOr(rows[0]).pipe(Option.map(toContact)),
    )

  const state = (key: string) =>
    Effect.map(sql<{ value: string }>`SELECT value FROM state WHERE key = ${key}`, (rows) => Option.fromNullishOr(rows[0]?.value))

  return { chats, chatName, messages, search, raw, contacts, contactByPhone, contactByLid, state }
}
