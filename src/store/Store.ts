import { Context, DateTime, Effect, Layer, Option, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import type { CallRecord, ChatSummary, ContactRecord, DeliveryStatus, Direction, SearchHit, StoredMessage } from "../domain/history.ts"
import type { InboundMessage, MessageKind } from "../domain/message.ts"
import type { TurnSummary } from "../engine/Turns.ts"
import { DatabaseLive, MigrationsLive } from "./Database.ts"
import { makeHistoryReads, millis, type PageOptions, type SearchOptions } from "./History.ts"

export { ftsQuery, type PageOptions, type SearchOptions } from "./History.ts"

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

/** A message the observer saw, in either direction, with its raw bytes when there are any. */
export interface ObservedItem {
  readonly message: InboundMessage
  readonly raw: Uint8Array | undefined
  readonly direction: Direction
}

/** What is known about a contact from one source; fields left out are kept as they were. */
export interface ContactUpsert {
  readonly jid: string
  readonly lid?: string | undefined
  readonly phone?: string | undefined
  readonly name?: string | undefined
  readonly pushName?: string | undefined
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
  /** Small facts that must survive a restart (who the account is, backfill progress). */
  readonly getState: (key: string) => Effect.Effect<Option.Option<string>, StoreError>
  readonly setState: (key: string, value: string) => Effect.Effect<void, StoreError>

  // History: writes are no-ops when disabled, reads fail with `HistoryDisabled`.
  readonly recordInbound: (message: InboundMessage, raw: Uint8Array) => Effect.Effect<void, StoreError>
  /** One message in either direction: the observer's live traffic. `out` is the account's own message. */
  readonly recordMessage: (message: InboundMessage, raw: Uint8Array | undefined, direction: Direction) => Effect.Effect<void, StoreError>
  /** Many messages in one transaction (backfill). Returns how many were new. */
  readonly recordBatch: (items: ReadonlyArray<ObservedItem>) => Effect.Effect<number, StoreError>
  readonly recordOutbound: (record: OutboundRecord) => Effect.Effect<void, StoreError>
  readonly recordCall: (call: CallRecord) => Effect.Effect<void, StoreError>
  readonly markEdited: (chatId: ChatId, messageId: MessageId, text: string) => Effect.Effect<void, StoreError>
  /** Tombstone: the text, media path and raw bytes go; id and timestamps stay. */
  readonly markDeleted: (chatId: ChatId, messageId: MessageId) => Effect.Effect<void, StoreError>
  /** An empty emoji removes the sender's reaction. */
  readonly recordReaction: (chatId: ChatId, messageId: MessageId, senderId: string, emoji: string) => Effect.Effect<void, StoreError>
  readonly updateStatus: (chatId: ChatId, messageId: MessageId, status: DeliveryStatus) => Effect.Effect<void, StoreError>
  readonly attachMedia: (chatId: ChatId, messageId: MessageId, path: string) => Effect.Effect<void, StoreError>
  readonly markMediaEvicted: (path: string) => Effect.Effect<void, StoreError>
  readonly setTurn: (chatId: ChatId, messageIds: ReadonlyArray<MessageId>, turnId: TurnId) => Effect.Effect<void, StoreError>
  readonly upsertContacts: (contacts: ReadonlyArray<ContactUpsert>) => Effect.Effect<void, StoreError>
  readonly upsertChat: (chatId: ChatId, name: string | undefined, isGroup: boolean) => Effect.Effect<void, StoreError>

  readonly chats: Effect.Effect<ReadonlyArray<ChatSummary>, StoreError | HistoryDisabled>
  /** The best name for a chat, when history knows one. */
  readonly chatName: (chatId: ChatId) => Effect.Effect<Option.Option<string>, StoreError>
  readonly messages: (chatId: ChatId, options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, StoreError | HistoryDisabled>
  readonly search: (query: string, options: SearchOptions) => Effect.Effect<ReadonlyArray<SearchHit>, StoreError | HistoryDisabled>
  readonly contacts: (query: string | undefined, limit: number) => Effect.Effect<ReadonlyArray<ContactRecord>, StoreError | HistoryDisabled>
  readonly contactByPhone: (phone: string) => Effect.Effect<Option.Option<ContactRecord>, StoreError>
  readonly contactByLid: (lid: string) => Effect.Effect<Option.Option<ContactRecord>, StoreError>
  /** The raw protobuf of an inbound message, when history kept it. */
  readonly raw: (chatId: ChatId, messageId: MessageId) => Effect.Effect<Option.Option<Uint8Array>, StoreError>
  /** Delete history rows older than the instant; returns how many. */
  readonly purgeBefore: (before: DateTime.Utc) => Effect.Effect<number, StoreError | HistoryDisabled>
}

export class Store extends Context.Service<Store, StoreShape>()("bridge/Store") {}

export const StoreLive = Layer.effect(
  Store,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const sql = yield* SqlClient.SqlClient
    const enabled = config.history
    const reads = makeHistoryReads(sql)

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
            sentAt: DateTime.makeUnsafe(row.sent_at),
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

    const getState = (key: string) => run("getState", reads.state(key))
    const setState = (key: string, value: string) =>
      run("setState", sql`INSERT INTO state (key, value) VALUES (${key}, ${value}) ON CONFLICT (key) DO UPDATE SET value = excluded.value`).pipe(Effect.asVoid)

    // ---- history ---------------------------------------------------------

    const indexText = (chatId: ChatId, messageId: MessageId, text: string | undefined) =>
      text === undefined || text.trim().length === 0
        ? Effect.void
        : sql`INSERT INTO messages_fts (text, chat_id, message_id) VALUES (${text}, ${chatId}, ${messageId})`

    const unindex = (chatId: ChatId, messageId: MessageId) =>
      sql`DELETE FROM messages_fts WHERE chat_id = ${chatId} AND message_id = ${messageId}`

    /** Inserts one message row; `true` when it was new. `out` rows carry `me` as the sender, whichever line wrote them. */
    const insertMessage = (m: InboundMessage, raw: Uint8Array | undefined, direction: Direction) =>
      Effect.gen(function* () {
        const inserted = yield* sql<{ message_id: string }>`INSERT INTO messages (chat_id, message_id, direction, sender_id, sender_name, is_group, timestamp, kind, text, mimetype, media_path, quoted_message_id, raw)
          VALUES (${m.chatId}, ${m.messageId}, ${direction}, ${direction === "out" ? "me" : m.senderId}, ${m.senderName ?? null}, ${m.isGroup ? 1 : 0}, ${millis(m.timestamp)}, ${m.kind},
                  ${m.text ?? null}, ${m.mimetype ?? null}, ${m.media?.path ?? null}, ${m.quotedMessageId ?? null}, ${raw ?? null})
          ON CONFLICT (chat_id, message_id) DO NOTHING RETURNING message_id`
        if (inserted.length === 0) return false
        yield* indexText(m.chatId, m.messageId, m.text)
        return true
      })

    const recordMessage = (m: InboundMessage, raw: Uint8Array | undefined, direction: Direction) =>
      whenEnabled("recordMessage", sql.withTransaction(insertMessage(m, raw, direction)))

    const recordInbound = (m: InboundMessage, raw: Uint8Array) => recordMessage(m, raw, "in")

    const recordBatch = (items: ReadonlyArray<ObservedItem>) =>
      !enabled || items.length === 0
        ? Effect.succeed(0)
        : run(
            "recordBatch",
            sql.withTransaction(
              Effect.gen(function* () {
                let added = 0
                for (const item of items) {
                  if (yield* insertMessage(item.message, item.raw, item.direction)) added += 1
                }
                return added
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

    const recordCall = (call: CallRecord) =>
      whenEnabled(
        "recordCall",
        sql.withTransaction(
          Effect.gen(function* () {
            const text = `${call.isVideo ? "video" : "voice"} call: ${call.status}`
            const messageId = MessageId.make(`call_${call.callId}`)
            yield* sql`INSERT INTO messages (chat_id, message_id, direction, sender_id, is_group, timestamp, kind, text)
              VALUES (${call.chatId}, ${messageId}, ${call.direction}, ${call.direction === "out" ? "me" : call.fromId}, ${call.isGroup ? 1 : 0}, ${millis(call.at)}, 'call', ${text})
              ON CONFLICT (chat_id, message_id) DO UPDATE SET text = excluded.text`
            yield* unindex(call.chatId, messageId)
            yield* indexText(call.chatId, messageId, text)
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

    const upsertContacts = (contacts: ReadonlyArray<ContactUpsert>) =>
      contacts.length === 0
        ? Effect.void
        : whenEnabled(
            "upsertContacts",
            sql.withTransaction(
              Effect.forEach(
                contacts,
                (c) =>
                  sql`INSERT INTO contacts (jid, lid, phone, name, push_name, updated_at)
                      VALUES (${c.jid}, ${c.lid ?? null}, ${c.phone ?? null}, ${c.name ?? null}, ${c.pushName ?? null}, ${Date.now()})
                      ON CONFLICT (jid) DO UPDATE SET
                        lid = COALESCE(excluded.lid, contacts.lid),
                        phone = COALESCE(excluded.phone, contacts.phone),
                        name = COALESCE(excluded.name, contacts.name),
                        push_name = COALESCE(excluded.push_name, contacts.push_name),
                        updated_at = excluded.updated_at`,
                { discard: true },
              ),
            ),
          )

    const upsertChat = (chatId: ChatId, name: string | undefined, isGroup: boolean) =>
      whenEnabled(
        "upsertChat",
        sql`INSERT INTO chats (chat_id, name, is_group, updated_at) VALUES (${chatId}, ${name ?? null}, ${isGroup ? 1 : 0}, ${Date.now()})
            ON CONFLICT (chat_id) DO UPDATE SET name = COALESCE(excluded.name, chats.name), is_group = excluded.is_group, updated_at = excluded.updated_at`,
      )

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
      getState,
      setState,
      recordInbound,
      recordMessage,
      recordBatch,
      recordOutbound,
      recordCall,
      markEdited,
      markDeleted,
      recordReaction,
      updateStatus,
      attachMedia,
      markMediaEvicted,
      setTurn,
      upsertContacts,
      upsertChat,
      chats: requireEnabled("chats", reads.chats),
      chatName: (chatId) => (enabled ? run("chatName", reads.chatName(chatId)) : Effect.succeed(Option.none<string>())),
      messages: (chatId, options) => requireEnabled("messages", reads.messages(chatId, options)),
      search: (query, options) => requireEnabled("search", reads.search(query, options)),
      contacts: (query, limit) => requireEnabled("contacts", reads.contacts(query, limit)),
      contactByPhone: (phone) => (enabled ? run("contactByPhone", reads.contactByPhone(phone)) : Effect.succeed(Option.none<ContactRecord>())),
      contactByLid: (lid) => (enabled ? run("contactByLid", reads.contactByLid(lid)) : Effect.succeed(Option.none<ContactRecord>())),
      raw: (chatId, messageId) => (enabled ? run("raw", reads.raw(chatId, messageId)) : Effect.succeed(Option.none<Uint8Array>())),
      purgeBefore,
    })
  }),
).pipe(Layer.provide(MigrationsLive))

/** The store on its own database: client, migrations, service. Needs `BridgeConfig` and the Node services. */
export const StoreWithDatabaseLive = StoreLive.pipe(Layer.provideMerge(DatabaseLive))
