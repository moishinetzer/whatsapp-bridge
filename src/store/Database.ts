import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-node"
import { Effect, FileSystem, Layer, Path } from "effect"
import { Migrator, SqlClient } from "effect/unstable/sql"
import { BridgeConfig } from "../config.ts"

/**
 * The bridge's one SQLite file. Bridge state (sent messages, scheduled sends,
 * turns, the media cache index) is always written; the history tables exist
 * from the first run but only fill when history is enabled.
 */
export const DatabaseLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    if (config.databasePath !== ":memory:") {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const file = path.resolve(config.databasePath)
      yield* fs.makeDirectory(path.dirname(file), { recursive: true, mode: 0o700 }).pipe(Effect.ignore)
      // SQLite gives the WAL and shared-memory files the database file's mode, so it is created 0600 before SQLite opens it.
      if (!(yield* fs.exists(file).pipe(Effect.orElseSucceed(() => false)))) {
        yield* fs.writeFile(file, new Uint8Array(), { mode: 0o600 }).pipe(Effect.ignore)
      }
      yield* fs.chmod(file, 0o600).pipe(Effect.ignore)
    }
    return SqliteClient.layer({ filename: config.databasePath, busyTimeout: "5 seconds" })
  }),
)

const statements = (...sql: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const client = yield* SqlClient.SqlClient
    for (const statement of sql) yield* client.unsafe(statement)
  })

/** Schema versions, applied in order; each runs once, recorded by the migrator. */
const migrations = {
  "0001_state": statements(
    `CREATE TABLE sent_messages (
      message_id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      sent_at INTEGER NOT NULL,
      raw BLOB NOT NULL,
      turn_id TEXT
    )`,
    `CREATE INDEX sent_messages_chat ON sent_messages (chat_id, sent_at)`,
    `CREATE TABLE scheduled (
      id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload TEXT NOT NULL,
      send_at INTEGER NOT NULL,
      deliver_when_online INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      message_id TEXT,
      detail TEXT,
      closed_at INTEGER
    )`,
    `CREATE INDEX scheduled_due ON scheduled (status, send_at)`,
    `CREATE TABLE media_cache (
      chat_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      path TEXT NOT NULL,
      size INTEGER NOT NULL,
      mimetype TEXT NOT NULL,
      kind TEXT NOT NULL,
      downloaded_at INTEGER NOT NULL,
      last_used_at INTEGER NOT NULL,
      PRIMARY KEY (chat_id, message_id)
    )`,
    `CREATE INDEX media_cache_lru ON media_cache (last_used_at)`,
    `CREATE TABLE turns (
      turn_id TEXT PRIMARY KEY,
      chat_id TEXT NOT NULL,
      state TEXT NOT NULL,
      opened_at INTEGER NOT NULL,
      closed_at INTEGER,
      summary TEXT NOT NULL
    )`,
    `CREATE INDEX turns_chat ON turns (chat_id, opened_at)`,
  ),
  "0002_history": statements(
    `CREATE TABLE messages (
      chat_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      direction TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      sender_name TEXT,
      is_group INTEGER NOT NULL,
      timestamp INTEGER NOT NULL,
      kind TEXT NOT NULL,
      text TEXT,
      edited_text TEXT,
      mimetype TEXT,
      media_path TEXT,
      media_evicted_at INTEGER,
      quoted_message_id TEXT,
      status TEXT,
      deleted_at INTEGER,
      turn_id TEXT,
      raw BLOB,
      PRIMARY KEY (chat_id, message_id)
    )`,
    `CREATE INDEX messages_chat_time ON messages (chat_id, timestamp)`,
    `CREATE INDEX messages_time ON messages (timestamp)`,
    `CREATE TABLE reactions (
      chat_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      emoji TEXT NOT NULL,
      at INTEGER NOT NULL,
      PRIMARY KEY (chat_id, message_id, sender_id)
    )`,
    `CREATE VIRTUAL TABLE messages_fts USING fts5 (text, chat_id UNINDEXED, message_id UNINDEXED, tokenize = 'unicode61')`,
    // Stable views for anyone opening the file with sqlite3: the tables may change, these keep their columns.
    `CREATE VIEW messages_v AS
      SELECT chat_id, message_id, direction, sender_id, sender_name, is_group,
             datetime(timestamp / 1000, 'unixepoch') AS sent_at, kind,
             COALESCE(edited_text, text) AS text, text AS original_text, mimetype, media_path,
             quoted_message_id, status, datetime(deleted_at / 1000, 'unixepoch') AS deleted_at, turn_id
      FROM messages`,
    `CREATE VIEW chats_v AS
      SELECT chat_id, MAX(is_group) AS is_group, datetime(MAX(timestamp) / 1000, 'unixepoch') AS last_message_at, COUNT(*) AS message_count
      FROM messages GROUP BY chat_id`,
  ),
}

export const MigrationsLive = SqliteMigrator.layer({ loader: Migrator.fromRecord(migrations) })
