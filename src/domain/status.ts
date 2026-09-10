import { Schema } from "effect"

/**
 * The bridge's connection state, the thing `GET /health` and `bridge health` report.
 *
 * - `starting`     the process is up but has not tried to connect yet
 * - `linking`      `bridge link` is showing a QR / pairing code and waiting for the phone
 * - `connecting`   a session exists and the socket is (re)connecting
 * - `ok`           connected; messages flow
 * - `needs_relink` WhatsApp rejected the session (logged out, banned, no session on disk)
 * - `stopped`      the daemon is not running (only the CLI reports this)
 */
export const BridgeState = Schema.Literals(["starting", "linking", "connecting", "ok", "needs_relink", "stopped"])
export type BridgeState = typeof BridgeState.Type

export const ResponderKind = Schema.Literals(["manual", "http", "pong"])
export type ResponderKind = typeof ResponderKind.Type

export const LinkedIdentity = Schema.Struct({
  jid: Schema.String,
  name: Schema.optionalKey(Schema.String),
})
export type LinkedIdentity = typeof LinkedIdentity.Type

/** Everything the health service tracks on its own. */
export const HealthSnapshot = Schema.Struct({
  state: BridgeState,
  detail: Schema.optionalKey(Schema.String),
  linkedAs: Schema.optionalKey(LinkedIdentity),
  responder: ResponderKind,
  version: Schema.String,
  startedAt: Schema.DateTimeUtcFromString,
  lastEventAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  lastInboundAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  lastOutboundAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  /** Socket closes seen this run (a reconnect follows each unless the close was terminal). */
  reconnects: Schema.Number,
  lastDisconnect: Schema.optionalKey(Schema.Struct({ at: Schema.DateTimeUtcFromString, reason: Schema.String })),
})
export type HealthSnapshot = typeof HealthSnapshot.Type

/** The health snapshot plus what the turn engine knows. This is the `GET /health` body. */
export const BridgeStatus = HealthSnapshot.pipe(
  Schema.fieldsAssign({
    openTurns: Schema.Number,
    /** The SQLite file; open it read-only with `sqlite3` for anything the API does not cover. */
    databasePath: Schema.String,
    /** Whether message history is being recorded. */
    history: Schema.Boolean,
  }),
)
export type BridgeStatus = typeof BridgeStatus.Type
