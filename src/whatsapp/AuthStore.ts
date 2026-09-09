import { makeCacheableSignalKeyStore, useMultiFileAuthState, type AuthenticationState } from "@whiskeysockets/baileys"
import type { ILogger } from "@whiskeysockets/baileys/lib/Utils/logger.js"
import { Context, Effect, FileSystem, Layer, Path, Schema, Scope } from "effect"
import { BridgeConfig } from "../config.ts"

export class AuthStoreError extends Schema.TaggedError<AuthStoreError>()("AuthStoreError", {
  operation: Schema.String,
  dir: Schema.String,
  detail: Schema.String,
}) {
  override get message() {
    return `${this.operation}: ${this.detail}`
  }
}

export interface LoadedAuth {
  readonly state: AuthenticationState
  readonly saveCreds: () => Promise<void>
}

export interface AuthStoreShape {
  /** Directory holding the session. Mode 0700; files inside 0600. */
  readonly dir: string
  /** True when a completed link exists on disk (`creds.json` says `registered`). */
  readonly isRegistered: Effect.Effect<boolean, AuthStoreError>
  /**
   * Take the single-process lock for this session (`<data dir>/bridge.lock`).
   * Two processes on one session make WhatsApp bounce between them, so the
   * second one fails here with the other's pid. Released when the scope closes.
   */
  readonly lock: Effect.Effect<void, AuthStoreError, Scope.Scope>
  /** Load (or initialise) the session and return the state plus its persister. */
  readonly load: (logger: ILogger) => Effect.Effect<LoadedAuth, AuthStoreError>
  /** Move the session aside (kept as `auth.invalid-<time>`, last three only) after WhatsApp rejected it. */
  readonly invalidate: (reason: string) => Effect.Effect<void, AuthStoreError>
  /** Delete the session entirely (`bridge unlink`). */
  readonly remove: Effect.Effect<void, AuthStoreError>
}

/**
 * Owns the on-disk WhatsApp session. Only this module knows where the
 * credentials live and how they are protected. Nothing in here is ever
 * logged.
 */
export class AuthStore extends Context.Service<AuthStore, AuthStoreShape>()("bridge/AuthStore") {}

const Creds = Schema.Struct({
  registered: Schema.optionalKey(Schema.Boolean),
})

const LockFile = Schema.Struct({
  pid: Schema.Number,
  startedAt: Schema.String,
})

const KEEP_INVALID = 3

const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0)
    return true
  } catch (e) {
    // EPERM: exists but not ours. ESRCH: gone.
    return (e as { code?: string }).code === "EPERM"
  }
}

export const AuthStoreLive = Layer.effect(
  AuthStore,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const dir = config.authDir
    const dataDir = config.dataDir
    const lockPath = path.join(dataDir, "bridge.lock")

    const fail = (operation: string) => (cause: unknown) =>
      new AuthStoreError({ operation, dir, detail: cause instanceof Error ? cause.message : String(cause) })

    const ensureDir = Effect.gen(function* () {
      yield* fs.makeDirectory(dir, { recursive: true })
      yield* fs.chmod(dataDir, 0o700)
      yield* fs.chmod(dir, 0o700)
    }).pipe(Effect.mapError(fail("ensureDir")))

    /** Best effort: the directory is already 0700, this closes the files themselves too. */
    const tightenFiles = Effect.gen(function* () {
      const entries = yield* fs.readDirectory(dir)
      yield* Effect.forEach(entries, (name) => fs.chmod(path.join(dir, name), 0o600).pipe(Effect.ignore), { discard: true })
    }).pipe(Effect.ignore)

    const isRegistered = Effect.gen(function* () {
      const file = path.join(dir, "creds.json")
      if (!(yield* fs.exists(file))) return false
      const raw = yield* fs.readFileString(file)
      const creds = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Creds))(raw)
      return creds.registered === true
    }).pipe(
      Effect.catchTag("SchemaError", () => Effect.succeed(false)),
      Effect.mapError(fail("isRegistered")),
    )

    const readLock = Effect.gen(function* () {
      const raw = yield* fs.readFileString(lockPath)
      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(LockFile))(raw)
    }).pipe(Effect.option)

    const tryWriteLock = fs
      .writeFileString(lockPath, JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }), { flag: "wx", mode: 0o600 })
      .pipe(Effect.as(true), Effect.orElseSucceed(() => false))

    const acquireLock = Effect.gen(function* () {
      yield* ensureDir
      if (yield* tryWriteLock) return
      const existing = yield* readLock
      if (existing._tag === "Some" && existing.value.pid !== process.pid && isAlive(existing.value.pid)) {
        return yield* new AuthStoreError({
          operation: "lock",
          dir,
          detail: `another bridge process (pid ${existing.value.pid}, started ${existing.value.startedAt}) holds this session; stop it first`,
        })
      }
      // Stale or unreadable lock: take it over.
      yield* fs.remove(lockPath, { force: true }).pipe(Effect.mapError(fail("lock")))
      if (!(yield* tryWriteLock)) {
        return yield* new AuthStoreError({ operation: "lock", dir, detail: `could not create ${lockPath}` })
      }
    })

    const releaseLock = Effect.gen(function* () {
      const existing = yield* readLock
      if (existing._tag === "Some" && existing.value.pid === process.pid) {
        yield* fs.remove(lockPath, { force: true })
      }
    }).pipe(Effect.ignore)

    const lock = Effect.acquireRelease(acquireLock, () => releaseLock)

    const load = Effect.fn("AuthStore.load")(function* (logger: ILogger) {
      yield* ensureDir
      const loaded = yield* Effect.tryPromise({
        try: () => useMultiFileAuthState(dir),
        catch: fail("load"),
      })
      yield* tightenFiles
      const state: AuthenticationState = {
        creds: loaded.state.creds,
        keys: makeCacheableSignalKeyStore(loaded.state.keys, logger),
      }
      const saveCreds = () => loaded.saveCreds()
      return { state, saveCreds }
    })

    const pruneInvalid = Effect.gen(function* () {
      const parent = path.dirname(dir)
      const base = path.basename(dir)
      const entries = yield* fs.readDirectory(parent)
      const stale = entries
        .filter((name) => name.startsWith(`${base}.invalid-`))
        .sort()
        .reverse()
        .slice(KEEP_INVALID)
      yield* Effect.forEach(stale, (name) => fs.remove(path.join(parent, name), { recursive: true, force: true }).pipe(Effect.ignore), {
        discard: true,
      })
    }).pipe(Effect.ignore)

    const invalidate = Effect.fn("AuthStore.invalidate")(function* (reason: string) {
      const exists = yield* fs.exists(dir).pipe(Effect.mapError(fail("invalidate")))
      if (!exists) return
      const stamp = new Date().toISOString().replace(/[:.]/g, "-")
      const target = `${dir}.invalid-${stamp}`
      yield* fs.rename(dir, target).pipe(Effect.mapError(fail("invalidate")))
      yield* pruneInvalid
      yield* Effect.logWarning("session moved aside; run `bridge link` to link again").pipe(
        Effect.annotateLogs({ reason, movedTo: target }),
      )
    })

    const remove = fs.remove(dir, { recursive: true, force: true }).pipe(Effect.mapError(fail("remove")))

    return AuthStore.of({ dir, isRegistered, lock, load, invalidate, remove })
  }),
)
