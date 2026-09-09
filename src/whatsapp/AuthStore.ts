import { makeCacheableSignalKeyStore, useMultiFileAuthState, type AuthenticationState } from "@whiskeysockets/baileys"
import { Context, Effect, FileSystem, Layer, Schema } from "effect"
import type { ILogger } from "@whiskeysockets/baileys/lib/Utils/logger.js"
import { BridgeConfig } from "../config.ts"

export class AuthStoreError extends Schema.TaggedError<AuthStoreError>()("AuthStoreError", {
  operation: Schema.String,
  dir: Schema.String,
  detail: Schema.String,
}) {}

export interface LoadedAuth {
  readonly state: AuthenticationState
  readonly saveCreds: () => Promise<void>
}

export interface AuthStoreShape {
  /** Absolute or cwd-relative directory holding the session. Mode 0700. */
  readonly dir: string
  /** True when a completed link exists on disk (`creds.json` says `registered`). */
  readonly isRegistered: Effect.Effect<boolean, AuthStoreError>
  /** Load (or initialise) the session and return the state plus its persister. */
  readonly load: (logger: ILogger) => Effect.Effect<LoadedAuth, AuthStoreError>
  /** Move the session aside (kept as `auth.invalid-<time>`) after WhatsApp rejected it. */
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

export const AuthStoreLive = Layer.effect(
  AuthStore,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const fs = yield* FileSystem.FileSystem
    const dir = config.authDir

    const fail = (operation: string) => (cause: unknown) =>
      new AuthStoreError({ operation, dir, detail: cause instanceof Error ? cause.message : String(cause) })

    const ensureDir = Effect.gen(function* () {
      yield* fs.makeDirectory(dir, { recursive: true })
      yield* fs.chmod(dir, 0o700)
    }).pipe(Effect.mapError(fail("ensureDir")))

    const isRegistered = Effect.gen(function* () {
      const file = `${dir}/creds.json`
      if (!(yield* fs.exists(file))) return false
      const raw = yield* fs.readFileString(file)
      const creds = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Creds))(raw)
      return creds.registered === true
    }).pipe(
      Effect.catchTag("SchemaError", () => Effect.succeed(false)),
      Effect.mapError(fail("isRegistered")),
    )

    const load = Effect.fn("AuthStore.load")(function* (logger: ILogger) {
      yield* ensureDir
      const loaded = yield* Effect.tryPromise({
        try: () => useMultiFileAuthState(dir),
        catch: fail("load"),
      })
      const state: AuthenticationState = {
        creds: loaded.state.creds,
        keys: makeCacheableSignalKeyStore(loaded.state.keys, logger),
      }
      return { state, saveCreds: loaded.saveCreds }
    })

    const invalidate = Effect.fn("AuthStore.invalidate")(function* (reason: string) {
      const exists = yield* fs.exists(dir).pipe(Effect.mapError(fail("invalidate")))
      if (!exists) return
      const stamp = new Date().toISOString().replace(/[:.]/g, "-")
      const target = `${dir}.invalid-${stamp}`
      yield* fs.rename(dir, target).pipe(Effect.mapError(fail("invalidate")))
      yield* Effect.logWarning("session moved aside; run `bridge link` to link again").pipe(
        Effect.annotateLogs({ reason, movedTo: target }),
      )
    })

    const remove = fs.remove(dir, { recursive: true, force: true }).pipe(Effect.mapError(fail("remove")))

    return AuthStore.of({ dir, isRegistered, load, invalidate, remove })
  }),
)
