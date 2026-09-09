import { Context, DateTime, Effect, Layer, Stream, SubscriptionRef } from "effect"
import { BridgeConfig } from "../config.ts"
import type { BridgeState, HealthSnapshot, LinkedIdentity } from "../domain/status.ts"
import { VERSION } from "../version.ts"

export type TouchKind = "event" | "inbound" | "outbound"

export interface HealthShape {
  readonly get: Effect.Effect<HealthSnapshot>
  readonly changes: Stream.Stream<HealthSnapshot>
  readonly setState: (state: BridgeState, detail?: string) => Effect.Effect<void>
  readonly setLinked: (linkedAs: LinkedIdentity) => Effect.Effect<void>
  readonly touch: (kind: TouchKind) => Effect.Effect<void>
  /** Record a socket close: bumps `reconnects` and remembers the reason. */
  readonly noteDisconnect: (reason: string) => Effect.Effect<void>
}

/**
 * The one place the bridge's liveness is recorded. The WhatsApp layer writes
 * connection state; the engine and transport stamp activity times; the HTTP
 * `/health` endpoint and the CLI read it.
 */
export class Health extends Context.Service<Health, HealthShape>()("bridge/Health") {}

export const HealthLive = Layer.effect(
  Health,
  Effect.gen(function* () {
      const config = yield* BridgeConfig
      const startedAt = yield* DateTime.now
      const ref = yield* SubscriptionRef.make<HealthSnapshot>({
        state: "starting",
        responder: config.responder,
        version: VERSION,
        startedAt,
        reconnects: 0,
      })

      const setState = Effect.fn("Health.setState")(function* (state: BridgeState, detail?: string) {
        yield* SubscriptionRef.update(ref, (s) => {
          const { detail: _dropped, ...rest } = s
          return detail === undefined ? { ...rest, state } : { ...rest, state, detail }
        })
        yield* Effect.logInfo("connection state").pipe(
          Effect.annotateLogs({ state, ...(detail === undefined ? {} : { detail }) }),
        )
      })

      const setLinked = Effect.fn("Health.setLinked")(function* (linkedAs: LinkedIdentity) {
        yield* SubscriptionRef.update(ref, (s) => ({ ...s, linkedAs }))
      })

      const touch = Effect.fn("Health.touch")(function* (kind: TouchKind) {
        const now = yield* DateTime.now
        yield* SubscriptionRef.update(ref, (s) => {
          switch (kind) {
            case "event":
              return { ...s, lastEventAt: now }
            case "inbound":
              return { ...s, lastEventAt: now, lastInboundAt: now }
            case "outbound":
              return { ...s, lastEventAt: now, lastOutboundAt: now }
          }
        })
      })

      const noteDisconnect = Effect.fn("Health.noteDisconnect")(function* (reason: string) {
        const at = yield* DateTime.now
        yield* SubscriptionRef.update(ref, (s) => ({ ...s, reconnects: s.reconnects + 1, lastDisconnect: { at, reason } }))
      })

      return Health.of({
        get: SubscriptionRef.get(ref),
        changes: SubscriptionRef.changes(ref),
        setState,
        setLinked,
        touch,
        noteDisconnect,
      })
  }),
)
