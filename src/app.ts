import { FetchHttpClient } from "effect/unstable/http"
import { NodeServices } from "@effect/platform-node"
import { Duration, Effect, FiberSet, Layer, Option } from "effect"
import { BridgeConfig, BridgeConfigLive, DotEnvLive, forObserver, type BridgeConfigShape } from "./config.ts"
import { EventsLive } from "./engine/Events.ts"
import { HealthLive } from "./engine/Health.ts"
import { MediaStoreLive } from "./engine/Media.ts"
import { OutboundLive } from "./engine/Outbound.ts"
import { StoreWithDatabaseLive } from "./store/Store.ts"
import { SchedulerLive } from "./store/Scheduled.ts"
import { FileSystem, Path, Schema } from "effect"
import { ResponderLive } from "./engine/Responder.ts"
import { ContextSourceLive } from "./engine/Context.ts"
import { TurnEngineLive, TurnRegistryLive } from "./engine/Turns.ts"
import { HttpServerLive } from "./http/Server.ts"
import { LoggingLive } from "./logging.ts"
import { AuthStoreLive } from "./whatsapp/AuthStore.ts"
import { BaileysLive, type BaileysOptions } from "./whatsapp/Baileys.ts"
import { MediaSourceFromWhatsApp } from "./whatsapp/MediaSource.ts"
import { Observer, ObserverLive, ObserverMediaSourceLive, type ObserverOptions } from "./whatsapp/Observer.ts"
import { ObserverHttpServerLive } from "./http/ObserverServer.ts"
import { MediaStore } from "./engine/Media.ts"
import { Store } from "./store/Store.ts"
import { DateTime, Schedule, Stream } from "effect"

/**
 * The last line of defence for the process. The protocol library runs a lot
 * of promise chains outside our fibers; a stray rejection must be logged with
 * its reason, not kill the daemon. An uncaught exception leaves the process in
 * an unknown state, so that one is logged and the process exits non-zero for
 * the supervisor to restart.
 */
export const ProcessGuardsLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const run = yield* FiberSet.makeRuntime<never>()
    const describe = (e: unknown) => (e instanceof Error ? `${e.name}: ${e.message}` : String(e))
    const onRejection = (reason: unknown) => {
      run(Effect.logError("unhandled promise rejection (kept running)").pipe(Effect.annotateLogs({ reason: describe(reason) })))
    }
    const onException = (error: Error) => {
      // Synchronous on purpose: the runtime may be unusable after this.
      console.error(`[whatsapp-bridge] uncaught exception, exiting: ${describe(error)}\n${error.stack ?? ""}`)
      process.exitCode = 1
      setTimeout(() => process.exit(1), 100).unref()
    }
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        process.on("unhandledRejection", onRejection)
        process.on("uncaughtException", onException)
      }),
      () =>
        Effect.sync(() => {
          process.off("unhandledRejection", onRejection)
          process.off("uncaughtException", onException)
        }),
    )
  }),
)

/** `BRIDGE_HISTORY=true` without the one-time acceptance: the daemon says what to run and stops. */
export class HistoryNotAccepted extends Schema.TaggedError<HistoryNotAccepted>()("HistoryNotAccepted", { file: Schema.String }) {
  override get message() {
    return `BRIDGE_HISTORY is on but history has not been enabled on this machine: run \`bridge history enable\` (it explains what gets stored and writes ${this.file})`
  }
}

/** Where `bridge history enable` records the acceptance. */
export const acceptanceFile = (dataDir: string) => `${dataDir.replace(/\/+$/, "")}/history.accepted`

export const HistoryGateLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    if (!config.history) return
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const file = path.resolve(acceptanceFile(config.dataDir))
    if (!(yield* fs.exists(file).pipe(Effect.orElseSucceed(() => false)))) return yield* new HistoryNotAccepted({ file })
    yield* Effect.logInfo("history is on").pipe(Effect.annotateLogs({ database: config.databasePath }))
  }),
)

/** One line at startup saying how the daemon is configured (never secrets). */
export const StartupLogLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const c = yield* BridgeConfig
    yield* Effect.logInfo("configuration").pipe(
      Effect.annotateLogs({
        api: `http://${c.host}:${c.port}`,
        apiToken: Option.isSome(c.apiToken) ? "set" : "none",
        responder: c.responder,
        handlerUrl: Option.getOrElse(c.handlerUrl, () => "n/a"),
        dataDir: c.dataDir,
        batchWindowMs: Duration.toMillis(c.batchWindow),
        batchMaxWaitMs: Duration.toMillis(c.batchMaxWait),
        turnTimeoutMs: Duration.toMillis(c.turnTimeout),
        groups: c.groups,
        allowedSenders: c.allowedSenders.length === 0 ? "anyone" : c.allowedSenders.length,
        mediaDir: c.mediaDir,
        mediaAuto: c.mediaAuto,
        mediaMaxBytes: c.mediaMaxBytes,
        mediaMaxDisk: c.mediaMaxDisk,
        database: c.databasePath,
        history: c.history,
        typing: c.typing,
        logBodies: c.logBodies,
      }),
    )
  }),
)

/** Environment variables, then `.env`, on top of the Node platform services. */
export const EnvLive = DotEnvLive.pipe(Layer.provide(NodeServices.layer))

/** Loggers and the log threshold. Provided under everything so every fiber inherits them. */
export const AmbientLive = LoggingLive.pipe(Layer.provide(EnvLive))

/**
 * The services every command shares: config, health, events, the session
 * store and the WhatsApp transport. `mode` decides whether the transport links
 * (`bridge link`) or serves.
 */
export const coreLayer = (transport: BaileysOptions, overrides: Partial<BridgeConfigShape> = {}) => {
  const Config = BridgeConfigLive(overrides).pipe(Layer.provide(EnvLive))
  const Health = HealthLive.pipe(Layer.provide(Config))
  const Events = EventsLive.pipe(Layer.provide(Health))
  const Auth = AuthStoreLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const Storage = StoreWithDatabaseLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const WhatsApp = BaileysLive(transport).pipe(Layer.provide([Config, Auth, Health, Events, Storage]))
  return Layer.mergeAll(Config, Health, Events, Auth, Storage, WhatsApp)
}

/** The daemon: transport + turn engine + local HTTP API. */
export const daemonLayer = (overrides: Partial<BridgeConfigShape> = {}) => {
  const Core = coreLayer({ mode: "serve" }, overrides)
  const Registry = TurnRegistryLive.pipe(Layer.provide(Core))
  const Media = MediaStoreLive.pipe(Layer.provide([Core, MediaSourceFromWhatsApp.pipe(Layer.provide(Core)), NodeServices.layer]))
  const Outbound = OutboundLive.pipe(Layer.provide([Core, NodeServices.layer]))
  const Scheduled = SchedulerLive.pipe(Layer.provide([Core, Outbound]))
  const Responder = ResponderLive.pipe(Layer.provide([Core, Outbound, FetchHttpClient.layer]))
  const Context = ContextSourceLive.pipe(Layer.provide([Core, NodeServices.layer, FetchHttpClient.layer]))
  const Engine = TurnEngineLive.pipe(Layer.provide([Core, Registry, Responder, Media, Context]))
  const Server = HttpServerLive.pipe(Layer.provide([Core, Registry, Media, Outbound, Scheduled]))
  const Gate = HistoryGateLive.pipe(Layer.provide([Core, NodeServices.layer]))
  const Startup = StartupLogLive.pipe(Layer.provide(Core))
  return Layer.mergeAll(Gate, Core, Registry, Media, Outbound, Scheduled, Responder, Context, Engine, Server, Startup, ProcessGuardsLive).pipe(
    Layer.provideMerge(AmbientLive),
  )
}

/** `bridge link`: the transport in link mode, nothing else. */
export const linkLayer = (phone: string | undefined) =>
  Layer.mergeAll(coreLayer({ mode: "link", phone }), ProcessGuardsLive).pipe(Layer.provideMerge(AmbientLive))

// ---------------------------------------------------------------------------
// The observer: a second process on the primary number, reading only.
// ---------------------------------------------------------------------------

/** The observer was configured without the one-time acceptance: the daemon says what to run and stops. */
export class ObserverNotAccepted extends Schema.TaggedError<ObserverNotAccepted>()("ObserverNotAccepted", { file: Schema.String }) {
  override get message() {
    return `the observer has not been enabled on this machine: run \`bridge observe enable\` (it explains what gets stored and writes ${this.file})`
  }
}

/** Where `bridge observe enable` records the acceptance, inside the observer's own directory. */
export const observerAcceptanceFile = (observerDir: string) => `${observerDir.replace(/\/+$/, "")}/observer.accepted`

const ObserverGateLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const file = path.resolve(observerAcceptanceFile(config.dataDir))
    if (!(yield* fs.exists(file).pipe(Effect.orElseSucceed(() => false)))) return yield* new ObserverNotAccepted({ file })
  }),
)

/** Attachments the policy wants, fetched one at a time with a pause, so a backfill never hammers WhatsApp. */
const ObserverMediaLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const observer = yield* Observer
    const media = yield* MediaStore
    yield* observer.mediaWanted.pipe(
      Stream.runForEach(({ chatId, messageId }) =>
        media.get(chatId, messageId).pipe(
          Effect.catch((e) => Effect.logDebug("observer media not fetched").pipe(Effect.annotateLogs({ chatId, messageId, reason: e.message }))),
          Effect.andThen(Effect.sleep("250 millis")),
        ),
      ),
      Effect.forkScoped,
    )
  }),
)

/** Daily: rows older than the retention go. Weekly: a line saying the observer is on, so it never becomes furniture. */
const ObserverMaintenanceLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const store = yield* Store
    const purge = Effect.gen(function* () {
      const now = yield* DateTime.now
      const removed = yield* store.purgeBefore(DateTime.subtractDuration(now, config.observeRetention))
      if (removed > 0) yield* Effect.logInfo("observer retention").pipe(Effect.annotateLogs({ removed, retentionMs: Duration.toMillis(config.observeRetention) }))
    }).pipe(Effect.catch((e) => Effect.logWarning("observer retention failed").pipe(Effect.annotateLogs({ reason: e.message }))))
    yield* purge.pipe(Effect.repeat(Schedule.spaced("24 hours")), Effect.forkScoped)
    yield* Effect.logInfo("the observer is recording every message on the primary number").pipe(
      Effect.annotateLogs({ database: config.databasePath, retentionMs: Duration.toMillis(config.observeRetention), exclude: config.observeExclude.length }),
      Effect.repeat(Schedule.spaced("7 days")),
      Effect.forkScoped,
    )
  }),
)

const ObserverStartupLogLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const c = yield* BridgeConfig
    yield* Effect.logInfo("observer configuration").pipe(
      Effect.annotateLogs({
        api: `http://${c.host}:${c.port}`,
        apiToken: Option.isSome(c.apiToken) ? "set" : "none",
        dataDir: c.dataDir,
        database: c.databasePath,
        mediaDir: c.mediaDir,
        mediaAuto: c.mediaAuto,
        mediaMaxDisk: c.mediaMaxDisk,
        backfillDays: c.observeBackfillDays,
        retentionMs: Duration.toMillis(c.observeRetention),
        excluded: c.observeExclude.length,
      }),
    )
  }),
)

/** Config, health, session, store and the observer transport on the observer's own directory. */
const observerCore = (options: ObserverOptions) => {
  const Config = BridgeConfigLive(forObserver).pipe(Layer.provide(EnvLive))
  const Health = HealthLive.pipe(Layer.provide(Config))
  const Auth = AuthStoreLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const Storage = StoreWithDatabaseLive.pipe(Layer.provide([Config, NodeServices.layer]))
  const Transport = ObserverLive(options).pipe(Layer.provide([Config, Auth, Health, Storage]))
  return Layer.mergeAll(Config, Health, Auth, Storage, Transport)
}

/** `bridge observe run`: the read-only daemon on the primary number. */
export const observerLayer = () => {
  const Core = observerCore({ mode: "serve" })
  const Source = ObserverMediaSourceLive.pipe(Layer.provide(Core))
  const Media = MediaStoreLive.pipe(Layer.provide([Core, Source, NodeServices.layer]))
  const Fetcher = ObserverMediaLive.pipe(Layer.provide([Core, Media]))
  const Server = ObserverHttpServerLive.pipe(Layer.provide([Core, Media]))
  const Gate = ObserverGateLive.pipe(Layer.provide([Core, NodeServices.layer]))
  // After the gate, so a refused start never logs "recording" first.
  const Maintenance = ObserverMaintenanceLive.pipe(Layer.provide([Core, Gate]))
  const Startup = ObserverStartupLogLive.pipe(Layer.provide(Core))
  return Layer.mergeAll(Gate, Core, Media, Fetcher, Server, Maintenance, Startup, ProcessGuardsLive).pipe(Layer.provideMerge(AmbientLive))
}

/** `bridge observe link`: the observer transport in link mode, nothing else. */
export const observerLinkLayer = (phone: string | undefined) =>
  Layer.mergeAll(observerCore({ mode: "link", phone }), ProcessGuardsLive).pipe(Layer.provideMerge(AmbientLive))
