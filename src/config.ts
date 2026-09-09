import { Config, ConfigProvider, Context, Duration, Effect, Layer, Option, Redacted } from "effect"
import type { ResponderKind } from "./domain/status.ts"

export interface BridgeConfigShape {
  readonly dataDir: string
  readonly authDir: string
  readonly host: string
  readonly port: number
  readonly apiToken: Option.Option<Redacted.Redacted<string>>
  readonly responder: ResponderKind
  readonly handlerUrl: Option.Option<string>
  readonly handlerTimeout: Duration.Duration
  readonly batchWindow: Duration.Duration
  readonly turnTimeout: Duration.Duration
  readonly seenEmoji: string
  readonly seenFallbackText: Option.Option<string>
  readonly groups: boolean
  /** Normalized to digits (for numbers) or full jids. Empty means everyone. */
  readonly allowedSenders: ReadonlyArray<string>
  readonly offlineWindow: Duration.Duration
  readonly typing: boolean
  readonly logBodies: boolean
  readonly baileysLogLevel: string
  readonly browserName: string
  /** Harness only: how long the pong responder pretends to think. */
  readonly harnessSlow: Duration.Duration
}

export class BridgeConfig extends Context.Service<BridgeConfig, BridgeConfigShape>()("bridge/Config") {}

const list = (raw: string): ReadonlyArray<string> =>
  raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((s) => (s.includes("@") ? s : s.replace(/[^0-9]/g, "")))

/** Every knob, read from the active `ConfigProvider` (env, then `.env`). */
export const bridgeConfig: Config.Config<BridgeConfigShape> = Config.all({
  dataDir: Config.string("BRIDGE_DATA_DIR").pipe(Config.withDefault("./data")),
  host: Config.string("BRIDGE_HOST").pipe(Config.withDefault("127.0.0.1")),
  port: Config.port("BRIDGE_PORT").pipe(Config.withDefault(8787)),
  apiToken: Config.option(Config.redacted("BRIDGE_API_TOKEN")),
  responder: Config.literals(["manual", "http", "pong"], "BRIDGE_RESPONDER").pipe(Config.withDefault("manual")),
  handlerUrl: Config.option(Config.string("INTRO_HANDLER_URL")),
  handlerTimeout: Config.duration("INTRO_HANDLER_TIMEOUT").pipe(Config.withDefault(Duration.seconds(120))),
  batchWindow: Config.duration("BRIDGE_BATCH_WINDOW").pipe(Config.withDefault(Duration.millis(1500))),
  turnTimeout: Config.duration("BRIDGE_TURN_TIMEOUT").pipe(Config.withDefault(Duration.minutes(10))),
  seenEmoji: Config.string("BRIDGE_SEEN_EMOJI").pipe(Config.withDefault("👀")),
  seenFallbackText: Config.option(Config.string("BRIDGE_SEEN_FALLBACK_TEXT")),
  groups: Config.boolean("BRIDGE_GROUPS").pipe(Config.withDefault(false)),
  allowedSenders: Config.string("BRIDGE_ALLOWED_SENDERS").pipe(Config.map(list), Config.withDefault([])),
  offlineWindow: Config.duration("BRIDGE_OFFLINE_WINDOW").pipe(Config.withDefault(Duration.zero)),
  typing: Config.boolean("BRIDGE_TYPING").pipe(Config.withDefault(true)),
  logBodies: Config.boolean("LOG_BODIES").pipe(Config.withDefault(false)),
  baileysLogLevel: Config.string("BAILEYS_LOG_LEVEL").pipe(Config.withDefault("silent")),
  browserName: Config.string("WA_BROWSER_NAME").pipe(Config.withDefault("Intro Bridge")),
  harnessSlow: Config.duration("BRIDGE_HARNESS_SLOW").pipe(Config.withDefault(Duration.zero)),
}).pipe(
  Config.map((c) => ({
    ...c,
    authDir: `${c.dataDir.replace(/\/+$/, "")}/auth`,
  })),
)

/** The config service, with optional overrides layered on top of the environment (CLI flags). */
export const BridgeConfigLive = (overrides: Partial<BridgeConfigShape> = {}) =>
  Layer.effect(
    BridgeConfig,
    Effect.map(bridgeConfig, (c) => BridgeConfig.of({ ...c, ...overrides })),
  )

/**
 * Environment variables first, then `.env` in the working directory as a
 * fallback. A missing `.env` is not an error.
 */
export const DotEnvLive = ConfigProvider.layerAdd(
  ConfigProvider.fromDotEnv({ path: ".env" }).pipe(
    Effect.orElseSucceed(() => ConfigProvider.fromUnknown({})),
  ),
)

/** Where the CLI client commands find the daemon. */
export const bridgeUrl: Config.Config<string> = Config.all({
  url: Config.option(Config.string("BRIDGE_URL")),
  host: Config.string("BRIDGE_HOST").pipe(Config.withDefault("127.0.0.1")),
  port: Config.port("BRIDGE_PORT").pipe(Config.withDefault(8787)),
}).pipe(Config.map(({ url, host, port }) => Option.getOrElse(url, () => `http://${host}:${port}`)))
