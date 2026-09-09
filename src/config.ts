import { Config, ConfigProvider, Context, Duration, Effect, Exit, Layer, Option, Redacted, Schema } from "effect"
import type { ResponderKind } from "./domain/status.ts"

/** A configuration value that could not be read, with the variable named so the fix is obvious. */
export class ConfigInvalid extends Schema.TaggedError<ConfigInvalid>()("ConfigInvalid", {
  variables: Schema.Array(Schema.String),
  detail: Schema.String,
}) {
  override get message() {
    const which = this.variables.length === 0 ? "a configuration value" : this.variables.join(", ")
    return `invalid configuration: ${which}: ${this.detail}`
  }
}

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
  /** A turn opens at most this long after its oldest message, even if messages keep arriving. */
  readonly batchMaxWait: Duration.Duration
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

interface Knob<A> {
  readonly variable: string
  readonly config: Config.Config<A>
}

const knob = <A>(variable: string, config: Config.Config<A>): Knob<A> => ({ variable, config })

/** Every knob with the environment variable it reads, so a failure can be blamed on a name. */
const knobs = {
  dataDir: knob("BRIDGE_DATA_DIR", Config.string("BRIDGE_DATA_DIR").pipe(Config.withDefault("./data"))),
  host: knob("BRIDGE_HOST", Config.string("BRIDGE_HOST").pipe(Config.withDefault("127.0.0.1"))),
  port: knob("BRIDGE_PORT", Config.port("BRIDGE_PORT").pipe(Config.withDefault(8787))),
  apiToken: knob("BRIDGE_API_TOKEN", Config.option(Config.redacted("BRIDGE_API_TOKEN"))),
  responder: knob(
    "BRIDGE_RESPONDER",
    Config.literals(["manual", "http", "pong"], "BRIDGE_RESPONDER").pipe(Config.withDefault("manual")),
  ),
  handlerUrl: knob("INTRO_HANDLER_URL", Config.option(Config.string("INTRO_HANDLER_URL"))),
  handlerTimeout: knob("INTRO_HANDLER_TIMEOUT", Config.duration("INTRO_HANDLER_TIMEOUT").pipe(Config.withDefault(Duration.seconds(120)))),
  batchWindow: knob("BRIDGE_BATCH_WINDOW", Config.duration("BRIDGE_BATCH_WINDOW").pipe(Config.withDefault(Duration.millis(1500)))),
  batchMaxWait: knob("BRIDGE_BATCH_MAX_WAIT", Config.duration("BRIDGE_BATCH_MAX_WAIT").pipe(Config.withDefault(Duration.seconds(5)))),
  turnTimeout: knob("BRIDGE_TURN_TIMEOUT", Config.duration("BRIDGE_TURN_TIMEOUT").pipe(Config.withDefault(Duration.minutes(10)))),
  seenEmoji: knob("BRIDGE_SEEN_EMOJI", Config.string("BRIDGE_SEEN_EMOJI").pipe(Config.withDefault("👀"))),
  seenFallbackText: knob("BRIDGE_SEEN_FALLBACK_TEXT", Config.option(Config.string("BRIDGE_SEEN_FALLBACK_TEXT"))),
  groups: knob("BRIDGE_GROUPS", Config.boolean("BRIDGE_GROUPS").pipe(Config.withDefault(false))),
  allowedSenders: knob("BRIDGE_ALLOWED_SENDERS", Config.string("BRIDGE_ALLOWED_SENDERS").pipe(Config.map(list), Config.withDefault([]))),
  offlineWindow: knob("BRIDGE_OFFLINE_WINDOW", Config.duration("BRIDGE_OFFLINE_WINDOW").pipe(Config.withDefault(Duration.zero))),
  typing: knob("BRIDGE_TYPING", Config.boolean("BRIDGE_TYPING").pipe(Config.withDefault(true))),
  logBodies: knob("LOG_BODIES", Config.boolean("LOG_BODIES").pipe(Config.withDefault(false))),
  baileysLogLevel: knob(
    "BAILEYS_LOG_LEVEL",
    Config.literals(["trace", "debug", "info", "warn", "error", "silent"], "BAILEYS_LOG_LEVEL").pipe(Config.withDefault("warn")),
  ),
  browserName: knob("WA_BROWSER_NAME", Config.string("WA_BROWSER_NAME").pipe(Config.withDefault("Intro Bridge"))),
  harnessSlow: knob("BRIDGE_HARNESS_SLOW", Config.duration("BRIDGE_HARNESS_SLOW").pipe(Config.withDefault(Duration.zero))),
}

type Knobs = typeof knobs
type KnobValues = { readonly [K in keyof Knobs]: Knobs[K] extends Knob<infer A> ? A : never }

const configs = Object.fromEntries(Object.entries(knobs).map(([k, v]) => [k, v.config])) as { [K in keyof Knobs]: Config.Config<KnobValues[K]> }

/** Every knob, read from the active `ConfigProvider` (env, then `.env`). */
export const bridgeConfig: Config.Config<BridgeConfigShape> = Config.all(configs).pipe(
  Config.map((c) => ({
    ...c,
    authDir: `${c.dataDir.replace(/\/+$/, "")}/auth`,
  })),
)

/** Re-reads each knob on its own to name the ones that failed. */
const blame = (error: Config.ConfigError) =>
  Effect.gen(function* () {
    const failures: Array<string> = []
    for (const { variable, config } of Object.values(knobs) as ReadonlyArray<Knob<unknown>>) {
      const exit = yield* Effect.exit(config)
      if (Exit.isFailure(exit)) failures.push(variable)
    }
    return yield* new ConfigInvalid({ variables: failures, detail: error.message })
  })

/** The config service, with optional overrides layered on top of the environment (CLI flags). */
export const BridgeConfigLive = (overrides: Partial<BridgeConfigShape> = {}) =>
  Layer.effect(
    BridgeConfig,
    bridgeConfig.pipe(
      Effect.map((c) => BridgeConfig.of({ ...c, ...overrides })),
      Effect.catchTag("ConfigError", blame),
    ),
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
