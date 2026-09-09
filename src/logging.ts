import { Config, Effect, Layer, Logger, References } from "effect"

/**
 * Console logging. `LOG_FORMAT=json` for machine-readable lines, otherwise a
 * colourised human format. `LOG_LEVEL` sets the threshold (Info by default).
 *
 * Privacy rule enforced by callers: message bodies are never passed to a log
 * call above Debug, and only when `LOG_BODIES=true`.
 */
export const LoggingLive = Layer.unwrap(
  Effect.gen(function* () {
    const level = yield* Config.logLevel("LOG_LEVEL").pipe(Config.withDefault("Info"))
    const format = yield* Config.literals(["pretty", "json"], "LOG_FORMAT").pipe(Config.withDefault("pretty"))
    const logger = format === "json" ? Logger.consoleJson : Logger.consolePretty()
    return Layer.mergeAll(Logger.layer([logger]), Layer.succeed(References.MinimumLogLevel, level))
  }),
)
