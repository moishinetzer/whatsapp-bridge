import { Config, Effect, Option, Redacted } from "effect"
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { bridgeUrl } from "../config.ts"
import { BridgeApi } from "./Api.ts"

/**
 * A typed client for the daemon, derived from the same `HttpApi` the server
 * mounts. Used by the CLI commands (`health`, `send`, `react`, ...).
 */
export const bridgeClient = Effect.gen(function* () {
  const baseUrl = yield* bridgeUrl
  const token = yield* Config.option(Config.redacted("BRIDGE_API_TOKEN"))
  return yield* HttpApiClient.make(BridgeApi, {
    baseUrl,
    transformClient: (client) =>
      Option.match(token, {
        onNone: () => client,
        onSome: (t) => client.pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(Redacted.value(t)))),
      }),
  })
}).pipe(Effect.provide(FetchHttpClient.layer))

export type BridgeClient = Effect.Success<typeof bridgeClient>
