import { NodeHttpServer } from "@effect/platform-node"
import { Duration, Effect, Layer } from "effect"
import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi"
import { SqlClient } from "effect/unstable/sql"
import { createServer } from "node:http"
import { BridgeConfig } from "../config.ts"
import { Health } from "../engine/Health.ts"
import { MediaStore, MediaStoreError } from "../engine/Media.ts"
import { Store } from "../store/Store.ts"
import { Observer } from "../whatsapp/Observer.ts"
import { ObserverApi, ObserverUnhealthy, type ObserverStatus } from "./ObserverApi.ts"
import { ClientErrorsLive, bearerAuth, requestLog } from "./Server.ts"

const status: Effect.Effect<ObserverStatus, never, Health | Store | Observer | MediaStore | BridgeConfig | SqlClient.SqlClient> = Effect.gen(function* () {
  const health = yield* Health
  const store = yield* Store
  const observer = yield* Observer
  const media = yield* MediaStore
  const config = yield* BridgeConfig
  const sql = yield* SqlClient.SqlClient
  const snapshot = yield* health.get
  const backfill = yield* observer.backfill
  const count = yield* sql<{ n: number }>`SELECT COUNT(*) AS n FROM messages`.pipe(
    Effect.map((rows) => rows[0]?.n ?? 0),
    Effect.orElseSucceed(() => 0),
  )
  return {
    ...snapshot,
    observer: true as const,
    databasePath: store.databasePath,
    mediaDir: media.dir,
    backfill,
    messages: count,
    retentionDays: Math.round(Duration.toMillis(config.observeRetention) / 86_400_000),
  }
})

export const ObserverHealthGroupLive = HttpApiBuilder.group(ObserverApi, "health", (handlers) =>
  handlers
    .handle("check", () =>
      Effect.gen(function* () {
        const s = yield* status
        if (s.state !== "ok") return yield* new ObserverUnhealthy({ status: s })
        return s
      }),
    )
    .handle("status", () => status),
)

export const ObserverMediaGroupLive = HttpApiBuilder.group(ObserverApi, "media", (handlers) =>
  handlers
    .handle("get", ({ params }) => MediaStore.use((m) => m.get(params.chatId, params.messageId)))
    .handle("content", ({ params }) =>
      Effect.gen(function* () {
        const media = yield* MediaStore
        const file = yield* media.get(params.chatId, params.messageId)
        return yield* HttpServerResponse.file(file.path, { contentType: file.mimetype }).pipe(
          Effect.mapError((e) => new MediaStoreError({ operation: "serve", detail: e.message })),
        )
      }),
    ),
)

export const ObserverApiLive = Layer.mergeAll(
  HttpApiBuilder.layer(ObserverApi, { openapiPath: "/openapi.json" }).pipe(Layer.provide([ObserverHealthGroupLive, ObserverMediaGroupLive])),
  HttpApiScalar.layer(ObserverApi, { path: "/docs" }),
  ClientErrorsLive,
)

/** The observer's listener, on `BRIDGE_OBSERVER_PORT` (loopback by default). */
export const ObserverHttpServerLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const auth = bearerAuth(config.apiToken)
    return HttpRouter.serve(ObserverApiLive, { disableLogger: true, middleware: (app) => requestLog(auth(app)) }).pipe(
      Layer.provide(NodeHttpServer.layer(createServer, { host: config.host, port: config.port, gracefulShutdownTimeout: "3 seconds" })),
    )
  }),
)
