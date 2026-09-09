import { describe, expect, it } from "@effect/vitest"
import { NodeHttpServer } from "@effect/platform-node"
import { Effect, Fiber, Layer, Option, Redacted, Ref, Stream } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { toChatId } from "../src/domain/ids.ts"
import { Events } from "../src/engine/Events.ts"
import { Health } from "../src/engine/Health.ts"
import { TurnId } from "../src/domain/ids.ts"
import { BridgeApi } from "../src/http/Api.ts"
import { ApiLive, bearerAuth } from "../src/http/Server.ts"
import { EngineTestLayer, FakeWhatsApp, TestResponder, external, message, settle, takeSoon } from "./support/fakes.ts"

const TOKEN = "s3cret"

const ServerTestLayer = Layer.unwrap(
  Effect.gen(function* () {
    const engine = EngineTestLayer({ apiToken: Option.some(Redacted.make(TOKEN)) })
    const server = HttpRouter.serve(ApiLive, { middleware: bearerAuth(Option.some(Redacted.make(TOKEN))) }).pipe(
      Layer.provide(engine),
      Layer.provideMerge(NodeHttpServer.layerTest),
    )
    return Layer.mergeAll(engine, server)
  }),
)

const client = (withToken: boolean) =>
  HttpApiClient.make(BridgeApi, {
    transformClient: (c) => (withToken ? c.pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(TOKEN))) : c),
  })

describe("HTTP API", () => {
  it.effect("health is 503 until the transport reports ok, then 200", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const health = yield* Health

      const unhealthy = yield* api.health.check().pipe(Effect.flip)
      expect(unhealthy._tag).toBe("Unhealthy")
      expect(unhealthy._tag === "Unhealthy" && unhealthy.status.state).toBe("starting")

      const status = yield* api.health.status()
      expect(status.state).toBe("starting")
      expect(status.openTurns).toBe(0)
      expect(status.responder).toBe("manual")

      yield* health.setState("ok")
      const ok = yield* api.health.check()
      expect(ok.state).toBe("ok")
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("requires the bearer token everywhere except /health", () =>
    Effect.gen(function* () {
      const anonymous = yield* client(false)
      const status = yield* anonymous.health.status().pipe(Effect.flip)
      expect(String(status)).toContain("401")
      const health = yield* anonymous.health.check().pipe(Effect.flip)
      expect(health._tag).toBe("Unhealthy")
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("sends text, reacts and unreacts through the transport", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const chatId = toChatId("+44 7700 900123")

      const sent = yield* api.chats.send({ params: { chatId }, payload: { text: "Open Grok Bot → approve the Link spend" } })
      expect(sent.chatId).toBe("447700900123@s.whatsapp.net")
      expect((yield* takeSoon(wa.sent)).text).toBe("Open Grok Bot → approve the Link spend")

      const messageId = message("x").messageId
      yield* api.chats.react({ params: { chatId, messageId }, payload: { emoji: "👀" } })
      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId, messageId, emoji: "👀" })
      yield* api.chats.unreact({ params: { chatId, messageId } })
      expect(yield* takeSoon(wa.reactions)).toEqual({ chatId, messageId, emoji: "" })

      yield* api.chats.typing({ params: { chatId }, payload: { composing: true } })
      expect(yield* Ref.get(wa.typing)).toEqual([{ chatId, composing: true }])
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("streams events over SSE and lets Intro answer a turn", () =>
    Effect.gen(function* () {
      const api = yield* client(true)
      const wa = yield* FakeWhatsApp
      const responder = yield* TestResponder
      yield* Ref.set(responder.behaviour, () => Effect.succeed(external))

      const stream = yield* api.events.stream({ query: {} })
      const eventsFiber = yield* stream.pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped)

      const m1 = message("via http")
      yield* wa.deliver(m1)
      yield* takeSoon(wa.reactions)
      const call = yield* takeSoon(responder.calls)
      yield* settle

      const open = yield* api.turns.list()
      expect(open.map((t) => t.turnId)).toEqual([call.turnId])
      expect((yield* api.turns.get({ params: { turnId: call.turnId } })).state).toBe("awaiting")

      const missing = yield* api.turns.reply({ params: { turnId: TurnId.make("turn_nope") }, payload: { text: "?" } }).pipe(Effect.flip)
      expect(missing._tag).toBe("TurnNotFound")

      const replyFiber = yield* api.turns.reply({ params: { turnId: call.turnId }, payload: { text: "answered" } }).pipe(Effect.forkScoped)
      expect((yield* takeSoon(wa.sent)).text).toBe("answered")
      const result = yield* Fiber.join(replyFiber)
      expect(result.turnId).toBe(call.turnId)
      expect(result.sentMessageIds.length).toBe(1)

      const seen = Array.from(yield* Fiber.join(eventsFiber))
      expect(seen.map((e) => e._tag)).toEqual(["Ready", "TurnOpened", "TurnCompleted"])
    }).pipe(Effect.provide(ServerTestLayer)),
  )

  it.effect("publishes connection changes on the event stream", () =>
    Effect.gen(function* () {
      const events = yield* Events
      const api = yield* client(true)
      yield* events.publish({ _tag: "ConnectionChanged", state: "ok" })
      const stream = yield* api.events.stream({ query: { since: 0 } })
      const fiber = yield* stream.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)
      const [ready, first] = Array.from(yield* Fiber.join(fiber))
      expect(ready?._tag).toBe("Ready")
      expect(ready?.seq).toBe(1)
      expect(first?._tag).toBe("ConnectionChanged")
      expect(first?.seq).toBe(1)
    }).pipe(Effect.provide(ServerTestLayer)),
  )
})
