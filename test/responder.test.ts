import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { Effect, Layer, Option, Ref } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { TurnId } from "../src/domain/ids.ts"
import { isMedia } from "../src/domain/outbound.ts"
import { OutboundLive } from "../src/engine/Outbound.ts"
import { HttpResponderLive, Responder, type TurnPayload } from "../src/engine/Responder.ts"
import { TestConfigLive, chat, message } from "./support/fakes.ts"

const turn: TurnPayload = { v: 1, turnId: TurnId.make("turn_test"), chatId: chat, isGroup: false, messages: [message("hi")] }

/** An `HttpClient` that answers every request with the given status and JSON body, recording what it was sent. */
const fakeHandler = (status: number, body: unknown) =>
  Effect.gen(function* () {
    const requests = yield* Ref.make<ReadonlyArray<string>>([])
    const client = HttpClient.make((request) =>
      Effect.gen(function* () {
        const text = request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : ""
        yield* Ref.update(requests, (r) => [...r, text])
        return HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } }),
        )
      }),
    )
    return { requests, layer: Layer.succeed(HttpClient.HttpClient, client) }
  })

const responder = (client: Layer.Layer<HttpClient.HttpClient>) =>
  HttpResponderLive.pipe(
    Layer.provide([
      TestConfigLive({ responder: "http", handlerUrl: Option.some("http://handler.test/turn") }),
      OutboundLive.pipe(Layer.provide([TestConfigLive(), NodeServices.layer])),
      client,
    ]),
  )

describe("http responder", () => {
  it.effect("POSTs the turn and returns text, replies and attachments in order", () =>
    Effect.gen(function* () {
      const handler = yield* fakeHandler(200, {
        replies: ["first"],
        text: "second",
        attachments: [{ data: Buffer.from([1, 2, 3]).toString("base64"), mimetype: "image/png", caption: "third" }],
      })
      const answer = yield* Responder.use((r) => r.respond(turn)).pipe(Effect.provide(responder(handler.layer)))
      expect(answer._tag).toBe("Replies")
      if (answer._tag !== "Replies") return
      expect(answer.replies.length).toBe(3)
      expect(answer.replies[0]).toBe("first")
      expect(answer.replies[1]).toBe("second")
      const file = answer.replies[2]
      expect(file !== undefined && isMedia(file) && file.kind === "image" && file.caption === "third").toBe(true)
      const posted = yield* Ref.get(handler.requests)
      expect(JSON.parse(posted[0] ?? "{}")).toMatchObject({ v: 1, turnId: "turn_test", chatId: chat })
    }),
  )

  it.effect("a non-2xx answer or an unsendable attachment fails the turn with a reason", () =>
    Effect.gen(function* () {
      const failing = yield* fakeHandler(500, { error: "boom" })
      const status = yield* Responder.use((r) => r.respond(turn)).pipe(Effect.provide(responder(failing.layer)), Effect.flip)
      expect(status._tag).toBe("HandlerFailed")
      expect(status._tag === "HandlerFailed" && status.detail).toContain("500")

      const badFile = yield* fakeHandler(200, { text: "look", attachments: [{ path: "/definitely/not/here.png" }] })
      const unreadable = yield* Responder.use((r) => r.respond(turn)).pipe(Effect.provide(responder(badFile.layer)), Effect.flip)
      expect(unreadable._tag).toBe("HandlerFailed")
      expect(unreadable._tag === "HandlerFailed" && unreadable.detail).toContain("OutboundFileUnreadable")

      const asyncAnswer = yield* fakeHandler(200, { async: true })
      const external = yield* Responder.use((r) => r.respond(turn)).pipe(Effect.provide(responder(asyncAnswer.layer)))
      expect(external._tag).toBe("External")
    }),
  )
})
