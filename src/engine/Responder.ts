import { Config, Context, Effect, Layer, Option, Schema, type Duration } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse, HttpIncomingMessage } from "effect/unstable/http"
import { BridgeConfig } from "../config.ts"
import { ChatId, TurnId } from "../domain/ids.ts"
import { InboundMessage } from "../domain/message.ts"
import type { ResponderKind } from "../domain/status.ts"

/**
 * What a responder receives: the whole batch that wants one reply. This is
 * also the JSON body the `http` responder POSTs to `INTRO_HANDLER_URL`.
 */
export const TurnPayload = Schema.Struct({
  v: Schema.Literal(1),
  turnId: TurnId,
  chatId: ChatId,
  isGroup: Schema.Boolean,
  messages: Schema.Array(InboundMessage),
  /** The turn this one replaces because newer messages interrupted it. */
  supersedes: Schema.optionalKey(TurnId),
})
export type TurnPayload = typeof TurnPayload.Type

/**
 * What an HTTP handler may answer. `text` and `replies` are both accepted;
 * `async: true` means "I will answer through POST /turns/{id}/reply".
 * An empty answer completes the turn without sending anything.
 */
export const HandlerResponse = Schema.Struct({
  text: Schema.optionalKey(Schema.NullOr(Schema.String)),
  replies: Schema.optionalKey(Schema.Array(Schema.String)),
  async: Schema.optionalKey(Schema.Boolean),
})
export type HandlerResponse = typeof HandlerResponse.Type

export type ResponderReply =
  | { readonly _tag: "Replies"; readonly replies: ReadonlyArray<string> }
  /** The reply will arrive later via the registry (`POST /turns/{id}/reply`). */
  | { readonly _tag: "External" }

export class HandlerFailed extends Schema.TaggedError<HandlerFailed>()("HandlerFailed", {
  turnId: TurnId,
  detail: Schema.String,
}) {}

export interface ResponderShape {
  readonly kind: ResponderKind
  /** Interruptible: when newer messages supersede the turn the call is cancelled. */
  readonly respond: (turn: TurnPayload) => Effect.Effect<ResponderReply, HandlerFailed>
}

/** Decides who answers a turn. The engine does not care which one is wired. */
export class Responder extends Context.Service<Responder, ResponderShape>()("bridge/Responder") {}

/** Intro pulls turns from `GET /events` and answers with `POST /turns/{id}/reply`. */
export const ManualResponderLive = Layer.succeed(
  Responder,
  Responder.of({ kind: "manual", respond: () => Effect.succeed({ _tag: "External" }) }),
)

const pongText = (turn: TurnPayload): string => {
  const n = turn.messages.length
  const parts = [n === 1 ? "pong" : `pong (${n} messages batched)`]
  if (turn.supersedes !== undefined) parts.push("— answered after an interrupt")
  return parts.join(" ")
}

/** The harness responder: waits `slow`, then answers "pong". */
export const PongResponderLive = (slow: Duration.Input) =>
  Layer.succeed(
    Responder,
    Responder.of({
      kind: "pong",
      respond: Effect.fn("Responder.pong")(function* (turn: TurnPayload) {
        yield* Effect.sleep(slow)
        return { _tag: "Replies", replies: [pongText(turn)] } as const
      }),
    }),
  )

const describeError = (e: { readonly _tag: string; readonly message?: string }): string =>
  e.message ? `${e._tag}: ${e.message}` : e._tag

/** POSTs every turn to `INTRO_HANDLER_URL`; the request is aborted if the turn is superseded. */
export const HttpResponderLive = Layer.effect(
  Responder,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const url = yield* Option.match(config.handlerUrl, {
      onNone: () => Config.string("INTRO_HANDLER_URL"),
      onSome: Effect.succeed,
    })
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.retryTransient({ times: 2, while: (e) => e.reason._tag === "TransportError" }),
    )

    const respond = Effect.fn("Responder.http")(
      function* (turn: TurnPayload) {
        const request = yield* HttpClientRequest.post(url).pipe(HttpClientRequest.schemaBodyJson(TurnPayload)(turn))
        const response = yield* client.execute(request).pipe(Effect.timeout(config.handlerTimeout))
        const ok = yield* HttpClientResponse.filterStatusOk(response)
        const body = yield* HttpIncomingMessage.schemaBodyJson(HandlerResponse)(ok)
        if (body.async === true) return { _tag: "External" } as const
        const replies = [...(body.replies ?? []), ...(body.text ? [body.text] : [])]
        return { _tag: "Replies", replies } as const
      },
      (effect, turn) =>
        effect.pipe(
          Effect.mapError((e) => new HandlerFailed({ turnId: turn.turnId, detail: describeError(e) })),
          Effect.annotateLogs({ turnId: turn.turnId, chatId: turn.chatId }),
        ),
    )

    yield* Effect.logInfo("http responder ready").pipe(Effect.annotateLogs({ url }))
    return Responder.of({ kind: "http", respond })
  }),
)

/** Picks the responder named by `BRIDGE_RESPONDER`. */
export const ResponderLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    switch (config.responder) {
      case "manual":
        return ManualResponderLive
      case "http":
        return HttpResponderLive
      case "pong":
        return PongResponderLive(config.harnessSlow)
    }
  }),
)
