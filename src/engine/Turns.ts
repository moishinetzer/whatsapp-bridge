import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  HashMap,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  Stream,
} from "effect"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId, TurnId, newTurnId } from "../domain/ids.ts"
import { InboundMessage } from "../domain/message.ts"
import { WhatsApp, type WhatsAppError } from "../whatsapp/WhatsApp.ts"
import { Events } from "./Events.ts"
import { Responder, type TurnPayload } from "./Responder.ts"

// ---------------------------------------------------------------------------
// Turn model
// ---------------------------------------------------------------------------

/**
 * - `batching`   the batch window is open; more messages may join
 * - `responding` the responder is working (or, in manual mode, Intro is)
 * - `awaiting`   manual mode: waiting for `POST /turns/{id}/reply`
 * - `committed`  reply sent, 👀 cleared
 * - `superseded` newer messages interrupted it; see `supersededBy`
 * - `failed`     no reply could be sent; 👀 left in place
 */
export const TurnState = Schema.Literals(["batching", "responding", "awaiting", "committed", "superseded", "failed"])
export type TurnState = typeof TurnState.Type

export const TurnSummary = Schema.Struct({
  turnId: TurnId,
  chatId: ChatId,
  isGroup: Schema.Boolean,
  state: TurnState,
  messages: Schema.Array(InboundMessage),
  supersedes: Schema.optionalKey(TurnId),
  supersededBy: Schema.optionalKey(TurnId),
  openedAt: Schema.DateTimeUtcFromString,
  sentMessageIds: Schema.optionalKey(Schema.Array(MessageId)),
  failure: Schema.optionalKey(Schema.String),
})
export type TurnSummary = typeof TurnSummary.Type

export const isOpenState = (state: TurnState): boolean =>
  state === "batching" || state === "responding" || state === "awaiting"

export class TurnNotFound extends Schema.TaggedError<TurnNotFound>()(
  "TurnNotFound",
  { turnId: TurnId },
  { httpApiStatus: 404 },
) {}

/** The turn was interrupted by newer messages; answer `currentTurnId` instead. */
export class TurnSuperseded extends Schema.TaggedError<TurnSuperseded>()(
  "TurnSuperseded",
  { turnId: TurnId, chatId: ChatId, currentTurnId: TurnId },
  { httpApiStatus: 409 },
) {}

export class TurnNotAwaitingReply extends Schema.TaggedError<TurnNotAwaitingReply>()(
  "TurnNotAwaitingReply",
  { turnId: TurnId, state: TurnState },
  { httpApiStatus: 409 },
) {}

export class TurnFailedError extends Schema.TaggedError<TurnFailedError>()(
  "TurnFailedError",
  { turnId: TurnId, reason: Schema.String },
  { httpApiStatus: 502 },
) {}

export class TurnTimedOut extends Schema.TaggedError<TurnTimedOut>()("TurnTimedOut", {
  turnId: TurnId,
  afterMs: Schema.Number,
}) {}

export type ReplyError = TurnNotFound | TurnSuperseded | TurnNotAwaitingReply | TurnFailedError

// ---------------------------------------------------------------------------
// Registry: the source of truth for turns. Read by the HTTP API, written by the engine.
// ---------------------------------------------------------------------------

interface TurnRecord {
  readonly summary: TurnSummary
  /** Present while an external reply is awaited (manual mode / `async` handlers). */
  readonly awaiting: Option.Option<Deferred.Deferred<ReadonlyArray<string>>>
  /** Resolves when the reply is on the wire, so `reply` callers learn the outcome. */
  readonly done: Deferred.Deferred<ReadonlyArray<MessageId>, TurnFailedError>
  /** Bumped on every write; see `ChatState.generation` for why structural equality bites here. */
  readonly revision: number
}

export interface TurnRegistryShape {
  /** Turns that still want a reply. */
  readonly list: Effect.Effect<ReadonlyArray<TurnSummary>>
  readonly get: (turnId: TurnId) => Effect.Effect<TurnSummary, TurnNotFound>
  readonly openCount: Effect.Effect<number>
  /**
   * Intro's answer for a turn. Resolves once the reply is sent and 👀 cleared,
   * with the ids of the sent messages. 409 when the turn was superseded (the
   * error names the current turn) or is not waiting for an external reply.
   */
  readonly reply: (turnId: TurnId, replies: ReadonlyArray<string>) => Effect.Effect<ReadonlyArray<MessageId>, ReplyError>

  // Engine side.
  readonly open: (summary: TurnSummary) => Effect.Effect<void>
  readonly setState: (turnId: TurnId, state: TurnState) => Effect.Effect<void>
  /** Marks the turn `awaiting` and waits for `reply`. Interruptible. */
  readonly await: (turnId: TurnId) => Effect.Effect<ReadonlyArray<string>>
  readonly complete: (turnId: TurnId, sent: ReadonlyArray<MessageId>) => Effect.Effect<void>
  readonly supersede: (turnId: TurnId, by: TurnId) => Effect.Effect<void>
  readonly fail: (turnId: TurnId, reason: string) => Effect.Effect<void>
}

export class TurnRegistry extends Context.Service<TurnRegistry, TurnRegistryShape>()("bridge/TurnRegistry") {}

/** Terminal turns kept for `GET /turns/{id}` and 409 answers. Oldest are pruned past this. */
const HISTORY = 500

export const TurnRegistryLive = Layer.effect(
  TurnRegistry,
  Effect.gen(function* () {
    const turns = yield* Ref.make(HashMap.empty<TurnId, TurnRecord>())

    const prune = (map: HashMap.HashMap<TurnId, TurnRecord>): HashMap.HashMap<TurnId, TurnRecord> => {
      if (HashMap.size(map) <= HISTORY) return map
      const closed = Array.from(HashMap.values(map))
        .filter((r) => !isOpenState(r.summary.state))
        .sort((a, b) => DateTime.toEpochMillis(a.summary.openedAt) - DateTime.toEpochMillis(b.summary.openedAt))
      const excess = HashMap.size(map) - HISTORY
      return closed.slice(0, excess).reduce((m, r) => HashMap.remove(m, r.summary.turnId), map)
    }

    const update = (turnId: TurnId, f: (record: TurnRecord) => TurnRecord) =>
      Ref.update(turns, (map) =>
        Option.match(HashMap.get(map, turnId), {
          onNone: () => map,
          onSome: (record) => HashMap.set(map, turnId, { ...f(record), revision: record.revision + 1 }),
        }),
      )

    const patch = (turnId: TurnId, changes: Partial<TurnSummary>) =>
      update(turnId, (r) => ({ ...r, summary: { ...r.summary, ...changes } }))

    const getRecord = (turnId: TurnId) =>
      Ref.get(turns).pipe(
        Effect.flatMap((map) =>
          Option.match(HashMap.get(map, turnId), {
            onNone: () => Effect.fail(new TurnNotFound({ turnId })),
            onSome: Effect.succeed,
          }),
        ),
      )

    const open = Effect.fn("TurnRegistry.open")(function* (summary: TurnSummary) {
      const done = yield* Deferred.make<ReadonlyArray<MessageId>, TurnFailedError>()
      yield* Ref.update(turns, (map) => {
        const existing = HashMap.get(map, summary.turnId)
        const record: TurnRecord = {
          summary,
          awaiting: Option.none(),
          done: Option.match(existing, { onNone: () => done, onSome: (r) => r.done }),
          revision: Option.match(existing, { onNone: () => 0, onSome: (r) => r.revision + 1 }),
        }
        return prune(HashMap.set(map, summary.turnId, record))
      })
    })

    const setState = (turnId: TurnId, state: TurnState) => patch(turnId, { state })

    const awaitReply = Effect.fn("TurnRegistry.await")(function* (turnId: TurnId) {
      const deferred = yield* Deferred.make<ReadonlyArray<string>>()
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.some(deferred), summary: { ...r.summary, state: "awaiting" } }))
      return yield* Deferred.await(deferred)
    })

    const complete = Effect.fn("TurnRegistry.complete")(function* (turnId: TurnId, sent: ReadonlyArray<MessageId>) {
      yield* update(turnId, (r) => ({
        ...r,
        awaiting: Option.none(),
        summary: { ...r.summary, state: "committed", sentMessageIds: sent },
      }))
      const record = yield* getRecord(turnId).pipe(Effect.option)
      if (Option.isSome(record)) yield* Deferred.succeed(record.value.done, sent)
    })

    const supersede = Effect.fn("TurnRegistry.supersede")(function* (turnId: TurnId, by: TurnId) {
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.none(), summary: { ...r.summary, state: "superseded", supersededBy: by } }))
    })

    const fail = Effect.fn("TurnRegistry.fail")(function* (turnId: TurnId, reason: string) {
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.none(), summary: { ...r.summary, state: "failed", failure: reason } }))
      const record = yield* getRecord(turnId).pipe(Effect.option)
      if (Option.isSome(record)) yield* Deferred.fail(record.value.done, new TurnFailedError({ turnId, reason }))
    })

    const reply = Effect.fn("TurnRegistry.reply")(function* (turnId: TurnId, replies: ReadonlyArray<string>) {
      const record = yield* getRecord(turnId)
      const { summary } = record
      if (summary.state === "superseded" && summary.supersededBy !== undefined) {
        return yield* new TurnSuperseded({ turnId, chatId: summary.chatId, currentTurnId: summary.supersededBy })
      }
      if (summary.state !== "awaiting" || Option.isNone(record.awaiting)) {
        return yield* new TurnNotAwaitingReply({ turnId, state: summary.state })
      }
      const accepted = yield* Deferred.succeed(record.awaiting.value, replies)
      if (!accepted) return yield* new TurnNotAwaitingReply({ turnId, state: summary.state })
      return yield* Deferred.await(record.done).pipe(
        Effect.timeout(Duration.seconds(90)),
        Effect.catchTag("TimeoutError", () =>
          new TurnFailedError({ turnId, reason: "the reply is still being sent after 90 seconds; check /turns/{id}" }),
        ),
      )
    })

    return TurnRegistry.of({
      list: Ref.get(turns).pipe(
        Effect.map((map) =>
          Array.from(HashMap.values(map))
            .map((r) => r.summary)
            .filter((s) => isOpenState(s.state))
            .sort((a, b) => DateTime.toEpochMillis(a.openedAt) - DateTime.toEpochMillis(b.openedAt)),
        ),
      ),
      get: (turnId) => Effect.map(getRecord(turnId), (r) => r.summary),
      openCount: Ref.get(turns).pipe(
        Effect.map((map) => Array.from(HashMap.values(map)).filter((r) => isOpenState(r.summary.state)).length),
      ),
      reply,
      open,
      setState,
      await: awaitReply,
      complete,
      supersede,
      fail,
    })
  }),
)

// ---------------------------------------------------------------------------
// Engine: one worker per chat; batching, interruption, 👀 lifecycle.
// ---------------------------------------------------------------------------

interface PendingMessage {
  readonly message: InboundMessage
  /** The fiber applying 👀. Awaited (briefly) before the reaction is cleared. */
  readonly seen: Fiber.Fiber<void>
}

interface ChatState {
  /** Acknowledged messages that have not been answered yet, oldest first. */
  readonly pending: ReadonlyArray<PendingMessage>
  readonly worker: "idle" | "running"
  /** Completed by the inbound handler to interrupt the running turn. */
  readonly wake: Deferred.Deferred<void>
  /**
   * Bumped on every write. Effect v4 compares plain objects structurally and
   * `HashMap.set` keeps the old entry when the new value is `Equal`; two fresh
   * `Deferred`s are equal, so without this counter a new `wake` would never be
   * stored and the running turn could never be interrupted.
   */
  readonly generation: number
}

type Phase = "batching" | "responding" | "committed"

const idleChat = (): ChatState => ({ pending: [], worker: "idle", wake: Deferred.makeUnsafe<void>(), generation: 0 })

const describeFailure = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.findErrorOption(cause)
  if (Option.isSome(error)) {
    const e = error.value as { readonly _tag?: string; readonly detail?: string; readonly afterMs?: number; readonly message?: string }
    switch (e._tag) {
      case "HandlerFailed":
        return `responder failed: ${e.detail}`
      case "TurnTimedOut":
        return `no reply within ${Duration.format(Duration.millis(e.afterMs ?? 0))}`
      case "SendFailed":
        return `sending failed: ${e.detail}`
      case "NotConnected":
        return `not connected: ${e.detail ?? "no socket"}`
      default:
        return e.message ?? String(e)
    }
  }
  return Cause.pretty(cause)
}

/** Outbound calls are retried with jittered backoff; a reconnect usually fits inside it. */
const sendSchedule = Schedule.exponential("500 millis", 2).pipe(Schedule.jittered, Schedule.upTo({ times: 5 }))

/**
 * Consumes `WhatsApp.inbound` and drives turns. Long-lived: fork into the app
 * layer's scope, nothing to call.
 */
export const TurnEngineLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const whatsapp = yield* WhatsApp
    const events = yield* Events
    const registry = yield* TurnRegistry
    const responder = yield* Responder
    const scope = yield* Effect.scope
    const chats = yield* Ref.make(HashMap.empty<ChatId, ChatState>())

    const retrySend = <A>(effect: Effect.Effect<A, WhatsAppError>) =>
      effect.pipe(
        Effect.retry({
          schedule: sendSchedule.pipe(
            Schedule.tap(({ input, attempt }) =>
              Effect.logDebug("retrying outbound call").pipe(Effect.annotateLogs({ attempt, error: (input as { _tag: string })._tag })),
            ),
          ),
        }),
      )

    const typing = (chatId: ChatId, composing: boolean) =>
      config.typing
        ? whatsapp.setTyping(chatId, composing).pipe(
            Effect.catch((e) => Effect.logDebug("typing indicator failed").pipe(Effect.annotateLogs({ error: e._tag }))),
          )
        : Effect.void

    /** The seen acknowledgment: 👀 within seconds, retried, with the optional text fallback. */
    const applySeen = (m: InboundMessage) =>
      whatsapp.react(m.chatId, m.messageId, config.seenEmoji).pipe(
        retrySend,
        Effect.catch((e) =>
          Option.match(config.seenFallbackText, {
            onNone: () =>
              Effect.logWarning("could not apply the seen reaction").pipe(Effect.annotateLogs({ messageId: m.messageId, error: e._tag })),
            onSome: (text) =>
              whatsapp.sendText(m.chatId, text).pipe(
                retrySend,
                Effect.asVoid,
                Effect.catch((e2) =>
                  Effect.logWarning("could not send the seen fallback text").pipe(Effect.annotateLogs({ messageId: m.messageId, error: e2._tag })),
                ),
              ),
          }),
        ),
        Effect.annotateLogs({ chatId: m.chatId, messageId: m.messageId }),
        Effect.forkIn(scope),
      )

    const clearSeen = (chatId: ChatId, pending: ReadonlyArray<PendingMessage>) =>
      Effect.forEach(
        pending,
        (p) =>
          Effect.gen(function* () {
            yield* Fiber.await(p.seen).pipe(Effect.timeout("5 seconds"), Effect.ignore)
            yield* whatsapp.unreact(chatId, p.message.messageId).pipe(
              retrySend,
              Effect.catch((e) =>
                Effect.logWarning("could not clear the seen reaction").pipe(Effect.annotateLogs({ messageId: p.message.messageId, error: e._tag })),
              ),
            )
          }),
        { discard: true },
      )

    const dropPending = (chatId: ChatId, pending: ReadonlyArray<PendingMessage>) => {
      const ids = new Set(pending.map((p) => p.message.messageId))
      return Ref.update(chats, (map) =>
        HashMap.modifyAt(
          map,
          chatId,
          Option.map(
            (s): ChatState => ({ ...s, pending: s.pending.filter((p) => !ids.has(p.message.messageId)), generation: s.generation + 1 }),
          ),
        ),
      )
    }

    const payloadOf = (turn: TurnSummary): TurnPayload => ({
      v: 1,
      turnId: turn.turnId,
      chatId: turn.chatId,
      isGroup: turn.isGroup,
      messages: turn.messages,
      ...(turn.supersedes === undefined ? {} : { supersedes: turn.supersedes }),
    })

    /** One attempt at answering a turn. Interruptible until the reply is committed. */
    const runTurn = (turn: TurnSummary, pending: ReadonlyArray<PendingMessage>, phase: Ref.Ref<Phase>) =>
      Effect.gen(function* () {
        if (!Duration.isZero(config.batchWindow)) yield* Effect.sleep(config.batchWindow)
        yield* Ref.set(phase, "responding")
        yield* registry.setState(turn.turnId, "responding")
        yield* events.publish({
          _tag: "TurnOpened",
          turnId: turn.turnId,
          chatId: turn.chatId,
          isGroup: turn.isGroup,
          messages: turn.messages,
          ...(turn.supersedes === undefined ? {} : { supersedes: turn.supersedes }),
        })
        yield* typing(turn.chatId, true)

        yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const answer = yield* restore(responder.respond(payloadOf(turn)))
            const replies =
              answer._tag === "Replies"
                ? answer.replies
                : yield* restore(
                    registry.await(turn.turnId).pipe(
                      Effect.timeout(config.turnTimeout),
                      Effect.catchTag("TimeoutError", () =>
                        new TurnTimedOut({ turnId: turn.turnId, afterMs: Duration.toMillis(config.turnTimeout) }),
                      ),
                    ),
                  )
            // From here on nothing interrupts us: the reply goes out and the 👀 come off.
            yield* Ref.set(phase, "committed")
            const sent = yield* Effect.forEach(replies, (text) => whatsapp.sendText(turn.chatId, text).pipe(retrySend))
            yield* clearSeen(turn.chatId, pending)
            yield* dropPending(turn.chatId, pending)
            yield* registry.complete(turn.turnId, sent)
            yield* events.publish({ _tag: "TurnCompleted", turnId: turn.turnId, chatId: turn.chatId, sentMessageIds: sent })
            yield* Effect.logInfo("turn completed").pipe(
              Effect.annotateLogs({ turnId: turn.turnId, chatId: turn.chatId, messages: turn.messages.length, sent: sent.length }),
            )
            yield* typing(turn.chatId, false)
          }),
        )
      })

    /** Serialises turns for one chat. Ends when nothing is pending; restarted by the next message. */
    const runWorker = (chatId: ChatId) =>
      Effect.gen(function* () {
        let nextTurnId: TurnId | undefined
        let supersedes: TurnId | undefined
        while (true) {
          const snapshot = yield* Ref.modify(chats, (map) => {
            const current = Option.getOrElse(HashMap.get(map, chatId), idleChat)
            if (current.pending.length === 0) {
              return [Option.none(), HashMap.set(map, chatId, { ...current, worker: "idle", generation: current.generation + 1 })] as const
            }
            const wake = Deferred.makeUnsafe<void>()
            return [
              Option.some({ pending: current.pending, wake }),
              HashMap.set(map, chatId, { ...current, wake, generation: current.generation + 1 }),
            ] as const
          })
          if (Option.isNone(snapshot)) return
          const { pending, wake } = snapshot.value
          const first = pending[0]
          if (first === undefined) return

          const turnId = nextTurnId ?? newTurnId()
          const turn: TurnSummary = {
            turnId,
            chatId,
            isGroup: first.message.isGroup,
            state: "batching",
            messages: pending.map((p) => p.message),
            ...(supersedes === undefined ? {} : { supersedes }),
            openedAt: yield* DateTime.now,
          }
          yield* registry.open(turn)
          const phase = yield* Ref.make<Phase>("batching")

          const fiber = yield* Effect.forkChild(runTurn(turn, pending, phase))
          const outcome = yield* Effect.raceFirst(
            Fiber.await(fiber).pipe(Effect.map((exit) => ({ _tag: "Finished", exit }) as const)),
            Deferred.await(wake).pipe(Effect.as({ _tag: "Woken" } as const)),
          )
          if (outcome._tag === "Woken") yield* Fiber.interrupt(fiber)
          const exit = outcome._tag === "Finished" ? outcome.exit : yield* Fiber.await(fiber)
          const finalPhase = yield* Ref.get(phase)
          const interruptedOnly = Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)

          if (Exit.isSuccess(exit) || (finalPhase === "committed" && interruptedOnly)) {
            nextTurnId = undefined
            supersedes = undefined
            continue
          }
          if (outcome._tag === "Woken" && interruptedOnly && finalPhase === "batching") {
            // Never opened: fold the new messages in under the same id.
            nextTurnId = turnId
            continue
          }
          if (outcome._tag === "Woken" && interruptedOnly && finalPhase === "responding") {
            const by = newTurnId()
            yield* registry.supersede(turnId, by)
            yield* events.publish({ _tag: "TurnSuperseded", turnId, chatId, by })
            yield* Effect.logInfo("turn superseded by newer messages").pipe(Effect.annotateLogs({ turnId, chatId, by }))
            nextTurnId = by
            supersedes = turnId
            continue
          }
          const reason = Exit.isFailure(exit) ? describeFailure(exit.cause) : "unknown"
          yield* dropPending(chatId, pending)
          yield* registry.fail(turnId, reason)
          yield* events.publish({ _tag: "TurnFailed", turnId, chatId, reason })
          yield* Effect.logError("turn failed").pipe(Effect.annotateLogs({ turnId, chatId, reason }))
          nextTurnId = undefined
          supersedes = undefined
        }
      }).pipe(
        Effect.catchCause((cause) => Effect.logError("chat worker crashed", cause).pipe(Effect.annotateLogs({ chatId }))),
        Effect.ensuring(
          Ref.update(chats, (map) =>
            HashMap.modifyAt(map, chatId, Option.map((s): ChatState => ({ ...s, worker: "idle", generation: s.generation + 1 }))),
          ),
        ),
      )

    const onInbound = Effect.fn("TurnEngine.onInbound")(function* (message: InboundMessage) {
      const seen = yield* applySeen(message)
      const { start, wake } = yield* Ref.modify(chats, (map) => {
        const current = Option.getOrElse(HashMap.get(map, message.chatId), idleChat)
        const next: ChatState = {
          pending: [...current.pending, { message, seen }],
          worker: "running",
          wake: current.wake,
          generation: current.generation + 1,
        }
        return [{ start: current.worker === "idle", wake: current.wake }, HashMap.set(map, message.chatId, next)] as const
      })
      if (start) {
        yield* runWorker(message.chatId).pipe(Effect.forkIn(scope))
      } else {
        yield* Deferred.succeed(wake, undefined)
      }
    })

    yield* whatsapp.inbound.pipe(
      Stream.runForEach((m) =>
        onInbound(m).pipe(
          Effect.catchCause((cause) => Effect.logError("inbound handler failed", cause).pipe(Effect.annotateLogs({ messageId: m.messageId }))),
        ),
      ),
      Effect.forkScoped,
    )
    yield* Effect.logInfo("turn engine ready").pipe(
      Effect.annotateLogs({ responder: responder.kind, batchWindowMs: Duration.toMillis(config.batchWindow) }),
    )
  }),
)
