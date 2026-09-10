import {
  Cache,
  Cause,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Equal,
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
import { InboundMessage, type MediaFile } from "../domain/message.ts"
import { isMedia, outgoing, type OutboundMedia, type Outgoing } from "../domain/outbound.ts"
import { WhatsApp, type InboundUpdate, type MessageError, type WhatsAppError } from "../whatsapp/WhatsApp.ts"
import { Events } from "./Events.ts"
import { MediaStore, type MediaError } from "./Media.ts"
import { Store } from "../store/Store.ts"
import { Responder, type TurnPayload } from "./Responder.ts"
import { prepareOutgoing } from "./text.ts"

// ---------------------------------------------------------------------------
// Turn model
// ---------------------------------------------------------------------------

/**
 * - `batching`   the batch window is open; more messages may join
 * - `responding` the responder is working (or, in manual mode, Intro is)
 * - `awaiting`   manual mode: waiting for `POST /turns/{id}/reply`
 * - `committed`  reply sent, the eye cleared
 * - `superseded` newer messages (or an edit) interrupted it; see `supersededBy`
 * - `failed`     no reply could be sent; the eye cleared and the failure text sent
 * - `cancelled`  every message was deleted by its sender before an answer went out
 */
export const TurnState = Schema.Literals(["batching", "responding", "awaiting", "committed", "superseded", "failed", "cancelled"])
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
  /** Groups: what was said in the chat just before this turn, oldest first. */
  context: Schema.optionalKey(Schema.Array(InboundMessage)),
})
export type TurnSummary = typeof TurnSummary.Type

/** How many earlier group messages a turn carries as context. */
const CONTEXT_SIZE = 10

export const isOpenState = (state: TurnState): boolean =>
  state === "batching" || state === "responding" || state === "awaiting"

export class TurnNotFound extends Schema.TaggedError<TurnNotFound>()(
  "TurnNotFound",
  { turnId: TurnId },
  { httpApiStatus: 404 },
) {
  override get message() {
    return `no such turn: ${this.turnId}`
  }
}

/** The turn was interrupted by newer messages; answer `currentTurnId` instead. */
export class TurnSuperseded extends Schema.TaggedError<TurnSuperseded>()(
  "TurnSuperseded",
  { turnId: TurnId, chatId: ChatId, currentTurnId: TurnId },
  { httpApiStatus: 409 },
) {
  override get message() {
    return `turn ${this.turnId} was superseded by ${this.currentTurnId}; answer that one`
  }
}

export class TurnNotAwaitingReply extends Schema.TaggedError<TurnNotAwaitingReply>()(
  "TurnNotAwaitingReply",
  { turnId: TurnId, state: TurnState },
  { httpApiStatus: 409 },
) {
  override get message() {
    return `turn ${this.turnId} is ${this.state}, not awaiting a reply`
  }
}

export class TurnFailedError extends Schema.TaggedError<TurnFailedError>()(
  "TurnFailedError",
  { turnId: TurnId, reason: Schema.String },
  { httpApiStatus: 502 },
) {
  override get message() {
    return `turn ${this.turnId} failed: ${this.reason}`
  }
}

/** Progress and abandon only make sense while the turn is being answered. */
export class TurnNotOpen extends Schema.TaggedError<TurnNotOpen>()(
  "TurnNotOpen",
  { turnId: TurnId, state: TurnState },
  { httpApiStatus: 409 },
) {
  override get message() {
    return `turn ${this.turnId} is ${this.state}, not open`
  }
}

/** The agent gave up on the turn through `POST /turns/{id}/abandon`. */
export class TurnAbandoned extends Schema.TaggedError<TurnAbandoned>()("TurnAbandoned", {
  turnId: TurnId,
  reason: Schema.String,
  text: Schema.optionalKey(Schema.String),
}) {
  override get message() {
    return `turn ${this.turnId} abandoned: ${this.reason}`
  }
}

export class TurnTimedOut extends Schema.TaggedError<TurnTimedOut>()("TurnTimedOut", {
  turnId: TurnId,
  afterMs: Schema.Number,
}) {
  override get message() {
    return `turn ${this.turnId}: no reply within ${this.afterMs} ms`
  }
}

/** A defect inside a turn (a bug, or a throwing responder), turned into a typed failure so the worker survives. */
class TurnCrashed extends Schema.TaggedError<TurnCrashed>()("TurnCrashed", {
  turnId: TurnId,
  detail: Schema.String,
}) {
  override get message() {
    return `turn ${this.turnId} crashed: ${this.detail}`
  }
}

export type ReplyError = TurnNotFound | TurnSuperseded | TurnNotAwaitingReply | TurnFailedError

/**
 * Effect v4 compares and hashes plain objects structurally, walking every key.
 * State that carries runtime handles (Fibers, Deferreds) must never be walked:
 * two fresh Deferreds compare equal (so `HashMap.set` would keep the stale
 * one) and hashing a Fiber reads function properties that throw. Marking the
 * object as reference-equal keeps Effect out of it entirely.
 */
const byRef = <T extends object>(value: T): T => Equal.byReferenceUnsafe(value)

// ---------------------------------------------------------------------------
// Registry: the source of truth for turns. Read by the HTTP API, written by the engine.
// ---------------------------------------------------------------------------

/** What `POST /turns/{id}/reply` hands the engine. */
export interface ReplyRequest {
  readonly items: ReadonlyArray<Outgoing>
  /** Edit the last progress bubble into the first text bubble instead of sending a new message. */
  readonly replaceProgress: boolean
  /** Adjacent images and videos go out as one album. */
  readonly album: boolean
}

export interface AbandonRequest {
  readonly reason: string
  /** Overrides `BRIDGE_FAILURE_TEXT` for this turn. */
  readonly text: Option.Option<string>
}

/** A progress call: extend the reply deadline, re-arm typing, remember the interim bubble. */
export interface ProgressOptions {
  readonly extendBy?: Duration.Duration | undefined
  readonly progressMessageId?: MessageId | undefined
}

interface TurnRecord {
  readonly summary: TurnSummary
  /** Present while an external reply is awaited (manual mode / `async` handlers). */
  readonly awaiting: Option.Option<Deferred.Deferred<ReplyRequest>>
  /** Resolves when the reply is on the wire, so `reply` callers learn the outcome. */
  readonly done: Deferred.Deferred<ReadonlyArray<MessageId>, TurnFailedError>
  /** Completed by `abandon`; the engine races the responder and the reply wait against it. */
  readonly abandon: Deferred.Deferred<AbandonRequest>
  /** When the wait for an external reply gives up. `progress` moves it, never past `MAX_EXTENSION` after opening. */
  readonly deadline: Ref.Ref<DateTime.Utc>
  /** The typing keep-alive stops once this passes; re-armed by `progress`. */
  readonly typingUntil: Ref.Ref<DateTime.Utc>
  /** The last interim bubble sent through `progress`, for `replaceProgress`. */
  readonly progressMessageId: Option.Option<MessageId>
  /** Bumped on every write; see `ChatState.generation` for why structural equality bites here. */
  readonly revision: number
}

/** A turn never waits longer than this after opening, however often progress is reported. */
const MAX_EXTENSION = Duration.hours(1)

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
  readonly reply: (
    turnId: TurnId,
    replies: ReadonlyArray<string | Outgoing>,
    options?: { readonly replaceProgress?: boolean | undefined; readonly album?: boolean | undefined },
  ) => Effect.Effect<ReadonlyArray<MessageId>, ReplyError>
  /** "Still working": extends the deadline (capped), re-arms typing, records the interim bubble. */
  readonly progress: (
    turnId: TurnId,
    options: ProgressOptions,
  ) => Effect.Effect<{ readonly deadline: DateTime.Utc; readonly typingUntil: DateTime.Utc }, TurnNotFound | TurnNotOpen>
  /** Give up on an open turn: it fails with `reason`; `text` replaces the configured failure text. */
  readonly abandon: (turnId: TurnId, reason: string, text?: string) => Effect.Effect<void, TurnNotFound | TurnNotOpen>

  // Engine side.
  readonly open: (summary: TurnSummary) => Effect.Effect<void>
  readonly setState: (turnId: TurnId, state: TurnState) => Effect.Effect<void>
  /** Marks the turn `awaiting` and waits for `reply`, the deadline, or `abandon`. Interruptible. */
  readonly await: (turnId: TurnId) => Effect.Effect<ReplyRequest, TurnTimedOut | TurnAbandoned>
  /** Resolves when the agent abandons the turn; never otherwise. */
  readonly abandonRequested: (turnId: TurnId) => Effect.Effect<AbandonRequest>
  /** Start the typing window (now + `BRIDGE_TYPING_MAX`). */
  readonly armTyping: (turnId: TurnId) => Effect.Effect<void>
  readonly typingUntil: (turnId: TurnId) => Effect.Effect<DateTime.Utc>
  readonly progressMessage: (turnId: TurnId) => Effect.Effect<Option.Option<MessageId>>
  readonly complete: (turnId: TurnId, sent: ReadonlyArray<MessageId>) => Effect.Effect<void>
  readonly supersede: (turnId: TurnId, by: TurnId) => Effect.Effect<void>
  readonly fail: (turnId: TurnId, reason: string) => Effect.Effect<void>
  /** Every message of the turn was deleted before an answer went out. */
  readonly cancel: (turnId: TurnId, reason: string) => Effect.Effect<void>
}

export class TurnRegistry extends Context.Service<TurnRegistry, TurnRegistryShape>()("bridge/TurnRegistry") {}

/** Terminal turns kept for `GET /turns/{id}` and 409 answers. Oldest are pruned past this. */
const HISTORY = 500

export const TurnRegistryLive = Layer.effect(
  TurnRegistry,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const store = yield* Store
    const turns = yield* Ref.make(HashMap.empty<TurnId, TurnRecord>())

    /** Every state change lands in the database; a failure there never touches the turn. */
    const persist = (turnId: TurnId) =>
      Effect.gen(function* () {
        const map = yield* Ref.get(turns)
        const record = HashMap.get(map, turnId)
        if (Option.isNone(record)) return
        yield* store.recordTurn(record.value.summary).pipe(
          Effect.catch((e) => Effect.logDebug("could not persist the turn").pipe(Effect.annotateLogs({ turnId, reason: e.message }))),
        )
      })

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
          onSome: (record) => HashMap.set(map, turnId, byRef({ ...f(record), revision: record.revision + 1 })),
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
      const abandon = yield* Deferred.make<AbandonRequest>()
      const deadline = yield* Ref.make(DateTime.addDuration(summary.openedAt, config.turnTimeout))
      const typingUntil = yield* Ref.make(summary.openedAt)
      yield* Ref.update(turns, (map) => {
        const existing = HashMap.get(map, summary.turnId)
        const record: TurnRecord = byRef({
          summary,
          awaiting: Option.none(),
          done: Option.match(existing, { onNone: () => done, onSome: (r) => r.done }),
          abandon: Option.match(existing, { onNone: () => abandon, onSome: (r) => r.abandon }),
          deadline: Option.match(existing, { onNone: () => deadline, onSome: (r) => r.deadline }),
          typingUntil: Option.match(existing, { onNone: () => typingUntil, onSome: (r) => r.typingUntil }),
          progressMessageId: Option.match(existing, { onNone: () => Option.none(), onSome: (r) => r.progressMessageId }),
          revision: Option.match(existing, { onNone: () => 0, onSome: (r) => r.revision + 1 }),
        })
        return prune(HashMap.set(map, summary.turnId, record))
      })
      yield* persist(summary.turnId)
    })

    const setState = (turnId: TurnId, state: TurnState) => Effect.andThen(patch(turnId, { state }), persist(turnId))

    const abandonRequested = (turnId: TurnId) => Effect.flatMap(getRecord(turnId).pipe(Effect.orDie), (r) => Deferred.await(r.abandon))

    const abandonFailure = (turnId: TurnId) =>
      abandonRequested(turnId).pipe(
        Effect.flatMap((req) =>
          new TurnAbandoned({ turnId, reason: req.reason, ...(Option.isSome(req.text) ? { text: req.text.value } : {}) }),
        ),
      )

    /** Fails once the (movable) deadline passes. Re-reads it after every sleep so `progress` can push it out. */
    const watchdog = (turnId: TurnId, record: TurnRecord, since: DateTime.Utc) =>
      Effect.gen(function* () {
        while (true) {
          const deadline = yield* Ref.get(record.deadline)
          const now = yield* DateTime.now
          const remaining = DateTime.toEpochMillis(deadline) - DateTime.toEpochMillis(now)
          if (remaining <= 0) {
            return yield* new TurnTimedOut({ turnId, afterMs: DateTime.toEpochMillis(now) - DateTime.toEpochMillis(since) })
          }
          yield* Effect.sleep(Duration.millis(remaining))
        }
      })

    const awaitReply = Effect.fn("TurnRegistry.await")(function* (turnId: TurnId) {
      const record = yield* getRecord(turnId).pipe(Effect.orDie)
      const deferred = yield* Deferred.make<ReplyRequest>()
      const now = yield* DateTime.now
      // The reply clock starts when the wait starts, not when the batch opened.
      yield* Ref.update(record.deadline, (d) => DateTime.max(d, DateTime.addDuration(now, config.turnTimeout)))
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.some(deferred), summary: { ...r.summary, state: "awaiting" } }))
      return yield* Effect.raceFirst(Deferred.await(deferred), Effect.raceFirst(watchdog(turnId, record, now), abandonFailure(turnId)))
    })

    const progress = Effect.fn("TurnRegistry.progress")(function* (turnId: TurnId, options: ProgressOptions) {
      const record = yield* getRecord(turnId)
      if (record.summary.state !== "responding" && record.summary.state !== "awaiting") {
        return yield* new TurnNotOpen({ turnId, state: record.summary.state })
      }
      const now = yield* DateTime.now
      const cap = DateTime.addDuration(record.summary.openedAt, MAX_EXTENSION)
      const wanted = DateTime.addDuration(now, options.extendBy ?? config.turnTimeout)
      yield* Ref.update(record.deadline, (d) => DateTime.max(d, DateTime.min(wanted, cap)))
      const typingUntil = DateTime.addDuration(now, config.typingMax)
      yield* Ref.set(record.typingUntil, typingUntil)
      if (options.progressMessageId !== undefined) {
        const id = options.progressMessageId
        yield* update(turnId, (r) => ({ ...r, progressMessageId: Option.some(id) }))
      }
      return { deadline: yield* Ref.get(record.deadline), typingUntil }
    })

    const abandon = Effect.fn("TurnRegistry.abandon")(function* (turnId: TurnId, reason: string, text?: string) {
      const record = yield* getRecord(turnId)
      if (record.summary.state !== "responding" && record.summary.state !== "awaiting") {
        return yield* new TurnNotOpen({ turnId, state: record.summary.state })
      }
      yield* Deferred.succeed(record.abandon, { reason, text: Option.fromNullishOr(text) })
    })

    const armTyping = (turnId: TurnId) =>
      Effect.gen(function* () {
        const record = yield* getRecord(turnId).pipe(Effect.orDie)
        const now = yield* DateTime.now
        yield* Ref.set(record.typingUntil, DateTime.addDuration(now, config.typingMax))
      })

    const typingUntil = (turnId: TurnId) => Effect.flatMap(getRecord(turnId).pipe(Effect.orDie), (r) => Ref.get(r.typingUntil))

    const progressMessage = (turnId: TurnId) => Effect.map(getRecord(turnId).pipe(Effect.option), Option.flatMap((r) => r.progressMessageId))

    const complete = Effect.fn("TurnRegistry.complete")(function* (turnId: TurnId, sent: ReadonlyArray<MessageId>) {
      yield* update(turnId, (r) => ({
        ...r,
        awaiting: Option.none(),
        summary: { ...r.summary, state: "committed", sentMessageIds: sent },
      }))
      const record = yield* getRecord(turnId).pipe(Effect.option)
      if (Option.isSome(record)) yield* Deferred.succeed(record.value.done, sent)
      yield* persist(turnId)
    })

    const supersede = Effect.fn("TurnRegistry.supersede")(function* (turnId: TurnId, by: TurnId) {
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.none(), summary: { ...r.summary, state: "superseded", supersededBy: by } }))
      yield* persist(turnId)
    })

    const fail = Effect.fn("TurnRegistry.fail")(function* (turnId: TurnId, reason: string) {
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.none(), summary: { ...r.summary, state: "failed", failure: reason } }))
      const record = yield* getRecord(turnId).pipe(Effect.option)
      if (Option.isSome(record)) yield* Deferred.fail(record.value.done, new TurnFailedError({ turnId, reason }))
      yield* persist(turnId)
    })

    const cancel = Effect.fn("TurnRegistry.cancel")(function* (turnId: TurnId, reason: string) {
      yield* update(turnId, (r) => ({ ...r, awaiting: Option.none(), summary: { ...r.summary, state: "cancelled" } }))
      const record = yield* getRecord(turnId).pipe(Effect.option)
      if (Option.isSome(record)) yield* Deferred.fail(record.value.done, new TurnFailedError({ turnId, reason: `cancelled: ${reason}` }))
      yield* persist(turnId)
    })

    const reply = Effect.fn("TurnRegistry.reply")(function* (
      turnId: TurnId,
      replies: ReadonlyArray<string | Outgoing>,
      options?: { readonly replaceProgress?: boolean | undefined; readonly album?: boolean | undefined },
    ) {
      const record = yield* getRecord(turnId)
      const { summary } = record
      if (summary.state === "superseded" && summary.supersededBy !== undefined) {
        return yield* new TurnSuperseded({ turnId, chatId: summary.chatId, currentTurnId: summary.supersededBy })
      }
      if (summary.state !== "awaiting" || Option.isNone(record.awaiting)) {
        return yield* new TurnNotAwaitingReply({ turnId, state: summary.state })
      }
      const request: ReplyRequest = { items: outgoing(replies), replaceProgress: options?.replaceProgress === true, album: options?.album === true }
      const accepted = yield* Deferred.succeed(record.awaiting.value, request)
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
      progress,
      abandon,
      open,
      setState,
      await: awaitReply,
      abandonRequested,
      armTyping,
      typingUntil,
      progressMessage,
      complete,
      supersede,
      fail,
      cancel,
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
  /** Local receipt time; bounds how long batching may hold a message (`BRIDGE_BATCH_MAX_WAIT`). */
  readonly acceptedAt: DateTime.Utc
  /** The automatic download of the attachment, when the policy asks for one. */
  readonly media: Option.Option<Fiber.Fiber<MediaFile, MediaError>>
}

interface ChatState {
  /** Acknowledged messages that have not been answered yet, oldest first. */
  readonly pending: ReadonlyArray<PendingMessage>
  readonly worker: "idle" | "running"
  /** The one message in this chat carrying the seen reaction, if any. */
  readonly eyed: Option.Option<MessageId>
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

const idleChat = (): ChatState => byRef({ pending: [], worker: "idle", eyed: Option.none(), wake: Deferred.makeUnsafe<void>(), generation: 0 })

/** The typing presence expires after a few seconds; re-send it this often while a turn is open. */
const TYPING_EVERY = Duration.seconds(8)
/** A "typing" report older than this no longer holds the batch window open. */
const TYPING_FRESH = Duration.seconds(6)

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
      case "TurnCrashed":
        return `crashed: ${e.detail}`
      case "TurnAbandoned":
        return `abandoned: ${(e as { reason?: string }).reason ?? ""}`
      case "MessageUnknown":
        return `message unknown: ${(e as { reason?: string }).reason ?? ""}`
      case "MessageTooOld":
        return `${(e as { operation?: string }).operation ?? "edit"} refused: the message is too old`
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
    const mediaStore = yield* MediaStore
    const store = yield* Store
    const scope = yield* Effect.scope
    const chats = yield* Ref.make(HashMap.empty<ChatId, ChatState>())
    // Groups: the last few messages per chat, handled or not, so a turn can say what was being discussed.
    const rings = yield* Ref.make(HashMap.empty<ChatId, ReadonlyArray<InboundMessage>>())
    // When each chat was last seen typing, from presence reports; the batch window stretches while they type.
    const typingSeen = yield* Ref.make(HashMap.empty<ChatId, DateTime.Utc>())

    const remember = (m: InboundMessage) =>
      m.isGroup
        ? Ref.update(rings, (map) => {
            const current = Option.getOrElse(HashMap.get(map, m.chatId), (): ReadonlyArray<InboundMessage> => [])
            const next = [...current.filter((x) => x.messageId !== m.messageId), m].slice(-CONTEXT_SIZE * 2)
            return HashMap.set(map, m.chatId, byRef(next))
          })
        : Effect.void

    /** The last messages in a group before the turn's own, oldest first. */
    const contextFor = (chatId: ChatId, turnMessages: ReadonlyArray<InboundMessage>) =>
      Effect.map(Ref.get(rings), (map) => {
        const own = new Set(turnMessages.map((m) => m.messageId))
        return Option.getOrElse(HashMap.get(map, chatId), (): ReadonlyArray<InboundMessage> => [])
          .filter((m) => !own.has(m.messageId))
          .slice(-CONTEXT_SIZE)
      })

    /** True when a presence report said "typing" within the last few seconds. */
    const recentlyTyping = (chatId: ChatId) =>
      Effect.gen(function* () {
        const seen = HashMap.get(yield* Ref.get(typingSeen), chatId)
        if (Option.isNone(seen)) return false
        const now = yield* DateTime.now
        return DateTime.toEpochMillis(now) - DateTime.toEpochMillis(seen.value) <= Duration.toMillis(TYPING_FRESH)
      })
    // Defence in depth: the transport dedupes too, but one 👀 and one turn per message is this engine's promise.
    const handled = yield* Cache.make<string, true>({ capacity: 20_000, lookup: () => Effect.succeed(true) })

    /** Retries the transient failures (no socket, WhatsApp rejected it); an unknown or too-old message is final. */
    const retrySend = <A, E extends WhatsAppError | MessageError>(effect: Effect.Effect<A, E>) =>
      effect.pipe(
        Effect.retry({
          while: (e: E) => e._tag === "NotConnected" || e._tag === "SendFailed",
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

    /**
     * The seen acknowledgment: the reaction within seconds, retried, with the
     * optional text fallback. One message per chat carries it: the previous
     * one loses it first, then the new one gets it.
     */
    const applySeen = (m: InboundMessage, previous: Option.Option<MessageId>) =>
      Effect.gen(function* () {
        if (Option.isSome(previous) && previous.value !== m.messageId) {
          yield* whatsapp.unreact(m.chatId, previous.value).pipe(
            retrySend,
            Effect.catch((e) =>
              Effect.logDebug("could not take the seen reaction off the previous message").pipe(
                Effect.annotateLogs({ previous: previous.value, error: e._tag }),
              ),
            ),
          )
        }
        yield* whatsapp.react(m.chatId, m.messageId, config.seenEmoji)
      }).pipe(
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

    /** Takes the eye off the turn's messages, if one of them carries it. A newer message keeps its own. */
    const releaseSeen = (chatId: ChatId, pending: ReadonlyArray<PendingMessage>) =>
      Effect.gen(function* () {
        const ids = new Set(pending.map((p) => p.message.messageId))
        const eyed = yield* Ref.modify(chats, (map) => {
          const current = HashMap.get(map, chatId)
          if (Option.isNone(current) || Option.isNone(current.value.eyed) || !ids.has(current.value.eyed.value)) {
            return [Option.none<MessageId>(), map] as const
          }
          return [
            current.value.eyed,
            HashMap.set(map, chatId, byRef({ ...current.value, eyed: Option.none(), generation: current.value.generation + 1 })),
          ] as const
        })
        if (Option.isNone(eyed)) return
        const carrier = pending.find((p) => p.message.messageId === eyed.value)
        if (carrier !== undefined) yield* Fiber.await(carrier.seen).pipe(Effect.timeout("3 seconds"), Effect.ignore)
        yield* whatsapp.unreact(chatId, eyed.value).pipe(
          retrySend,
          Effect.catch((e) => Effect.logWarning("could not clear the seen reaction").pipe(Effect.annotateLogs({ messageId: eyed.value, error: e._tag }))),
        )
      })

    /** The failure text, quoting the turn's newest message, so the user sees why nothing came back. */
    const sendFailureText = (chatId: ChatId, pending: ReadonlyArray<PendingMessage>, override: Option.Option<string>) =>
      Effect.gen(function* () {
        const text = Option.isSome(override) ? override : config.failureText
        const last = pending[pending.length - 1]
        if (Option.isNone(text) || last === undefined || text.value.trim().length === 0) return
        yield* whatsapp.sendText(chatId, text.value, { quoteMessageId: last.message.messageId }).pipe(
          Effect.catchTag("MessageUnknown", () => whatsapp.sendText(chatId, text.value)),
          retrySend,
          Effect.catch((e) => Effect.logWarning("could not send the failure text").pipe(Effect.annotateLogs({ chatId, error: e._tag }))),
        )
      })

    /** Re-sends the typing presence until the turn ends or its typing window closes. */
    const keepTyping = (turnId: TurnId, chatId: ChatId) =>
      config.typing
        ? Effect.gen(function* () {
            yield* registry.armTyping(turnId)
            while (true) {
              const until = yield* registry.typingUntil(turnId)
              const now = yield* DateTime.now
              if (DateTime.toEpochMillis(now) >= DateTime.toEpochMillis(until)) return
              yield* typing(chatId, true)
              yield* Effect.sleep(TYPING_EVERY)
            }
          }).pipe(Effect.ensuring(typing(chatId, false)), Effect.forkChild({ startImmediately: true }))
        : Effect.forkChild(Effect.void)

    const dropPending = (chatId: ChatId, pending: ReadonlyArray<PendingMessage>) => {
      const ids = new Set(pending.map((p) => p.message.messageId))
      return Ref.update(chats, (map) =>
        HashMap.modifyAt(
          map,
          chatId,
          Option.map(
            (s): ChatState => byRef({ ...s, pending: s.pending.filter((p) => !ids.has(p.message.messageId)), generation: s.generation + 1 }),
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
      ...(turn.context === undefined ? {} : { context: turn.context }),
    })

    /**
     * How long to keep batching: the window, unless the oldest pending message
     * is about to hit `batchMaxWait`, so a steady trickle of messages can never
     * postpone a reply forever.
     */
    const batchDelay = (pending: ReadonlyArray<PendingMessage>, typing: boolean) =>
      Effect.map(DateTime.now, (now) => {
        const oldest = pending.reduce(
          (min, p) => Math.min(min, DateTime.toEpochMillis(p.acceptedAt)),
          Number.POSITIVE_INFINITY,
        )
        const waited = Duration.millis(Math.max(0, DateTime.toEpochMillis(now) - oldest))
        // Someone still typing gets the longer cap; a steady trickle without typing does not.
        const cap = typing ? Duration.max(config.batchMaxWait, config.batchTypingWait) : config.batchMaxWait
        const remaining = Duration.subtract(cap, waited)
        return Duration.min(config.batchWindow, Duration.isNegative(remaining) ? Duration.zero : remaining)
      })

    /** Attach the downloaded file to each message whose download finished within `mediaWait`; the rest go out as they are. */
    const withMedia = (pending: ReadonlyArray<PendingMessage>) =>
      Effect.forEach(
        pending,
        (p) =>
          Option.match(p.media, {
            onNone: () => Effect.succeed(p.message),
            onSome: (fiber) =>
              Fiber.await(fiber).pipe(
                Effect.timeout(config.mediaWait),
                Effect.map((exit) => (Exit.isSuccess(exit) ? { ...p.message, media: exit.value } : p.message)),
                Effect.catchTag("TimeoutError", () =>
                  Effect.logDebug("media not ready in time; opening the turn without it").pipe(
                    Effect.annotateLogs({ messageId: p.message.messageId }),
                    Effect.as(p.message),
                  ),
                ),
              ),
          }),
        { concurrency: "unbounded" },
      )

    /** One attempt at answering a turn. Interruptible until the reply is committed. */
    const runTurn = (opened: TurnSummary, pending: ReadonlyArray<PendingMessage>, phase: Ref.Ref<Phase>) =>
      Effect.gen(function* () {
        // The window restarts with each message (a wake restarts the whole turn); typing keeps it open a little longer.
        while (true) {
          const delay = yield* batchDelay(pending, yield* recentlyTyping(opened.chatId))
          if (Duration.isZero(delay)) break
          yield* Effect.sleep(delay)
          if (!(yield* recentlyTyping(opened.chatId))) break
        }
        const messages = yield* withMedia(pending)
        const context = opened.isGroup ? yield* contextFor(opened.chatId, messages) : []
        const turn: TurnSummary = { ...opened, messages, ...(context.length === 0 ? {} : { context }) }
        yield* Ref.set(phase, "responding")
        yield* registry.open(turn)
        yield* registry.setState(turn.turnId, "responding")
        yield* events.publish({
          _tag: "TurnOpened",
          turnId: turn.turnId,
          chatId: turn.chatId,
          isGroup: turn.isGroup,
          messages: turn.messages,
          ...(turn.supersedes === undefined ? {} : { supersedes: turn.supersedes }),
          ...(turn.context === undefined ? {} : { context: turn.context }),
        })
        yield* store.setTurn(turn.chatId, turn.messages.map((m) => m.messageId), turn.turnId).pipe(Effect.ignore)
        const typingFiber = yield* keepTyping(turn.turnId, turn.chatId)

        yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const answer = yield* restore(
              Effect.raceFirst(
                responder.respond(payloadOf(turn)),
                registry.abandonRequested(turn.turnId).pipe(
                  Effect.flatMap((req) =>
                    new TurnAbandoned({ turnId: turn.turnId, reason: req.reason, ...(Option.isSome(req.text) ? { text: req.text.value } : {}) }),
                  ),
                ),
              ),
            )
            const reply: ReplyRequest =
              answer._tag === "Replies"
                ? { items: answer.replies, replaceProgress: false, album: answer.album === true }
                : yield* restore(registry.await(turn.turnId))
            // From here on nothing interrupts us: the reply goes out and the eye comes off.
            yield* Ref.set(phase, "committed")
            const progressId = yield* registry.progressMessage(turn.turnId)
            const sendItem = (item: Outgoing, index: number) =>
              Effect.gen(function* () {
                if (index === 0 && reply.replaceProgress && Option.isSome(progressId) && !isMedia(item)) {
                  yield* whatsapp.editText(turn.chatId, progressId.value, item.text).pipe(retrySend)
                  return progressId.value
                }
                // One id per item, fixed before the retries, so a timed-out send that went through is not sent twice.
                const messageId = yield* whatsapp.newOutboundId
                const send = (quoteMessageId: MessageId | undefined) =>
                  isMedia(item)
                    ? whatsapp.sendMedia(turn.chatId, item, { messageId, quoteMessageId, turnId: turn.turnId })
                    : whatsapp.sendText(turn.chatId, item.text, { messageId, quoteMessageId, turnId: turn.turnId })
                // A quote the bridge cannot resolve must not sink the reply: send it unquoted.
                return yield* send(item.quoteMessageId).pipe(
                  Effect.catchTag("MessageUnknown", (e) =>
                    Effect.logWarning("quoted message unknown; sending without the quote").pipe(
                      Effect.annotateLogs({ turnId: turn.turnId, quoted: e.messageId }),
                      Effect.andThen(send(undefined)),
                    ),
                  ),
                  retrySend,
                )
              })
            const sent: Array<MessageId> = []
            const prepared = prepareOutgoing(reply.items)
            for (let index = 0; index < prepared.length; index++) {
              const item = prepared[index]
              if (item === undefined) continue
              // With `album`, a run of adjacent images and videos becomes one album.
              if (reply.album && isMedia(item) && (item.kind === "image" || item.kind === "video")) {
                const run: Array<OutboundMedia> = []
                while (index < prepared.length) {
                  const next = prepared[index]
                  if (next === undefined || !isMedia(next) || (next.kind !== "image" && next.kind !== "video")) break
                  run.push(next)
                  index++
                }
                index--
                if (run.length > 1) {
                  const ids = yield* whatsapp.sendAlbum(turn.chatId, run, { turnId: turn.turnId }).pipe(retrySend)
                  sent.push(...ids)
                  continue
                }
                sent.push(yield* sendItem(run[0] ?? item, index))
                continue
              }
              sent.push(yield* sendItem(item, index))
            }
            yield* releaseSeen(turn.chatId, pending)
            yield* dropPending(turn.chatId, pending)
            yield* registry.complete(turn.turnId, sent)
            yield* events.publish({ _tag: "TurnCompleted", turnId: turn.turnId, chatId: turn.chatId, sentMessageIds: sent })
            yield* Effect.logInfo("turn completed").pipe(
              Effect.annotateLogs({ turnId: turn.turnId, chatId: turn.chatId, messages: turn.messages.length, sent: sent.length }),
            )
            yield* Fiber.interrupt(typingFiber)
          }),
        )
      })

    /** What the next iteration should carry over. */
    interface Carry {
      readonly nextTurnId: TurnId | undefined
      readonly supersedes: TurnId | undefined
    }

    const fresh: Carry = { nextTurnId: undefined, supersedes: undefined }

    const failTurn = (
      turnId: TurnId,
      chatId: ChatId,
      pending: ReadonlyArray<PendingMessage>,
      reason: string,
      text: Option.Option<string> = Option.none(),
    ) =>
      Effect.gen(function* () {
        yield* dropPending(chatId, pending)
        yield* registry.fail(turnId, reason)
        yield* events.publish({ _tag: "TurnFailed", turnId, chatId, reason })
        yield* Effect.logError("turn failed").pipe(Effect.annotateLogs({ turnId, chatId, reason }))
        yield* releaseSeen(chatId, pending)
        yield* sendFailureText(chatId, pending, text)
      })

    const cancelTurn = (turnId: TurnId, chatId: ChatId, reason: string) =>
      Effect.gen(function* () {
        yield* registry.cancel(turnId, reason)
        yield* events.publish({ _tag: "TurnCancelled", turnId, chatId, reason })
        yield* Effect.logInfo("turn cancelled").pipe(Effect.annotateLogs({ turnId, chatId, reason }))
      })

    const pendingOf = (chatId: ChatId) =>
      Effect.map(Ref.get(chats), (map) => Option.match(HashMap.get(map, chatId), { onNone: () => [], onSome: (s) => s.pending }))

    /** One turn for one chat: returns what the next iteration carries, or `None` when nothing is pending. */
    const iteration = (chatId: ChatId, carry: Carry) =>
      Effect.gen(function* () {
        const snapshot = yield* Ref.modify(chats, (map) => {
          const current = Option.getOrElse(HashMap.get(map, chatId), idleChat)
          if (current.pending.length === 0) {
            return [Option.none(), HashMap.set(map, chatId, byRef({ ...current, worker: "idle", generation: current.generation + 1 }))] as const
          }
          const wake = Deferred.makeUnsafe<void>()
          return [
            Option.some({ pending: current.pending, wake }),
            HashMap.set(map, chatId, byRef({ ...current, wake, generation: current.generation + 1 })),
          ] as const
        })
        if (Option.isNone(snapshot)) return Option.none<Carry>()
        const { pending, wake } = snapshot.value
        const first = pending[0]
        if (first === undefined) return Option.none<Carry>()

        const turnId = carry.nextTurnId ?? newTurnId()
        const turn: TurnSummary = {
          turnId,
          chatId,
          isGroup: first.message.isGroup,
          state: "batching",
          messages: pending.map((p) => p.message),
          ...(carry.supersedes === undefined ? {} : { supersedes: carry.supersedes }),
          openedAt: yield* DateTime.now,
        }
        yield* registry.open(turn)
        const phase = yield* Ref.make<Phase>("batching")

        // Typed failures stay typed; only defects (bugs, throwing responders) become `TurnCrashed`.
        const body = runTurn(turn, pending, phase).pipe(
          Effect.catchCauseIf(
            (cause) => Cause.hasDies(cause) && !Cause.hasInterrupts(cause),
            (cause) => Effect.fail(new TurnCrashed({ turnId, detail: describeFailure(cause) })),
          ),
        )
        const fiber = yield* Effect.forkChild(body)
        const outcome = yield* Effect.raceFirst(
          Fiber.await(fiber).pipe(Effect.map((exit) => ({ _tag: "Finished", exit }) as const)),
          Deferred.await(wake).pipe(Effect.as({ _tag: "Woken" } as const)),
        )
        if (outcome._tag === "Woken") yield* Fiber.interrupt(fiber)
        const exit = outcome._tag === "Finished" ? outcome.exit : yield* Fiber.await(fiber)
        const finalPhase = yield* Ref.get(phase)
        const interruptedOnly = Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)

        if (Exit.isSuccess(exit) || (finalPhase === "committed" && interruptedOnly)) {
          return Option.some(fresh)
        }
        if (outcome._tag === "Woken" && interruptedOnly && finalPhase !== "committed" && (yield* pendingOf(chatId)).length === 0) {
          // Woken by a delete that emptied the chat: nothing left to answer.
          yield* cancelTurn(turnId, chatId, "every message was deleted by its sender")
          return Option.some(fresh)
        }
        if (outcome._tag === "Woken" && interruptedOnly && finalPhase === "batching") {
          // Never opened: fold the new messages in under the same id.
          return Option.some<Carry>({ nextTurnId: turnId, supersedes: carry.supersedes })
        }
        if (outcome._tag === "Woken" && interruptedOnly && finalPhase === "responding") {
          const by = newTurnId()
          yield* registry.supersede(turnId, by)
          yield* events.publish({ _tag: "TurnSuperseded", turnId, chatId, by })
          yield* Effect.logInfo("turn superseded by newer messages").pipe(Effect.annotateLogs({ turnId, chatId, by }))
          return Option.some<Carry>({ nextTurnId: by, supersedes: turnId })
        }
        const reason = Exit.isFailure(exit) ? describeFailure(exit.cause) : "unknown"
        const abandoned = Exit.isFailure(exit) ? Option.getOrUndefined(Cause.findErrorOption(exit.cause)) : undefined
        const text =
          abandoned !== undefined && typeof abandoned === "object" && (abandoned as { _tag?: string })._tag === "TurnAbandoned"
            ? Option.fromNullishOr((abandoned as { text?: string }).text)
            : Option.none<string>()
        yield* failTurn(turnId, chatId, pending, reason, text)
        return Option.some(fresh)
      })

    /**
     * Serialises turns for one chat. Ends when nothing is pending; restarted by
     * the next message. A defect in one iteration is logged and the worker
     * carries on with the next message rather than dying with messages pending.
     */
    const runWorker = (chatId: ChatId) =>
      Effect.gen(function* () {
        let carry: Carry = fresh
        while (true) {
          const next = yield* iteration(chatId, carry).pipe(
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterrupts(cause),
              (cause) =>
                Effect.logError("chat worker iteration crashed; continuing", cause).pipe(
                  Effect.annotateLogs({ chatId }),
                  Effect.andThen(
                    // Drop whatever this iteration was holding so the crash cannot repeat on the same input.
                    Ref.update(chats, (map) =>
                      HashMap.modifyAt(map, chatId, Option.map((s): ChatState => byRef({ ...s, pending: [], generation: s.generation + 1 }))),
                    ),
                  ),
                  Effect.as(Option.some(fresh)),
                ),
            ),
          )
          if (Option.isNone(next)) return
          carry = next.value
        }
      }).pipe(
        Effect.ensuring(
          Ref.update(chats, (map) =>
            HashMap.modifyAt(map, chatId, Option.map((s): ChatState => byRef({ ...s, worker: "idle", generation: s.generation + 1 }))),
          ),
        ),
      )

    const onInbound = Effect.fn("TurnEngine.onInbound")(function* (message: InboundMessage) {
      const key = `${message.chatId}:${message.messageId}`
      if (yield* Cache.has(handled, key)) {
        yield* Effect.logDebug("message already handled; ignoring duplicate").pipe(Effect.annotateLogs({ messageId: message.messageId }))
        return
      }
      yield* Cache.set(handled, key, true)
      yield* remember(message)
      const acceptedAt = yield* DateTime.now
      const previous = yield* Ref.modify(chats, (map) => {
        const current = Option.getOrElse(HashMap.get(map, message.chatId), idleChat)
        return [current.eyed, HashMap.set(map, message.chatId, byRef({ ...current, eyed: Option.some(message.messageId), generation: current.generation + 1 }))] as const
      })
      const seen = yield* applySeen(message, previous)
      if (config.readReceipts) {
        yield* whatsapp.markRead(message.chatId, [message.messageId]).pipe(
          retrySend,
          Effect.catch((e) => Effect.logWarning("could not mark the message read").pipe(Effect.annotateLogs({ messageId: message.messageId, error: e._tag }))),
          Effect.forkIn(scope),
        )
      }
      const media =
        message.hasMedia && mediaStore.autoKinds.has(message.kind)
          ? Option.some(
              yield* mediaStore.get(message.chatId, message.messageId).pipe(
                Effect.tapError((e) =>
                  Effect.logWarning("automatic media download failed").pipe(Effect.annotateLogs({ messageId: message.messageId, reason: e.message })),
                ),
                Effect.forkIn(scope),
              ),
            )
          : Option.none()
      const { start, wake } = yield* Ref.modify(chats, (map) => {
        const current = Option.getOrElse(HashMap.get(map, message.chatId), idleChat)
        const next: ChatState = byRef({
          pending: [...current.pending, { message, seen, acceptedAt, media }],
          worker: "running",
          eyed: current.eyed,
          wake: current.wake,
          generation: current.generation + 1,
        })
        return [{ start: current.worker === "idle", wake: current.wake }, HashMap.set(map, message.chatId, next)] as const
      })
      if (start) {
        // The first message of a chat subscribes to its presence, so typing can hold the batch window.
        yield* whatsapp.presence(message.chatId).pipe(Effect.ignore, Effect.forkIn(scope))
        yield* runWorker(message.chatId).pipe(Effect.forkIn(scope))
      } else {
        yield* Deferred.succeed(wake, undefined)
      }
    })

    const onPresence = (info: { readonly chatId: ChatId; readonly state: string; readonly at?: DateTime.Utc | undefined }) =>
      info.state === "composing" || info.state === "recording"
        ? Effect.flatMap(DateTime.now, (now) => Ref.update(typingSeen, (map) => HashMap.set(map, info.chatId, info.at ?? now)))
        : Ref.update(typingSeen, (map) => HashMap.remove(map, info.chatId))

    /** Edits and deletes of pending messages change the turn; reactions are only reported. */
    const onUpdate = Effect.fn("TurnEngine.onUpdate")(function* (u: InboundUpdate) {
      switch (u._tag) {
        case "Reaction":
          yield* events.publish({
            _tag: "ReactionReceived",
            chatId: u.chatId,
            messageId: u.messageId,
            senderId: u.senderId,
            emoji: u.emoji,
            onOurMessage: u.onOurMessage,
          })
          return
        case "Edited": {
          yield* events.publish({ _tag: "MessageEdited", chatId: u.chatId, messageId: u.messageId, text: u.text })
          const wake = yield* Ref.modify(chats, (map) => {
            const current = HashMap.get(map, u.chatId)
            if (Option.isNone(current) || !current.value.pending.some((p) => p.message.messageId === u.messageId)) {
              return [Option.none<Deferred.Deferred<void>>(), map] as const
            }
            const pending = current.value.pending.map((p) =>
              p.message.messageId === u.messageId ? { ...p, message: { ...p.message, text: u.text } } : p,
            )
            return [
              Option.some(current.value.wake),
              HashMap.set(map, u.chatId, byRef({ ...current.value, pending, generation: current.value.generation + 1 })),
            ] as const
          })
          // Batching: folds under the same turn. Responding or awaiting: supersedes with the corrected text.
          if (Option.isSome(wake)) yield* Deferred.succeed(wake.value, undefined)
          return
        }
        case "Deleted": {
          yield* events.publish({ _tag: "MessageDeleted", chatId: u.chatId, messageId: u.messageId })
          const outcome = yield* Ref.modify(chats, (map) => {
            const current = HashMap.get(map, u.chatId)
            if (Option.isNone(current) || !current.value.pending.some((p) => p.message.messageId === u.messageId)) {
              return [{ wake: Option.none<Deferred.Deferred<void>>(), reEye: Option.none<InboundMessage>() }, map] as const
            }
            const pending = current.value.pending.filter((p) => p.message.messageId !== u.messageId)
            const lostEye = Option.isSome(current.value.eyed) && current.value.eyed.value === u.messageId
            const newest = pending[pending.length - 1]
            const eyed = lostEye ? (newest === undefined ? Option.none<MessageId>() : Option.some(newest.message.messageId)) : current.value.eyed
            return [
              { wake: Option.some(current.value.wake), reEye: lostEye && newest !== undefined ? Option.some(newest.message) : Option.none<InboundMessage>() },
              HashMap.set(map, u.chatId, byRef({ ...current.value, pending, eyed, generation: current.value.generation + 1 })),
            ] as const
          })
          // The eye was on the deleted message: it moves back to the newest one still waiting.
          if (Option.isSome(outcome.reEye)) yield* applySeen(outcome.reEye.value, Option.none())
          if (Option.isSome(outcome.wake)) yield* Deferred.succeed(outcome.wake.value, undefined)
          return
        }
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
    yield* whatsapp.updates.pipe(
      Stream.runForEach((u) =>
        onUpdate(u).pipe(
          Effect.catchCause((cause) => Effect.logError("update handler failed", cause).pipe(Effect.annotateLogs({ messageId: u.messageId }))),
        ),
      ),
      Effect.forkScoped,
    )
    yield* whatsapp.observed.pipe(Stream.runForEach(remember), Effect.forkScoped)
    yield* whatsapp.presenceUpdates.pipe(Stream.runForEach(onPresence), Effect.forkScoped)
    yield* Effect.logInfo("turn engine ready").pipe(
      Effect.annotateLogs({
        responder: responder.kind,
        batchWindowMs: Duration.toMillis(config.batchWindow),
        batchMaxWaitMs: Duration.toMillis(config.batchMaxWait),
      }),
    )
  }),
)
