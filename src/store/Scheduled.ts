import { Context, DateTime, Duration, Effect, Latch, Layer, Option, Result, Schema } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { randomUUID } from "node:crypto"
import { ChatId, MessageId } from "../domain/ids.ts"
import { OutboundMediaPayload } from "../domain/outbound.ts"
import { Events } from "../engine/Events.ts"
import { Outbound } from "../engine/Outbound.ts"
import { WhatsApp } from "../whatsapp/WhatsApp.ts"
import { StoreError } from "./Store.ts"

export const ScheduledKind = Schema.Literals(["text", "media"])
export const ScheduledStatus = Schema.Literals(["pending", "sent", "skipped", "failed", "cancelled"])

/** A send the bridge holds until `sendAt` (and, when asked, until the recipient is online). */
export const ScheduledSend = Schema.Struct({
  scheduledId: Schema.String,
  chatId: ChatId,
  kind: ScheduledKind,
  sendAt: Schema.DateTimeUtcFromString,
  deliverWhenOnline: Schema.Boolean,
  status: ScheduledStatus,
  createdAt: Schema.DateTimeUtcFromString,
  /** Set once sent. */
  messageId: Schema.optionalKey(MessageId),
  /** Why it was skipped or failed. */
  detail: Schema.optionalKey(Schema.String),
})
export type ScheduledSend = typeof ScheduledSend.Type

export class ScheduledNotFound extends Schema.TaggedError<ScheduledNotFound>()(
  "ScheduledNotFound",
  { scheduledId: Schema.String },
  { httpApiStatus: 404 },
) {
  override get message() {
    return `no pending scheduled send ${this.scheduledId}`
  }
}

/** The text form of what to send; media payloads keep their base64 or path. */
const TextPayload = Schema.Struct({ text: Schema.String, quoteMessageId: Schema.optionalKey(MessageId) })

export type ScheduleInput =
  | { readonly kind: "text"; readonly chatId: ChatId; readonly payload: typeof TextPayload.Type }
  | { readonly kind: "media"; readonly chatId: ChatId; readonly payload: OutboundMediaPayload }

export interface SchedulerShape {
  readonly schedule: (input: ScheduleInput, sendAt: DateTime.Utc, deliverWhenOnline: boolean) => Effect.Effect<ScheduledSend, StoreError>
  /** Pending sends, then the ones closed in the last day, newest first. */
  readonly list: Effect.Effect<ReadonlyArray<ScheduledSend>, StoreError>
  readonly cancel: (scheduledId: string) => Effect.Effect<void, StoreError | ScheduledNotFound>
}

export class Scheduler extends Context.Service<Scheduler, SchedulerShape>()("bridge/Scheduler") {}

/** Sends missed while the daemon was down still go out when they are at most this late. */
export const MISSED_GRACE = Duration.hours(1)
/** How long `deliverWhenOnline` keeps a send waiting for the recipient. */
export const ONLINE_HOLD = Duration.hours(24)
/** How often a held or not-yet-connected send is looked at again. */
const RECHECK = Duration.seconds(60)

interface Row {
  readonly id: string
  readonly chat_id: string
  readonly kind: string
  readonly payload: string
  readonly send_at: number
  readonly deliver_when_online: number
  readonly created_at: number
  readonly status: string
  readonly message_id: string | null
  readonly detail: string | null
}

const toSend = (row: Row): ScheduledSend => ({
  scheduledId: row.id,
  chatId: ChatId.make(row.chat_id),
  kind: row.kind === "media" ? "media" : "text",
  sendAt: DateTime.makeUnsafe(row.send_at),
  deliverWhenOnline: row.deliver_when_online === 1,
  status: row.status as ScheduledSend["status"],
  createdAt: DateTime.makeUnsafe(row.created_at),
  ...(row.message_id === null ? {} : { messageId: MessageId.make(row.message_id) }),
  ...(row.detail === null ? {} : { detail: row.detail }),
})

export const SchedulerLive = Layer.effect(
  Scheduler,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const whatsapp = yield* WhatsApp
    const outbound = yield* Outbound
    const events = yield* Events
    const wake = yield* Latch.make(false)
    const startedAt = yield* DateTime.now

    const storeError = (operation: string) => (cause: unknown) =>
      new StoreError({ operation, detail: cause instanceof Error ? cause.message : String(cause) })

    const close = (id: string, status: "sent" | "skipped" | "failed" | "cancelled", messageId?: MessageId, detail?: string) =>
      Effect.flatMap(DateTime.now, (now) =>
        sql`UPDATE scheduled SET status = ${status}, message_id = ${messageId ?? null}, detail = ${detail ?? null}, closed_at = ${DateTime.toEpochMillis(now)}
            WHERE id = ${id}`,
      ).pipe(Effect.asVoid)

    const schedule = Effect.fn("Scheduler.schedule")(function* (input: ScheduleInput, sendAt: DateTime.Utc, deliverWhenOnline: boolean) {
      const id = `sched_${randomUUID().replace(/-/g, "").slice(0, 16)}`
      const now = yield* DateTime.now
      const payload = input.kind === "text" ? Schema.encodeSync(Schema.fromJsonString(TextPayload))(input.payload) : Schema.encodeSync(Schema.fromJsonString(OutboundMediaPayload))(input.payload)
      yield* sql`INSERT INTO scheduled (id, chat_id, kind, payload, send_at, deliver_when_online, created_at, status)
        VALUES (${id}, ${input.chatId}, ${input.kind}, ${payload}, ${DateTime.toEpochMillis(sendAt)}, ${deliverWhenOnline ? 1 : 0}, ${DateTime.toEpochMillis(now)}, 'pending')`.pipe(
        Effect.mapError(storeError("schedule")),
      )
      yield* wake.open
      yield* Effect.logInfo("send scheduled").pipe(Effect.annotateLogs({ scheduledId: id, chatId: input.chatId, kind: input.kind, sendAt: DateTime.formatIso(sendAt), deliverWhenOnline }))
      return {
        scheduledId: id,
        chatId: input.chatId,
        kind: input.kind,
        sendAt,
        deliverWhenOnline,
        status: "pending" as const,
        createdAt: now,
      }
    })

    const list = Effect.flatMap(DateTime.now, (now) =>
      sql<Row>`SELECT * FROM scheduled WHERE status = 'pending' OR closed_at > ${DateTime.toEpochMillis(now) - Duration.toMillis(Duration.days(1))}
               ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, send_at ASC`,
    ).pipe(
      Effect.map((rows) => rows.map(toSend)),
      Effect.mapError(storeError("list")),
    )

    const cancel = Effect.fn("Scheduler.cancel")(function* (scheduledId: string) {
      const rows = yield* sql<Row>`SELECT * FROM scheduled WHERE id = ${scheduledId} AND status = 'pending'`.pipe(Effect.mapError(storeError("cancel")))
      if (rows.length === 0) return yield* new ScheduledNotFound({ scheduledId })
      yield* close(scheduledId, "cancelled").pipe(Effect.mapError(storeError("cancel")))
      yield* wake.open
      yield* Effect.logInfo("scheduled send cancelled").pipe(Effect.annotateLogs({ scheduledId }))
    })

    /** Sends one due row now. Returns whether it is settled (true) or should be looked at again later (false). */
    const attempt = (row: Row) =>
      Effect.gen(function* () {
        const send = toSend(row)
        const now = yield* DateTime.now
        const late = DateTime.toEpochMillis(now) - row.send_at
        if (late > Duration.toMillis(MISSED_GRACE) && row.send_at < DateTime.toEpochMillis(startedAt)) {
          const reason = `missed while the daemon was down (${Math.round(late / 60_000)} minutes late)`
          yield* close(row.id, "skipped", undefined, reason)
          yield* events.publish({ _tag: "ScheduledSkipped", scheduledId: row.id, chatId: send.chatId, reason })
          return true
        }
        if (send.deliverWhenOnline) {
          const presence = yield* whatsapp.presence(send.chatId).pipe(Effect.option)
          const online = Option.isSome(presence) && (presence.value.state === "available" || presence.value.state === "composing" || presence.value.state === "recording")
          if (!online) {
            if (late > Duration.toMillis(ONLINE_HOLD)) {
              const reason = "recipient did not come online within 24 hours"
              yield* close(row.id, "skipped", undefined, reason)
              yield* events.publish({ _tag: "ScheduledSkipped", scheduledId: row.id, chatId: send.chatId, reason })
              return true
            }
            return false
          }
        }
        const outcome = yield* Effect.gen(function* () {
          if (row.kind === "media") {
            const payload = Schema.decodeUnknownSync(Schema.fromJsonString(OutboundMediaPayload))(row.payload)
            const media = yield* outbound.load(payload)
            return yield* whatsapp.sendMedia(send.chatId, media)
          }
          const payload = Schema.decodeUnknownSync(Schema.fromJsonString(TextPayload))(row.payload)
          return yield* whatsapp.sendText(send.chatId, payload.text, { quoteMessageId: payload.quoteMessageId })
        }).pipe(Effect.result)
        if (Result.isSuccess(outcome)) {
          yield* close(row.id, "sent", outcome.success)
          yield* events.publish({ _tag: "ScheduledSent", scheduledId: row.id, chatId: send.chatId, messageId: outcome.success })
          yield* Effect.logInfo("scheduled send went out").pipe(Effect.annotateLogs({ scheduledId: row.id, messageId: outcome.success }))
          return true
        }
        const error = outcome.failure
        // No socket: WhatsApp may be reconnecting; keep trying for the grace period.
        if (error._tag === "NotConnected" && late <= Duration.toMillis(MISSED_GRACE)) return false
        const reason = error.message
        yield* close(row.id, "failed", undefined, reason)
        yield* events.publish({ _tag: "ScheduledFailed", scheduledId: row.id, chatId: send.chatId, reason })
        yield* Effect.logWarning("scheduled send failed").pipe(Effect.annotateLogs({ scheduledId: row.id, reason }))
        return true
      })

    const tick = Effect.gen(function* () {
      const now = yield* DateTime.now
      const due = yield* sql<Row>`SELECT * FROM scheduled WHERE status = 'pending' AND send_at <= ${DateTime.toEpochMillis(now)} ORDER BY send_at ASC`
      let held = false
      for (const row of due) {
        const settled = yield* attempt(row).pipe(
          Effect.catchCause((cause) => Effect.logError("scheduled send crashed", cause).pipe(Effect.annotateLogs({ scheduledId: row.id }), Effect.as(false))),
        )
        if (!settled) held = true
      }
      const next = yield* sql<{ send_at: number | null }>`SELECT MIN(send_at) AS send_at FROM scheduled WHERE status = 'pending'`
      const nextAt = next[0]?.send_at ?? null
      const after = yield* DateTime.now
      const untilNext = nextAt === null ? Duration.infinity : Duration.millis(Math.max(0, nextAt - DateTime.toEpochMillis(after)))
      return held ? Duration.min(untilNext, RECHECK) : untilNext
    })

    const runner = Effect.gen(function* () {
      while (true) {
        const sleep = yield* tick.pipe(
          Effect.catchCause((cause) => Effect.logError("scheduler tick failed", cause).pipe(Effect.as(RECHECK))),
        )
        yield* wake.close
        yield* Effect.raceFirst(Effect.sleep(Duration.isFinite(sleep) ? sleep : Duration.days(365)), wake.await)
      }
    })

    yield* Effect.forkScoped(runner)
    yield* Effect.logInfo("scheduler ready")
    return Scheduler.of({ schedule, list, cancel })
  }),
)
