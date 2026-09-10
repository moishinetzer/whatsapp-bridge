import makeWASocket, {
  type AnyMessageContent,
  Browsers,
  DisconnectReason,
  downloadMediaMessage,
  extensionForMediaMessage,
  fetchLatestBaileysVersion,
  generateMessageIDV2,
  getContentType,
  normalizeMessageContent,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  type ConnectionState,
  WAMessageStubType,
  type WAMessage,
  type WAMessageKey,
  type WAMessageUpdate,
  type WASocket,
  type WAVersion,
  type proto,
} from "@whiskeysockets/baileys"
import { Cache, Cause, DateTime, Deferred, Duration, Effect, FiberSet, Layer, Option, Queue, Ref, Schedule, Schema, Stream } from "effect"

/** protobuf's 64-bit numbers arrive as `Long` objects; only `toNumber` is needed here. */
interface LongLike {
  toNumber(): number
}
import qrcode from "qrcode-terminal"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId, phoneOfJid } from "../domain/ids.ts"
import type { InboundMessage } from "../domain/message.ts"
import type { OutboundMedia } from "../domain/outbound.ts"
import type { LinkedIdentity } from "../domain/status.ts"
import { Events } from "../engine/Events.ts"
import { Health } from "../engine/Health.ts"
import { AuthStore } from "./AuthStore.ts"
import { isBaileysLogLevel, makeBaileysLogger } from "./BaileysLogger.ts"
import { extractInbound, textOf, type Extracted } from "./extract.ts"
import {
  MediaDownloadFailed,
  MediaTooLarge,
  MediaUnavailable,
  MessageTooOld,
  MessageUnknown,
  NotConnected,
  SendFailed,
  WhatsApp,
  type DownloadedMedia,
  type InboundUpdate,
  type SendOptions,
} from "./WhatsApp.ts"

export interface BaileysOptions {
  /**
   * `link`: show a QR (or request a pairing code for `phone`) and resolve
   * `linked` once the phone accepted. `serve`: require an existing session and
   * keep it connected forever; report `needs_relink` instead of connecting when
   * there is none.
   */
  readonly mode: "link" | "serve"
  readonly phone?: string | undefined
}

/** Everything the socket's emitter tells us, funnelled into one queue so it is handled inside Effect. */
type SocketEvent =
  | { readonly _tag: "Connection"; readonly update: Partial<ConnectionState> }
  | { readonly _tag: "Creds" }
  | { readonly _tag: "Messages"; readonly messages: ReadonlyArray<WAMessage>; readonly type: "append" | "notify" }
  | { readonly _tag: "Updates"; readonly updates: ReadonlyArray<WAMessageUpdate> }
  | { readonly _tag: "Reactions"; readonly reactions: ReadonlyArray<{ readonly key: WAMessageKey; readonly reaction: proto.IReaction }> }

/** WhatsApp's own limits on touching a sent message. */
const EDIT_WINDOW = Duration.minutes(15)
const DELETE_WINDOW = Duration.days(2)

export type CloseOutcome =
  | { readonly _tag: "Reconnect"; readonly immediate: boolean; readonly detail: string; readonly uptime: Duration.Duration }
  | { readonly _tag: "Terminal"; readonly detail: string; readonly loggedOut: boolean }
  | { readonly _tag: "Linked" }

/** Typed failure that drives the reconnect schedule: the socket closed soon after opening. */
class ReconnectNeeded extends Schema.TaggedError<ReconnectNeeded>()("ReconnectNeeded", {
  detail: Schema.String,
  uptimeMs: Schema.Number,
}) {
  override get message() {
    return `reconnect needed: ${this.detail}`
  }
}

class SocketCreateFailed extends Schema.TaggedError<SocketCreateFailed>()("SocketCreateFailed", {
  detail: Schema.String,
}) {
  override get message() {
    return `socket setup failed: ${this.detail}`
  }
}

class SupervisorCrashed extends Schema.TaggedError<SupervisorCrashed>()("SupervisorCrashed", {
  detail: Schema.String,
}) {
  override get message() {
    return `supervisor crashed: ${this.detail}`
  }
}

const statusCodeOf = (error: unknown): number | undefined => {
  if (typeof error !== "object" || error === null) return undefined
  const output = (error as { output?: { statusCode?: unknown } }).output
  return typeof output?.statusCode === "number" ? output.statusCode : undefined
}

const messageOf = (error: unknown): string =>
  error instanceof Error ? error.message : error === undefined ? "connection closed" : String(error)

/** What a socket close means for us. Exported for tests. */
export const classifyClose = (error: unknown, uptime: Duration.Duration): CloseOutcome => {
  const code = statusCodeOf(error)
  const detail = `${messageOf(error)}${code === undefined ? "" : ` (${code})`}`
  switch (code) {
    case DisconnectReason.loggedOut:
      return { _tag: "Terminal", detail: `logged out by WhatsApp: ${detail}`, loggedOut: true }
    case DisconnectReason.forbidden:
      return { _tag: "Terminal", detail: `forbidden, the number may be banned: ${detail}`, loggedOut: false }
    case DisconnectReason.multideviceMismatch:
    case DisconnectReason.badSession:
      return { _tag: "Terminal", detail: `session rejected: ${detail}`, loggedOut: true }
    case DisconnectReason.restartRequired:
      return { _tag: "Reconnect", immediate: true, detail, uptime }
    default:
      return { _tag: "Reconnect", immediate: false, detail, uptime }
  }
}

/** Exponential backoff with jitter, capped at a minute, forever. A healthy run resets it. */
/** One text, reaction or presence call. */
const SEND_TIMEOUT = Duration.seconds(30)
/** A file has to be encrypted and uploaded first; large documents take a while. */
const MEDIA_SEND_TIMEOUT = Duration.minutes(3)

const reconnectSchedule: Schedule.Schedule<Duration.Duration, ReconnectNeeded> = Schedule.exponential("1 second", 2).pipe(
  Schedule.jittered,
  Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, Duration.seconds(60)))),
  Schedule.tap(({ input, attempt, duration }) =>
    Effect.logWarning("connection closed, reconnecting").pipe(
      Effect.annotateLogs({ reason: input.detail, attempt, delayMs: Duration.toMillis(duration), uptimeMs: input.uptimeMs }),
    ),
  ),
)

/** A connection that stayed up this long counts as healthy: its close starts a fresh backoff. */
const healthyUptime = Duration.seconds(30)

/** "Restart required" closes reconnect at once, but only this many times in a row before backing off. */
const MAX_IMMEDIATE_RECONNECTS = 3

const notInterrupted = (cause: Cause.Cause<unknown>): boolean => !Cause.hasInterrupts(cause)

const dedupeKey = (chatId: ChatId, messageId: MessageId): string => `${chatId}:${messageId}`

/** Errors thrown by the socket's promise API, turned into `SendFailed`. */
const sendFailed = (operation: string, chatId: ChatId) => (cause: unknown) =>
  new SendFailed({ operation, chatId, detail: messageOf(cause) })

export const BaileysLive = (options: BaileysOptions) =>
  Layer.effect(
    WhatsApp,
    Effect.gen(function* () {
      const config = yield* BridgeConfig
      const auth = yield* AuthStore
      const health = yield* Health
      const events = yield* Events

      // Logs from inside the protocol library land in our logger, redacted.
      const runFork = yield* FiberSet.makeRuntime<never>()
      const logger = makeBaileysLogger(
        (effect) => runFork(effect),
        isBaileysLogLevel(config.baileysLogLevel) ? config.baileysLogLevel : "warn",
      )

      // One process per session; a second one fails the layer with the first one's pid.
      yield* auth.lock

      const inbound = yield* Queue.unbounded<InboundMessage>()
      const updates = yield* Queue.unbounded<InboundUpdate>()
      const socket = yield* Ref.make<Option.Option<WASocket>>(Option.none())
      const linked = yield* Deferred.make<LinkedIdentity>()
      // Every accepted inbound message, raw: reactions need the full key, quoting and reading need the message.
      const inboundRaw = yield* Cache.make<string, WAMessage>({
        capacity: 5_000,
        lookup: (key) => Effect.die(`no message cached for ${key}`),
      })
      // Idempotency: one 👀 and one turn per message, even if WhatsApp re-delivers.
      const seen = yield* Cache.make<string, true>({ capacity: 20_000, lookup: () => Effect.succeed(true) })
      // Raw media messages seen this run: the encrypted blob's location and key live in them.
      const rawMedia = yield* Cache.make<string, { readonly raw: WAMessage; readonly message: InboundMessage }>({
        capacity: 2_000,
        lookup: (key) => Effect.die(`no media message cached for ${key}`),
      })
      // What we sent recently: answers WhatsApp's "resend this" requests, and is what edits, deletes and quotes of our own messages need.
      const outbox = yield* Cache.make<string, { readonly raw: WAMessage; readonly sentAt: DateTime.Utc }>({
        capacity: 1_000,
        lookup: (id) => Effect.die(`not in outbox: ${id}`),
      })
      const lastVersion = yield* Ref.make<Option.Option<WAVersion>>(Option.none())
      const immediateStreak = yield* Ref.make(0)

      /** The current WhatsApp Web version, refreshed per attempt; falls back to the last known, then to the library's bundled one. */
      const fetchVersion = Effect.tryPromise(() => fetchLatestBaileysVersion()).pipe(
        Effect.timeout("10 seconds"),
        Effect.map((r) => r.version),
        Effect.tap((v) => Ref.set(lastVersion, Option.some(v))),
        Effect.map(Option.some),
        Effect.catchCause((cause) =>
          Effect.logDebug("WhatsApp Web version lookup failed; using the last known").pipe(
            Effect.annotateLogs({ reason: messageOf(Cause.squash(cause)) }),
            Effect.andThen(Ref.get(lastVersion)),
          ),
        ),
      )

      const allowedSender = (senderId: string, chatId: string, senderNumber: string | undefined): boolean => {
        if (config.allowedSenders.length === 0) return true
        return config.allowedSenders.some((s) => s === senderId || s === chatId || (senderNumber !== undefined && s === senderNumber))
      }
      const allowed = (message: InboundMessage): boolean => allowedSender(message.senderId, message.chatId, message.senderNumber)

      const decode = (raw: WAMessage, now: DateTime.Utc) =>
        Effect.try({ try: () => extractInbound(raw, now), catch: (e) => e }).pipe(
          Effect.catch((e) =>
            Effect.logWarning("could not decode an inbound message; skipped").pipe(
              Effect.annotateLogs({ reason: messageOf(e), messageId: raw.key?.id ?? undefined }),
              Effect.as(Option.none<Extracted>()),
            ),
          ),
        )

      const acceptMessage = (raw: WAMessage, type: "append" | "notify") =>
        Effect.gen(function* () {
          const now = yield* DateTime.now
          const extracted = yield* decode(raw, now)
          if (Option.isNone(extracted)) return
          const { message } = extracted.value
          const log = Effect.annotateLogs({ chatId: message.chatId, messageId: message.messageId, type })

          if (type === "append") {
            const age = Duration.millis(DateTime.toEpochMillis(now) - DateTime.toEpochMillis(message.timestamp))
            if (Duration.isZero(config.offlineWindow) || Duration.isGreaterThan(age, config.offlineWindow)) {
              yield* Effect.logDebug("ignoring replayed message outside the offline window").pipe(log)
              return
            }
          }
          if (message.isGroup && !config.groups) {
            yield* Effect.logDebug("ignoring group message (BRIDGE_GROUPS=false)").pipe(log)
            return
          }
          if (!allowed(message)) {
            yield* Effect.logInfo("ignoring message from a sender outside BRIDGE_ALLOWED_SENDERS").pipe(log)
            return
          }
          const id = dedupeKey(message.chatId, message.messageId)
          if (yield* Cache.has(seen, id)) {
            yield* Effect.logDebug("duplicate delivery ignored").pipe(log)
            return
          }
          yield* Cache.set(seen, id, true)
          yield* Cache.set(inboundRaw, id, raw)
          if (message.hasMedia) yield* Cache.set(rawMedia, id, { raw, message })
          yield* health.touch("inbound")
          yield* Effect.logInfo("inbound message").pipe(log, Effect.annotateLogs({ kind: message.kind, isGroup: message.isGroup }))
          if (config.logBodies) yield* Effect.logDebug("inbound body").pipe(log, Effect.annotateLogs({ text: message.text }))
          yield* events.publish({ _tag: "MessageReceived", message })
          yield* Queue.offer(inbound, message)
        })

      /** Edits and deletes of messages we accepted. Anything else (our own messages, unknown chats) is ignored. */
      const acceptUpdate = (u: WAMessageUpdate) =>
        Effect.gen(function* () {
          const id = u.key.id ?? undefined
          const chat = u.key.remoteJid ?? undefined
          if (!id || !chat || u.key.fromMe) return
          const cached = yield* Cache.getOption(inboundRaw, dedupeKey(ChatId.make(chat), MessageId.make(id)))
          if (Option.isNone(cached)) return
          const chatId = ChatId.make(chat)
          const messageId = MessageId.make(id)
          const log = Effect.annotateLogs({ chatId, messageId })
          if (u.update.message === null || u.update.messageStubType === WAMessageStubType.REVOKE) {
            yield* Effect.logInfo("message deleted by its sender").pipe(log)
            yield* Queue.offer(updates, { _tag: "Deleted", chatId, messageId })
            return
          }
          const edited = u.update.message?.editedMessage?.message ?? undefined
          if (edited) {
            const content = normalizeMessageContent(edited)
            const text = content ? (textOf(content) ?? "") : ""
            yield* Cache.set(inboundRaw, dedupeKey(chatId, messageId), { ...cached.value, message: edited })
            yield* Effect.logInfo("message edited by its sender").pipe(log)
            if (config.logBodies) yield* Effect.logDebug("edited body").pipe(log, Effect.annotateLogs({ text }))
            yield* Queue.offer(updates, { _tag: "Edited", chatId, messageId, text })
          }
        })

      /** Reactions in handled chats, ours excluded (the seen reaction comes back as an event too). */
      const acceptReaction = (item: { readonly key: WAMessageKey; readonly reaction: proto.IReaction }) =>
        Effect.gen(function* () {
          const target = item.key
          const reactor = item.reaction.key ?? undefined
          const chat = target.remoteJid ?? undefined
          const id = target.id ?? undefined
          if (!chat || !id || reactor?.fromMe) return
          if (isJidBroadcast(chat) || isJidNewsletter(chat)) return
          const isGroup = isJidGroup(chat) === true
          if (isGroup && !config.groups) return
          const senderId = reactor?.participant ?? reactor?.remoteJid ?? chat
          if (!allowedSender(senderId, chat, phoneOfJid(senderId))) return
          const chatId = ChatId.make(chat)
          const messageId = MessageId.make(id)
          const emoji = item.reaction.text ?? ""
          yield* Effect.logInfo(emoji === "" ? "reaction removed" : "reaction received").pipe(
            Effect.annotateLogs({ chatId, messageId, senderId, emoji, onOurMessage: target.fromMe === true }),
          )
          yield* Queue.offer(updates, { _tag: "Reaction", chatId, messageId, senderId, emoji, onOurMessage: target.fromMe === true })
        })

      const showQr = (qr: string) =>
        Effect.sync(() => {
          qrcode.generate(qr, { small: true }, (rendered) => {
            console.log("\nScan this QR with the dedicated phone: WhatsApp → Linked devices → Link a device\n")
            console.log(rendered)
          })
        })

      /** Serves WhatsApp's "send me that message again" requests from the outbox. Runs outside any fiber. */
      const getMessage = async (key: WAMessageKey): Promise<proto.IMessage | undefined> => {
        const id = key.id ?? undefined
        if (!id) return undefined
        const found = await Effect.runPromise(Cache.getOption(outbox, id))
        return Option.isSome(found) ? (found.value.raw.message ?? undefined) : undefined
      }

      /** One socket lifetime: connect, pump events, resolve when it closes (or, in link mode, when linked). */
      const connectOnce = Effect.gen(function* () {
        const loaded = yield* auth.load(logger)
        const version = yield* fetchVersion
        const closed = yield* Deferred.make<CloseOutcome>()
        const openedAt = yield* Ref.make<Option.Option<DateTime.Utc>>(Option.none())
        const pairingRequested = yield* Ref.make(false)
        const socketEvents = yield* Queue.unbounded<SocketEvent>()

        const sock = yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              makeWASocket({
                auth: loaded.state,
                logger,
                ...(Option.isSome(version) ? { version: version.value } : {}),
                browser: Browsers.ubuntu(config.browserName),
                markOnlineOnConnect: false,
                syncFullHistory: false,
                shouldSyncHistoryMessage: () => false,
                generateHighQualityLinkPreview: false,
                shouldIgnoreJid: (jid) => isJidBroadcast(jid) === true || isJidNewsletter(jid) === true,
                getMessage,
              }),
            catch: (e) => new SocketCreateFailed({ detail: messageOf(e) }),
          }),
          (sock) =>
            Effect.gen(function* () {
              yield* Ref.set(socket, Option.none())
              yield* Effect.promise(() => sock.end(undefined)).pipe(
                Effect.timeout("5 seconds"),
                Effect.ignoreCause({ log: "Debug", message: "closing the socket failed" }),
              )
            }),
        )

        sock.ev.on("connection.update", (update) => Queue.offerUnsafe(socketEvents, { _tag: "Connection", update }))
        sock.ev.on("creds.update", () => Queue.offerUnsafe(socketEvents, { _tag: "Creds" }))
        sock.ev.on("messages.upsert", ({ messages, type }) => Queue.offerUnsafe(socketEvents, { _tag: "Messages", messages, type }))
        sock.ev.on("messages.update", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Updates", updates: items }))
        sock.ev.on("messages.reaction", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Reactions", reactions: items }))

        const saveCreds = Effect.tryPromise(() => loaded.saveCreds()).pipe(
          Effect.retry({ schedule: Schedule.exponential("100 millis").pipe(Schedule.upTo({ times: 3 })) }),
          Effect.tapError((e) =>
            Effect.logError("could not persist session credentials; the next restart may need a re-link").pipe(
              Effect.annotateLogs({ reason: messageOf(e) }),
            ),
          ),
          Effect.ignore,
        )

        const requestPairingCode = (phone: string) =>
          Effect.gen(function* () {
            const already = yield* Ref.getAndSet(pairingRequested, true)
            if (already) return
            const digits = phone.replace(/[^0-9]/g, "")
            const code = yield* Effect.tryPromise(() => sock.requestPairingCode(digits)).pipe(
              Effect.timeout("30 seconds"),
              Effect.tapError((e) => Effect.logError("pairing code request failed").pipe(Effect.annotateLogs({ reason: messageOf(e) }))),
              Effect.option,
            )
            if (Option.isSome(code)) {
              const pretty = code.value.match(/.{1,4}/g)?.join("-") ?? code.value
              console.log(`\nPairing code for +${digits}: ${pretty}\n`)
              console.log("On the dedicated phone: WhatsApp → Linked devices → Link a device → Link with phone number instead\n")
            }
          })

        const onConnection = (update: Partial<ConnectionState>) =>
          Effect.gen(function* () {
            if (update.qr !== undefined) {
              if (options.mode === "serve") {
                yield* Deferred.succeed(closed, {
                  _tag: "Terminal",
                  detail: "WhatsApp asked for a new link; the stored session is not usable",
                  loggedOut: true,
                })
                return
              }
              yield* health.setState("linking", "waiting for the phone")
              if (options.phone !== undefined) {
                yield* requestPairingCode(options.phone)
              } else {
                yield* showQr(update.qr)
              }
            }
            if (update.connection === "connecting") {
              yield* health.setState("connecting")
            }
            if (update.connection === "open") {
              yield* Ref.set(socket, Option.some(sock))
              yield* Ref.set(openedAt, Option.some(yield* DateTime.now))
              const me = sock.user
              const identity: LinkedIdentity = {
                jid: me?.id ?? "unknown",
                ...(me?.name ? { name: me.name } : {}),
              }
              yield* health.setLinked(identity)
              yield* health.setState("ok")
              yield* events.publish({ _tag: "ConnectionChanged", state: "ok" })
              yield* Effect.logInfo("linked").pipe(Effect.annotateLogs({ jid: identity.jid, number: phoneOfJid(identity.jid) }))
              yield* Deferred.succeed(linked, identity)
              if (options.mode === "link") {
                yield* saveCreds
                yield* Deferred.succeed(closed, { _tag: "Linked" })
              }
            }
            if (update.connection === "close") {
              yield* Ref.set(socket, Option.none())
              const now = yield* DateTime.now
              const opened = yield* Ref.get(openedAt)
              const uptime = Option.match(opened, {
                onNone: () => Duration.zero,
                onSome: (t) => Duration.millis(DateTime.toEpochMillis(now) - DateTime.toEpochMillis(t)),
              })
              const outcome = classifyClose(update.lastDisconnect?.error, uptime)
              yield* health.noteDisconnect(outcome._tag === "Linked" ? "linked" : outcome.detail)
              yield* Deferred.succeed(closed, outcome)
            }
          })

        const handle = (event: SocketEvent) => {
          switch (event._tag) {
            case "Creds":
              return saveCreds
            case "Connection":
              return onConnection(event.update)
            case "Messages":
              return Effect.forEach(event.messages, (m) => acceptMessage(m, event.type), { discard: true })
            case "Updates":
              return Effect.forEach(event.updates, acceptUpdate, { discard: true })
            case "Reactions":
              return Effect.forEach(event.reactions, acceptReaction, { discard: true })
          }
        }

        yield* Stream.fromQueue(socketEvents).pipe(
          Stream.runForEach((event) =>
            handle(event).pipe(Effect.catchCauseIf(notInterrupted, (cause) => Effect.logError("socket event handler failed", cause))),
          ),
          Effect.forkScoped,
        )

        return yield* Deferred.await(closed)
      }).pipe(Effect.scoped)

      /**
       * One attempt, made total: session errors are terminal, socket setup
       * errors and crashes reconnect. A quick close (before `healthyUptime`) is
       * a typed failure so `Effect.retry` paces the next attempt; a close after a
       * healthy run succeeds out of the retry so the backoff starts over.
       * "Restart required" closes skip the backoff, but only a few in a row.
       */
      const attempt = connectOnce.pipe(
        Effect.catchTag("AuthStoreError", (e) =>
          Effect.succeed<CloseOutcome>({ _tag: "Terminal", detail: `cannot load the session (${e.operation}): ${e.detail}`, loggedOut: false }),
        ),
        Effect.catchTag("SocketCreateFailed", (e) =>
          Effect.succeed<CloseOutcome>({ _tag: "Reconnect", immediate: false, detail: `socket setup failed: ${e.detail}`, uptime: Duration.zero }),
        ),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause)
            ? Effect.failCause(cause)
            : Effect.logError("connection attempt crashed", cause).pipe(
                Effect.as<CloseOutcome>({ _tag: "Reconnect", immediate: false, detail: `crashed: ${messageOf(Cause.squash(cause))}`, uptime: Duration.zero }),
              ),
        ),
        Effect.flatMap((outcome) =>
          Effect.gen(function* () {
            if (outcome._tag !== "Reconnect") return outcome
            if (Duration.isGreaterThanOrEqualTo(outcome.uptime, healthyUptime)) {
              yield* Ref.set(immediateStreak, 0)
              return outcome
            }
            if (outcome.immediate) {
              const streak = yield* Ref.updateAndGet(immediateStreak, (n) => n + 1)
              if (streak <= MAX_IMMEDIATE_RECONNECTS) return outcome
            }
            return yield* new ReconnectNeeded({ detail: outcome.detail, uptimeMs: Duration.toMillis(outcome.uptime) })
          }),
        ),
        Effect.tapError((e) =>
          Effect.all([
            health.setState("connecting", `reconnecting: ${e.detail}`),
            events.publish({ _tag: "ConnectionChanged", state: "connecting", detail: e.detail }),
          ]),
        ),
        Effect.retry({ schedule: reconnectSchedule }),
      )

      const supervise = Effect.gen(function* () {
        if (options.mode === "serve") {
          const registered = yield* auth.isRegistered.pipe(Effect.orElseSucceed(() => false))
          if (!registered) {
            yield* health.setState("needs_relink", "no linked session on disk: run `bridge link`")
            yield* events.publish({ _tag: "ConnectionChanged", state: "needs_relink", detail: "no session" })
            return
          }
        }
        while (true) {
          const outcome = yield* attempt
          switch (outcome._tag) {
            case "Linked":
              return
            case "Terminal": {
              yield* health.setState("needs_relink", outcome.detail)
              yield* events.publish({ _tag: "ConnectionChanged", state: "needs_relink", detail: outcome.detail })
              if (outcome.loggedOut) {
                yield* auth
                  .invalidate(outcome.detail)
                  .pipe(Effect.catch((e) => Effect.logError("could not move the invalid session aside").pipe(Effect.annotateLogs({ reason: e.detail }))))
              }
              return
            }
            case "Reconnect": {
              yield* health.setState("connecting", `reconnecting: ${outcome.detail}`)
              yield* events.publish({ _tag: "ConnectionChanged", state: "connecting", detail: outcome.detail })
              yield* Effect.logInfo("reconnecting").pipe(
                Effect.annotateLogs({ reason: outcome.detail, immediate: outcome.immediate, uptimeMs: Duration.toMillis(outcome.uptime) }),
              )
            }
          }
        }
      })

      /** The supervisor itself must not die: a crash is logged and it restarts after a pause. */
      const superviseForever = supervise.pipe(
        Effect.catchCauseIf(notInterrupted, (cause) =>
          Effect.logError("whatsapp supervisor crashed; restarting in 5 seconds", cause).pipe(
            Effect.andThen(Effect.fail(new SupervisorCrashed({ detail: messageOf(Cause.squash(cause)) }))),
          ),
        ),
        Effect.retry({ schedule: Schedule.spaced("5 seconds") }),
      )

      yield* superviseForever.pipe(Effect.forkScoped)

      const withSocket = <A>(operation: string, chatId: ChatId, f: (sock: WASocket) => Promise<A>, timeout: Duration.Duration = SEND_TIMEOUT) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(socket)
          if (Option.isNone(current)) {
            const status = yield* health.get
            return yield* new NotConnected({ state: status.state, ...(status.detail === undefined ? {} : { detail: status.detail }) })
          }
          return yield* Effect.tryPromise({ try: () => f(current.value), catch: sendFailed(operation, chatId) }).pipe(
            Effect.timeout(timeout),
            Effect.catchTag("TimeoutError", () => new SendFailed({ operation, chatId, detail: `timed out after ${Duration.format(timeout)}` })),
          )
        })

      const keyFor = (chatId: ChatId, messageId: MessageId) =>
        Cache.getOption(inboundRaw, dedupeKey(chatId, messageId)).pipe(
          Effect.map(Option.match({ onNone: (): WAMessageKey => ({ remoteJid: chatId, id: messageId, fromMe: false }), onSome: (m) => m.key })),
        )

      /** A message we saw or sent this run, for quoting. */
      const rawFor = (chatId: ChatId, messageId: MessageId) =>
        Effect.gen(function* () {
          const theirs = yield* Cache.getOption(inboundRaw, dedupeKey(chatId, messageId))
          if (Option.isSome(theirs)) return theirs.value
          const ours = yield* Cache.getOption(outbox, messageId)
          if (Option.isSome(ours) && ours.value.raw.key.remoteJid === chatId) return ours.value.raw
          return yield* new MessageUnknown({ chatId, messageId, reason: "not a message the bridge saw or sent this run" })
        })

      const quoteFor = (chatId: ChatId, options?: SendOptions) =>
        options?.quoteMessageId === undefined
          ? Effect.succeed({} as { quoted?: WAMessage })
          : Effect.map(rawFor(chatId, options.quoteMessageId), (quoted) => ({ quoted }))

      /** One of our own messages, still inside WhatsApp's window for `operation`. */
      const ownMessage = (chatId: ChatId, messageId: MessageId, operation: string, window: Duration.Duration) =>
        Effect.gen(function* () {
          const entry = yield* Cache.getOption(outbox, messageId)
          if (Option.isNone(entry) || entry.value.raw.key.remoteJid !== chatId) {
            return yield* new MessageUnknown({ chatId, messageId, reason: "not a message the bridge sent this run" })
          }
          const now = yield* DateTime.now
          const ageMs = DateTime.toEpochMillis(now) - DateTime.toEpochMillis(entry.value.sentAt)
          if (ageMs > Duration.toMillis(window)) {
            return yield* new MessageTooOld({ chatId, messageId, operation, limitMs: Duration.toMillis(window), ageMs })
          }
          return entry.value.raw
        })

      const remember = (sent: WAMessage) =>
        Effect.gen(function* () {
          const id = sent.key.id ?? undefined
          if (!id) return
          const sentAt = yield* DateTime.now
          yield* Cache.set(outbox, id, { raw: sent, sentAt })
        })

      const newOutboundId = Effect.map(Ref.get(socket), (s) =>
        generateMessageIDV2(Option.isSome(s) ? s.value.user?.id : undefined) as MessageId,
      )

      const sendText = Effect.fn("WhatsApp.sendText")(function* (chatId: ChatId, text: string, options?: SendOptions) {
        const quote = yield* quoteFor(chatId, options)
        const sent = yield* withSocket("sendText", chatId, (s) =>
          s.sendMessage(chatId, { text }, { ...quote, ...(options?.messageId === undefined ? {} : { messageId: options.messageId }) }),
        )
        const id = sent?.key?.id
        if (!id) return yield* new SendFailed({ operation: "sendText", chatId, detail: "WhatsApp returned no message id" })
        yield* remember(sent)
        yield* health.touch("outbound")
        yield* Effect.logInfo("sent text").pipe(Effect.annotateLogs({ chatId, messageId: id }))
        if (config.logBodies) yield* Effect.logDebug("outbound body").pipe(Effect.annotateLogs({ chatId, text }))
        return id as MessageId
      })

      /** The library needs a Buffer; it uploads, encrypts and (for images and videos) makes the thumbnail itself. */
      const mediaContent = (media: OutboundMedia): AnyMessageContent => {
        const buffer = Buffer.from(media.bytes)
        const caption = media.caption === undefined ? {} : { caption: media.caption }
        switch (media.kind) {
          case "image":
            return { image: buffer, mimetype: media.mimetype, ...caption }
          case "video":
            return { video: buffer, mimetype: media.mimetype, ...caption }
          case "audio":
            return { audio: buffer, mimetype: media.mimetype, ptt: media.voiceNote === true }
          case "document":
            return { document: buffer, mimetype: media.mimetype, fileName: media.fileName ?? "file", ...caption }
        }
      }

      const sendMedia = Effect.fn("WhatsApp.sendMedia")(function* (chatId: ChatId, media: OutboundMedia, options?: SendOptions) {
        const quote = yield* quoteFor(chatId, { ...options, quoteMessageId: options?.quoteMessageId ?? media.quoteMessageId })
        const sent = yield* withSocket(
          `send${media.kind[0]?.toUpperCase()}${media.kind.slice(1)}`,
          chatId,
          (s) =>
            s.sendMessage(chatId, mediaContent(media), {
              ...quote,
              mediaUploadTimeoutMs: Duration.toMillis(MEDIA_SEND_TIMEOUT),
              ...(options?.messageId === undefined ? {} : { messageId: options.messageId }),
            }),
          MEDIA_SEND_TIMEOUT,
        )
        const id = sent?.key?.id
        if (!id) return yield* new SendFailed({ operation: `send ${media.kind}`, chatId, detail: "WhatsApp returned no message id" })
        yield* remember(sent)
        yield* health.touch("outbound")
        yield* Effect.logInfo("sent media").pipe(
          Effect.annotateLogs({ chatId, messageId: id, kind: media.kind, mimetype: media.mimetype, bytes: media.bytes.byteLength }),
        )
        return id as MessageId
      })

      const react = Effect.fn("WhatsApp.react")(function* (chatId: ChatId, messageId: MessageId, emoji: string) {
        const key = yield* keyFor(chatId, messageId)
        yield* withSocket("react", chatId, (s) => s.sendMessage(chatId, { react: { text: emoji, key } }))
        yield* health.touch("outbound")
        yield* Effect.logDebug(emoji === "" ? "reaction removed" : "reaction added").pipe(Effect.annotateLogs({ chatId, messageId, emoji }))
      })

      const unreact = (chatId: ChatId, messageId: MessageId) => react(chatId, messageId, "")

      const editText = Effect.fn("WhatsApp.editText")(function* (chatId: ChatId, messageId: MessageId, text: string) {
        const original = yield* ownMessage(chatId, messageId, "edit", EDIT_WINDOW)
        const sent = yield* withSocket("edit", chatId, (s) => s.sendMessage(chatId, { text, edit: original.key }))
        // Keep the edited body so a later quote or resend shows the new text.
        if (sent?.message) yield* Cache.set(outbox, messageId, { raw: { ...original, message: sent.message }, sentAt: yield* sentAtOf(messageId) })
        yield* health.touch("outbound")
        yield* Effect.logInfo("edited message").pipe(Effect.annotateLogs({ chatId, messageId }))
      })

      const sentAtOf = (messageId: MessageId) =>
        Cache.getOption(outbox, messageId).pipe(
          Effect.flatMap(Option.match({ onNone: () => DateTime.now, onSome: (e) => Effect.succeed(e.sentAt) })),
        )

      const deleteMessage = Effect.fn("WhatsApp.deleteMessage")(function* (chatId: ChatId, messageId: MessageId) {
        const original = yield* ownMessage(chatId, messageId, "delete", DELETE_WINDOW)
        yield* withSocket("delete", chatId, (s) => s.sendMessage(chatId, { delete: original.key }))
        yield* Cache.invalidate(outbox, messageId)
        yield* health.touch("outbound")
        yield* Effect.logInfo("deleted message").pipe(Effect.annotateLogs({ chatId, messageId }))
      })

      const markRead = Effect.fn("WhatsApp.markRead")(function* (chatId: ChatId, messageIds: ReadonlyArray<MessageId>) {
        const keys: Array<WAMessageKey> = []
        for (const messageId of messageIds) {
          const cached = yield* Cache.getOption(inboundRaw, dedupeKey(chatId, messageId))
          if (Option.isSome(cached)) keys.push(cached.value.key)
        }
        if (keys.length === 0) return
        yield* withSocket("read", chatId, (s) => s.readMessages(keys))
        yield* Effect.logDebug("marked read").pipe(Effect.annotateLogs({ chatId, count: keys.length }))
      })

      const setTyping = Effect.fn("WhatsApp.setTyping")(function* (chatId: ChatId, composing: boolean) {
        yield* withSocket("presence", chatId, (s) => s.sendPresenceUpdate(composing ? "composing" : "paused", chatId))
      })

      const toNumber = (n: number | LongLike | null | undefined): number | undefined => {
        if (n === null || n === undefined) return undefined
        if (typeof n === "number") return n
        return typeof n === "object" && "toNumber" in n && typeof n.toNumber === "function" ? n.toNumber() : undefined
      }

      const downloadMedia = Effect.fn("WhatsApp.downloadMedia")(function* (
        chatId: ChatId,
        messageId: MessageId,
        options: { readonly maxBytes: number },
      ) {
        const cached = yield* Cache.getOption(rawMedia, dedupeKey(chatId, messageId))
        if (Option.isNone(cached)) {
          return yield* new MediaUnavailable({ chatId, messageId, reason: "not a media message seen this run (the bridge may have restarted)" })
        }
        const { raw, message } = cached.value
        const content = normalizeMessageContent(raw.message)
        const contentType = content === undefined ? undefined : getContentType(content)
        const media = content && contentType ? (content[contentType] as { mimetype?: string | null; fileLength?: number | LongLike | null } | null) : null
        if (!media || typeof media !== "object") {
          return yield* new MediaUnavailable({ chatId, messageId, reason: "message carries no downloadable media" })
        }
        const declared = toNumber(media.fileLength)
        if (declared !== undefined && declared > options.maxBytes) {
          return yield* new MediaTooLarge({ chatId, messageId, size: declared, maxBytes: options.maxBytes })
        }
        const sock = yield* Ref.get(socket)
        const context = Option.isSome(sock) ? { reuploadRequest: (m: WAMessage) => sock.value.updateMediaMessage(m), logger } : undefined
        const bytes = yield* Effect.tryPromise({
          try: () => downloadMediaMessage(raw, "buffer", {}, context),
          catch: (e) => new MediaDownloadFailed({ chatId, messageId, detail: messageOf(e) }),
        }).pipe(
          Effect.timeout("60 seconds"),
          Effect.catchTag("TimeoutError", () => new MediaDownloadFailed({ chatId, messageId, detail: "timed out after 60 seconds" })),
        )
        if (bytes.byteLength > options.maxBytes) {
          return yield* new MediaTooLarge({ chatId, messageId, size: bytes.byteLength, maxBytes: options.maxBytes })
        }
        const mimetype = (media.mimetype ?? message.mimetype ?? "application/octet-stream").split(";")[0] ?? "application/octet-stream"
        const extension = (content ? extensionForMediaMessage(content) : undefined) ?? mimetype.split("/")[1] ?? "bin"
        yield* Effect.logInfo("media downloaded").pipe(Effect.annotateLogs({ chatId, messageId, bytes: bytes.byteLength, mimetype }))
        const result: DownloadedMedia = { bytes: new Uint8Array(bytes), mimetype, extension: extension.replace(/[^a-z0-9]/gi, "").toLowerCase() || "bin", kind: message.kind }
        return result
      })

      return WhatsApp.of({
        inbound: Stream.fromQueue(inbound),
        linked: Deferred.await(linked),
        newOutboundId,
        sendText,
        sendMedia,
        editText,
        deleteMessage,
        markRead,
        react,
        unreact,
        setTyping,
        downloadMedia,
        updates: Stream.fromQueue(updates),
      })
    }),
  )
