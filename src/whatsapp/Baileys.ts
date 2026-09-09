import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  isJidBroadcast,
  type ConnectionState,
  type WAMessage,
  type WAMessageKey,
  type WASocket,
  type WAVersion,
} from "@whiskeysockets/baileys"
import { Cache, DateTime, Deferred, Duration, Effect, Layer, Option, Queue, Ref, Schedule, Schema, Stream } from "effect"
import pino from "pino"
import qrcode from "qrcode-terminal"
import { BridgeConfig } from "../config.ts"
import { phoneOfJid, type ChatId, type MessageId } from "../domain/ids.ts"
import type { InboundMessage } from "../domain/message.ts"
import type { LinkedIdentity } from "../domain/status.ts"
import { Events } from "../engine/Events.ts"
import { Health } from "../engine/Health.ts"
import { AuthStore } from "./AuthStore.ts"
import { extractInbound } from "./extract.ts"
import { NotConnected, SendFailed, WhatsApp } from "./WhatsApp.ts"

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

type CloseOutcome =
  | { readonly _tag: "Reconnect"; readonly immediate: boolean; readonly detail: string; readonly uptime: Duration.Duration }
  | { readonly _tag: "Terminal"; readonly detail: string; readonly loggedOut: boolean }
  | { readonly _tag: "Linked" }

/** Typed failure that drives the reconnect schedule: the socket closed soon after opening. */
class ReconnectNeeded extends Schema.TaggedError<ReconnectNeeded>()("ReconnectNeeded", {
  detail: Schema.String,
  uptimeMs: Schema.Number,
}) {}

const statusCodeOf = (error: unknown): number | undefined => {
  if (typeof error !== "object" || error === null) return undefined
  const output = (error as { output?: { statusCode?: unknown } }).output
  return typeof output?.statusCode === "number" ? output.statusCode : undefined
}

const messageOf = (error: unknown): string =>
  error instanceof Error ? error.message : error === undefined ? "connection closed" : String(error)

const classifyClose = (error: unknown, uptime: Duration.Duration): CloseOutcome => {
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

/** Exponential backoff with jitter, capped at a minute, forever. Resets after a connection that lived a while. */
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
      const logger = pino({ level: config.baileysLogLevel })

      const inbound = yield* Queue.unbounded<InboundMessage>()
      const socket = yield* Ref.make<Option.Option<WASocket>>(Option.none())
      const linked = yield* Deferred.make<LinkedIdentity>()
      // Reaction keys must carry `participant` / `fromMe`; remember the ones we saw.
      const keys = yield* Cache.make<MessageId, WAMessageKey>({
        capacity: 5_000,
        lookup: (id) => Effect.die(`no key cached for ${id}`),
      })
      // Idempotency: one 👀 and one turn per message id, even if WhatsApp re-delivers.
      const seen = yield* Cache.make<string, true>({ capacity: 20_000, lookup: () => Effect.succeed(true) })

      const version: WAVersion | undefined = yield* Effect.tryPromise(() => fetchLatestBaileysVersion()).pipe(
        Effect.timeout("10 seconds"),
        Effect.map((r) => r.version),
        Effect.tapError((e) => Effect.logDebug("using bundled WhatsApp Web version").pipe(Effect.annotateLogs({ reason: messageOf(e) }))),
        Effect.orElseSucceed(() => undefined),
      )

      const allowed = (message: InboundMessage): boolean => {
        if (config.allowedSenders.length === 0) return true
        return config.allowedSenders.some(
          (s) => s === message.senderId || s === message.chatId || (message.senderNumber !== undefined && s === message.senderNumber),
        )
      }

      const acceptMessage = (raw: WAMessage, type: "append" | "notify") =>
        Effect.gen(function* () {
          const now = yield* DateTime.now
          const extracted = extractInbound(raw, now)
          if (Option.isNone(extracted)) return
          const { message, key } = extracted.value
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
          if (yield* Cache.has(seen, message.messageId)) {
            yield* Effect.logDebug("duplicate delivery ignored").pipe(log)
            return
          }
          yield* Cache.set(seen, message.messageId, true)
          yield* Cache.set(keys, message.messageId, key)
          yield* health.touch("inbound")
          yield* Effect.logInfo("inbound message").pipe(
            log,
            Effect.annotateLogs({ kind: message.kind, isGroup: message.isGroup }),
          )
          if (config.logBodies) yield* Effect.logDebug("inbound body").pipe(log, Effect.annotateLogs({ text: message.text }))
          yield* events.publish({ _tag: "MessageReceived", message })
          yield* Queue.offer(inbound, message)
        })

      const showQr = (qr: string) =>
        Effect.sync(() => {
          qrcode.generate(qr, { small: true }, (rendered) => {
            console.log("\nScan this QR with the dedicated phone: WhatsApp → Linked devices → Link a device\n")
            console.log(rendered)
          })
        })

      /** One socket lifetime: connect, pump events, resolve when it closes (or, in link mode, when linked). */
      const connectOnce = Effect.gen(function* () {
        const loaded = yield* auth.load(logger)
        const closed = yield* Deferred.make<CloseOutcome>()
        const openedAt = yield* Ref.make<Option.Option<DateTime.Utc>>(Option.none())
        const pairingRequested = yield* Ref.make(false)
        const socketEvents = yield* Queue.unbounded<SocketEvent>()

        const sock = yield* Effect.acquireRelease(
          Effect.sync(() =>
            makeWASocket({
              auth: loaded.state,
              logger,
              ...(version === undefined ? {} : { version }),
              browser: Browsers.ubuntu(config.browserName),
              markOnlineOnConnect: false,
              syncFullHistory: false,
              shouldSyncHistoryMessage: () => false,
              generateHighQualityLinkPreview: false,
              shouldIgnoreJid: (jid) => isJidBroadcast(jid) === true,
            }),
          ),
          (sock) =>
            Effect.gen(function* () {
              yield* Ref.set(socket, Option.none())
              yield* Effect.promise(() => sock.end(undefined)).pipe(Effect.ignore)
            }),
        )

        sock.ev.on("connection.update", (update) => Queue.offerUnsafe(socketEvents, { _tag: "Connection", update }))
        sock.ev.on("creds.update", () => Queue.offerUnsafe(socketEvents, { _tag: "Creds" }))
        sock.ev.on("messages.upsert", ({ messages, type }) => Queue.offerUnsafe(socketEvents, { _tag: "Messages", messages, type }))

        const saveCreds = Effect.tryPromise(() => loaded.saveCreds()).pipe(
          Effect.retry({ schedule: Schedule.exponential("100 millis").pipe(Schedule.upTo({ times: 3 })) }),
          Effect.tapError((e) => Effect.logError("could not persist session credentials").pipe(Effect.annotateLogs({ reason: messageOf(e) }))),
          Effect.ignore,
        )

        const onConnection = (update: Partial<ConnectionState>) =>
          Effect.gen(function* () {
            if (update.qr !== undefined) {
              if (options.mode === "serve") {
                yield* Deferred.succeed(closed, { _tag: "Terminal", detail: "WhatsApp asked for a new link; the stored session is not usable", loggedOut: true })
                return
              }
              yield* health.setState("linking", "waiting for the phone")
              if (options.phone !== undefined) {
                const already = yield* Ref.getAndSet(pairingRequested, true)
                if (!already) {
                  const digits = options.phone.replace(/[^0-9]/g, "")
                  const code = yield* Effect.tryPromise(() => sock.requestPairingCode(digits)).pipe(
                    Effect.tapError((e) => Effect.logError("pairing code request failed").pipe(Effect.annotateLogs({ reason: messageOf(e) }))),
                    Effect.option,
                  )
                  if (Option.isSome(code)) {
                    const pretty = code.value.match(/.{1,4}/g)?.join("-") ?? code.value
                    console.log(`\nPairing code for +${digits}: ${pretty}\n`)
                    console.log("On the dedicated phone: WhatsApp → Linked devices → Link a device → Link with phone number instead\n")
                  }
                }
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
              yield* Deferred.succeed(closed, classifyClose(update.lastDisconnect?.error, uptime))
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
          }
        }

        yield* Stream.fromQueue(socketEvents).pipe(
          Stream.runForEach((event) =>
            handle(event).pipe(
              Effect.catchCause((cause) => Effect.logError("socket event handler failed", cause)),
            ),
          ),
          Effect.forkScoped,
        )

        return yield* Deferred.await(closed)
      }).pipe(
        Effect.scoped,
        Effect.catchTag("AuthStoreError", (e) =>
          Effect.succeed<CloseOutcome>({ _tag: "Terminal", detail: `cannot load the session (${e.operation}): ${e.detail}`, loggedOut: false }),
        ),
      )

      /**
       * One attempt. A quick close (the socket died before `healthyUptime`) is a
       * typed failure so `Effect.retry` paces the next attempt; a close after a
       * healthy run succeeds out of the retry so the backoff starts over.
       */
      const attempt = connectOnce.pipe(
        Effect.flatMap((outcome) => {
          if (outcome._tag !== "Reconnect" || outcome.immediate || Duration.isGreaterThanOrEqualTo(outcome.uptime, healthyUptime)) {
            return Effect.succeed(outcome)
          }
          return Effect.fail(new ReconnectNeeded({ detail: outcome.detail, uptimeMs: Duration.toMillis(outcome.uptime) }))
        }),
        Effect.tapError((e) =>
          Effect.all([
            health.setState("connecting", `reconnecting: ${e.detail}`),
            events.publish({ _tag: "ConnectionChanged", state: "connecting", detail: e.detail }),
          ]),
        ),
        Effect.retry({ schedule: reconnectSchedule }),
      )

      const supervise = Effect.gen(function* () {
        if (options.mode === "serve" && !(yield* auth.isRegistered)) {
          yield* health.setState("needs_relink", "no linked session on disk: run `bridge link`")
          yield* events.publish({ _tag: "ConnectionChanged", state: "needs_relink", detail: "no session" })
          return
        }
        while (true) {
          const outcome = yield* attempt
          switch (outcome._tag) {
            case "Linked":
              return
            case "Terminal": {
              yield* health.setState("needs_relink", outcome.detail)
              yield* events.publish({ _tag: "ConnectionChanged", state: "needs_relink", detail: outcome.detail })
              if (outcome.loggedOut) yield* auth.invalidate(outcome.detail)
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

      yield* supervise.pipe(
        Effect.catchCause((cause) => Effect.logError("whatsapp supervisor crashed", cause)),
        Effect.forkScoped,
      )

      const withSocket = <A>(operation: string, chatId: ChatId, f: (sock: WASocket) => Promise<A>) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(socket)
          if (Option.isNone(current)) {
            const status = yield* health.get
            return yield* new NotConnected({ state: status.state, ...(status.detail === undefined ? {} : { detail: status.detail }) })
          }
          return yield* Effect.tryPromise({ try: () => f(current.value), catch: sendFailed(operation, chatId) }).pipe(
            Effect.timeout("30 seconds"),
            Effect.catchTag("TimeoutError", () => new SendFailed({ operation, chatId, detail: "timed out after 30 seconds" })),
          )
        })

      const keyFor = (chatId: ChatId, messageId: MessageId) =>
        Cache.getOption(keys, messageId).pipe(
          Effect.map(Option.getOrElse((): WAMessageKey => ({ remoteJid: chatId, id: messageId, fromMe: false }))),
        )

      const sendText = Effect.fn("WhatsApp.sendText")(function* (chatId: ChatId, text: string) {
        const sent = yield* withSocket("sendText", chatId, (s) => s.sendMessage(chatId, { text }))
        const id = sent?.key?.id
        if (!id) return yield* new SendFailed({ operation: "sendText", chatId, detail: "WhatsApp returned no message id" })
        yield* health.touch("outbound")
        yield* Effect.logInfo("sent text").pipe(Effect.annotateLogs({ chatId, messageId: id }))
        if (config.logBodies) yield* Effect.logDebug("outbound body").pipe(Effect.annotateLogs({ chatId, text }))
        return id as MessageId
      })

      const react = Effect.fn("WhatsApp.react")(function* (chatId: ChatId, messageId: MessageId, emoji: string) {
        const key = yield* keyFor(chatId, messageId)
        yield* withSocket("react", chatId, (s) => s.sendMessage(chatId, { react: { text: emoji, key } }))
        yield* health.touch("outbound")
        yield* Effect.logDebug(emoji === "" ? "reaction removed" : "reaction added").pipe(Effect.annotateLogs({ chatId, messageId, emoji }))
      })

      const unreact = (chatId: ChatId, messageId: MessageId) => react(chatId, messageId, "")

      const setTyping = Effect.fn("WhatsApp.setTyping")(function* (chatId: ChatId, composing: boolean) {
        yield* withSocket("presence", chatId, (s) => s.sendPresenceUpdate(composing ? "composing" : "paused", chatId))
      })

      return WhatsApp.of({
        inbound: Stream.fromQueue(inbound),
        linked: Deferred.await(linked),
        sendText,
        react,
        unreact,
        setTyping,
      })
    }),
  )

