import makeWASocket, {
  Browsers,
  fetchLatestBaileysVersion,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  jidNormalizedUser,
  normalizeMessageContent,
  WAMessageStubType,
  proto,
  type Chat,
  type ConnectionState,
  type Contact,
  type WACallEvent,
  type WAMessage,
  type WAMessageKey,
  type WAMessageUpdate,
  type WASocket,
  type WAVersion,
} from "@whiskeysockets/baileys"
import { Cache, Cause, Context, DateTime, Deferred, Duration, Effect, FiberSet, Layer, Option, Queue, Ref, Schedule, Schema, Stream } from "effect"
import qrcode from "qrcode-terminal"
import { BridgeConfig } from "../config.ts"
import { ChatId, MessageId, phoneOfJid } from "../domain/ids.ts"
import type { InboundMessage } from "../domain/message.ts"
import type { LinkedIdentity } from "../domain/status.ts"
import { Health } from "../engine/Health.ts"
import { Store, type ContactUpsert, type ObservedItem } from "../store/Store.ts"
import { AuthStore } from "./AuthStore.ts"
import {
  MAX_IMMEDIATE_RECONNECTS,
  ReconnectNeeded,
  classifyClose,
  decodeRaw,
  dedupeKey,
  encodeRaw,
  healthyUptime,
  messageOf,
  notInterrupted,
  reconnectSchedule,
  type CloseOutcome,
} from "./Baileys.ts"
import { isBaileysLogLevel, makeBaileysLogger } from "./BaileysLogger.ts"
import { downloadFromRaw, toNumber } from "./download.ts"
import { extractInbound, textOf } from "./extract.ts"
import { MediaSource, type MediaSourceShape } from "./MediaSource.ts"
import { MediaUnavailable } from "./WhatsApp.ts"

export interface ObserverOptions {
  /** `link`: show a QR (or a pairing code for `phone`) and stop once linked. `serve`: keep the session connected. */
  readonly mode: "link" | "serve"
  readonly phone?: string | undefined
}

/** How far the link-time backfill got. Persisted, so `/status` still knows after a restart. */
export interface BackfillProgress {
  readonly chunksAccepted: number
  readonly chunksRefused: number
  readonly messages: number
  readonly mediaQueued: number
  readonly complete: boolean
}

export interface ObserverShape {
  /** Resolves once the socket is open and the account is known. */
  readonly linked: Effect.Effect<LinkedIdentity>
  readonly backfill: Effect.Effect<BackfillProgress>
  /** Attachments the media policy wants on disk, live and backfilled, in the order they were seen. */
  readonly mediaWanted: Stream.Stream<{ readonly chatId: ChatId; readonly messageId: MessageId }>
  readonly downloadMedia: MediaSourceShape["downloadMedia"]
}

/**
 * A read-only linked device on the primary number. It has no send methods: the
 * only things it can do are receive, store, and download an attachment.
 */
export class Observer extends Context.Service<Observer, ObserverShape>()("bridge/Observer") {}

/** The observer as the media store's source. */
export const ObserverMediaSourceLive = Layer.effect(
  MediaSource,
  Effect.map(Observer, (observer) => MediaSource.of({ downloadMedia: observer.downloadMedia })),
)

type SocketEvent =
  | { readonly _tag: "Connection"; readonly update: Partial<ConnectionState> }
  | { readonly _tag: "Creds" }
  | { readonly _tag: "Messages"; readonly messages: ReadonlyArray<WAMessage> }
  | { readonly _tag: "History"; readonly chunk: HistoryChunk }
  | { readonly _tag: "HistoryStatus"; readonly syncType: proto.HistorySync.HistorySyncType; readonly status: "complete" | "paused" }
  | { readonly _tag: "Updates"; readonly updates: ReadonlyArray<WAMessageUpdate> }
  | { readonly _tag: "Reactions"; readonly reactions: ReadonlyArray<{ readonly key: WAMessageKey; readonly reaction: proto.IReaction }> }
  | { readonly _tag: "Contacts"; readonly contacts: ReadonlyArray<Partial<Contact>> }
  | { readonly _tag: "Chats"; readonly chats: ReadonlyArray<Partial<Chat>> }
  | { readonly _tag: "LidMapping"; readonly pn: string; readonly lid: string }
  | { readonly _tag: "Calls"; readonly calls: ReadonlyArray<WACallEvent> }

interface HistoryChunk {
  readonly chats: ReadonlyArray<Chat>
  readonly contacts: ReadonlyArray<Contact>
  readonly messages: ReadonlyArray<WAMessage>
  readonly lidPnMappings: ReadonlyArray<{ readonly pn: string; readonly lid: string }>
  readonly chunkOrder: number | undefined
  readonly progress: number | undefined
}

class SocketCreateFailed extends Schema.TaggedError<SocketCreateFailed>()("ObserverSocketCreateFailed", { detail: Schema.String }) {
  override get message() {
    return `socket setup failed: ${this.detail}`
  }
}

class SupervisorCrashed extends Schema.TaggedError<SupervisorCrashed>()("ObserverSupervisorCrashed", { detail: Schema.String }) {
  override get message() {
    return `supervisor crashed: ${this.detail}`
  }
}

/** The backfill is bounded on three axes: age (config), count and size. */
const MAX_CHUNKS = 20
const MAX_CHUNK_BYTES = 8 * 1024 * 1024
/** Rows per insert transaction during backfill. */
const BATCH = 500
const STATE_ME = "me"
const STATE_BACKFILL = "backfill"

const noBackfill: BackfillProgress = { chunksAccepted: 0, chunksRefused: 0, messages: 0, mediaQueued: 0, complete: false }

/** What a chunk notice says, reduced to what the decision needs. */
export interface ChunkNotice {
  readonly syncType: number | null | undefined
  readonly fileLength: number | undefined
  readonly oldestMs: number | undefined
}

/**
 * Whether to download a history chunk. Bootstrap and recent chunks only, never
 * the full archive; at most `MAX_CHUNKS`, each under `MAX_CHUNK_BYTES`; recent
 * chunks older than the backfill window are refused. The bootstrap chunk spans
 * every chat, so its oldest message says nothing about it.
 */
export const chunkWanted = (n: ChunkNotice, state: { readonly backfillDays: number; readonly accepted: number; readonly nowMs: number }): boolean => {
  const bootstrap = n.syncType === proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP
  const recent = n.syncType === proto.HistorySync.HistorySyncType.RECENT
  if (state.backfillDays <= 0 || !(bootstrap || recent) || state.accepted >= MAX_CHUNKS) return false
  if (n.fileLength !== undefined && n.fileLength > MAX_CHUNK_BYTES) return false
  const cutoff = state.nowMs - state.backfillDays * 24 * 60 * 60 * 1000
  if (recent && n.oldestMs !== undefined && n.oldestMs < cutoff) return false
  return true
}

export const contactOf = (c: Partial<Contact>): ContactUpsert | undefined => {
  const jid = c.id
  if (!jid) return undefined
  const lid = c.lid ?? (jid.endsWith("@lid") ? jid : undefined)
  const phone = c.phoneNumber ? phoneOfJid(c.phoneNumber) : phoneOfJid(jid)
  return {
    jid,
    ...(lid === undefined ? {} : { lid }),
    ...(phone === undefined ? {} : { phone }),
    ...(c.name ? { name: c.name } : {}),
    ...(c.notify ? { pushName: c.notify } : {}),
  }
}

export const ObserverLive = (options: ObserverOptions) =>
  Layer.effect(
    Observer,
    Effect.gen(function* () {
      const config = yield* BridgeConfig
      const auth = yield* AuthStore
      const health = yield* Health
      const store = yield* Store

      const runFork = yield* FiberSet.makeRuntime<never>()
      const logger = makeBaileysLogger((effect) => runFork(effect), isBaileysLogLevel(config.baileysLogLevel) ? config.baileysLogLevel : "warn")

      yield* auth.lock

      const socket = yield* Ref.make<Option.Option<WASocket>>(Option.none())
      const linked = yield* Deferred.make<LinkedIdentity>()
      const meJids = yield* Ref.make(new Set<string>())
      const mediaWanted = yield* Queue.unbounded<{ readonly chatId: ChatId; readonly messageId: MessageId }>()
      // Raw media messages seen this run; older ones come back from the database.
      const rawMedia = yield* Cache.make<string, { readonly raw: WAMessage; readonly message: InboundMessage }>({
        capacity: 2_000,
        lookup: (key) => Effect.die(`no media message cached for ${key}`),
      })
      // Backfill chunks wait here for the writer; a full queue suspends the socket's event pump, which is the point.
      const chunks = yield* Queue.bounded<HistoryChunk>(4)
      const lastVersion = yield* Ref.make<Option.Option<WAVersion>>(Option.none())
      const immediateStreak = yield* Ref.make(0)

      const persisted = yield* store.getState(STATE_BACKFILL).pipe(Effect.orElseSucceed(() => Option.none<string>()))
      const progress = yield* Ref.make<BackfillProgress>(
        Option.match(persisted, {
          onNone: () => noBackfill,
          onSome: (json) => {
            try {
              return { ...noBackfill, ...(JSON.parse(json) as Partial<BackfillProgress>) }
            } catch {
              return noBackfill
            }
          },
        }),
      )
      const saveProgress = Effect.flatMap(Ref.get(progress), (p) => store.setState(STATE_BACKFILL, JSON.stringify(p)).pipe(Effect.ignore))
      const bump = (f: (p: BackfillProgress) => BackfillProgress) => Ref.update(progress, f).pipe(Effect.andThen(saveProgress))

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

      // ---- what to keep ----------------------------------------------------

      const cutoffMs = () => Date.now() - config.observeBackfillDays * 24 * 60 * 60 * 1000

      const excluded = (chatId: string): boolean => {
        if (config.observeExclude.length === 0) return false
        const phone = phoneOfJid(chatId)
        return config.observeExclude.some((e) => e === chatId || (phone !== undefined && e === phone))
      }

      const skipChat = (jid: string | null | undefined): boolean =>
        !jid || isJidBroadcast(jid) === true || isJidNewsletter(jid) === true || jid === "status@broadcast" || excluded(jid)

      // Called by the library before it downloads a chunk; false means the chunk is never fetched.
      const filter = { accepted: 0, refused: 0 }
      const shouldSyncHistoryMessage = (n: proto.Message.IHistorySyncNotification): boolean => {
        const size = toNumber(n.fileLength)
        const oldest = toNumber(n.oldestMsgInChunkTimestampSec)
        const ok = chunkWanted(
          { syncType: n.syncType, fileLength: size, oldestMs: oldest === undefined ? undefined : oldest * 1000 },
          { backfillDays: config.observeBackfillDays, accepted: filter.accepted, nowMs: Date.now() },
        )
        if (ok) filter.accepted += 1
        else filter.refused += 1
        runFork(
          Effect.logInfo(ok ? "history chunk accepted" : "history chunk refused").pipe(
            Effect.annotateLogs({ syncType: n.syncType ?? undefined, chunk: n.chunkOrder ?? undefined, bytes: size, oldestMs: oldest === undefined ? undefined : oldest * 1000, progress: n.progress ?? undefined }),
          ),
        )
        runFork(bump((p) => ({ ...p, chunksAccepted: filter.accepted, chunksRefused: filter.refused })))
        return ok
      }

      const wantsMedia = (m: InboundMessage): boolean => {
        if (!m.hasMedia) return false
        switch (config.mediaAuto) {
          case "none":
            return false
          case "images":
            return m.kind === "image" || m.kind === "sticker"
          case "all":
            return true
        }
      }

      const isMe = (jid: string | null | undefined) =>
        jid ? Effect.map(Ref.get(meJids), (me) => me.has(jidNormalizedUser(jid))) : Effect.succeed(false)

      /** One raw message as a row to write, or nothing when it is noise, excluded, or too old for the backfill. */
      const toItem = (raw: WAMessage, now: DateTime.Utc, source: "live" | "history") =>
        Effect.gen(function* () {
          if (skipChat(raw.key.remoteJid)) return undefined
          const extracted = yield* Effect.try({ try: () => extractInbound(raw, now, { keepOwn: true }), catch: (e) => e }).pipe(
            Effect.catch((e) =>
              Effect.logDebug("could not decode a message; skipped").pipe(Effect.annotateLogs({ reason: messageOf(e), messageId: raw.key.id ?? undefined }), Effect.as(Option.none<never>())),
            ),
          )
          if (Option.isNone(extracted)) return undefined
          const { message, fromMe } = extracted.value
          if (source === "history" && DateTime.toEpochMillis(message.timestamp) < cutoffMs()) return undefined
          if (message.hasMedia) yield* Cache.set(rawMedia, dedupeKey(message.chatId, message.messageId), { raw, message })
          const item: ObservedItem = { message, raw: encodeRaw(raw), direction: fromMe ? "out" : "in" }
          return item
        })

      const queueMedia = (items: ReadonlyArray<ObservedItem>) =>
        Effect.gen(function* () {
          let queued = 0
          for (const { message } of items) {
            if (!wantsMedia(message)) continue
            yield* Queue.offer(mediaWanted, { chatId: message.chatId, messageId: message.messageId })
            queued += 1
          }
          return queued
        })

      const acceptLive = (raw: WAMessage) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now
          const item = yield* toItem(raw, now, "live")
          if (item === undefined) return
          yield* store.recordMessage(item.message, item.raw, item.direction)
          if (raw.pushName && item.direction === "in") yield* store.upsertContacts([{ jid: item.message.senderId, pushName: raw.pushName }]).pipe(Effect.ignore)
          yield* queueMedia([item])
          yield* health.touch("inbound")
          yield* Effect.logDebug("observed").pipe(
            Effect.annotateLogs({ chatId: item.message.chatId, messageId: item.message.messageId, kind: item.message.kind, direction: item.direction }),
          )
        })

      const ingestChunk = (chunk: HistoryChunk) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now
          const contacts = chunk.contacts.map(contactOf).filter((c): c is ContactUpsert => c !== undefined)
          const mappings = chunk.lidPnMappings.map((m): ContactUpsert => ({ jid: m.pn, lid: m.lid, ...(phoneOfJid(m.pn) === undefined ? {} : { phone: phoneOfJid(m.pn) }) }))
          yield* store.upsertContacts([...contacts, ...mappings]).pipe(Effect.ignore)
          for (const c of chunk.chats) {
            if (c.id && c.name && !skipChat(c.id)) yield* store.upsertChat(ChatId.make(c.id), c.name, isJidGroup(c.id) === true).pipe(Effect.ignore)
          }
          const items: Array<ObservedItem> = []
          for (const raw of chunk.messages) {
            const item = yield* toItem(raw, now, "history")
            if (item !== undefined) items.push(item)
          }
          let added = 0
          for (let i = 0; i < items.length; i += BATCH) {
            added += yield* store.recordBatch(items.slice(i, i + BATCH))
          }
          const queued = yield* queueMedia(items)
          yield* bump((p) => ({ ...p, messages: p.messages + added, mediaQueued: p.mediaQueued + queued }))
          yield* Effect.logInfo("history chunk stored").pipe(
            Effect.annotateLogs({ chunk: chunk.chunkOrder, progress: chunk.progress, received: chunk.messages.length, kept: items.length, added, mediaQueued: queued }),
          )
        })

      // The writer: one chunk at a time, in batches, so the socket never waits on SQLite for long.
      yield* Stream.fromQueue(chunks).pipe(
        Stream.runForEach((chunk) => ingestChunk(chunk).pipe(Effect.catchCauseIf(notInterrupted, (cause) => Effect.logError("history chunk failed", cause)))),
        Effect.forkScoped,
      )

      const acceptUpdate = (u: WAMessageUpdate) =>
        Effect.gen(function* () {
          const id = u.key.id ?? undefined
          const chat = u.key.remoteJid ?? undefined
          if (!id || !chat || skipChat(chat)) return
          const chatId = ChatId.make(chat)
          const messageId = MessageId.make(id)
          if (u.update.message === null || u.update.messageStubType === WAMessageStubType.REVOKE) {
            yield* store.markDeleted(chatId, messageId)
            return
          }
          const edited = u.update.message?.editedMessage?.message ?? undefined
          if (edited) {
            const content = normalizeMessageContent(edited)
            yield* store.markEdited(chatId, messageId, content ? (textOf(content) ?? "") : "")
          }
        })

      const acceptReaction = (item: { readonly key: WAMessageKey; readonly reaction: proto.IReaction }) =>
        Effect.gen(function* () {
          const chat = item.key.remoteJid ?? undefined
          const id = item.key.id ?? undefined
          if (!chat || !id || skipChat(chat)) return
          const reactor = item.reaction.key ?? undefined
          const senderId = reactor?.fromMe ? "me" : (reactor?.participant ?? reactor?.remoteJid ?? chat)
          yield* store.recordReaction(ChatId.make(chat), MessageId.make(id), senderId, item.reaction.text ?? "")
        })

      const acceptCall = (c: WACallEvent) =>
        Effect.gen(function* () {
          if (skipChat(c.chatId)) return
          if (c.status === "transport" || c.status === "relaylatency" || c.status === "preaccept") return
          const out = yield* isMe(c.from)
          yield* store.recordCall({
            callId: c.id,
            chatId: ChatId.make(c.chatId),
            fromId: c.from,
            direction: out ? "out" : "in",
            at: DateTime.makeUnsafe(c.date.getTime()),
            status: c.status,
            isVideo: c.isVideo === true,
            isGroup: c.isGroup === true,
          })
        })

      const showQr = (qr: string) =>
        Effect.sync(() => {
          qrcode.generate(qr, { small: true }, (rendered) => {
            console.log("\nScan this QR with the phone that holds YOUR number: WhatsApp → Linked devices → Link a device\n")
            console.log(rendered)
          })
        })

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
                // Never online, never a read receipt, never a full history: a linked device that only listens.
                markOnlineOnConnect: false,
                syncFullHistory: false,
                shouldSyncHistoryMessage,
                generateHighQualityLinkPreview: false,
                shouldIgnoreJid: (jid) => isJidBroadcast(jid) === true || isJidNewsletter(jid) === true,
                getMessage: async () => undefined,
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
        sock.ev.on("messages.upsert", ({ messages }) => Queue.offerUnsafe(socketEvents, { _tag: "Messages", messages }))
        sock.ev.on("messaging-history.set", (h) =>
          Queue.offerUnsafe(socketEvents, {
            _tag: "History",
            chunk: {
              chats: h.chats,
              contacts: h.contacts,
              messages: h.messages,
              lidPnMappings: h.lidPnMappings ?? [],
              chunkOrder: h.chunkOrder ?? undefined,
              progress: h.progress ?? undefined,
            },
          }),
        )
        sock.ev.on("messaging-history.status", ({ syncType, status }) => Queue.offerUnsafe(socketEvents, { _tag: "HistoryStatus", syncType, status }))
        sock.ev.on("messages.update", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Updates", updates: items }))
        sock.ev.on("messages.reaction", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Reactions", reactions: items }))
        sock.ev.on("contacts.upsert", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Contacts", contacts: items }))
        sock.ev.on("contacts.update", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Contacts", contacts: items }))
        sock.ev.on("chats.upsert", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Chats", chats: items }))
        sock.ev.on("chats.update", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Chats", chats: items }))
        sock.ev.on("lid-mapping.update", ({ pn, lid }) => Queue.offerUnsafe(socketEvents, { _tag: "LidMapping", pn, lid }))
        sock.ev.on("call", (items) => Queue.offerUnsafe(socketEvents, { _tag: "Calls", calls: items }))

        const saveCreds = Effect.tryPromise(() => loaded.saveCreds()).pipe(
          Effect.retry({ schedule: Schedule.exponential("100 millis").pipe(Schedule.upTo({ times: 3 })) }),
          Effect.tapError((e) =>
            Effect.logError("could not persist session credentials; the next restart may need a re-link").pipe(Effect.annotateLogs({ reason: messageOf(e) })),
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
              console.log("On your phone: WhatsApp → Linked devices → Link a device → Link with phone number instead\n")
            }
          })

        const onConnection = (update: Partial<ConnectionState>) =>
          Effect.gen(function* () {
            if (update.qr !== undefined) {
              if (options.mode === "serve") {
                yield* Deferred.succeed(closed, { _tag: "Terminal", detail: "WhatsApp asked for a new link; the stored session is not usable", loggedOut: true })
                return
              }
              yield* health.setState("linking", "waiting for the phone")
              if (options.phone !== undefined) yield* requestPairingCode(options.phone)
              else yield* showQr(update.qr)
            }
            if (update.connection === "connecting") yield* health.setState("connecting")
            if (update.connection === "open") {
              yield* Ref.set(socket, Option.some(sock))
              yield* Ref.set(openedAt, Option.some(yield* DateTime.now))
              const me = sock.user
              const identity: LinkedIdentity = {
                jid: me?.id ?? "unknown",
                ...(me?.name ? { name: me.name } : {}),
                ...(me?.lid ? { lid: jidNormalizedUser(me.lid) } : {}),
              }
              yield* Ref.set(meJids, new Set([me?.id, me?.lid].filter((j): j is string => typeof j === "string").map(jidNormalizedUser)))
              yield* store
                .setState(STATE_ME, JSON.stringify({ jid: jidNormalizedUser(identity.jid), lid: identity.lid, phone: phoneOfJid(identity.jid), name: identity.name }))
                .pipe(Effect.ignore)
              yield* health.setLinked(identity)
              yield* health.setState("ok")
              yield* Effect.logInfo("observer linked").pipe(Effect.annotateLogs({ jid: identity.jid, number: phoneOfJid(identity.jid) }))
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

        const handle = (event: SocketEvent): Effect.Effect<unknown, unknown> => {
          switch (event._tag) {
            case "Creds":
              return saveCreds
            case "Connection":
              return onConnection(event.update)
            case "Messages":
              return Effect.forEach(event.messages, acceptLive, { discard: true })
            case "History":
              return Queue.offer(chunks, event.chunk)
            case "HistoryStatus":
              return event.syncType === proto.HistorySync.HistorySyncType.RECENT || event.syncType === proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP
                ? bump((p) => ({ ...p, complete: true })).pipe(
                    Effect.andThen(Effect.logInfo("history backfill finished").pipe(Effect.annotateLogs({ syncType: event.syncType, status: event.status }))),
                  )
                : Effect.void
            case "Updates":
              return Effect.forEach(event.updates, acceptUpdate, { discard: true })
            case "Reactions":
              return Effect.forEach(event.reactions, acceptReaction, { discard: true })
            case "Contacts":
              return store.upsertContacts(event.contacts.map(contactOf).filter((c): c is ContactUpsert => c !== undefined))
            case "Chats":
              return Effect.forEach(
                event.chats,
                (c) => (c.id && c.name && !skipChat(c.id) ? store.upsertChat(ChatId.make(c.id), c.name, isJidGroup(c.id) === true) : Effect.void),
                { discard: true },
              )
            case "LidMapping":
              return store.upsertContacts([{ jid: event.pn, lid: event.lid, ...(phoneOfJid(event.pn) === undefined ? {} : { phone: phoneOfJid(event.pn) }) }])
            case "Calls":
              return Effect.forEach(event.calls, acceptCall, { discard: true })
          }
        }

        yield* Stream.fromQueue(socketEvents).pipe(
          Stream.runForEach((event) => handle(event).pipe(Effect.catchCauseIf(notInterrupted, (cause) => Effect.logError("observer event handler failed", cause)))),
          Effect.forkScoped,
        )

        return yield* Deferred.await(closed)
      }).pipe(Effect.scoped)

      const attempt = connectOnce.pipe(
        Effect.catchTag("AuthStoreError", (e) =>
          Effect.succeed<CloseOutcome>({ _tag: "Terminal", detail: `cannot load the session (${e.operation}): ${e.detail}`, loggedOut: false }),
        ),
        Effect.catchTag("ObserverSocketCreateFailed", (e) =>
          Effect.succeed<CloseOutcome>({ _tag: "Reconnect", immediate: false, detail: `socket setup failed: ${e.detail}`, uptime: Duration.zero }),
        ),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause)
            ? Effect.failCause(cause)
            : Effect.logError("observer connection attempt crashed", cause).pipe(
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
        Effect.tapError((e) => health.setState("connecting", `reconnecting: ${e.detail}`)),
        Effect.retry({ schedule: reconnectSchedule }),
      )

      const supervise = Effect.gen(function* () {
        if (options.mode === "serve") {
          const registered = yield* auth.isRegistered.pipe(Effect.orElseSucceed(() => false))
          if (!registered) {
            yield* health.setState("needs_relink", "no linked session on disk: run `bridge observe link`")
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
              if (outcome.loggedOut) {
                yield* auth
                  .invalidate(outcome.detail)
                  .pipe(Effect.catch((e) => Effect.logError("could not move the invalid session aside").pipe(Effect.annotateLogs({ reason: e.detail }))))
              }
              return
            }
            case "Reconnect": {
              yield* health.setState("connecting", `reconnecting: ${outcome.detail}`)
              yield* Effect.logInfo("observer reconnecting").pipe(
                Effect.annotateLogs({ reason: outcome.detail, immediate: outcome.immediate, uptimeMs: Duration.toMillis(outcome.uptime) }),
              )
            }
          }
        }
      })

      yield* supervise
        .pipe(
          Effect.catchCauseIf(notInterrupted, (cause) =>
            Effect.logError("observer supervisor crashed; restarting in 5 seconds", cause).pipe(
              Effect.andThen(Effect.fail(new SupervisorCrashed({ detail: messageOf(Cause.squash(cause)) }))),
            ),
          ),
          Effect.retry({ schedule: Schedule.spaced("5 seconds") }),
        )
        .pipe(Effect.forkScoped)

      /** A media message seen this run, or one the database kept. */
      const rawMediaFor = (chatId: ChatId, messageId: MessageId) =>
        Effect.gen(function* () {
          const cached = yield* Cache.getOption(rawMedia, dedupeKey(chatId, messageId))
          if (Option.isSome(cached)) return cached.value
          const kept = yield* store.raw(chatId, messageId).pipe(Effect.orElseSucceed(() => Option.none<Uint8Array>()))
          if (Option.isNone(kept)) return yield* new MediaUnavailable({ chatId, messageId, reason: "not a message the observer kept" })
          const raw = decodeRaw(kept.value)
          const extracted = extractInbound(raw, yield* DateTime.now, { keepOwn: true })
          if (Option.isNone(extracted) || !extracted.value.message.hasMedia) {
            return yield* new MediaUnavailable({ chatId, messageId, reason: "message carries no downloadable media" })
          }
          return { raw, message: extracted.value.message }
        })

      const downloadMedia: MediaSourceShape["downloadMedia"] = Effect.fn("Observer.downloadMedia")(function* (chatId, messageId, options) {
        const { raw, message } = yield* rawMediaFor(chatId, messageId)
        const sock = yield* Ref.get(socket)
        return yield* downloadFromRaw(raw, message, { maxBytes: options.maxBytes, socket: Option.getOrUndefined(sock), logger })
      })

      return Observer.of({
        linked: Deferred.await(linked),
        backfill: Ref.get(progress),
        mediaWanted: Stream.fromQueue(mediaWanted),
        downloadMedia,
      })
    }),
  )
