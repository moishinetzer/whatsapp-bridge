import { SqliteClient } from "@effect/sql-sqlite-node"
import { Cause, Context, DateTime, Effect, FileSystem, Layer, Option, Path, Ref, Schema, Scope, Semaphore } from "effect"
import { HttpClient } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { BridgeConfig, observerDatabasePath } from "../config.ts"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import type { ChatSummary, ContactRecord, SearchHit, StoredMessage } from "../domain/history.ts"
import type { InboundMessage, MediaFile } from "../domain/message.ts"
import { ObserverApi } from "../http/ObserverApi.ts"
import { makeHistoryReads, type HistoryReads, type PageOptions, type SearchOptions } from "../store/History.ts"
import { HistoryDisabled, Store, StoreError } from "../store/Store.ts"
import {
  GroupInfo,
  LookupFailed,
  MediaDownloadFailed,
  MediaTooLarge,
  MediaUnavailable,
  NotConnected,
  WhatsApp,
} from "../whatsapp/WhatsApp.ts"
import type { MediaStoreError } from "./Media.ts"

/** The observer's database or port could not be reached. Line reads are unaffected. */
export class ObserverUnavailable extends Schema.TaggedError<ObserverUnavailable>()("ObserverUnavailable", { detail: Schema.String }, { httpApiStatus: 503 }) {
  override get message() {
    return `observer unavailable: ${this.detail}`
  }
}

export const TurnAccessKind = Schema.Literals(["owner", "chat"])
export type TurnAccessKind = typeof TurnAccessKind.Type

/** The turn's capability does not cover this read. */
export class ContextNotAllowed extends Schema.TaggedError<ContextNotAllowed>()(
  "ContextNotAllowed",
  { turnId: TurnId, access: TurnAccessKind, detail: Schema.String },
  { httpApiStatus: 403 },
) {
  override get message() {
    return `turn ${this.turnId} (${this.access} access): ${this.detail}`
  }
}

/** What the bridge knows about one chat: history summary, and for groups the members. */
export const ChatInfo = Schema.Struct({
  chatId: ChatId,
  isGroup: Schema.Boolean,
  name: Schema.optionalKey(Schema.String),
  lastMessageAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  messageCount: Schema.optionalKey(Schema.Number),
  group: Schema.optionalKey(GroupInfo),
})
export type ChatInfo = typeof ChatInfo.Type

/** Reads over the personal line's own history, every chat. */
export interface LineReads {
  readonly chats: Effect.Effect<ReadonlyArray<ChatSummary>, HistoryDisabled | StoreError>
  readonly chat: (chatId: ChatId) => Effect.Effect<ChatInfo, StoreError | NotConnected | LookupFailed>
  readonly messages: (chatId: ChatId, options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, HistoryDisabled | StoreError>
  readonly search: (query: string, options: SearchOptions) => Effect.Effect<ReadonlyArray<SearchHit>, HistoryDisabled | StoreError>
}

export type PrimaryMediaError = ObserverUnavailable | MediaUnavailable | MediaTooLarge | MediaDownloadFailed | MediaStoreError

/** Reads over the observer's history: the primary number, every chat. */
export interface PrimaryReads {
  readonly chats: Effect.Effect<ReadonlyArray<ChatSummary>, ObserverUnavailable>
  readonly chat: (chatId: ChatId) => Effect.Effect<ChatInfo, ObserverUnavailable>
  readonly messages: (chatId: ChatId, options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, ObserverUnavailable>
  readonly search: (query: string, options: SearchOptions) => Effect.Effect<ReadonlyArray<SearchHit>, ObserverUnavailable>
  readonly contacts: (query: string | undefined, limit: number) => Effect.Effect<ReadonlyArray<ContactRecord>, ObserverUnavailable>
  /** An attachment from the primary number, fetched through the observer onto local disk. */
  readonly media: (chatId: ChatId, messageId: MessageId) => Effect.Effect<MediaFile, PrimaryMediaError>
}

/** What a turn in the owner's DM may read: both lines, the address book, primary attachments. */
export interface OwnerContextShape {
  readonly line: LineReads
  readonly primary: PrimaryReads
}

/** What any other turn may read: its own chat on the personal line, nothing else. */
export interface ChatContextShape {
  readonly chatId: ChatId
  readonly chat: Effect.Effect<ChatInfo, StoreError | NotConnected | LookupFailed>
  readonly messages: (options: PageOptions) => Effect.Effect<ReadonlyArray<StoredMessage>, HistoryDisabled | StoreError>
  readonly search: (query: string, options: Omit<SearchOptions, "chatId">) => Effect.Effect<ReadonlyArray<SearchHit>, HistoryDisabled | StoreError>
}

/**
 * The capabilities, as services. Code that reads the primary number requires
 * `OwnerContext`; the only place one is built is the engine, for a turn that
 * is a DM from the owner. A group turn cannot be written to read it.
 */
export class OwnerContext extends Context.Service<OwnerContext, OwnerContextShape>()("bridge/OwnerContext") {}
export class ChatContext extends Context.Service<ChatContext, ChatContextShape>()("bridge/ChatContext") {}

export type TurnAccess =
  | { readonly kind: "owner"; readonly context: OwnerContextShape }
  | { readonly kind: "chat"; readonly context: ChatContextShape }

export interface ContextSourceShape {
  /** Whether an owner is configured, so primary reads can exist at all. */
  readonly primaryConfigured: boolean
  /** Decide and build a turn's capability: `owner` for a DM whose every message is from the owner, `chat` otherwise. */
  readonly forTurn: (turn: { readonly chatId: ChatId; readonly isGroup: boolean; readonly messages: ReadonlyArray<InboundMessage> }) => TurnAccess
}

export class ContextSource extends Context.Service<ContextSource, ContextSourceShape>()("bridge/ContextSource") {}

// ---------------------------------------------------------------------------
// The reads, as effects that require the capability.
// ---------------------------------------------------------------------------

export const primaryChats = OwnerContext.use((c) => c.primary.chats)
export const primaryChat = (chatId: ChatId) => OwnerContext.use((c) => c.primary.chat(chatId))
export const primaryMessages = (chatId: ChatId, options: PageOptions) => OwnerContext.use((c) => c.primary.messages(chatId, options))
export const primarySearch = (query: string, options: SearchOptions) => OwnerContext.use((c) => c.primary.search(query, options))
export const primaryContacts = (query: string | undefined, limit: number) => OwnerContext.use((c) => c.primary.contacts(query, limit))
export const primaryMedia = (chatId: ChatId, messageId: MessageId) => OwnerContext.use((c) => c.primary.media(chatId, messageId))
export const lineChats = OwnerContext.use((c) => c.line.chats)
export const lineSearch = (query: string, options: SearchOptions) => OwnerContext.use((c) => c.line.search(query, options))

// ---------------------------------------------------------------------------
// Building the readers.
// ---------------------------------------------------------------------------

const makeLineReads = (store: Store["Service"], whatsapp: WhatsApp["Service"]): LineReads => {
  const chat = (chatId: ChatId) =>
    Effect.gen(function* () {
      const isGroup = chatId.endsWith("@g.us")
      const known = yield* store.chats.pipe(Effect.catchTag("HistoryDisabled", () => Effect.succeed([])))
      const summary = known.find((c) => c.chatId === chatId)
      const group = isGroup ? yield* whatsapp.groupInfo(chatId) : undefined
      const info: ChatInfo = {
        chatId,
        isGroup,
        ...(summary?.name === undefined ? (group === undefined ? {} : { name: group.subject }) : { name: summary.name }),
        ...(summary === undefined ? {} : { lastMessageAt: summary.lastMessageAt, messageCount: summary.messageCount }),
        ...(group === undefined ? {} : { group }),
      }
      return info
    })
  return { chats: store.chats, chat, messages: store.messages, search: store.search }
}

const noPrimary = (detail: string): PrimaryReads => {
  const fail = Effect.fail(new ObserverUnavailable({ detail }))
  return { chats: fail, chat: () => fail, messages: () => fail, search: () => fail, contacts: () => fail, media: () => fail }
}

const isPrimaryMediaError = (e: unknown): e is Exclude<PrimaryMediaError, ObserverUnavailable> =>
  typeof e === "object" &&
  e !== null &&
  "_tag" in e &&
  (e._tag === "MediaUnavailable" || e._tag === "MediaTooLarge" || e._tag === "MediaDownloadFailed" || e._tag === "MediaStoreError")

/**
 * The observer's database, opened read-only the first time it is needed and
 * kept for the daemon's lifetime; its port, for attachments. Both fail with
 * `ObserverUnavailable` and nothing else, so the line keeps working without it.
 */
const makePrimaryReads = Effect.gen(function* () {
  const config = yield* BridgeConfig
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const scope = yield* Effect.scope
  const httpClient = yield* HttpClient.HttpClient
  const dbPath = path.resolve(observerDatabasePath(config.observerDir))
  const opened = yield* Ref.make<Option.Option<HistoryReads>>(Option.none())
  const gate = yield* Semaphore.make(1)

  const open = gate.withPermits(1)(
    Effect.gen(function* () {
      const current = yield* Ref.get(opened)
      if (Option.isSome(current)) return current.value
      const exists = yield* fs.exists(dbPath).pipe(Effect.orElseSucceed(() => false))
      if (!exists) return yield* new ObserverUnavailable({ detail: `observer database not found at ${dbPath}; run \`bridge observe link\` and \`bridge observe run\`` })
      // The client constructor throws on an unopenable file; that is a defect to the library and a plain failure to us.
      const client = yield* SqliteClient.make({ filename: dbPath, readonly: true, busyTimeout: "5 seconds" }).pipe(
        Scope.provide(scope),
        Effect.provide(Reactivity.layer),
        Effect.catchCause((cause) => {
          const squashed = Cause.squash(cause)
          return Effect.fail(new ObserverUnavailable({ detail: `could not open ${dbPath}: ${squashed instanceof Error ? squashed.message : String(squashed)}` }))
        }),
      )
      const reads = makeHistoryReads(client)
      yield* Ref.set(opened, Option.some(reads))
      yield* Effect.logInfo("observer database opened read-only").pipe(Effect.annotateLogs({ database: dbPath }))
      return reads
    }),
  )

  const withReads = <A>(f: (reads: HistoryReads) => Effect.Effect<A, unknown>) =>
    Effect.flatMap(open, (reads) =>
      f(reads).pipe(Effect.mapError((e) => new ObserverUnavailable({ detail: `observer database: ${e instanceof Error ? e.message : String(e)}` }))),
    )

  const client = yield* HttpApiClient.make(ObserverApi, { baseUrl: config.observerUrl }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

  const media = (chatId: ChatId, messageId: MessageId): Effect.Effect<MediaFile, PrimaryMediaError> =>
    client.media.get({ params: { chatId, messageId } }).pipe(
      Effect.catch((e: unknown) =>
        Effect.fail<PrimaryMediaError>(
          isPrimaryMediaError(e) ? e : new ObserverUnavailable({ detail: `observer at ${config.observerUrl}: ${e instanceof Error ? e.message : String(e)}` }),
        ),
      ),
    )

  const chat = (chatId: ChatId) =>
    withReads((reads) =>
      Effect.gen(function* () {
        const known = yield* reads.chats
        const summary = known.find((c) => c.chatId === chatId)
        const name = summary?.name ?? Option.getOrUndefined(yield* reads.chatName(chatId))
        const info: ChatInfo = {
          chatId,
          isGroup: chatId.endsWith("@g.us"),
          ...(name === undefined ? {} : { name }),
          ...(summary === undefined ? {} : { lastMessageAt: summary.lastMessageAt, messageCount: summary.messageCount }),
        }
        return info
      }),
    )

  const reads: PrimaryReads = {
    chats: withReads((r) => r.chats),
    chat,
    messages: (chatId, options) => withReads((r) => r.messages(chatId, options)),
    search: (query, options) => withReads((r) => r.search(query, options)),
    contacts: (query, limit) => withReads((r) => r.contacts(query, limit)),
    media,
  }
  return reads
})

export const ContextSourceLive = Layer.effect(
  ContextSource,
  Effect.gen(function* () {
    const config = yield* BridgeConfig
    const store = yield* Store
    const whatsapp = yield* WhatsApp
    const line = makeLineReads(store, whatsapp)
    const primaryConfigured = Option.isSome(config.owner)
    const primary = primaryConfigured ? yield* makePrimaryReads : noPrimary("no owner configured (BRIDGE_OWNER), so there is no primary number to read")

    const forTurn: ContextSourceShape["forTurn"] = (turn) => {
      const owner = primaryConfigured && !turn.isGroup && turn.messages.length > 0 && turn.messages.every((m) => m.fromOwner === true)
      if (owner) return { kind: "owner", context: { line, primary } }
      const chatId = turn.chatId
      return {
        kind: "chat",
        context: {
          chatId,
          chat: line.chat(chatId),
          messages: (options) => line.messages(chatId, options),
          search: (query, options) => line.search(query, { ...options, chatId }),
        },
      }
    }

    yield* Effect.logInfo("context source ready").pipe(
      Effect.annotateLogs({ access: primaryConfigured ? "owner" : "open", observerDir: config.observerDir, observerUrl: config.observerUrl }),
    )
    return ContextSource.of({ primaryConfigured, forTurn })
  }),
)

/** How old the newest of the turn's messages is; used only for logging what a read was about. */
export const ageOf = (turn: { readonly messages: ReadonlyArray<InboundMessage> }, now: DateTime.Utc): number => {
  const newest = turn.messages.reduce((max, m) => Math.max(max, DateTime.toEpochMillis(m.timestamp)), 0)
  return newest === 0 ? 0 : DateTime.toEpochMillis(now) - newest
}
