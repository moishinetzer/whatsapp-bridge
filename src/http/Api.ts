import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import { VERSION } from "../version.ts"
import { BridgeEvent } from "../domain/events.ts"
import { ChatId, MessageId, TurnId } from "../domain/ids.ts"
import { MediaFile } from "../domain/message.ts"
import { BridgeStatus } from "../domain/status.ts"
import { MediaStoreError } from "../engine/Media.ts"
import { OutboundFileUnreadable, OutboundInvalid, OutboundTooLarge } from "../engine/Outbound.ts"
import { OutboundMediaPayload, ReplyItem, exactlyOneSource } from "../domain/outbound.ts"
import { ChatSummary, ContactRecord, HistoryRow } from "../domain/history.ts"
import { HistoryDisabled, StoreError } from "../store/Store.ts"
import { ScheduledNotFound, ScheduledSend } from "../store/Scheduled.ts"
import { TurnFailedError, TurnNotAwaitingReply, TurnNotFound, TurnNotOpen, TurnSummary, TurnSuperseded } from "../engine/Turns.ts"
import { ChatInfo, ContextNotAllowed, ObserverUnavailable, TurnAccessKind } from "../engine/Context.ts"
import {
  ContactInfo,
  LookupFailed,
  PresenceInfo,
  MediaDownloadFailed,
  MediaTooLarge,
  MediaUnavailable,
  MessageTooOld,
  MessageUnknown,
  NotAllowlisted,
  NotConnected,
  SendFailed,
} from "../whatsapp/WhatsApp.ts"

/** `GET /health` answers 503 with the same body when the bridge is not `ok`. */
export class Unhealthy extends Schema.TaggedError<Unhealthy>()("Unhealthy", { status: BridgeStatus }, { httpApiStatus: 503 }) {
  override get message() {
    return `bridge is ${this.status.state}${this.status.detail === undefined ? "" : `: ${this.status.detail}`}`
  }
}

/** Hold the send until this instant; with `deliverWhenOnline`, until the recipient is online too (24 hours at most). */
const scheduling = {
  sendAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  deliverWhenOnline: Schema.optionalKey(Schema.Boolean),
}

export const SendTextPayload = Schema.Struct({
  text: Schema.String.pipe(Schema.check(Schema.isNonEmpty())),
  /** Send as a reply to this message (one the bridge saw or sent this run). */
  quoteMessageId: Schema.optionalKey(MessageId),
  /** Jids to @-mention (groups); write `@<digits>` in the text for each. */
  mentions: Schema.optionalKey(Schema.Array(Schema.String)),
  ...scheduling,
})
export const LocationPayload = Schema.Struct({
  latitude: Schema.Number,
  longitude: Schema.Number,
  name: Schema.optionalKey(Schema.String),
  address: Schema.optionalKey(Schema.String),
  quoteMessageId: Schema.optionalKey(MessageId),
})
export const ContactsPayload = Schema.Struct({
  contacts: Schema.Array(Schema.Struct({ name: Schema.String.pipe(Schema.check(Schema.isNonEmpty())), phone: Schema.String.pipe(Schema.check(Schema.isNonEmpty())) })).pipe(
    Schema.check(Schema.isMinLength(1)),
  ),
  quoteMessageId: Schema.optionalKey(MessageId),
})
export const PollPayload = Schema.Struct({
  question: Schema.String.pipe(Schema.check(Schema.isNonEmpty())),
  /** Two to twelve, as WhatsApp allows. */
  options: Schema.Array(Schema.String.pipe(Schema.check(Schema.isNonEmpty()))).pipe(Schema.check(Schema.isMinLength(2), Schema.isMaxLength(12))),
  /** Several options may be picked. Default: one. */
  multiple: Schema.optionalKey(Schema.Boolean),
  quoteMessageId: Schema.optionalKey(MessageId),
})
export const AlbumPayload = Schema.Struct({
  /** Images and videos only; anything else is refused. */
  items: Schema.Array(OutboundMediaPayload).pipe(Schema.check(Schema.isMinLength(2))),
  quoteMessageId: Schema.optionalKey(MessageId),
})
export const SentAlbum = Schema.Struct({ chatId: ChatId, messageIds: Schema.Array(MessageId) })
export const SendMediaPayload = Schema.Struct({ ...OutboundMediaPayload.fields, ...scheduling }).check(exactlyOneSource)
export const ForwardPayload = Schema.Struct({ fromChatId: ChatId, messageId: MessageId })

/** A send the bridge is holding; answered instead of `SentMessage` when `sendAt` was given. */
export const ScheduledAccepted = Schema.Struct({
  scheduledId: Schema.String,
  sendAt: Schema.DateTimeUtcFromString,
  deliverWhenOnline: Schema.Boolean,
}).annotate({ httpApiStatus: 202 })

export const HistoryQuery = Schema.Struct({
  /** Only messages older than this one. */
  before: Schema.optionalKey(MessageId),
  /** Only messages newer than this instant. */
  since: Schema.optionalKey(Schema.DateTimeUtcFromString),
  /** Page size, 50 by default, 500 at most. */
  limit: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(1)))),
  /** Full-text search: words become prefix terms, a quoted string one phrase. */
  q: Schema.optionalKey(Schema.String),
})
export const SearchQuery = Schema.Struct({
  q: Schema.String.pipe(Schema.check(Schema.isNonEmpty())),
  before: Schema.optionalKey(MessageId),
  limit: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(1)))),
})

export { ChatInfo }

/** `GET /turns/{turnId}/context`: what this turn may read. */
export const ContextSummary = Schema.Struct({
  turnId: TurnId,
  chatId: ChatId,
  access: TurnAccessKind,
  /** Whether a primary number is configured at all (`BRIDGE_OWNER`); the observer may still be down. */
  primary: Schema.Boolean,
})
export const ContactsQuery = Schema.Struct({
  /** Matches saved names, push names, phone digits and jids. Empty lists everyone with a name. */
  q: Schema.optionalKey(Schema.String),
  limit: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(1)))),
})
export const EditPayload = Schema.Struct({ text: Schema.String.pipe(Schema.check(Schema.isNonEmpty())) })
export const SentMessage = Schema.Struct({ chatId: ChatId, messageId: MessageId })
/** Two answers: `200 SentMessage` now, or `202 ScheduledAccepted` when `sendAt` was given. */
export const SendOutcome = [SentMessage, ScheduledAccepted] as const
export const ReactionPayload = Schema.Struct({ emoji: Schema.String.pipe(Schema.check(Schema.isNonEmpty())) })
export const TypingPayload = Schema.Struct({ composing: Schema.Boolean })
export const ReplyPayload = Schema.Struct({
  text: Schema.optionalKey(Schema.String),
  /** Bubbles sent before `text`; a string, or `{text, quoteMessageId}` to quote a message. */
  replies: Schema.optionalKey(Schema.Array(ReplyItem)),
  /** Files to send after the text bubbles, in order. */
  attachments: Schema.optionalKey(Schema.Array(OutboundMediaPayload)),
  /** Quote this message with the first bubble (unless that bubble carries its own quote). */
  quoteMessageId: Schema.optionalKey(MessageId),
  /** Edit the last progress bubble into the first text bubble instead of sending a new message. */
  replaceProgress: Schema.optionalKey(Schema.Boolean),
  /** Adjacent images and videos in `attachments` go out as one album. */
  album: Schema.optionalKey(Schema.Boolean),
})
export const ReplyResult = Schema.Struct({ turnId: TurnId, sentMessageIds: Schema.Array(MessageId) })

export const ProgressPayload = Schema.Struct({
  /** An interim bubble ("still checking"). The reply can edit it later with `replaceProgress`. */
  text: Schema.optionalKey(Schema.String.pipe(Schema.check(Schema.isNonEmpty()))),
  /** Push the reply deadline this far out from now (default `BRIDGE_TURN_TIMEOUT`), never past one hour after opening. */
  extendBy: Schema.optionalKey(Schema.DurationFromString),
})
export const ProgressResult = Schema.Struct({
  turnId: TurnId,
  deadline: Schema.DateTimeUtcFromString,
  typingUntil: Schema.DateTimeUtcFromString,
  progressMessageId: Schema.optionalKey(MessageId),
})
export const AbandonPayload = Schema.Struct({
  reason: Schema.String.pipe(Schema.check(Schema.isNonEmpty())),
  /** Replaces `BRIDGE_FAILURE_TEXT` for this turn. An empty string sends nothing. */
  text: Schema.optionalKey(Schema.String),
})

const chatParams = { chatId: ChatId }
const messageParams = { chatId: ChatId, messageId: MessageId }
const turnParams = { turnId: TurnId }

export const HealthGroup = HttpApiGroup.make("health").add(
  HttpApiEndpoint.get("check", "/health", { success: BridgeStatus, error: Unhealthy }),
  HttpApiEndpoint.get("status", "/status", { success: BridgeStatus }),
)

export const EventsQuery = Schema.Struct({
  /**
   * Replay buffered events with `seq` greater than this before going live.
   * Default: live only. Must be a non-negative integer: `NumberFromString`
   * alone turns "abc" into NaN, which would silently filter out every event.
   */
  since: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)))),
})

export const EventsGroup = HttpApiGroup.make("events").add(
  HttpApiEndpoint.get("stream", "/events", { query: EventsQuery, success: HttpApiSchema.StreamSse({ data: BridgeEvent }) }),
)

export const ChatsGroup = HttpApiGroup.make("chats").add(
  HttpApiEndpoint.post("send", "/chats/:chatId/messages", {
    params: chatParams,
    payload: SendTextPayload,
    success: SendOutcome,
    error: [NotConnected, SendFailed, MessageUnknown, StoreError, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.post("sendLocation", "/chats/:chatId/location", {
    params: chatParams,
    payload: LocationPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed, MessageUnknown, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.post("sendContacts", "/chats/:chatId/contacts", {
    params: chatParams,
    payload: ContactsPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed, MessageUnknown, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.post("sendPoll", "/chats/:chatId/polls", {
    params: chatParams,
    payload: PollPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed, MessageUnknown, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.post("sendAlbum", "/chats/:chatId/album", {
    params: chatParams,
    payload: AlbumPayload,
    success: SentAlbum,
    error: [NotConnected, SendFailed, MessageUnknown, OutboundInvalid, OutboundFileUnreadable, OutboundTooLarge, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.get("presence", "/chats/:chatId/presence", { params: chatParams, success: PresenceInfo, error: NotConnected }),
  HttpApiEndpoint.post("forward", "/chats/:chatId/forward", {
    params: chatParams,
    payload: ForwardPayload,
    success: SentMessage,
    error: [NotConnected, SendFailed, MessageUnknown, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.patch("edit", "/chats/:chatId/messages/:messageId", {
    params: messageParams,
    payload: EditPayload,
    error: [NotConnected, SendFailed, MessageUnknown, MessageTooOld],
  }),
  HttpApiEndpoint.delete("deleteMessage", "/chats/:chatId/messages/:messageId", {
    params: messageParams,
    error: [NotConnected, SendFailed, MessageUnknown, MessageTooOld],
  }),
  HttpApiEndpoint.post("sendMedia", "/chats/:chatId/media", {
    params: chatParams,
    payload: SendMediaPayload,
    success: SendOutcome,
    error: [NotConnected, SendFailed, MessageUnknown, OutboundInvalid, OutboundFileUnreadable, OutboundTooLarge, StoreError, NotAllowlisted, LookupFailed],
  }),
  HttpApiEndpoint.post("react", "/chats/:chatId/messages/:messageId/reactions", {
    params: messageParams,
    payload: ReactionPayload,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.delete("unreact", "/chats/:chatId/messages/:messageId/reactions", {
    params: messageParams,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.post("typing", "/chats/:chatId/typing", {
    params: chatParams,
    payload: TypingPayload,
    error: [NotConnected, SendFailed],
  }),
  HttpApiEndpoint.get("media", "/chats/:chatId/messages/:messageId/media", {
    params: messageParams,
    success: MediaFile,
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
  HttpApiEndpoint.get("mediaContent", "/chats/:chatId/messages/:messageId/media/content", {
    params: messageParams,
    success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
    error: [MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
)

/** Live lookups, not stored context: nothing here reads the history. */
export const ContactsGroup = HttpApiGroup.make("contacts").add(
  HttpApiEndpoint.get("contact", "/contacts/:jid", { params: { jid: Schema.String }, success: ContactInfo, error: [NotConnected, LookupFailed] }),
)

const contextChatParams = { turnId: TurnId, chatId: ChatId }
const contextMessageParams = { turnId: TurnId, chatId: ChatId, messageId: MessageId }
const gate = [TurnNotFound, TurnNotOpen, ContextNotAllowed] as const
const lineErrors = [...gate, HistoryDisabled, StoreError] as const
const primaryErrors = [...gate, ObserverUnavailable] as const

/**
 * Stored context, readable only through an open turn. `line` is the personal
 * line's history; `primary` is the observer's. A turn in the owner's DM may
 * read everything; any other turn only its own chat on the line.
 */
export const ContextGroup = HttpApiGroup.make("context").add(
  HttpApiEndpoint.get("summary", "/turns/:turnId/context", { params: turnParams, success: ContextSummary, error: [TurnNotFound, TurnNotOpen] }),
  HttpApiEndpoint.get("lineChats", "/turns/:turnId/context/line/chats", { params: turnParams, success: Schema.Array(ChatSummary), error: lineErrors }),
  HttpApiEndpoint.get("lineChat", "/turns/:turnId/context/line/chats/:chatId", {
    params: contextChatParams,
    success: ChatInfo,
    error: [...gate, StoreError, NotConnected, LookupFailed],
  }),
  HttpApiEndpoint.get("lineMessages", "/turns/:turnId/context/line/chats/:chatId/messages", {
    params: contextChatParams,
    query: HistoryQuery,
    success: Schema.Array(HistoryRow),
    error: lineErrors,
  }),
  HttpApiEndpoint.get("lineSearch", "/turns/:turnId/context/line/messages", { params: turnParams, query: SearchQuery, success: Schema.Array(HistoryRow), error: lineErrors }),
  HttpApiEndpoint.get("primaryChats", "/turns/:turnId/context/primary/chats", { params: turnParams, success: Schema.Array(ChatSummary), error: primaryErrors }),
  HttpApiEndpoint.get("primaryChat", "/turns/:turnId/context/primary/chats/:chatId", { params: contextChatParams, success: ChatInfo, error: primaryErrors }),
  HttpApiEndpoint.get("primaryMessages", "/turns/:turnId/context/primary/chats/:chatId/messages", {
    params: contextChatParams,
    query: HistoryQuery,
    success: Schema.Array(HistoryRow),
    error: primaryErrors,
  }),
  HttpApiEndpoint.get("primarySearch", "/turns/:turnId/context/primary/messages", {
    params: turnParams,
    query: SearchQuery,
    success: Schema.Array(HistoryRow),
    error: primaryErrors,
  }),
  HttpApiEndpoint.get("primaryContacts", "/turns/:turnId/context/primary/contacts", {
    params: turnParams,
    query: ContactsQuery,
    success: Schema.Array(ContactRecord),
    error: primaryErrors,
  }),
  HttpApiEndpoint.get("primaryMedia", "/turns/:turnId/context/primary/chats/:chatId/messages/:messageId/media", {
    params: contextMessageParams,
    success: MediaFile,
    error: [...primaryErrors, MediaUnavailable, MediaTooLarge, MediaDownloadFailed, MediaStoreError],
  }),
)

export const ScheduledGroup = HttpApiGroup.make("scheduled").add(
  HttpApiEndpoint.get("list", "/scheduled", { success: Schema.Array(ScheduledSend), error: StoreError }),
  HttpApiEndpoint.delete("cancel", "/scheduled/:scheduledId", { params: { scheduledId: Schema.String }, error: [ScheduledNotFound, StoreError] }),
)

export const TurnsGroup = HttpApiGroup.make("turns").add(
  HttpApiEndpoint.get("list", "/turns", { success: Schema.Array(TurnSummary) }),
  HttpApiEndpoint.get("get", "/turns/:turnId", { params: turnParams, success: TurnSummary, error: TurnNotFound }),
  HttpApiEndpoint.post("reply", "/turns/:turnId/reply", {
    params: turnParams,
    payload: ReplyPayload,
    success: ReplyResult,
    error: [TurnNotFound, TurnSuperseded, TurnNotAwaitingReply, TurnFailedError, OutboundInvalid, OutboundFileUnreadable, OutboundTooLarge],
  }),
  HttpApiEndpoint.post("progress", "/turns/:turnId/progress", {
    params: turnParams,
    payload: ProgressPayload,
    success: ProgressResult,
    error: [TurnNotFound, TurnNotOpen, NotConnected, SendFailed],
  }),
  HttpApiEndpoint.post("abandon", "/turns/:turnId/abandon", {
    params: turnParams,
    payload: AbandonPayload,
    error: [TurnNotFound, TurnNotOpen],
  }),
)

/**
 * The daemon's local API. `HttpApiBuilder` derives the router from this and
 * `HttpApiClient` derives the typed client the CLI uses; `/openapi.json` and
 * `/docs` are generated from the same definition.
 */
export const BridgeApi = HttpApi.make("whatsapp-bridge")
  .add(HealthGroup, EventsGroup, ChatsGroup, ContactsGroup, ContextGroup, ScheduledGroup, TurnsGroup)
  .annotateMerge(
    OpenApi.annotations({
      title: "whatsapp-bridge",
      version: VERSION,
      description: "Local API of the WhatsApp ↔ Intro bridge. Events stream over SSE from /events; replies go to /turns/{id}/reply; stored context is read through /turns/{id}/context.",
    }),
  )
