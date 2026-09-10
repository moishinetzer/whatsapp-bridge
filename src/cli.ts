import { NodeServices } from "@effect/platform-node"
import { Console, Context, DateTime, Duration, Effect, FileSystem, Layer, Option, Path, Schema, Stream } from "effect"
import { Command, Flag, Prompt } from "effect/unstable/cli"
import { HttpClientError } from "effect/unstable/http"
import { AmbientLive, EnvLive, acceptanceFile, coreLayer, daemonLayer, linkLayer } from "./app.ts"
import { BridgeConfig, BridgeConfigLive, bridgeUrl } from "./config.ts"
import { MessageId, TurnId, toChatId } from "./domain/ids.ts"
import { Store, StoreWithDatabaseLive } from "./store/Store.ts"
import { OutboundKind, type OutboundMediaPayload } from "./domain/outbound.ts"
import { Events } from "./engine/Events.ts"
import { bridgeClient } from "./http/Client.ts"
import { VERSION } from "./version.ts"
import { AuthStore } from "./whatsapp/AuthStore.ts"
import { WhatsApp } from "./whatsapp/WhatsApp.ts"

class DaemonUnreachable extends Schema.TaggedError<DaemonUnreachable>()("DaemonUnreachable", {
  url: Schema.String,
  detail: Schema.String,
}) {
  override get message() {
    return `daemon not reachable at ${this.url}: ${this.detail}`
  }
}

class ConfirmationRequired extends Schema.TaggedError<ConfirmationRequired>()("ConfirmationRequired", {
  hint: Schema.String,
}) {
  override get message() {
    return this.hint
  }
}

class NothingToSend extends Schema.TaggedError<NothingToSend>()("NothingToSend", {
  hint: Schema.String,
}) {
  override get message() {
    return this.hint
  }
}

class LinkTimedOut extends Schema.TaggedError<LinkTimedOut>()("LinkTimedOut", {
  after: Schema.String,
  hint: Schema.String,
}) {
  override get message() {
    return `not linked after ${this.after}: ${this.hint}`
  }
}

const json = (value: unknown) => Console.log(JSON.stringify(value, null, 2))

/** The verdict of `bridge health` travels in the exit code; the JSON stays clean. */
const exitCode = (code: number) => Effect.sync(() => void (process.exitCode = code))

type Client = Effect.Success<typeof bridgeClient>

/** Runs a client call; a daemon that is not listening becomes `DaemonUnreachable`. */
const withClient = <A, E>(f: (client: Client) => Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const url = yield* bridgeUrl
    const client = yield* bridgeClient
    return yield* f(client).pipe(
      Effect.mapError((e) =>
        HttpClientError.isHttpClientError(e) ? new DaemonUnreachable({ url, detail: e.message }) : e,
      ),
    )
  }).pipe(Effect.provide(EnvLive))

// ---------------------------------------------------------------------------

const link = Command.make(
  "link",
  {
    phone: Flag.string("phone").pipe(
      Flag.optional,
      Flag.withDescription("Link with a pairing code for this number (digits, with country code) instead of a QR"),
    ),
    timeout: Flag.string("timeout").pipe(
      Flag.withSchema(Schema.DurationFromString),
      Flag.withDefault(Duration.minutes(5)),
      Flag.withDescription("Give up if the phone has not linked within this long, e.g. \"10 minutes\""),
    ),
  },
  ({ phone, timeout }) =>
    Effect.gen(function* () {
      const whatsapp = yield* WhatsApp
      yield* Console.log("Linking the dedicated number. Keep this terminal open until it says linked.")
      const identity = yield* whatsapp.linked.pipe(
        Effect.timeout(timeout),
        Effect.catchTag("TimeoutError", () =>
          new LinkTimedOut({ after: Duration.format(timeout), hint: "the phone never completed the link; run `bridge link` again" }),
        ),
      )
      // Give the credential writer a moment to flush the last keys.
      yield* Effect.sleep("2 seconds")
      yield* Console.log(`\nLinked as ${identity.jid}${identity.name ? ` (${identity.name})` : ""}.`)
      yield* Console.log("The session is saved. Start the daemon with: npm run bridge -- run")
    }).pipe(Effect.provide(linkLayer(Option.getOrUndefined(phone)))),
).pipe(Command.withDescription("Link the dedicated WhatsApp number (QR by default, or a pairing code with --phone)"))

const run = Command.make("run", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo("whatsapp-bridge starting").pipe(Effect.annotateLogs({ version: VERSION }))
    yield* Effect.never
  }).pipe(Effect.provide(daemonLayer())),
).pipe(Command.withDescription("Run the bridge daemon: WhatsApp connection, turn engine and the local HTTP API"))

const harness = Command.make(
  "harness",
  {
    slow: Flag.string("slow").pipe(
      Flag.withSchema(Schema.DurationFromString),
      Flag.withDefault(Duration.zero),
      Flag.withDescription("How long the pong responder pretends to think, e.g. \"15 seconds\" (send a second message meanwhile to see an interrupt)"),
    ),
    once: Flag.boolean("once").pipe(Flag.withDescription("Exit after the first completed turn")),
  },
  ({ slow, once }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(daemonLayer({ responder: "pong", harnessSlow: slow }))
        const events = Context.get(context, Events)
        yield* Effect.logInfo("harness running: send a text to the dedicated number and watch for 👀 → pong → 👀 cleared").pipe(
          Effect.annotateLogs({ slowMs: Duration.toMillis(slow), once }),
        )
        if (once) {
          yield* events.stream.pipe(
            Stream.filter((e) => e._tag === "TurnCompleted"),
            Stream.take(1),
            Stream.runDrain,
          )
          yield* Effect.logInfo("harness: first turn completed, exiting")
        } else {
          yield* Effect.never
        }
      }),
    ),
).pipe(Command.withDescription("Run the daemon with the built-in pong responder: 👀 on inbound, reply \"pong\", clear 👀"))

const health = Command.make("health", {}, () =>
  withClient((client) => client.health.status()).pipe(
    Effect.tap((status) => json(status)),
    Effect.flatMap((status) => exitCode(status.state === "ok" ? 0 : 1)),
    Effect.catchTag("DaemonUnreachable", (e) =>
      json({ state: "stopped", detail: `daemon not reachable at ${e.url}: ${e.detail}` }).pipe(Effect.andThen(exitCode(1))),
    ),
  ),
).pipe(Command.withDescription("Print the daemon's status as JSON; exit code 0 only when the state is ok"))

const fileFlags = {
  file: Flag.string("file").pipe(Flag.optional, Flag.withDescription("Path of an image, video, audio or document to send")),
  caption: Flag.string("caption").pipe(Flag.optional, Flag.withDescription("Caption under the file (image, video, document)")),
  as: Flag.choice("as", OutboundKind.literals).pipe(
    Flag.optional,
    Flag.withDescription("Force how WhatsApp shows the file (default: from the file type)"),
  ),
}

const quoteFlag = Flag.string("quote").pipe(Flag.optional, Flag.withDescription("Send as a reply to this message id"))

const attachment = (
  file: string,
  caption: Option.Option<string>,
  as: Option.Option<OutboundKind>,
  quote: Option.Option<string> = Option.none(),
): OutboundMediaPayload => ({
  path: file,
  ...(Option.isSome(caption) ? { caption: caption.value } : {}),
  ...(Option.isSome(as) ? { kind: as.value } : {}),
  ...(Option.isSome(quote) ? { quoteMessageId: MessageId.make(quote.value) } : {}),
})

const send = Command.make(
  "send",
  {
    to: Flag.string("to").pipe(Flag.withDescription("Chat jid or phone number with country code")),
    text: Flag.string("text").pipe(Flag.optional, Flag.withDescription("The message; with --file, use --caption instead")),
    ...fileFlags,
    quote: quoteFlag,
    at: Flag.string("at").pipe(
      Flag.withSchema(Schema.DateTimeUtcFromString),
      Flag.optional,
      Flag.withDescription("Hold the send until this instant (ISO-8601), e.g. 2026-09-11T08:00:00Z"),
    ),
    whenOnline: Flag.boolean("when-online").pipe(Flag.withDescription("With --at: also wait until the recipient is online (24 hours at most)")),
  },
  ({ to, text, file, caption, as, quote, at, whenOnline }) =>
    Effect.gen(function* () {
      const chatId = toChatId(to)
      const scheduling = Option.isSome(at) ? { sendAt: at.value, deliverWhenOnline: whenOnline } : {}
      if (Option.isSome(file)) {
        return yield* withClient((client) =>
          client.chats.sendMedia({ params: { chatId }, payload: { ...attachment(file.value, caption, as, quote), ...scheduling } }),
        )
      }
      if (Option.isSome(text)) {
        const payload = { text: text.value, ...(Option.isSome(quote) ? { quoteMessageId: MessageId.make(quote.value) } : {}), ...scheduling }
        return yield* withClient((client) => client.chats.send({ params: { chatId }, payload }))
      }
      return yield* new NothingToSend({ hint: "pass --text or --file" })
    }).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Send a text message or a file to a chat, now or at a given time"))

const forward = Command.make(
  "forward",
  {
    from: Flag.string("from").pipe(Flag.withDescription("Chat the message is in")),
    message: Flag.string("message").pipe(Flag.withDescription("The message id")),
    to: Flag.string("to").pipe(Flag.withDescription("Chat jid or phone number to forward to")),
  },
  ({ from, message, to }) =>
    withClient((client) =>
      client.chats.forward({ params: { chatId: toChatId(to) }, payload: { fromChatId: toChatId(from), messageId: MessageId.make(message) } }),
    ).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Forward a message the bridge saw or sent to another chat"))

const contact = Command.make(
  "contact",
  { jid: Flag.string("jid").pipe(Flag.withDescription("A jid or a phone number with country code")) },
  ({ jid }) => withClient((client) => client.history.contact({ params: { jid: jid.includes("@") ? jid : toChatId(jid) } })).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Look a contact up: push name, phone, picture URL, whether the number is on WhatsApp"))

const chats = Command.make("chats", {}, () => withClient((client) => client.history.chats()).pipe(Effect.flatMap(json))).pipe(
  Command.withDescription("List the chats in the history (needs BRIDGE_HISTORY)"),
)

const messages = Command.make(
  "messages",
  {
    chat: Flag.string("chat"),
    q: Flag.string("q").pipe(Flag.optional, Flag.withDescription("Full-text search within the chat")),
    before: Flag.string("before").pipe(Flag.optional, Flag.withDescription("Page back from this message id")),
    limit: Flag.integer("limit").pipe(Flag.withDefault(50)),
  },
  ({ chat, q, before, limit }) =>
    withClient((client) =>
      client.history.chatMessages({
        params: { chatId: toChatId(chat) },
        query: { limit, ...(Option.isSome(q) ? { q: q.value } : {}), ...(Option.isSome(before) ? { before: MessageId.make(before.value) } : {}) },
      }),
    ).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Show a chat's history, newest first (needs BRIDGE_HISTORY)"))

const search = Command.make(
  "search",
  { q: Flag.string("q"), limit: Flag.integer("limit").pipe(Flag.withDefault(50)) },
  ({ q, limit }) => withClient((client) => client.history.search({ query: { q, limit } })).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Full-text search across every chat (needs BRIDGE_HISTORY)"))

const scheduled = Command.make("scheduled", {}, () => withClient((client) => client.scheduled.list()).pipe(Effect.flatMap(json))).pipe(
  Command.withDescription("List scheduled sends: pending ones and those closed in the last day"),
)

const unschedule = Command.make("unschedule", { id: Flag.string("id") }, ({ id }) =>
  withClient((client) => client.scheduled.cancel({ params: { scheduledId: id } })).pipe(Effect.andThen(json({ ok: true }))),
).pipe(Command.withDescription("Cancel a pending scheduled send"))

const ACCEPTANCE = `History records every message this number sends and receives, in both directions:
the text, captions, edits, deletions (as tombstones), reactions, delivery state, the path of
downloaded attachments, and the raw message so replies and forwards work after a restart.
It is kept indefinitely, in plain text, in the bridge's SQLite file, and searchable by anyone
who can read that file or reach the API. Delete with \`bridge history purge\`.`

const historyEnable = Command.make(
  "enable",
  { yes: Flag.boolean("yes").pipe(Flag.withDescription("Accept without the prompt (for scripts)")) },
  ({ yes }) =>
    Effect.gen(function* () {
      const config = yield* BridgeConfig
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const file = path.resolve(acceptanceFile(config.dataDir))
      yield* Console.log(ACCEPTANCE)
      const accepted = yes ? true : yield* Prompt.run(Prompt.confirm({ message: "Record message history on this machine?", initial: false }))
      if (!accepted) return yield* Console.log("Not enabled. Nothing was written.")
      yield* fs.makeDirectory(path.dirname(file), { recursive: true, mode: 0o700 }).pipe(Effect.ignore)
      yield* fs.writeFileString(file, `${DateTime.formatIso(yield* DateTime.now)}\n`, { mode: 0o600 })
      yield* Console.log(`Accepted: ${file}`)
      yield* Console.log(config.history ? "BRIDGE_HISTORY is on; restart the daemon." : "Now set BRIDGE_HISTORY=true and restart the daemon.")
    }).pipe(Effect.provide(Layer.mergeAll(BridgeConfigLive().pipe(Layer.provide(EnvLive)), NodeServices.layer))),
).pipe(Command.withDescription("Accept, once, that every message gets recorded; required before BRIDGE_HISTORY=true"))

const historyPurge = Command.make(
  "purge",
  {
    before: Flag.string("before").pipe(Flag.withSchema(Schema.DateTimeUtcFromString), Flag.withDescription("Delete history older than this instant (ISO-8601)")),
    yes: Flag.boolean("yes").pipe(Flag.withDescription("Really delete")),
  },
  ({ before, yes }) =>
    Effect.gen(function* () {
      if (!yes) return yield* new ConfirmationRequired({ hint: "pass --yes to delete history" })
      const store = yield* Store
      const removed = yield* store.purgeBefore(before)
      yield* Console.log(`Removed ${removed} message(s) older than ${DateTime.formatIso(before)}.`)
    }).pipe(
      Effect.provide(
        Layer.mergeAll(StoreWithDatabaseLive, BridgeConfigLive({ history: true })).pipe(
          Layer.provideMerge(Layer.mergeAll(BridgeConfigLive({ history: true }).pipe(Layer.provide(EnvLive)), NodeServices.layer)),
        ),
      ),
    ),
).pipe(Command.withDescription("Delete history older than a date (the daemon may keep running)"))

const history = Command.make("history").pipe(
  Command.withDescription("Message history: enable it once, or purge old rows"),
  Command.withSubcommands([historyEnable, historyPurge]),
)

const edit = Command.make(
  "edit",
  { chat: Flag.string("chat"), message: Flag.string("message").pipe(Flag.withDescription("Id of a message the bridge sent")), text: Flag.string("text") },
  ({ chat, message, text }) =>
    withClient((client) =>
      client.chats.edit({ params: { chatId: toChatId(chat), messageId: MessageId.make(message) }, payload: { text } }),
    ).pipe(Effect.andThen(json({ ok: true }))),
).pipe(Command.withDescription("Edit a message the bridge sent (WhatsApp allows it for 15 minutes)"))

const del = Command.make(
  "delete",
  { chat: Flag.string("chat"), message: Flag.string("message").pipe(Flag.withDescription("Id of a message the bridge sent")) },
  ({ chat, message }) =>
    withClient((client) => client.chats.deleteMessage({ params: { chatId: toChatId(chat), messageId: MessageId.make(message) } })).pipe(
      Effect.andThen(json({ ok: true })),
    ),
).pipe(Command.withDescription("Delete for everyone a message the bridge sent (WhatsApp allows it for about 2 days)"))

const progress = Command.make(
  "progress",
  {
    turn: Flag.string("turn"),
    text: Flag.string("text").pipe(Flag.optional, Flag.withDescription("An interim bubble, e.g. \"still checking\"")),
    extend: Flag.string("extend").pipe(
      Flag.withSchema(Schema.DurationFromString),
      Flag.optional,
      Flag.withDescription("Push the reply deadline this far out, e.g. \"20 minutes\""),
    ),
  },
  ({ turn, text, extend }) =>
    withClient((client) =>
      client.turns.progress({
        params: { turnId: TurnId.make(turn) },
        payload: { ...(Option.isSome(text) ? { text: text.value } : {}), ...(Option.isSome(extend) ? { extendBy: extend.value } : {}) },
      }),
    ).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Tell the bridge a turn is still being worked on: keeps typing alive and extends the deadline"))

const abandon = Command.make(
  "abandon",
  {
    turn: Flag.string("turn"),
    reason: Flag.string("reason"),
    text: Flag.string("text").pipe(Flag.optional, Flag.withDescription("What the user is told instead of BRIDGE_FAILURE_TEXT")),
  },
  ({ turn, reason, text }) =>
    withClient((client) =>
      client.turns.abandon({ params: { turnId: TurnId.make(turn) }, payload: { reason, ...(Option.isSome(text) ? { text: text.value } : {}) } }),
    ).pipe(Effect.andThen(json({ ok: true }))),
).pipe(Command.withDescription("Give up on an open turn: it fails with the reason and the user gets the failure text"))

const react = Command.make(
  "react",
  {
    chat: Flag.string("chat"),
    message: Flag.string("message").pipe(Flag.withDescription("The WhatsApp message id")),
    emoji: Flag.string("emoji").pipe(Flag.withDefault("👀")),
  },
  ({ chat, message, emoji }) =>
    withClient((client) =>
      client.chats.react({ params: { chatId: toChatId(chat), messageId: MessageId.make(message) }, payload: { emoji } }),
    ).pipe(Effect.andThen(json({ ok: true }))),
).pipe(Command.withDescription("Add a reaction to a message"))

const unreact = Command.make(
  "unreact",
  { chat: Flag.string("chat"), message: Flag.string("message") },
  ({ chat, message }) =>
    withClient((client) => client.chats.unreact({ params: { chatId: toChatId(chat), messageId: MessageId.make(message) } })).pipe(
      Effect.andThen(json({ ok: true })),
    ),
).pipe(Command.withDescription("Remove our reaction from a message"))

const media = Command.make(
  "media",
  {
    chat: Flag.string("chat"),
    message: Flag.string("message").pipe(Flag.withDescription("The WhatsApp message id carrying the attachment")),
  },
  ({ chat, message }) =>
    withClient((client) => client.chats.media({ params: { chatId: toChatId(chat), messageId: MessageId.make(message) } })).pipe(
      Effect.flatMap(json),
    ),
).pipe(Command.withDescription("Download a message's attachment (if not already on disk) and print where it is"))

const turns = Command.make("turns", {}, () => withClient((client) => client.turns.list()).pipe(Effect.flatMap(json))).pipe(
  Command.withDescription("List the turns still waiting for a reply"),
)

const reply = Command.make(
  "reply",
  {
    turn: Flag.string("turn"),
    text: Flag.string("text").pipe(Flag.optional, Flag.withDescription("The reply text; omit it to answer with just the file")),
    ...fileFlags,
    quote: quoteFlag,
    replaceProgress: Flag.boolean("replace-progress").pipe(Flag.withDescription("Edit the last progress bubble into the reply instead of sending a new one")),
  },
  ({ turn, text, file, caption, as, quote, replaceProgress }) =>
    Effect.gen(function* () {
      if (Option.isNone(text) && Option.isNone(file)) return yield* new NothingToSend({ hint: "pass --text and/or --file" })
      const payload = {
        ...(Option.isSome(text) ? { text: text.value } : {}),
        ...(Option.isSome(file) ? { attachments: [attachment(file.value, caption, as)] } : {}),
        ...(Option.isSome(quote) ? { quoteMessageId: MessageId.make(quote.value) } : {}),
        ...(replaceProgress ? { replaceProgress: true } : {}),
      }
      return yield* withClient((client) => client.turns.reply({ params: { turnId: TurnId.make(turn) }, payload }))
    }).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Answer an open turn (manual responder mode) with text, a file, or both"))

const unlink = Command.make(
  "unlink",
  { yes: Flag.boolean("yes").pipe(Flag.withDescription("Really delete the saved session")) },
  ({ yes }) =>
    Effect.gen(function* () {
      if (!yes) return yield* new ConfirmationRequired({ hint: "pass --yes to delete the saved session" })
      const store = yield* AuthStore
      yield* store.remove
      yield* Console.log(`Removed ${store.dir}. Run \`bridge link\` to link again.`)
    }).pipe(Effect.provide(Layer.mergeAll(coreLayer({ mode: "link" }), AmbientLive))),
).pipe(Command.withDescription("Delete the saved WhatsApp session so the next `link` starts clean"))

const bridge = Command.make("bridge").pipe(
  Command.withDescription("WhatsApp ↔ Intro bridge on a dedicated number"),
  Command.withSubcommands([link, run, harness, health, send, forward, edit, del, react, unreact, media, contact, chats, messages, search, scheduled, unschedule, turns, reply, progress, abandon, history, unlink]),
)

/** The whole CLI, needing only the Node platform services. */
export const cli = Command.run(bridge, { version: VERSION }).pipe(Effect.provide(NodeServices.layer))
