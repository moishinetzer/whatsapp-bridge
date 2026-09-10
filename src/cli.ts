import { NodeServices } from "@effect/platform-node"
import { Console, Context, Duration, Effect, Layer, Option, Schema, Stream } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { HttpClientError } from "effect/unstable/http"
import { AmbientLive, EnvLive, coreLayer, daemonLayer, linkLayer } from "./app.ts"
import { bridgeUrl } from "./config.ts"
import { MessageId, TurnId, toChatId } from "./domain/ids.ts"
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
  },
  ({ to, text, file, caption, as, quote }) =>
    Effect.gen(function* () {
      const chatId = toChatId(to)
      if (Option.isSome(file)) {
        return yield* withClient((client) => client.chats.sendMedia({ params: { chatId }, payload: attachment(file.value, caption, as, quote) }))
      }
      if (Option.isSome(text)) {
        const payload = { text: text.value, ...(Option.isSome(quote) ? { quoteMessageId: MessageId.make(quote.value) } : {}) }
        return yield* withClient((client) => client.chats.send({ params: { chatId }, payload }))
      }
      return yield* new NothingToSend({ hint: "pass --text or --file" })
    }).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Send a text message or a file to a chat"))

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
  Command.withSubcommands([link, run, harness, health, send, edit, del, react, unreact, media, turns, reply, progress, abandon, unlink]),
)

/** The whole CLI, needing only the Node platform services. */
export const cli = Command.run(bridge, { version: VERSION }).pipe(Effect.provide(NodeServices.layer))
