import { NodeServices } from "@effect/platform-node"
import { Console, Context, Duration, Effect, Layer, Option, Schema, Stream } from "effect"
import { Command, Flag } from "effect/unstable/cli"
import { HttpClientError } from "effect/unstable/http"
import { AmbientLive, EnvLive, coreLayer, daemonLayer, linkLayer } from "./app.ts"
import { bridgeUrl } from "./config.ts"
import { MessageId, TurnId, toChatId } from "./domain/ids.ts"
import { Events } from "./engine/Events.ts"
import { bridgeClient } from "./http/Client.ts"
import { VERSION } from "./version.ts"
import { AuthStore } from "./whatsapp/AuthStore.ts"
import { WhatsApp } from "./whatsapp/WhatsApp.ts"

class DaemonUnreachable extends Schema.TaggedError<DaemonUnreachable>()("DaemonUnreachable", {
  url: Schema.String,
  detail: Schema.String,
}) {}

class ConfirmationRequired extends Schema.TaggedError<ConfirmationRequired>()("ConfirmationRequired", {
  hint: Schema.String,
}) {}

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
  },
  ({ phone }) =>
    Effect.gen(function* () {
      const whatsapp = yield* WhatsApp
      yield* Console.log("Linking the dedicated number. Keep this terminal open until it says linked.")
      const identity = yield* whatsapp.linked
      // Give the credential writer a moment to flush the last keys.
      yield* Effect.sleep("2 seconds")
      yield* Console.log(`\nLinked as ${identity.jid}${identity.name ? ` (${identity.name})` : ""}.`)
      yield* Console.log("The session is saved. Start the daemon with: npm run bridge -- run")
    }).pipe(Effect.provide(linkLayer(Option.getOrUndefined(phone)))),
).pipe(Command.withDescription("Link the dedicated WhatsApp number (QR by default, or a pairing code with --phone)"))

const run = Command.make("run", {}, () =>
  Effect.gen(function* () {
    yield* Effect.logInfo("intro-whatsapp-bridge starting").pipe(Effect.annotateLogs({ version: VERSION }))
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

const send = Command.make(
  "send",
  {
    to: Flag.string("to").pipe(Flag.withDescription("Chat jid or phone number with country code")),
    text: Flag.string("text"),
  },
  ({ to, text }) =>
    withClient((client) => client.chats.send({ params: { chatId: toChatId(to) }, payload: { text } })).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Send a text message to a chat"))

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

const turns = Command.make("turns", {}, () => withClient((client) => client.turns.list()).pipe(Effect.flatMap(json))).pipe(
  Command.withDescription("List the turns still waiting for a reply"),
)

const reply = Command.make(
  "reply",
  { turn: Flag.string("turn"), text: Flag.string("text") },
  ({ turn, text }) =>
    withClient((client) => client.turns.reply({ params: { turnId: TurnId.make(turn) }, payload: { text } })).pipe(Effect.flatMap(json)),
).pipe(Command.withDescription("Answer an open turn (manual responder mode)"))

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
  Command.withSubcommands([link, run, harness, health, send, react, unreact, turns, reply, unlink]),
)

/** The whole CLI, needing only the Node platform services. */
export const cli = Command.run(bridge, { version: VERSION }).pipe(Effect.provide(NodeServices.layer))
