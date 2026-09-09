# intro-whatsapp-bridge

A local daemon that puts **Intro** (the Grok Bot agent) on a **dedicated WhatsApp number**. It receives messages, acknowledges them with 👀 within seconds, hands them to Intro as one *turn* per burst, sends Intro's reply back in-thread, clears the 👀, and interrupts in-flight work when newer messages arrive. Intro's own reasoning stays outside this repo: the bridge is a messaging I/O layer with a small HTTP API on localhost.

Built as an [Effect v4](https://effect.website) service from top to bottom (services, layers, schemas, schedules, fibers, typed errors) over [Baileys](https://github.com/WhiskeySockets/Baileys) for the WhatsApp Web protocol.

> **Read this first.** This uses the *unofficial* WhatsApp Web protocol. Meta can and does ban numbers that use it. Only ever link the **dedicated** number, never a personal one, and accept that the number may be lost. The bridge never sends bulk or cold messages; it only talks in chats that talk to it.

## Contents

- [Quick start (15 minutes)](#quick-start-15-minutes)
- [Linking the number](#linking-the-number)
- [Running](#running)
- [How a turn works](#how-a-turn-works)
- [Integrating Intro](#integrating-intro)
- [HTTP API](#http-api)
- [Event schema (v1)](#event-schema-v1)
- [Configuration](#configuration)
- [Health and failure modes](#health-and-failure-modes)
- [Security and privacy](#security-and-privacy)
- [Development](#development)

## Quick start (15 minutes)

Requirements: Node 22+, the dedicated phone with WhatsApp installed and online.

```sh
git clone git@github.com:moishinetzer/intro-whatsapp-bridge.git
cd intro-whatsapp-bridge
npm install            # or: pnpm install
cp .env.example .env   # every value has a default; edit if you must

npm run link           # 1. scan the QR with the dedicated phone (see below)
npm run harness        # 2. text the number from another phone: 👀 → "pong" → 👀 gone
npm start              # 3. the real daemon, in manual responder mode
npm run health         # 4. {"state":"ok", ...}, exit code 0
```

Then point Intro at `http://127.0.0.1:8787` (see [Integrating Intro](#integrating-intro)). `examples/manual-loop.mjs` is a working 60-line Intro stand-in you can run right away:

```sh
node examples/manual-loop.mjs
```

All commands are also available as `npm run bridge -- <command>`; `npm run bridge -- --help` lists them.

## Linking the number

`npm run link` shows a QR code in the terminal. On the dedicated phone: **WhatsApp → Settings → Linked devices → Link a device → scan**. The command prints `Linked as <jid>` and exits; the session is saved under `./data/auth` (mode 0700) and restarts of the daemon do not need a new scan.

Prefer a pairing code (no camera needed)?

```sh
npm run link -- --phone 447700900123      # digits with country code, no +
```

On the phone: **Linked devices → Link a device → Link with phone number instead**, then type the code.

**Re-linking.** When WhatsApp invalidates the session (you unlinked the device on the phone, the number was logged out or banned), the daemon reports `needs_relink`, moves the old session aside as `data/auth.invalid-<time>` and keeps serving `/health` so Intro can see the state. Run `npm run link` again; nothing else changes. To start from scratch on purpose: `npm run bridge -- unlink --yes`.

## Running

```sh
npm start                          # = bridge run: WhatsApp + turn engine + HTTP API on 127.0.0.1:8787
npm run harness -- --slow "15 seconds" --once   # the acceptance harness, see below
```

`run` is long-lived; stop it with Ctrl-C (in-flight replies finish first). Run it under whatever keeps processes alive on Intro's computer (launchd, systemd, pm2, a tmux pane). Logs go to stdout; `LOG_FORMAT=json` for machines.

**The harness** (`bridge harness`) is the daemon with the built-in *pong* responder: it applies 👀, waits `--slow`, replies `pong` (or `pong (2 messages batched)`), then clears the 👀. `--once` exits after the first completed turn. With `--slow 15 seconds`, send a second message while it "thinks": you will see the first turn superseded and one reply that accounts for both messages. That is the interrupt path.

## How a turn works

```
inbound message ──▶ 👀 within seconds ──▶ batch window (1.5 s) ──▶ TurnOpened ──▶ responder ──▶ reply sent ──▶ 👀 cleared ──▶ TurnCompleted
                                                 ▲                                  │
                                                 └──── newer message arrives ◀──────┘   (TurnSuperseded: the responder call is cancelled,
                                                                                         a new turn carries all pending messages)
```

- **Seen acknowledgment.** Every accepted inbound message gets the 👀 reaction (retried with backoff). If the reaction cannot be applied after retries, a warning is logged; set `BRIDGE_SEEN_FALLBACK_TEXT` to send a short text instead.
- **Batching.** Messages from the same chat that arrive within `BRIDGE_BATCH_WINDOW` of each other are one turn. A message that arrives during the window restarts it.
- **Interrupts.** If a message arrives while the responder is working (or while Intro is composing a reply in manual mode), the turn is *superseded*: the responder call is interrupted (an HTTP handler sees its request aborted), a `TurnSuperseded` event names the replacement, and a new `TurnOpened` carries every unanswered message. A reply posted for the old turn is refused with 409 so a stale answer is never sent.
- **Commit is atomic.** Once the responder has answered, the send-and-clear step cannot be interrupted: the reply goes out, the 👀 come off, then any newer message starts a fresh turn of its own.
- **Idempotency.** Each WhatsApp message id is handled once per daemon run: one 👀, one turn. Echoes of the bridge's own messages are ignored (no reply loops).
- **Typing.** "typing…" is shown while a turn is being answered (`BRIDGE_TYPING=false` to disable). It is a courtesy, never the seen signal.
- **Failure.** If the responder fails, times out (`BRIDGE_TURN_TIMEOUT` in manual mode), or the send fails after retries, the turn is `failed`, a `TurnFailed` event says why, and the 👀 stays on as the visible "not handled" marker. The next message starts clean.

## Integrating Intro

The integration style is **A: local daemon + API**. Intro never touches the WhatsApp protocol; it talks HTTP to `127.0.0.1:8787`. Pick one of two ways to answer turns:

### Manual (default): Intro pulls events and posts replies

1. `GET /events` (server-sent events). The first event is always `Ready`; then `TurnOpened` events arrive with the batched messages.
2. Generate the reply.
3. `POST /turns/{turnId}/reply` with `{"text": "..."}` (or `{"replies": ["...", "..."]}` for several bubbles). The call returns once the messages are sent and the 👀 cleared, with the sent message ids.
4. On `409 TurnSuperseded`, drop the stale answer; the event stream already carries the replacement turn (`currentTurnId`).

`examples/manual-loop.mjs` implements exactly this loop, including reconnecting with `?since=<last seq>` so nothing is missed.

### HTTP handler: the bridge pushes turns to Intro

Set `BRIDGE_RESPONDER=http` and `INTRO_HANDLER_URL=http://127.0.0.1:9000/turn`. The bridge POSTs a `TurnPayload` (below) for each turn and sends whatever the handler returns: `{"text": "..."}`, `{"replies": [...]}`, `{}` (nothing to say; the turn completes and 👀 clears), or `{"async": true}` (answer later through `POST /turns/{id}/reply`). If newer messages supersede the turn, the request is **aborted** and a new one arrives. `examples/http-handler.mjs` is a working stub.

### Proactive messages and escalation

Any time, in either mode, Intro can `POST /chats/{chatId}/messages` to say something unprompted. That is how it asks Moishi to open the Grok Bot app: *"This needs the app: open Grok Bot → approve the Link spend for X."* The bridge never pretends an app-side action happened; that is Intro's rule to keep (see `docs/intro-integration.md` for all product rules Intro follows on this line).

### Chat ids

A DM's `chatId` is `<digits>@s.whatsapp.net`; a group's ends in `@g.us`; newer WhatsApp builds may hand out `<id>@lid`. Always send to the `chatId` you received. The CLI accepts a phone number and converts it (`bridge send --to "+44 7700 900123" --text hi`).

## HTTP API

Interactive docs: `http://127.0.0.1:8787/docs`; the OpenAPI document: `/openapi.json`. Every body is JSON. Errors are JSON objects with a `_tag`.

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| GET | `/health` | | `BridgeStatus` (200 only when `state` is `ok`) | 503 `Unhealthy{status}` |
| GET | `/status` | | `BridgeStatus`, always 200 | |
| GET | `/events?since=N` | | SSE stream of `BridgeEvent` | |
| POST | `/chats/{chatId}/messages` | `{text}` | `{chatId, messageId}` | 503 `NotConnected`, 502 `SendFailed` |
| POST | `/chats/{chatId}/messages/{messageId}/reactions` | `{emoji}` | 204 | same |
| DELETE | `/chats/{chatId}/messages/{messageId}/reactions` | | 204 | same |
| POST | `/chats/{chatId}/typing` | `{composing: bool}` | 204 | same |
| GET | `/turns` | | open `TurnSummary[]` | |
| GET | `/turns/{turnId}` | | `TurnSummary` | 404 `TurnNotFound` |
| POST | `/turns/{turnId}/reply` | `{text}` or `{replies: []}` | `{turnId, sentMessageIds}` | 404 `TurnNotFound`, 409 `TurnSuperseded{currentTurnId}`, 409 `TurnNotAwaitingReply{state}`, 502 `TurnFailedError{reason}` |

`GET /events` replays buffered events with `seq > since` (the last 200 are kept) before going live; without `since` you get live events only. The stream sends a `Heartbeat` every 25 seconds.

With `BRIDGE_API_TOKEN` set, every endpoint except `/health`, `/docs` and `/openapi.json` requires `Authorization: Bearer <token>`.

The same commands exist on the CLI for hand testing: `bridge health | send | react | unreact | turns | reply`.

## Event schema (v1)

Every event carries the envelope `{ "_tag", "v": 1, "seq", "at" }` (`seq` increases per real event; `at` is ISO-8601 UTC).

| `_tag` | Fields | Meaning |
|---|---|---|
| `Ready` | | First event on a connection; `seq` is the latest real seq (your cursor) |
| `Heartbeat` | | Keep-alive every 25 s |
| `MessageReceived` | `message` | A message was accepted; 👀 is being applied |
| `TurnOpened` | `turnId, chatId, isGroup, messages[], supersedes?` | These messages want one reply |
| `TurnSuperseded` | `turnId, chatId, by` | Stop working on `turnId`; `by` has everything |
| `TurnCompleted` | `turnId, chatId, sentMessageIds[]` | Reply sent, 👀 cleared |
| `TurnFailed` | `turnId, chatId, reason` | No reply could be sent; 👀 stays |
| `ConnectionChanged` | `state, detail?` | Mirrors `/health` |

`message` (`InboundMessage`):

```json
{
  "chatId": "447700900123@s.whatsapp.net",
  "messageId": "3EB0C8F1A2B3C4D5E6F7",
  "senderId": "447700900123@s.whatsapp.net",
  "senderNumber": "447700900123",
  "senderName": "Moishi",
  "isGroup": false,
  "timestamp": "2026-09-09T10:15:30.000Z",
  "kind": "text",
  "text": "can you check the calendar for tomorrow?",
  "hasMedia": false,
  "mimetype": "image/jpeg",
  "quotedMessageId": "..."
}
```

`kind` is one of `text, image, video, audio, document, sticker, location, contact, other`. **Media bytes are not delivered in v1**: `hasMedia`, `kind` and `mimetype` tell Intro something was attached and `text` carries the caption. Fetching media is planned for a later version.

`TurnPayload` (what the `http` responder POSTs and what `TurnOpened` contains): `{ "v": 1, "turnId", "chatId", "isGroup", "messages": [InboundMessage], "supersedes"? }`.

The exact schemas live in `src/domain/*.ts` and are what the OpenAPI document is generated from.

## Configuration

Environment variables, with `.env` in the working directory as a fallback (`.env.example` documents all of them).

| Variable | Default | Purpose |
|---|---|---|
| `BRIDGE_DATA_DIR` | `./data` | Session lives in `$BRIDGE_DATA_DIR/auth` |
| `BRIDGE_HOST` / `BRIDGE_PORT` | `127.0.0.1` / `8787` | Where the API listens |
| `BRIDGE_API_TOKEN` | unset | Bearer token for the API |
| `BRIDGE_RESPONDER` | `manual` | `manual`, `http` or `pong` |
| `INTRO_HANDLER_URL` | | Required for `http` |
| `INTRO_HANDLER_TIMEOUT` | `120 seconds` | Per handler call |
| `BRIDGE_BATCH_WINDOW` | `1500 millis` | Batch window per chat |
| `BRIDGE_TURN_TIMEOUT` | `10 minutes` | Manual mode: how long a turn waits for a reply |
| `BRIDGE_SEEN_EMOJI` | `👀` | The seen reaction |
| `BRIDGE_SEEN_FALLBACK_TEXT` | unset | Text ack if the reaction fails |
| `BRIDGE_GROUPS` | `false` | Handle group chats |
| `BRIDGE_ALLOWED_SENDERS` | unset (anyone) | Comma-separated numbers/jids allowed to talk to the line |
| `BRIDGE_OFFLINE_WINDOW` | `0 seconds` | Handle messages replayed after downtime that are younger than this |
| `BRIDGE_TYPING` | `true` | Typing indicator while answering |
| `LOG_LEVEL` / `LOG_FORMAT` / `LOG_BODIES` | `Info` / `pretty` / `false` | Logging |
| `BAILEYS_LOG_LEVEL` | `silent` | The protocol library's logger |
| `WA_BROWSER_NAME` | `Intro Bridge` | Name in the phone's Linked devices list |
| `BRIDGE_URL` | `http://$HOST:$PORT` | Where the CLI client commands find the daemon |

Durations use Effect's format: `"1500 millis"`, `"2 seconds"`, `"10 minutes"`.

## Health and failure modes

`GET /health` and `bridge health` (exit code 0 only when `ok`):

```json
{ "state": "ok", "linkedAs": { "jid": "4479...@s.whatsapp.net", "name": "Intro" }, "responder": "manual",
  "version": "0.1.0", "startedAt": "...", "lastEventAt": "...", "lastInboundAt": "...", "lastOutboundAt": "...", "openTurns": 0 }
```

| `state` | Meaning | What to do |
|---|---|---|
| `starting` | Process is up, not connected yet | Wait a few seconds |
| `linking` | `bridge link` is waiting for the phone | Scan / enter the code |
| `connecting` | Session exists, socket (re)connecting | Wait; backoff is jittered and capped at 60 s |
| `ok` | Messages flow | |
| `needs_relink` | No session, logged out, or banned (`detail` says which) | `npm run link` |
| `stopped` | Daemon not running (CLI only) | Start it |

While `connecting`, sends fail with `NotConnected`; the engine retries outbound calls with backoff for about half a minute, which covers a normal reconnect. `TurnFailed` events and the `failure` field on a turn carry the reason when it does not.

Message bodies never appear in logs above Debug, and only with `LOG_BODIES=true`. Session credentials never appear in logs at all.

## Security and privacy

- The session under `data/auth` is the keys to the number. The directory is created with mode 0700 and is git-ignored, as are `.env` files. Never commit or share it.
- The API binds to loopback by default. If you must expose it, set `BRIDGE_API_TOKEN` and put it behind something that terminates TLS.
- The bridge makes no calls to OpenAI, xAI or any model provider. Its only outbound connections are WhatsApp's servers, the WhatsApp Web version lookup at startup, and `INTRO_HANDLER_URL` if you configure one.
- `markOnlineOnConnect` is off so the phone keeps receiving notifications.

## Development

```sh
npm run typecheck     # tsc, strict
npm test              # vitest: engine (batching, interrupts, atomic commit, failures, manual replies), API, extraction
npm run check         # both
npm run build         # dist/ + bin/bridge.js
```

Layout: `src/domain` (ids, message, event and status schemas), `src/whatsapp` (the transport: Baileys layer, session store, message extraction), `src/engine` (health, events, responders, the turn registry and per-chat workers), `src/http` (the `HttpApi` definition, handlers, the typed client), `src/cli.ts`. Tests drive the real engine against an in-memory transport and a scriptable responder under Effect's `TestClock`.

Notes on the Effect v4 APIs and the traps met while building this are in `docs/effect-notes.md`. How Intro should behave on this line is in `docs/intro-integration.md`.
