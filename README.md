# whatsapp-bridge

A local daemon that puts **Intro** (the Grok Bot agent) on a **dedicated WhatsApp number**. It receives messages (attachments included), acknowledges them with 👀 within seconds, hands them to Intro as one *turn* per burst, sends Intro's reply back in-thread (text, images, files), clears the 👀, and interrupts in-flight work when newer messages arrive. Intro's own reasoning stays outside this repo: the bridge is a messaging I/O layer with a small HTTP API on localhost.

Built as an [Effect v4](https://effect.website) service from top to bottom (services, layers, schemas, schedules, fibers, typed errors) over [Baileys](https://github.com/WhiskeySockets/Baileys) for the WhatsApp Web protocol.

> **Read this first.** This uses the *unofficial* WhatsApp Web protocol. Meta can and does ban numbers that use it. Only ever link the **dedicated** number, never a personal one, and accept that the number may be lost. The bridge never sends bulk or cold messages; it only talks in chats that talk to it.

## Contents

- [Quick start (15 minutes)](#quick-start-15-minutes)
- [Linking the number](#linking-the-number)
- [Running](#running)
- [How a turn works](#how-a-turn-works)
- [Integrating Intro](#integrating-intro)
- [Attachments](#attachments)
- [History](#history)
- [Reference for the agent](#reference-for-the-agent)
  - [Conventions](#conventions)
  - [Endpoints](#endpoints)
  - [Events](#events)
  - [Objects](#objects)
  - [Errors](#errors)
  - [Responder contracts](#responder-contracts)
  - [CLI](#cli)
  - [Playbooks](#playbooks)
- [Configuration](#configuration)
- [Health and failure modes](#health-and-failure-modes)
- [What happens when things go wrong](#what-happens-when-things-go-wrong)
- [Security and privacy](#security-and-privacy)
- [Development](#development)

## Quick start (15 minutes)

Requirements: Node 22+, the dedicated phone with WhatsApp installed and online.

```sh
git clone git@github.com:moishinetzer/whatsapp-bridge.git
cd whatsapp-bridge
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

`npm run link -- --timeout "10 minutes"` gives up if the phone never completes the link (default 5 minutes).

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

- **Seen acknowledgment.** Every accepted inbound message is marked read (blue ticks, `BRIDGE_READ_RECEIPTS`) and gets the 👀 reaction within seconds, retried with backoff. One message per chat carries the eye: when the next message arrives, the eye comes off the previous one and goes onto the new one. If the reaction cannot be applied after retries, a warning is logged; set `BRIDGE_SEEN_FALLBACK_TEXT` to send a short text instead.
- **Batching.** Messages from the same chat that arrive within `BRIDGE_BATCH_WINDOW` of each other are one turn. A message that arrives during the window restarts it, but a turn always opens within `BRIDGE_BATCH_MAX_WAIT` of its oldest message, so a steady trickle cannot postpone a reply forever. While the sender is still typing (WhatsApp's presence says so) the window keeps stretching, up to `BRIDGE_BATCH_TYPING_WAIT` (15 seconds) from the oldest message, so "wait, and also…" lands in the same turn.
- **Groups.** With `BRIDGE_GROUPS=true`, a group message opens a turn only when it @-mentions the bridge's number or quotes one of its messages (`BRIDGE_GROUP_TRIGGER=mention`; `always` treats every message as a DM would). Other group messages are stored with history on, emit no event, and feed a per-group ring of the last messages: every group `TurnOpened` carries `context`, the last 10 messages before the trigger, so the agent sees what was being discussed. Reading a group message marks it read; only turn messages get the eye.
- **Interrupts.** If a message arrives while the responder is working (or while Intro is composing a reply in manual mode), the turn is *superseded*: the responder call is interrupted (an HTTP handler sees its request aborted), a `TurnSuperseded` event names the replacement, and a new `TurnOpened` carries every unanswered message. A reply posted for the old turn is refused with 409 so a stale answer is never sent. An edit of a pending message does the same with the corrected text; a delete drops the message, and a turn with nothing left is `cancelled`.
- **Commit is atomic.** Once the responder has answered, the send-and-clear step cannot be interrupted: the reply goes out, the 👀 come off, then any newer message starts a fresh turn of its own.
- **Idempotency.** Each WhatsApp message id is handled once per daemon run: one 👀, one turn. Echoes of the bridge's own messages are ignored (no reply loops).
- **Typing.** "typing…" is re-sent every 8 seconds while a turn is being answered, for at most `BRIDGE_TYPING_MAX` (3 minutes), re-armed by each progress call (`BRIDGE_TYPING=false` to disable). It is a courtesy, never the seen signal.
- **Progress.** A long turn can say "still working": `POST /turns/{id}/progress` extends the deadline, re-arms typing, and can send an interim bubble that the final reply edits into its own text.
- **Failure.** If the responder fails, times out (`BRIDGE_TURN_TIMEOUT` in manual mode), is abandoned by the agent, or the send fails after retries, the turn is `failed`, a `TurnFailed` event says why, the eye comes off, and the bridge replies to the newest message with `BRIDGE_FAILURE_TEXT` ("Something went wrong."). The next message starts clean.

## Integrating Intro

The integration style is **A: local daemon + API**. Intro never touches the WhatsApp protocol; it talks HTTP to `127.0.0.1:8787`. Pick one of two ways to answer turns; everything they call is specified in the [reference](#reference-for-the-agent).

### Manual (default): Intro pulls events and posts replies

1. `GET /events` (server-sent events). The first event is always `Ready`; then `TurnOpened` events arrive with the batched messages.
2. Generate the reply.
3. `POST /turns/{turnId}/reply` with `{"text": "..."}` (or `{"replies": ["...", "..."]}` for several bubbles, plus `"attachments": [{"path": "/tmp/chart.png", "caption": "..."}]` for files). The call returns once the messages are sent and the 👀 cleared, with the sent message ids.
4. On `409 TurnSuperseded`, drop the stale answer; the event stream already carries the replacement turn (`currentTurnId`).
5. Taking a while? `POST /turns/{turnId}/progress {"text": "Checking…"}` keeps the typing indicator alive, pushes the deadline out, and the final reply can pass `replaceProgress: true` to edit that bubble instead of adding one. Giving up? `POST /turns/{turnId}/abandon {"reason": "..."}`.

`examples/manual-loop.mjs` implements exactly this loop, including reconnecting with `?since=<last seq>` so nothing is missed.

### HTTP handler: the bridge pushes turns to Intro

Set `BRIDGE_RESPONDER=http` and `INTRO_HANDLER_URL=http://127.0.0.1:9000/turn`. The bridge POSTs a `TurnPayload` for each turn and sends whatever the handler returns: `{"text": "..."}`, `{"replies": [...]}`, `{"attachments": [...]}` (files, see below), `{}` (nothing to say; the turn completes and 👀 clears), or `{"async": true}` (answer later through `POST /turns/{id}/reply`). If newer messages supersede the turn, the request is **aborted** and a new one arrives. `examples/http-handler.mjs` is a working stub.

### Proactive messages and escalation

Any time, in either mode, Intro can `POST /chats/{chatId}/messages` to say something unprompted, `POST /chats/{chatId}/media` to send a file, `.../album` for several pictures at once, `.../location`, `.../contacts` and `.../polls` for a pin, a card or a vote. That is how it asks Moishi to open the Grok Bot app: *"This needs the app: open Grok Bot → approve the Link spend for X."* The bridge never pretends an app-side action happened; that is Intro's rule to keep (see `docs/intro-integration.md` for all product rules Intro follows on this line).

### Chat ids

A DM's `chatId` is `<digits>@s.whatsapp.net`; a group's ends in `@g.us`; newer WhatsApp builds may hand out `<id>@lid`. Always send to the `chatId` you received. The CLI accepts a phone number and converts it (`bridge send --to "+44 7700 900123" --text hi`).

## Attachments

Images and stickers are downloaded before the turn opens, so `TurnOpened` messages carry `media.path`, a local file Intro can open directly:

```json
{ "kind": "image", "hasMedia": true, "mimetype": "image/jpeg", "text": "what is this?",
  "media": { "path": "/home/box/whatsapp-bridge/data/media/447700900123@s.whatsapp.net/3EB0C8F1.jpeg",
             "mimetype": "image/jpeg", "size": 183211, "kind": "image", "downloadedAt": "2026-09-09T10:15:31.204Z" } }
```

Anything else (video, audio, documents), or an image whose download was slower than `BRIDGE_MEDIA_WAIT`, is fetched on demand: `GET /chats/{chatId}/messages/{messageId}/media` returns the same record after downloading (once; later calls are served from disk, restarts included), and `.../media/content` streams the bytes.

| | |
|---|---|
| Where | `BRIDGE_MEDIA_DIR` (default `<data dir>/media`), one folder per chat, `<messageId>.<ext>` plus a `<messageId>.json` sidecar with the record above. Files 0600, directories 0700. |
| What is automatic | `BRIDGE_MEDIA_AUTO=images` (default): image and sticker. `all`: video, audio and documents too. `none`: only on demand. |
| How long a turn waits | `BRIDGE_MEDIA_WAIT` (8 s). Slower downloads finish in the background and are served on demand. |
| Size limit | `BRIDGE_MEDIA_MAX_BYTES` (25 MB), checked against WhatsApp's declared size before downloading: `413 MediaTooLarge`. |
| Retention | A disk budget, `BRIDGE_MEDIA_MAX_DISK` (1 GB): past it, the least recently used files go first. A use is anything the bridge sees: the download, delivery on a turn, a fetch through the media endpoints, the file appearing in a history or search result. Reading the file by path is invisible to the bridge. `BRIDGE_MEDIA_RETENTION` is an optional maximum age on top, unset by default; the sweep runs every `BRIDGE_MEDIA_SWEEP_EVERY` (1 hour). With history on, a row whose file went says so in `mediaEvictedAt`. |
| After a restart | Already-downloaded files are still served (the sidecar is the index). A message that arrived before the restart and was never downloaded cannot be fetched any more: `404 MediaUnavailable`. |
| Expired links | WhatsApp media URLs expire; the bridge asks the phone to re-upload and retries once. |

Voice notes arrive as `audio/ogg; codecs=opus`; transcription is Intro's job.

### Sending files to the user

The other direction works the same way, by path. Intro writes the file somewhere on the bridge machine and hands over its path, either as a turn reply or on its own:

```sh
# as the answer to a turn: text first, then the file
curl -X POST http://127.0.0.1:8787/turns/turn_1f3c9e2ab7d04c6e8a1b/reply -H 'content-type: application/json' \
  -d '{"text": "Here is Q3.", "attachments": [{"path": "/tmp/q3.png", "caption": "Revenue by month"}]}'

# unprompted, any time
curl -X POST http://127.0.0.1:8787/chats/447700900123@s.whatsapp.net/media -H 'content-type: application/json' \
  -d '{"path": "/tmp/q3.png", "caption": "Revenue by month"}'
```

Or from a shell: `npm run bridge -- send --to 447700900123 --file /tmp/q3.png --caption "Revenue by month"`. A consumer on another machine sends the bytes instead of a path: `{"data": "<base64>", "mimetype": "image/png"}`. Images, videos, audio (voice notes included) and any document work; WhatsApp decides the presentation from the `kind`, which the bridge infers from the file type. The full contract is [`POST /chats/{chatId}/media`](#post-chatschatidmedia) and [`OutboundMediaPayload`](#outboundmediapayload).

Several pictures at once go as one **album**, the grid WhatsApp shows for a batch of photos: `POST /chats/{chatId}/album {"items": [...]}` with two or more images or videos, or `"album": true` on a turn reply, which groups each run of adjacent image and video attachments and leaves documents and audio as separate bubbles.

## History

Off by default. When on, the bridge records every message in both directions in its SQLite file, and the agent gets memory across restarts: `GET /chats`, `GET /chats/{chatId}/messages`, full-text search with `GET /messages?q=`, and quoting or forwarding anything ever seen.

Turning it on takes two explicit steps, because it is the whole conversation of the number in plain text on disk:

```sh
npm run bridge -- history enable      # prints exactly what will be stored and asks y/N (--yes for scripts)
# then set BRIDGE_HISTORY=true and restart the daemon
```

With `BRIDGE_HISTORY=true` and no acceptance on this machine, the daemon refuses to start and says so. What is stored: text and captions, edits (the latest body alongside the original), deletions as tombstones (the text goes, the id and times stay), reactions, delivery state of the bridge's own messages, the path of downloaded attachments, the turn each message belonged to, and the raw message bytes so replies and forwards keep working after a restart. Nothing expires; `npm run bridge -- history purge --before 2026-01-01T00:00:00Z --yes` deletes.

The file is `data/bridge.db` (`GET /status` reports the path), created 0600. It exists with history off too: the bridge keeps its own state there (what it sent, scheduled sends, turn outcomes, the media cache index); the message tables only fill when history is on. Two views are stable for anyone opening it read-only with `sqlite3`: `messages_v` (one row per message, latest text first) and `chats_v`.

## Reference for the agent

Everything Intro can call, with exact shapes. The daemon also serves this as OpenAPI at `GET /openapi.json` and as interactive docs at `GET /docs`; the schemas in `src/domain/*.ts`, `src/http/Api.ts` and `src/engine/*.ts` are what both are generated from, so the code wins if this page and the code ever disagree.

### Conventions

| | |
|---|---|
| Base URL | `http://127.0.0.1:8787` (`BRIDGE_HOST`, `BRIDGE_PORT`). Loopback only unless you change the host. |
| Authentication | Only when `BRIDGE_API_TOKEN` is set: send `Authorization: Bearer <token>` on **every** request except `GET /health`, `GET /docs` and `GET /openapi.json`. `GET /status` does need it. Missing or wrong token: `401 {"_tag":"Unauthorized","message":"missing or wrong bearer token"}`. |
| Request bodies | JSON with `Content-Type: application/json`. Any other content type is refused with `415` and a plain-text body. A body that is not valid JSON or fails the schema: `400 InvalidRequest` (see [Errors](#errors)). |
| Responses | JSON, except `GET /events` (`text/event-stream`) and `GET .../media/content` (the file's bytes). Endpoints with nothing to return answer `204` with an empty body. |
| Errors | Always a JSON object with a `_tag` and the fields listed in the [error catalogue](#errors). Dispatch on `_tag`, not on the status code alone. The human-readable `message` is **not** on the wire (except for `Unauthorized` and `InvalidRequest`); build one from the fields. |
| Ids | `chatId`: a WhatsApp jid (`447700900123@s.whatsapp.net`, `<id>@g.us`, `<id>@lid`). `messageId`: WhatsApp's id, stable across devices. `turnId`: `turn_` plus 20 hex characters, assigned by the bridge, unique per daemon run. Use them verbatim in paths (`@` is fine in a path segment). |
| Time | Every timestamp is an ISO-8601 UTC string (`2026-09-09T10:15:30.000Z`). |
| Ordering | `messages` arrays are oldest first. `GET /turns` is oldest first. Events are in `seq` order. |
| Concurrency | One worker per chat. Turns from different chats are independent and may be answered concurrently; within a chat there is at most one open turn. |
| Text limits | A turn reply longer than 4000 characters is split at paragraph, line, sentence or space boundaries into several messages; whitespace-only replies are dropped. `POST /chats/{chatId}/messages` sends its text as **one** message and does not split, so keep it short. |
| Files | Outgoing files are described by an [`OutboundMediaPayload`](#outboundmediapayload): a local `path` (preferred, the bridge reads it) or base64 `data` with a `mimetype`. `BRIDGE_MEDIA_MAX_BYTES` (25 MB) applies in both directions. Files are read and checked before anything is sent, so a bad path is a 4xx and nothing goes out. |
| Idempotency | Sending a message twice sends it twice. A turn accepts exactly one reply; the second gets `409 TurnNotAwaitingReply` with `state: "committed"`. Inbound messages are deduplicated by id inside the bridge. |
| Quoting | Any send can quote a message with `quoteMessageId`: a message the bridge saw or sent this run. On a proactive send an unknown id is `404 MessageUnknown`; on a turn reply the bubble goes out unquoted and a warning is logged, so a stale id never sinks the answer. |
| Own messages | The bridge can edit (`PATCH`) or delete (`DELETE`) a message it sent, within WhatsApp's windows: 15 minutes to edit, about 2 days to delete. Past that: `409 MessageTooOld`. Sent messages are remembered in the database, so this survives a restart; a message the bridge never sent is `404 MessageUnknown`. |
| Scheduling | Any text or file send takes `sendAt` (ISO-8601) and answers `202 {scheduledId, sendAt, deliverWhenOnline}` instead of sending. `deliverWhenOnline: true` also waits for the recipient to be online, at most 24 hours. Scheduled sends survive restarts; one missed by more than an hour of downtime is dropped with a `ScheduledSkipped` event. |
| History | Endpoints under [History](#history) answer `409 HistoryDisabled` until history is enabled. |
| Groups | Off unless `BRIDGE_GROUPS=true`. A turn opens for a group message only when it mentions the bridge or quotes one of its messages; every group turn carries `context`, the last 10 messages seen in that group. Answer `{}` when the mention needs nothing. `mentions` on a text send makes an @-mention real (WhatsApp only links a mention when the jid is listed). |
| Albums | `POST /chats/{chatId}/album` and `album: true` on a reply send adjacent images and videos as one album. Documents and audio never group. |
| Presence | The bridge subscribes to a chat's presence the first time it messages the line, to hold the batch window while they type. `PresenceChanged` events flow only for chats asked about through `GET /chats/{chatId}/presence`. |
| Schema version | Every event carries `v: 1`. The version only changes on an incompatible change; new optional fields do not bump it. |

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | [`/health`](#get-health) | Liveness: 200 only when the line is `ok` |
| GET | [`/status`](#get-status) | The same body, always 200 |
| GET | [`/events`](#get-events) | Server-sent events: turns, completions, connection changes |
| POST | [`/chats/{chatId}/messages`](#post-chatschatidmessages) | Send a text |
| PATCH | [`/chats/{chatId}/messages/{messageId}`](#patch-chatschatidmessagesmessageid) | Edit a message the bridge sent |
| DELETE | [`/chats/{chatId}/messages/{messageId}`](#delete-chatschatidmessagesmessageid) | Delete for everyone a message the bridge sent |
| POST | [`/chats/{chatId}/media`](#post-chatschatidmedia) | Send an image, video, audio or document |
| POST | [`/chats/{chatId}/forward`](#post-chatschatidforward) | Forward a message the bridge saw or sent |
| POST | [`/chats/{chatId}/album`](#post-chatschatidalbum) | Send several images or videos as one album |
| POST | [`/chats/{chatId}/location`](#post-chatschatidlocation) | Send a location pin |
| POST | [`/chats/{chatId}/contacts`](#post-chatschatidcontacts) | Send one or more contact cards |
| POST | [`/chats/{chatId}/polls`](#post-chatschatidpolls) | Send a poll; votes arrive as `PollVote` events |
| GET | [`/chats/{chatId}/presence`](#get-chatschatidpresence) | Last known presence; subscribes the chat to `PresenceChanged` events |
| GET | [`/chats`](#get-chats) | Chats in the history |
| GET | [`/chats/{chatId}`](#get-chatschatid) | One chat: history summary, group subject and members |
| GET | [`/chats/{chatId}/messages`](#get-chatschatidmessages) | A chat's history, or a search within it |
| GET | [`/messages`](#get-messages) | Full-text search across every chat |
| GET | [`/contacts/{jid}`](#get-contactsjid) | Push name, phone, picture, on WhatsApp or not |
| GET | [`/scheduled`](#get-scheduled) | Pending scheduled sends and those closed in the last day |
| DELETE | [`/scheduled/{scheduledId}`](#delete-scheduledscheduledid) | Cancel a pending scheduled send |
| POST | [`/chats/{chatId}/messages/{messageId}/reactions`](#post-chatschatidmessagesmessageidreactions) | React to a message |
| DELETE | [`/chats/{chatId}/messages/{messageId}/reactions`](#delete-chatschatidmessagesmessageidreactions) | Remove our reaction |
| POST | [`/chats/{chatId}/typing`](#post-chatschatidtyping) | Typing indicator on/off |
| GET | [`/chats/{chatId}/messages/{messageId}/media`](#get-chatschatidmessagesmessageidmedia) | Download an attachment to disk, get its path |
| GET | [`/chats/{chatId}/messages/{messageId}/media/content`](#get-chatschatidmessagesmessageidmediacontent) | The attachment's bytes |
| GET | [`/turns`](#get-turns) | Turns still waiting for a reply |
| GET | [`/turns/{turnId}`](#get-turnsturnid) | One turn, open or finished |
| POST | [`/turns/{turnId}/reply`](#post-turnsturnidreply) | Answer a turn |
| POST | [`/turns/{turnId}/progress`](#post-turnsturnidprogress) | "Still working": extend the deadline, keep typing, send an interim bubble |
| POST | [`/turns/{turnId}/abandon`](#post-turnsturnidabandon) | Give up on a turn with a reason |
| GET | `/openapi.json`, `/docs` | The OpenAPI document and a browser UI for it (no token needed) |

#### `GET /health`

No token needed. `200` with a [`BridgeStatus`](#bridgestatus) when `state` is `ok`; otherwise `503` with the same status wrapped in an `Unhealthy` error, so a probe can use the status code and a reader can still see why.

```http
GET /health
```

```json
200 {"state":"ok","linkedAs":{"jid":"447700900999@s.whatsapp.net","name":"Intro"},"responder":"manual","version":"0.2.0",
     "startedAt":"2026-09-09T09:00:00.000Z","lastEventAt":"2026-09-09T10:15:31.300Z","lastInboundAt":"2026-09-09T10:15:30.900Z",
     "lastOutboundAt":"2026-09-09T10:15:35.100Z","reconnects":0,"openTurns":1}

503 {"_tag":"Unhealthy","status":{"state":"needs_relink","detail":"no linked session on disk: run `bridge link`","responder":"manual",
     "version":"0.2.0","startedAt":"2026-09-09T09:00:00.000Z","lastEventAt":"2026-09-09T09:00:00.070Z","reconnects":0,"openTurns":0}}
```

#### `GET /status`

Token required when one is set. Always `200` with the [`BridgeStatus`](#bridgestatus), whatever the state. Use it when you want the details without treating a non-`ok` state as a transport failure.

#### `GET /events`

Token required when one is set. A long-lived `text/event-stream` response. Each frame is one line `data: <JSON BridgeEvent>` followed by a blank line; there are no `event:` or `id:` lines, and no comments.

```http
GET /events?since=41
Accept: text/event-stream
```

```
data: {"_tag":"Ready","v":1,"seq":43,"at":"2026-09-09T10:16:00.000Z"}

data: {"_tag":"TurnOpened","v":1,"seq":42,"at":"2026-09-09T10:15:32.000Z","turnId":"turn_1f3c9e2ab7d04c6e8a1b","chatId":"447700900123@s.whatsapp.net","isGroup":false,"messages":[...]}

data: {"_tag":"TurnCompleted","v":1,"seq":43,"at":"2026-09-09T10:15:35.100Z","turnId":"turn_1f3c9e2ab7d04c6e8a1b","chatId":"447700900123@s.whatsapp.net","sentMessageIds":["3EB0A1B2C3D4E5F60718"]}

data: {"_tag":"Heartbeat","v":1,"seq":43,"at":"2026-09-09T10:16:25.000Z"}
```

| Query | Type | Meaning |
|---|---|---|
| `since` | non-negative integer, optional | Replay buffered events with `seq > since` before going live. Omit it for live events only. Anything else (`abc`, `-1`, `1.5`) is `400 InvalidRequest` with `kind: "Query"`. |

Rules of the stream:

- **`Ready` comes first, immediately**, on every connection. Its `seq` is the number of the last real event the daemon published (`0` before the first). It is your cursor if you had none.
- **`seq` is monotonic per daemon run** and identifies an event. Store the `seq` of every event you process, including `Ready` and `Heartbeat` (theirs is the latest real seq at that moment, so storing it is safe).
- **Replay covers the last 200 events.** If `since` is older than that, replay starts at the oldest buffered event; you detect the gap because the first replayed event's `seq` is more than `since + 1`. Recover by calling `GET /turns` for anything still open.
- **A `seq` smaller than the one you last saw means the daemon restarted.** Its turns are gone (`GET /turns` is empty), its `startedAt` in `/status` is new. Messages that arrived while it was down are not replayed unless `BRIDGE_OFFLINE_WINDOW` is set.
- **`Heartbeat` every 25 seconds** while nothing else happens. No frame for about 60 seconds means the connection is dead: close it and reconnect with `?since=<last seq>`.
- **Every subscriber sees every event.** Two consumers get the same stream; the bridge does not track who consumed what.
- **Slow consumers lose old events, not new ones.** If your client stops reading, its server-side buffer slides (oldest dropped); reconnect with `?since=` to catch up.

The events themselves are catalogued under [Events](#events).

#### `POST /chats/{chatId}/messages`

Send one text message to a chat, unprompted. Works in any responder mode and at any time; use it for proactive notes and for "this needs the app" escalations. The text goes out as **one** WhatsApp message (no splitting), so keep it under 4000 characters.

```http
POST /chats/447700900123@s.whatsapp.net/messages
Content-Type: application/json

{"text": "This needs the app: open Grok Bot → approve the Link spend for Acme."}
```

```json
200 {"chatId":"447700900123@s.whatsapp.net","messageId":"3EB0A1B2C3D4E5F60718"}
```

| Field | Type | Rule |
|---|---|---|
| `text` | string | non-empty, else `400 InvalidRequest` |
| `quoteMessageId` | string, optional | Send as a reply to that message. `404 MessageUnknown` if the bridge never saw or sent it. |
| `mentions` | string[], optional | Jids to @-mention. Write `@<digits>` in the text and list `<digits>@s.whatsapp.net` here; WhatsApp turns it into a tappable mention that notifies the person, which matters in groups. |
| `sendAt` | ISO-8601, optional | Hold the send until then. The answer is `202 {scheduledId, sendAt, deliverWhenOnline}` and nothing is sent now. |
| `deliverWhenOnline` | boolean, optional | With `sendAt`: also wait until the recipient is online, up to 24 hours after `sendAt`, then send anyway. |

```json
202 {"scheduledId":"sched_c4b3d5de2f514669","sendAt":"2026-09-11T08:00:00.000Z","deliverWhenOnline":false}
```

Errors: `503 NotConnected`, `502 SendFailed`, `404 MessageUnknown`, `500 StoreError` (scheduling could not be written). Direct sends are **not** retried by the bridge (turn replies are); a single attempt is capped at 30 seconds. On `NotConnected` with `state: "connecting"` wait and retry; on `SendFailed` retry once after a pause, then give up and tell the user another way.

#### `PATCH /chats/{chatId}/messages/{messageId}`

Replace the text of a message the bridge sent this run. WhatsApp shows it as edited.

```http
PATCH /chats/447700900123@s.whatsapp.net/messages/3EB0A1B2C3D4E5F60718
Content-Type: application/json

{"text": "Correction: the dentist is at 11, not 10."}
```

`204` with an empty body. Errors: `404 MessageUnknown` (not sent by the bridge this run), `409 MessageTooOld` (WhatsApp allows edits for 15 minutes; `ageMs` and `limitMs` say by how much), `503 NotConnected`, `502 SendFailed`. Captions of files can be edited the same way.

#### `DELETE /chats/{chatId}/messages/{messageId}`

Delete for everyone a message the bridge sent this run. `204` with an empty body. Errors: `404 MessageUnknown`, `409 MessageTooOld` (about 2 days), `503 NotConnected`, `502 SendFailed`.

#### `POST /chats/{chatId}/media`

Send one file to a chat, unprompted, in any responder mode. The body is an [`OutboundMediaPayload`](#outboundmediapayload). The bridge reads the file (or decodes the bytes), checks the size, uploads it to WhatsApp and returns the sent message id. Sending blocks until the upload is done, up to 3 minutes for a large document.

```http
POST /chats/447700900123@s.whatsapp.net/media
Content-Type: application/json

{"path": "/tmp/q3.png", "caption": "Revenue by month"}
```

```json
200 {"chatId":"447700900123@s.whatsapp.net","messageId":"3EB0A1B2C3D4E5F60719"}
```

The same with bytes instead of a path (for a consumer that cannot see the bridge's disk):

```json
{"data": "iVBORw0KGgoAAAANSUhEUgAA...", "mimetype": "image/png", "caption": "Revenue by month"}
```

`sendAt` and `deliverWhenOnline` work here as on text sends: the file is read and checked now, held, and sent later; the answer is `202 {scheduledId, sendAt, deliverWhenOnline}`.

#### `POST /chats/{chatId}/forward`

Forward a message to this chat: one the bridge received or sent, this run or, with history on, ever. WhatsApp shows it as forwarded.

```http
POST /chats/447700900456@s.whatsapp.net/forward
Content-Type: application/json

{"fromChatId": "447700900123@s.whatsapp.net", "messageId": "3EB0C8F1A2B3C4D5E6F7"}
```

`200 {"chatId","messageId"}`. Errors: `404 MessageUnknown`, `503 NotConnected`, `502 SendFailed`.

| Status | `_tag` | When |
|---|---|---|
| 400 | `InvalidRequest` | Neither or both of `path` and `data`, `data` not valid base64, an unknown `kind`. |
| 400 | `OutboundInvalid` | `data` without a `mimetype`, or an empty file. `detail` says which. |
| 404 | `OutboundFileUnreadable` | `path` does not exist, is a directory, or the daemon's user cannot read it. |
| 413 | `OutboundTooLarge` | Over `BRIDGE_MEDIA_MAX_BYTES`; `size` and `maxBytes` in bytes. |
| 503 | `NotConnected` | No WhatsApp socket; nothing was read. |
| 502 | `SendFailed` | The upload or the send was rejected or took too long. Not retried; retry yourself. |

What WhatsApp shows depends on `kind`: `image` and `video` get an inline preview with the caption under it (WhatsApp recompresses them); `audio` is a playable clip, or a voice-note bubble with `voiceNote: true` (which needs `audio/ogg; codecs=opus`); `document` is a file card with `fileName`, full quality, for anything else or for an image you want delivered untouched (`"kind": "document"`).

#### `POST /chats/{chatId}/album`

Send two or more images and videos as one album (WhatsApp shows a grid with the first caption under it).

```http
POST /chats/447700900123@s.whatsapp.net/album
Content-Type: application/json

{"items": [{"path": "/tmp/front.jpg", "caption": "The house"}, {"path": "/tmp/back.jpg"}, {"path": "/tmp/tour.mp4"}]}
```

`200 {"chatId", "messageIds": []}`, one id per item, in order. `items` are [`OutboundMediaPayload`](#outboundmediapayload)s with the same file checks as `POST .../media`; at least two, else `400 InvalidRequest`. An item that is not an image or video is `400 OutboundInvalid` and nothing goes out. `quoteMessageId` quotes a message with the album. Not schedulable. Errors: `400 InvalidRequest`, `400 OutboundInvalid`, `404 OutboundFileUnreadable`, `413 OutboundTooLarge`, `404 MessageUnknown`, `503 NotConnected`, `502 SendFailed`.

#### `POST /chats/{chatId}/location`

Send a location pin.

```http
POST /chats/447700900123@s.whatsapp.net/location
Content-Type: application/json

{"latitude": 51.5007, "longitude": -0.1246, "name": "Big Ben", "address": "London SW1A 0AA"}
```

`200 {"chatId", "messageId"}`. `name` and `address` are optional labels on the pin; `quoteMessageId` works as everywhere. Errors: `404 MessageUnknown`, `503 NotConnected`, `502 SendFailed`.

#### `POST /chats/{chatId}/contacts`

Send one or more contact cards. The bridge builds the vCard.

```http
POST /chats/447700900123@s.whatsapp.net/contacts
Content-Type: application/json

{"contacts": [{"name": "Dana Levi", "phone": "447700900555"}]}
```

`200 {"chatId", "messageId"}` (several cards are one message). `phone` is digits with the country code; WhatsApp links the card to that account when it exists. At least one contact, non-empty `name` and `phone`, else `400 InvalidRequest`. Errors: `404 MessageUnknown`, `503 NotConnected`, `502 SendFailed`.

#### `POST /chats/{chatId}/polls`

Send a poll.

```http
POST /chats/447700900123@s.whatsapp.net/polls
Content-Type: application/json

{"question": "Lunch?", "options": ["Pizza", "Sushi", "Salad"], "multiple": false}
```

`200 {"chatId", "messageId"}`. Two to twelve non-empty `options`; `multiple: true` lets a voter pick several. Every vote (and every change of vote) arrives as a [`PollVote`](#events) event carrying the poll's `messageId` and the voter's current selection. Votes on a poll sent before a daemon restart can only be read with history on; without it the vote is logged and dropped. Errors: `404 MessageUnknown`, `503 NotConnected`, `502 SendFailed`.

#### `GET /chats/{chatId}/presence`

The last known presence of a chat, and from now on `PresenceChanged` events for it.

```json
200 {"chatId":"447700900123@s.whatsapp.net","state":"composing","at":"2026-09-10T10:15:30.000Z"}
```

`state` is `available`, `unavailable`, `composing`, `recording`, `paused` or `unknown` (nothing heard yet: the subscription was just made, or the person hides presence). `at` is when the bridge heard it; `lastSeenAt` appears when WhatsApp shares it, which most privacy settings prevent. Calling it again is harmless. Errors: `503 NotConnected`.

#### `POST /chats/{chatId}/messages/{messageId}/reactions`

Put a reaction on a message. WhatsApp keeps one reaction per sender per message, so this **replaces** whatever reaction the bridge already has there, including the 👀 seen marker. Prefer leaving the 👀 lifecycle to the bridge and reacting only on messages that are not part of an open turn.

```http
POST /chats/447700900123@s.whatsapp.net/messages/3EB0C8F1A2B3C4D5E6F7/reactions
Content-Type: application/json

{"emoji": "✅"}
```

`204` with an empty body. `emoji` must be non-empty. Errors: `503 NotConnected`, `502 SendFailed`.

#### `DELETE /chats/{chatId}/messages/{messageId}/reactions`

Remove the bridge's reaction from a message (any emoji, the 👀 included). `204` with an empty body. Errors: `503 NotConnected`, `502 SendFailed`.

#### `POST /chats/{chatId}/typing`

Turn the "typing…" indicator on or off in a chat. The bridge already shows typing while a turn is being answered and clears it when the turn ends; call this only for long proactive work outside a turn, and remember to turn it off.

```http
POST /chats/447700900123@s.whatsapp.net/typing
Content-Type: application/json

{"composing": true}
```

`204` with an empty body. `composing` is a boolean. Errors: `503 NotConnected`, `502 SendFailed`.

#### `GET /chats/{chatId}/messages/{messageId}/media`

The attachment of a message as a local file. Downloads it if it is not on disk yet (any kind: image, sticker, video, audio, document), then answers with the [`MediaFile`](#mediafile) record. Later calls, restarts included, are served from disk. Concurrent calls for the same message share one download. A failed download is not remembered, so calling again retries.

```http
GET /chats/447700900123@s.whatsapp.net/messages/3EB0C8F1A2B3C4D5E6F7/media
```

```json
200 {"path":"/home/box/whatsapp-bridge/data/media/447700900123@s.whatsapp.net/3EB0C8F1A2B3C4D5E6F7.ogg",
     "mimetype":"audio/ogg; codecs=opus","size":48213,"kind":"audio","downloadedAt":"2026-09-09T10:15:31.204Z"}
```

| Status | `_tag` | When |
|---|---|---|
| 404 | `MediaUnavailable` | The message carried no media, or the bridge has not seen it this run (it restarted) and has no file on disk for it. `reason` says which. |
| 413 | `MediaTooLarge` | WhatsApp's declared size is over `BRIDGE_MEDIA_MAX_BYTES`. `size` and `maxBytes` are in bytes. |
| 502 | `MediaDownloadFailed` | WhatsApp did not hand over the bytes (also after a re-upload request). Retry later. |
| 500 | `MediaStoreError` | The disk under the media directory misbehaved. `operation` and `detail` explain. |

The download can take a while for large files; the call blocks until it is done.

#### `GET /chats/{chatId}/messages/{messageId}/media/content`

The same as above, but the response body is the file itself and `Content-Type` is the file's mimetype. Use it when Intro runs on another machine than the bridge and cannot open `path`. Same errors, as JSON.

#### `GET /chats`

The chats in the history, most recent first. `409 HistoryDisabled` when history is off.

```json
200 [{"chatId":"447700900123@s.whatsapp.net","name":"Moishi","isGroup":false,"lastMessageAt":"2026-09-10T10:15:35.100Z","messageCount":412}]
```

`name` is the push name of the last person who wrote in the chat.

#### `GET /chats/{chatId}`

What the bridge knows about one chat. Always answers, history or not. For a group it fetches the subject, description, owner and members (cached for 5 minutes).

```json
200 {"chatId":"123-456@g.us","isGroup":true,"name":"Family","lastMessageAt":"...","messageCount":88,
     "group":{"chatId":"123-456@g.us","subject":"Family","participants":[{"jid":"447700900123@s.whatsapp.net","phone":"447700900123","admin":true}]}}
```

Errors: `503 NotConnected` and `502 LookupFailed` (groups only), `500 StoreError`.

#### `GET /chats/{chatId}/messages`

A chat's history, newest first, both directions. `409 HistoryDisabled` when history is off.

| Query | Meaning |
|---|---|
| `limit` | Page size, 50 by default, 500 at most. |
| `before` | Only messages older than this message id: the cursor for paging back. |
| `since` | Only messages newer than this instant (ISO-8601): "what happened since I last looked". |
| `q` | Full-text search within the chat instead of a page; each hit carries a `snippet`. |

Each row is a [`HistoryRow`](#historyrow). A row with a `mediaPath` counts as a use of that file for the cache.

#### `GET /messages`

Full-text search across every chat: `?q=dentist&limit=20&before=<messageId>`. Words become prefix terms joined with AND, so `dent tomo` finds "dentist tomorrow"; a quoted query is one phrase. Text, captions and edited text are indexed; tombstoned messages are not. Newest first, each hit a [`HistoryRow`](#historyrow) with a `snippet` where the match is marked `[like this]`. `409 HistoryDisabled` when history is off.

#### `GET /contacts/{jid}`

```json
200 {"jid":"447700900123@s.whatsapp.net","phone":"447700900123","name":"Moishi","pictureUrl":"https://pps.whatsapp.net/...","onWhatsApp":true}
```

`name` is the push name (what the person set on WhatsApp), known once they have written to the line. `pictureUrl` expires; fetch again when needed. A phone number (digits, or a jid) is checked with WhatsApp: `onWhatsApp: false` means nothing can be sent there. Errors: `503 NotConnected`, `502 LookupFailed`.

#### `GET /scheduled`

Pending scheduled sends, then the ones closed in the last day.

```json
200 [{"scheduledId":"sched_c4b3d5de2f514669","chatId":"447700900123@s.whatsapp.net","kind":"text","sendAt":"2026-09-11T08:00:00.000Z",
      "deliverWhenOnline":false,"status":"pending","createdAt":"2026-09-10T12:53:18.114Z"}]
```

`status` is `pending`, `sent` (with `messageId`), `skipped` or `failed` (with `detail`), or `cancelled`.

#### `DELETE /scheduled/{scheduledId}`

Cancel a pending scheduled send. `204`, or `404 ScheduledNotFound` when it is not pending.

#### `GET /turns`

The turns that still want a reply, oldest first: state `batching`, `responding` or `awaiting`. Finished turns are not listed. Empty array when nothing is open.

```json
200 [{"turnId":"turn_1f3c9e2ab7d04c6e8a1b","chatId":"447700900123@s.whatsapp.net","isGroup":false,"state":"awaiting",
      "messages":[...],"openedAt":"2026-09-09T10:15:32.000Z"}]
```

Each entry is a [`TurnSummary`](#turnsummary). This is the catch-up call after a reconnect or after your own restart.

#### `GET /turns/{turnId}`

One turn, in any state. The bridge keeps up to 500 turns in memory (finished ones are pruned oldest first past that), so a turn you answered a moment ago is still here with its `sentMessageIds`, and a failed one with its `failure`. `404 TurnNotFound` when it is unknown or pruned, and always after a daemon restart.

#### `POST /turns/{turnId}/reply`

Answer a turn. The call **blocks until the messages are on the wire and the 👀 reactions are cleared**, then returns the sent message ids. Expect it to take a second or two; it gives up waiting after 90 seconds with `502 TurnFailedError` (the send may still be in progress; check `GET /turns/{turnId}`).

```http
POST /turns/turn_1f3c9e2ab7d04c6e8a1b/reply
Content-Type: application/json

{"text": "Tomorrow you have the dentist at 10 and lunch with Sam at 1."}
```

```json
200 {"turnId":"turn_1f3c9e2ab7d04c6e8a1b","sentMessageIds":["3EB0A1B2C3D4E5F60718"]}
```

| Field | Type | Rule |
|---|---|---|
| `text` | string, optional | One reply bubble. |
| `replies` | (string or `{text, quoteMessageId?}`)[], optional | Several bubbles, sent in order. When both are given, `replies` go first, then `text`. An object form quotes a message with that bubble. |
| `attachments` | [`OutboundMediaPayload`](#outboundmediapayload)[], optional | Files, sent after the text bubbles, in order. Each may carry its own `caption` and `quoteMessageId`. |
| `quoteMessageId` | string, optional | Quote this message with the first bubble, unless that bubble carries its own. Unknown ids are dropped with a warning, never a failure. |
| `replaceProgress` | boolean, optional | Edit the last interim bubble sent through `progress` into the first text bubble instead of sending a new message. Ignored when there is no interim bubble. |
| `album` | boolean, optional | Send each run of adjacent image and video attachments as one album. Documents and audio still go one by one. `sentMessageIds` keeps one id per item. |

Everything may be omitted: `{}` completes the turn **without sending anything** (the 👀 still clear, `sentMessageIds` is `[]`). Use that when the messages need no answer. Whitespace-only strings are dropped; strings over 4000 characters are split. `sentMessageIds` has one id per bubble and per file, in the order they went out.

```json
{"text": "Here is Q3.", "attachments": [{"path": "/tmp/q3.png", "caption": "Revenue by month"}]}
```

| Status | `_tag` | Meaning | What to do |
|---|---|---|---|
| 404 | `TurnNotFound` | Unknown turn id (or the daemon restarted). | Drop the answer. |
| 409 | `TurnSuperseded` | Newer messages replaced this turn. `currentTurnId` names the replacement. | Drop the answer; answer `currentTurnId` instead (its `TurnOpened` carries every unanswered message). |
| 409 | `TurnNotAwaitingReply` | The turn is not waiting for an external reply; `state` says what it is doing. `responding`: the `http` handler call is still running. `committed`: it was already answered. `failed`: it failed before your reply arrived. `cancelled`: the user deleted every message of it. `batching`: it has not opened yet. | Nothing to send. |
| 502 | `TurnFailedError` | The reply was accepted but sending failed after retries, or took over 90 seconds. `reason` explains. | The 👀 stays on. Tell the user through a fresh `POST /chats/{chatId}/messages` if the line is back, or wait for their next message. |
| 400 / 404 / 413 | `OutboundInvalid` / `OutboundFileUnreadable` / `OutboundTooLarge` | An attachment could not be read or is too big. Checked **before** the turn is touched: it stays `awaiting`. | Fix the file and post the reply again. |

In manual mode a turn waits `BRIDGE_TURN_TIMEOUT` (10 minutes) for this call, counted from when it opened; `progress` pushes that out. After the deadline it fails with `TurnFailed`, the eye comes off, the failure text goes out, and a late reply gets `409 TurnNotAwaitingReply{state:"failed"}`.

#### `POST /turns/{turnId}/progress`

"Still working." Re-arms the typing indicator for another `BRIDGE_TYPING_MAX`, pushes the reply deadline out, and optionally sends an interim bubble. Call it as often as you like; each call resets typing and can only ever move the deadline later, never past one hour after the turn opened.

```http
POST /turns/turn_1f3c9e2ab7d04c6e8a1b/progress
Content-Type: application/json

{"text": "Checking both calendars…", "extendBy": "15 minutes"}
```

```json
200 {"turnId":"turn_1f3c9e2ab7d04c6e8a1b","deadline":"2026-09-10T10:31:00.000Z","typingUntil":"2026-09-10T10:19:00.000Z","progressMessageId":"3EB0A1B2C3D4E5F60720"}
```

| Field | Type | Meaning |
|---|---|---|
| `text` | string, optional | Sent as its own bubble now. The reply can edit it into the answer with `replaceProgress: true`; otherwise it stays. |
| `extendBy` | duration string, optional | How far from now the deadline moves (`"15 minutes"`, `"2 hours"` is capped). Default `BRIDGE_TURN_TIMEOUT`. |

Errors: `404 TurnNotFound`, `409 TurnNotOpen` (`state` is not `responding` or `awaiting`), and for the interim bubble `503 NotConnected`, `502 SendFailed`. Works in both responder modes; in `http` mode only typing and the deadline of a later `{"async": true}` wait are affected.

#### `POST /turns/{turnId}/abandon`

Give up on an open turn. It fails at once with `reason` (visible as `abandoned: <reason>` in `TurnFailed` and `failure`), the eye comes off, and the user gets a quoted reply with `text`, or `BRIDGE_FAILURE_TEXT` when `text` is absent. An empty `text` sends nothing.

```http
POST /turns/turn_1f3c9e2ab7d04c6e8a1b/abandon
Content-Type: application/json

{"reason": "needs the app", "text": "This needs the app: open Grok Bot → approve the Link spend."}
```

`204` with an empty body. Errors: `404 TurnNotFound`, `409 TurnNotOpen`. In `http` mode the running handler request is aborted.

### Events

Every event is a JSON object with the envelope `{"_tag", "v": 1, "seq", "at"}` plus the fields below. `seq` is an integer, monotonic per daemon run; `at` is an ISO-8601 UTC timestamp of when the bridge published it.

| `_tag` | Extra fields | When |
|---|---|---|
| `Ready` | | First frame on every `GET /events` connection; `seq` is the latest real seq. |
| `Heartbeat` | | Every 25 seconds of silence; `seq` is the latest real seq. |
| `MessageReceived` | `message` | A message passed the filters and was accepted; 👀 is being applied. Informational: do not answer it, wait for the turn. |
| `TurnOpened` | `turnId`, `chatId`, `isGroup`, `messages[]`, `supersedes?`, `context?` | The batch window closed; these messages want one reply. In groups, `context` is the last 10 messages seen in the chat before these, oldest first, handled or not. |
| `TurnSuperseded` | `turnId`, `chatId`, `by` | Newer messages arrived mid-turn; stop working on `turnId`. The `TurnOpened` for `by` follows and carries everything. |
| `TurnCompleted` | `turnId`, `chatId`, `sentMessageIds[]` | The reply went out and the 👀 were cleared. Empty `sentMessageIds` means the turn was completed with nothing to send. |
| `TurnFailed` | `turnId`, `chatId`, `reason` | No reply could be sent, or the agent abandoned the turn; the eye came off and the failure text went out. |
| `TurnCancelled` | `turnId`, `chatId`, `reason` | The user deleted every message of the turn before it was answered. Nothing was sent. |
| `ReactionReceived` | `chatId`, `messageId`, `senderId`, `emoji`, `onOurMessage` | Someone reacted in a handled chat. Empty `emoji` means removed. `onOurMessage` is true when the target is a message the bridge sent. Informational; never opens a turn. |
| `MessageEdited` | `chatId`, `messageId`, `text` | The sender edited an accepted message; `text` is the new body. A pending message is corrected in place (batching) or its turn is superseded (responding, awaiting). |
| `MessageDeleted` | `chatId`, `messageId` | The sender deleted an accepted message for everyone. A pending message is dropped; a turn left empty is cancelled. |
| `MessageDelivered` | `chatId`, `messageId`, `participantId?` | A message the bridge sent reached a device. In groups one per participant. |
| `MessageRead` | `chatId`, `messageId`, `participantId?` | A message the bridge sent was read (or a voice note played). In groups one per participant. Needs the recipient's read receipts on. |
| `ScheduledSent` | `scheduledId`, `chatId`, `messageId` | A scheduled send went out. |
| `ScheduledSkipped` | `scheduledId`, `chatId`, `reason` | A scheduled send was dropped: over an hour late after downtime, or the recipient never came online within 24 hours. |
| `ScheduledFailed` | `scheduledId`, `chatId`, `reason` | A scheduled send failed at the transport. |
| `PollVote` | `chatId`, `pollMessageId`, `voterId`, `selected[]` | Someone voted on a poll the bridge sent, or changed their vote. `selected` is the voter's whole current choice (option texts), empty when they retracted. |
| `PresenceChanged` | `chatId`, `state`, `lastSeenAt?` | Presence changed in a chat asked about through `GET /chats/{chatId}/presence`. `state` as on that endpoint. |
| `ConnectionChanged` | `state`, `detail?` | The WhatsApp connection changed state; same values as `/health`. |

```json
{"_tag":"MessageReceived","v":1,"seq":41,"at":"2026-09-09T10:15:30.900Z",
 "message":{"chatId":"447700900123@s.whatsapp.net","messageId":"3EB0C8F1A2B3C4D5E6F7","senderId":"447700900123@s.whatsapp.net",
            "senderNumber":"447700900123","senderName":"Moishi","isGroup":false,"timestamp":"2026-09-09T10:15:30.000Z",
            "kind":"text","text":"can you check the calendar for tomorrow?","hasMedia":false}}

{"_tag":"TurnOpened","v":1,"seq":42,"at":"2026-09-09T10:15:32.400Z","turnId":"turn_1f3c9e2ab7d04c6e8a1b",
 "chatId":"447700900123@s.whatsapp.net","isGroup":false,
 "messages":[{"chatId":"447700900123@s.whatsapp.net","messageId":"3EB0C8F1A2B3C4D5E6F7","senderId":"447700900123@s.whatsapp.net",
              "senderNumber":"447700900123","senderName":"Moishi","isGroup":false,"timestamp":"2026-09-09T10:15:30.000Z",
              "kind":"text","text":"can you check the calendar for tomorrow?","hasMedia":false}]}

{"_tag":"TurnSuperseded","v":1,"seq":43,"at":"2026-09-09T10:15:40.000Z","turnId":"turn_1f3c9e2ab7d04c6e8a1b",
 "chatId":"447700900123@s.whatsapp.net","by":"turn_9b8d7c6e5f4a3b2c1d0e"}

{"_tag":"TurnOpened","v":1,"seq":44,"at":"2026-09-09T10:15:40.010Z","turnId":"turn_9b8d7c6e5f4a3b2c1d0e",
 "chatId":"447700900123@s.whatsapp.net","isGroup":false,"supersedes":"turn_1f3c9e2ab7d04c6e8a1b",
 "messages":[{"...":"the original message"},{"...":"the newer one"}]}

{"_tag":"TurnCompleted","v":1,"seq":45,"at":"2026-09-09T10:15:46.300Z","turnId":"turn_9b8d7c6e5f4a3b2c1d0e",
 "chatId":"447700900123@s.whatsapp.net","sentMessageIds":["3EB0A1B2C3D4E5F60718"]}

{"_tag":"TurnFailed","v":1,"seq":46,"at":"2026-09-09T10:26:00.000Z","turnId":"turn_0a1b2c3d4e5f60718293",
 "chatId":"447700900123@s.whatsapp.net","reason":"no reply within 10 minutes"}

{"_tag":"ConnectionChanged","v":1,"seq":47,"at":"2026-09-09T10:30:00.000Z","state":"connecting","detail":"Connection Terminated (428)"}
```

```json
{"_tag":"ReactionReceived","v":1,"seq":48,"at":"2026-09-10T10:32:00.000Z","chatId":"447700900123@s.whatsapp.net",
 "messageId":"3EB0A1B2C3D4E5F60718","senderId":"447700900123@s.whatsapp.net","emoji":"👍","onOurMessage":true}

{"_tag":"MessageEdited","v":1,"seq":49,"at":"2026-09-10T10:33:00.000Z","chatId":"447700900123@s.whatsapp.net",
 "messageId":"3EB0C8F1A2B3C4D5E6F7","text":"can you check the calendar for thursday?"}

{"_tag":"TurnCancelled","v":1,"seq":50,"at":"2026-09-10T10:34:00.000Z","turnId":"turn_0a1b2c3d4e5f60718293",
 "chatId":"447700900123@s.whatsapp.net","reason":"every message was deleted by its sender"}

{"_tag":"PollVote","v":1,"seq":51,"at":"2026-09-10T10:40:00.000Z","chatId":"447700900123@s.whatsapp.net",
 "pollMessageId":"3EB0D4E5F6A7B8C9D0E1","voterId":"447700900123@s.whatsapp.net","selected":["Sushi"]}

{"_tag":"PresenceChanged","v":1,"seq":52,"at":"2026-09-10T10:41:00.000Z","chatId":"447700900123@s.whatsapp.net","state":"composing"}

{"_tag":"TurnOpened","v":1,"seq":53,"at":"2026-09-10T10:42:00.000Z","turnId":"turn_5e4d3c2b1a0f9e8d7c6b",
 "chatId":"120363012345678901@g.us","isGroup":true,
 "context":[{"...":"the ten group messages before the one below, oldest first"}],
 "messages":[{"chatId":"120363012345678901@g.us","messageId":"3EB0F0E1D2C3B4A59687","senderId":"447700900456@s.whatsapp.net",
              "senderNumber":"447700900456","senderName":"Sam","isGroup":true,"timestamp":"2026-09-10T10:41:58.000Z",
              "kind":"text","text":"@447700900999 which one is closer?","hasMedia":false,
              "mentionedIds":["447700900999@s.whatsapp.net"],"mentionsMe":true}]}
```

`TurnFailed.reason` is one of `responder failed: <detail>`, `no reply within <duration>`, `abandoned: <reason>`, `sending failed: <detail>`, `not connected: <detail>`, `crashed: <detail>`.

A chat's lifecycle on the stream is always `MessageReceived`* → `TurnOpened` → (`TurnSuperseded` → `TurnOpened`)* → `TurnCompleted` | `TurnFailed` | `TurnCancelled`, with `MessageEdited`, `MessageDeleted` and `ReactionReceived` possible at any point. A `TurnOpened` follows the first `MessageReceived` within `BRIDGE_BATCH_MAX_WAIT`, plus up to `BRIDGE_MEDIA_WAIT` when an attachment is being downloaded for it.

### Objects

#### `InboundMessage`

One WhatsApp message as Intro sees it, inside `MessageReceived.message`, `TurnOpened.messages[]`, `TurnPayload.messages[]` and `TurnSummary.messages[]`.

| Field | Type | Present | Meaning |
|---|---|---|---|
| `chatId` | string | always | The chat to reply to. |
| `messageId` | string | always | WhatsApp's id for this message. |
| `senderId` | string | always | Jid of the sender: the chat itself for DMs, the participant for groups. |
| `senderNumber` | string | when known | Digits of the sender's phone number. May be absent when WhatsApp only exposed a `@lid` id. |
| `senderName` | string | when known | The sender's WhatsApp display name. |
| `isGroup` | boolean | always | |
| `timestamp` | ISO-8601 | always | When WhatsApp says it was sent. |
| `kind` | `text` `image` `video` `audio` `document` `sticker` `location` `contact` `other` | always | |
| `text` | string | when non-empty | The body, or the caption of an image, video or document. Absent for media without a caption, stickers, locations, contacts. |
| `hasMedia` | boolean | always | True for `image`, `video`, `audio`, `document`, `sticker`. |
| `mimetype` | string | when `hasMedia` | As declared by WhatsApp, e.g. `image/jpeg`, `audio/ogg; codecs=opus`, `application/pdf`. |
| `quotedMessageId` | string | when quoting | The message this one replies to. |
| `mentionedIds` | string[] | when it @-mentions anyone | Jids mentioned in the text. |
| `mentionsMe` | boolean | when true | The bridge's own number was mentioned. In groups this is what opened the turn. |
| `quotesMe` | boolean | when true | The message quotes one the bridge sent. In groups this also opens a turn. |
| `location` | [`Location`](#location) | when `kind` is `location` | The pin, or the first point of a live location. |
| `contacts` | [`SharedContact`](#sharedcontact)[] | when `kind` is `contact` | The cards. |
| `media` | [`MediaFile`](#mediafile) | when downloaded in time | The local file. Present on `TurnOpened` for automatically downloaded kinds whose download finished within `BRIDGE_MEDIA_WAIT`; never on `MessageReceived`. Otherwise fetch it via the media endpoint. |

Messages that never reach Intro: the bridge's own sends, status and broadcast traffic, newsletters, reactions, protocol bookkeeping, group messages when `BRIDGE_GROUPS=false`, senders outside `BRIDGE_ALLOWED_SENDERS`, and messages replayed after downtime that are older than `BRIDGE_OFFLINE_WINDOW`. Group messages that neither mention nor quote the bridge reach it only as `context` on a later turn (and in the history).

#### `Location`

`{latitude, longitude, name?, address?, url?, live?}`. `name`, `address` and `url` are the labels WhatsApp attached to a pin; `live: true` marks a live-location share, of which the bridge reports the first point only.

#### `SharedContact`

`{name, phone?, vcard}`. `phone` is the first number on the card, digits only, when there is one; `vcard` is the card as WhatsApp sent it, for anything else on it.

#### `PresenceInfo`

`{chatId, state, at?, lastSeenAt?}`, the body of [`GET /chats/{chatId}/presence`](#get-chatschatidpresence).

#### `MediaFile`

| Field | Type | Meaning |
|---|---|---|
| `path` | string | Absolute path on the bridge machine, readable by the user the daemon runs as. Deleted after `BRIDGE_MEDIA_RETENTION`; copy what you want to keep. |
| `mimetype` | string | |
| `size` | integer | Bytes on disk. |
| `kind` | `MessageKind` | |
| `downloadedAt` | ISO-8601 | |

#### `TurnSummary`

What `GET /turns` and `GET /turns/{turnId}` return.

| Field | Type | Present | Meaning |
|---|---|---|---|
| `turnId` | string | always | |
| `chatId` | string | always | |
| `isGroup` | boolean | always | |
| `state` | see below | always | |
| `messages` | `InboundMessage[]` | always | The batch, oldest first. |
| `context` | `InboundMessage[]` | groups, when there is any | The last 10 messages seen in the group before `messages`, oldest first. |
| `supersedes` | string | when it replaced a turn | |
| `supersededBy` | string | when `state` is `superseded` | The turn to answer instead. |
| `openedAt` | ISO-8601 | always | |
| `sentMessageIds` | string[] | when `state` is `committed` | |
| `failure` | string | when `state` is `failed` | Same text as `TurnFailed.reason`. |

| `state` | Open? | Meaning |
|---|---|---|
| `batching` | yes | Collecting messages; the window has not closed. |
| `responding` | yes | The responder is working (`http` handler call in flight). |
| `awaiting` | yes | Waiting for `POST /turns/{turnId}/reply` (manual mode, or an `http` handler that answered `{"async": true}`). |
| `committed` | no | Replied; `sentMessageIds` set. |
| `superseded` | no | Replaced by `supersededBy`. |
| `failed` | no | `failure` says why; the eye came off and the failure text went out. |
| `cancelled` | no | Every message was deleted by its sender before an answer went out. |

Transitions: `batching` → `responding` → (`awaiting`) → `committed`, with `superseded` possible from any open state (a newer message or an edit), `failed` from `responding` or `awaiting`, and `cancelled` from any open state (a delete that left nothing).

#### `TurnPayload`

What the `http` responder POSTs to `INTRO_HANDLER_URL`. Same fields as `TurnOpened` minus the envelope: `{"v": 1, "turnId", "chatId", "isGroup", "messages": [InboundMessage], "supersedes"?, "context"?}`.

#### `HandlerResponse`

What an `http` handler answers with. All fields optional.

| Field | Type | Meaning |
|---|---|---|
| `text` | string or null | One bubble. |
| `replies` | string[] | Several bubbles, in order; sent before `text` when both are present. |
| `attachments` | [`OutboundMediaPayload`](#outboundmediapayload)[] | Files, sent after the text. An attachment that cannot be read fails the turn (`responder failed: OutboundFileUnreadable: ...`). |
| `album` | boolean | Adjacent image and video attachments go out as one album. |
| `async` | boolean | `true`: "I will answer through `POST /turns/{turnId}/reply`"; the turn moves to `awaiting`. |

`{}` completes the turn without sending anything.

#### `OutboundMediaPayload`

A file to send: the body of `POST /chats/{chatId}/media`, each element of `attachments` on a reply and in a `HandlerResponse`. Exactly one of `path` and `data`.

| Field | Type | Meaning |
|---|---|---|
| `path` | string | Absolute path of a file on the bridge machine, readable by the user the daemon runs as. Preferred when Intro runs there too. |
| `data` | string | The bytes, base64 (standard alphabet, padded). Needs `mimetype`. |
| `mimetype` | string | Required with `data`; with `path` inferred from the extension (`jpg png gif webp mp4 mov 3gp mp3 m4a aac wav ogg oga opus pdf txt md csv json zip doc docx xls xlsx ppt pptx`), else `application/octet-stream`. |
| `kind` | `image` `video` `audio` `document` | How WhatsApp presents it. Inferred from the mimetype's major type (`image/*`, `video/*`, `audio/*`, anything else a document); set it to `document` to send an image or video as a file, untouched. |
| `caption` | string | Text under an image, video or document. Ignored for audio. |
| `fileName` | string | Shown on document cards. Defaults to the path's basename, or `file.<ext>` for raw data. |
| `voiceNote` | boolean | Audio only: send as a voice-note bubble. WhatsApp plays those only as `audio/ogg; codecs=opus`. |

#### `HistoryRow`

One message from the history, as `GET /chats/{chatId}/messages` and `GET /messages` return it.

| Field | Type | Present | Meaning |
|---|---|---|---|
| `chatId`, `messageId` | string | always | |
| `direction` | `in` or `out` | always | Received by the bridge, or sent by it. |
| `senderId` | string | always | The sender's jid; `me` for the bridge's own messages. |
| `senderName` | string | when known | Push name. |
| `isGroup` | boolean | always | |
| `timestamp` | ISO-8601 | always | |
| `kind` | `MessageKind` | always | |
| `text` | string | unless deleted | The body or caption as first seen. |
| `editedText` | string | after an edit | The latest body. Read `editedText ?? text`. |
| `mimetype` | string | for attachments | |
| `mediaPath` | string | when downloaded | The local file; gone once `mediaEvictedAt` is set. |
| `mediaEvictedAt` | ISO-8601 | after eviction | The cache let the file go. Fetch it again through the media endpoint. |
| `quotedMessageId` | string | when quoting | |
| `status` | `sent`, `delivered`, `read` | outbound | Never goes backwards. |
| `deletedAt` | ISO-8601 | on a tombstone | The sender deleted it for everyone; the text is gone. |
| `turnId` | string | when part of a turn | The turn it belonged to (inbound) or answered (outbound). |
| `reactions` | `[{senderId, emoji, at}]` | always | Current reactions on it. |
| `snippet` | string | search results | The matched text with the hit marked `[like this]`. |

#### `ScheduledSend`

`{scheduledId, chatId, kind: "text" | "media", sendAt, deliverWhenOnline, status, createdAt, messageId?, detail?}`. See [`GET /scheduled`](#get-scheduled).

#### `BridgeStatus`

The body of `GET /health` and `GET /status`, and the `status` field of `Unhealthy`.

| Field | Type | Present | Meaning |
|---|---|---|---|
| `state` | `starting` `linking` `connecting` `ok` `needs_relink` `stopped` | always | See [Health and failure modes](#health-and-failure-modes). `stopped` is only ever produced by the CLI when the daemon is not running. |
| `detail` | string | when there is something to say | Why the state is what it is (the close reason, "no linked session on disk: run `bridge link`", …). |
| `linkedAs` | `{jid, name?}` | once linked | The dedicated number's own jid and name. |
| `responder` | `manual` `http` `pong` | always | Which responder is wired. |
| `version` | string | always | The bridge version. |
| `startedAt` | ISO-8601 | always | Changes on every daemon restart. |
| `lastEventAt`, `lastInboundAt`, `lastOutboundAt` | ISO-8601 | once it happened | |
| `reconnects` | integer | always | Socket closes seen this run. |
| `lastDisconnect` | `{at, reason}` | after the first close | |
| `openTurns` | integer | always | Turns in an open state. |
| `databasePath` | string | always | The SQLite file, for opening read-only with `sqlite3`. |
| `history` | boolean | always | Whether message history is being recorded. |

#### `SentMessage` and `ReplyResult`

`POST /chats/{chatId}/messages` returns `{"chatId", "messageId"}`; `POST /turns/{turnId}/reply` returns `{"turnId", "sentMessageIds": []}`.

### Errors

Every error body is `{"_tag": "...", ...fields}`. The status code is fixed per tag.

| Status | `_tag` | Fields | Meaning | What to do |
|---|---|---|---|---|
| 400 | `InvalidRequest` | `kind` (`Params` `Query` `Headers` `Payload`), `message`, `method`, `path` | The request failed its schema: a missing or empty field, a bad type, a `since` that is not a non-negative integer, a body that is not JSON. `message` names the field. | Fix the request; do not retry as is. |
| 401 | `Unauthorized` | `message` | Bearer token missing or wrong. | Send the configured token. |
| 404 | `RouteNotFound` | `method`, `path` | No such endpoint (also an empty path segment, e.g. `/turns//reply`). | Check the path. |
| 404 | `TurnNotFound` | `turnId` | Unknown or pruned turn, or the daemon restarted. | Drop the answer. |
| 404 | `MessageUnknown` | `chatId`, `messageId`, `reason` | The message to quote, edit or delete is not one the bridge saw or sent this run. | Check the id; after a restart, only new messages are known. |
| 409 | `MessageTooOld` | `chatId`, `messageId`, `operation`, `limitMs`, `ageMs` | WhatsApp's window for the edit (15 minutes) or delete (about 2 days) has passed. | Send a correction as a new message instead. |
| 409 | `TurnNotOpen` | `turnId`, `state` | `progress` or `abandon` on a turn that is not `responding` or `awaiting`. | Nothing to do. |
| 409 | `HistoryDisabled` | | A history endpoint was called with history off. | Enable history, or use the live turn data. |
| 404 | `ScheduledNotFound` | `scheduledId` | No pending scheduled send with that id (already sent, cancelled, or unknown). | Check `GET /scheduled`. |
| 502 | `LookupFailed` | `subject`, `detail` | WhatsApp refused or timed out a contact or group lookup. | Retry later. |
| 500 | `StoreError` | `operation`, `detail` | The database misbehaved. | Check disk space and the log. |
| 404 | `MediaUnavailable` | `chatId`, `messageId`, `reason` | No media on that message, or not seen this run and nothing on disk. | Say so in the reply rather than pretending the attachment was seen. |
| 404 | `OutboundFileUnreadable` | `path`, `detail` | The file to send does not exist, is a directory, or is not readable by the daemon. | Check the path (absolute, on the bridge machine). |
| 400 | `OutboundInvalid` | `detail` | Raw `data` without a `mimetype`, an empty file, or an album item that is not an image or video. | Fix the payload. |
| 413 | `OutboundTooLarge` | `size`, `maxBytes` | The file to send is over `BRIDGE_MEDIA_MAX_BYTES`. | Shrink it, or raise the limit. |
| 409 | `TurnSuperseded` | `turnId`, `chatId`, `currentTurnId` | The turn was replaced by newer messages. | Answer `currentTurnId`. |
| 409 | `TurnNotAwaitingReply` | `turnId`, `state` | The turn is not waiting for a reply (`responding`, `committed`, `failed`, `batching`). | Nothing to send. |
| 413 | `MediaTooLarge` | `chatId`, `messageId`, `size`, `maxBytes` | Over `BRIDGE_MEDIA_MAX_BYTES`. | Tell the user; raise the limit if it should have been allowed. |
| 415 | (plain text) | | Request body was not `application/json`. | Set the header. |
| 502 | `SendFailed` | `operation`, `chatId`, `detail` | The socket was up but WhatsApp rejected the send, react, unreact or typing call, or it took over 30 seconds. Direct calls get one attempt; turn replies are retried with backoff for about 30 seconds before the turn fails. | Retry later. |
| 502 | `MediaDownloadFailed` | `chatId`, `messageId`, `detail` | WhatsApp did not deliver the bytes. | Retry later; not cached. |
| 502 | `TurnFailedError` | `turnId`, `reason` | The reply was accepted but the send failed, or the wait for it passed 90 seconds. | Check `GET /turns/{turnId}`; do not resend blindly. |
| 503 | `NotConnected` | `state`, `detail?` | No live WhatsApp socket. `state` says whether waiting helps: `connecting` yes (backoff is capped at 60 s); `needs_relink` no, a human must run `bridge link`. | Wait and retry, or escalate. |
| 503 | `Unhealthy` | `status` (`BridgeStatus`) | `GET /health` only: the line is not `ok`. | Read `status.state`. |
| 500 | `MediaStoreError` | `operation`, `detail` | Disk trouble under the media directory. | Check disk space and permissions. |
| 500 | (empty) | | A defect: a bug in the bridge. The daemon logs it and keeps running. | Report it with the log line. |

### Responder contracts

`BRIDGE_RESPONDER` decides who answers a turn. The events, the turn registry and every endpoint above behave the same in all three modes.

**`manual`** (default). The bridge does nothing on its own: on `TurnOpened` the turn is `awaiting` and waits `BRIDGE_TURN_TIMEOUT` (10 minutes) for `POST /turns/{turnId}/reply`, longer with `progress`. Intro drives the loop from `GET /events`. When the deadline passes, `TurnFailed` with `no reply within ...`, the eye comes off, and the failure text goes out.

**`http`**. The bridge POSTs each turn to `INTRO_HANDLER_URL` and sends back what the handler returns.

| | |
|---|---|
| Request | `POST <INTRO_HANDLER_URL>`, `Content-Type: application/json`, body a [`TurnPayload`](#turnpayload). No authentication header is added. |
| Timeout | `INTRO_HANDLER_TIMEOUT` (120 seconds) for the whole call. |
| Retries | A transport error (connection refused, reset) is retried twice more. A non-2xx status is **not** retried: it fails the turn with `responder failed: HttpClientError: ...` naming the status and URL. |
| Response | `2xx` with a JSON [`HandlerResponse`](#handlerresponse): `{"text"}`, `{"replies": []}`, `{"attachments": []}`, any mix, `{}` (nothing to send), or `{"async": true}` (answer later through `POST /turns/{turnId}/reply`, within `BRIDGE_TURN_TIMEOUT`). A body that is not valid `HandlerResponse`, or an attachment that cannot be read, fails the turn. |
| Interrupts | When newer messages supersede the turn, the request is **aborted** (the socket closes). A slow handler should watch for the close and stop working; the replacement turn arrives as a new request with `supersedes` set. |
| While the call runs | The turn's `state` is `responding`; `POST /turns/{turnId}/reply` answers `409 TurnNotAwaitingReply` until the handler returns `{"async": true}`. |

**`pong`**. The harness: waits `BRIDGE_HARNESS_SLOW` (or `bridge harness --slow`), answers `pong` (or `pong (N messages batched)`, with `— answered after an interrupt` when the turn superseded another). For verifying the line end to end, never for production.

### CLI

`npm run bridge -- <command> [flags]` from the checkout, or `bridge <command>` after `npm run build`. `npm run link`, `npm start`, `npm run harness` and `npm run health` are shortcuts for `link`, `run`, `harness` and `health`. Every command prints `--help`. Global flags: `--log-level <all|trace|debug|info|warn|error|fatal|none>`, `--version`.

The client commands (everything but `link`, `run`, `harness`, `history enable`, `history purge` and `unlink`) talk to a running daemon at `BRIDGE_URL` (default `http://$BRIDGE_HOST:$BRIDGE_PORT`) and send `BRIDGE_API_TOKEN` when it is set. They print the daemon's JSON answer on stdout, or the error's message on stderr with a non-zero exit code. When the daemon is not listening they fail with `daemon not reachable at <url>: <detail>`.

| Command | Flags | Does | Prints |
|---|---|---|---|
| `link` | `--phone <digits>` pairing code instead of QR; `--timeout <duration>` (5 minutes) | Links the dedicated number and saves the session under `$BRIDGE_DATA_DIR/auth`. Refuses to run while a daemon holds the session lock. | `Linked as <jid> (<name>)` |
| `run` | | The daemon: WhatsApp connection, turn engine, HTTP API. Long-lived; Ctrl-C stops it after in-flight replies finish. | Logs |
| `harness` | `--slow <duration>` (0), `--once` | `run` with the `pong` responder. `--once` exits after the first completed turn. | Logs |
| `health` | | `GET /status` | The `BridgeStatus`; **exit code 0 only when `state` is `ok`**, 1 otherwise. Unreachable daemon: `{"state":"stopped","detail":"daemon not reachable at ..."}` and exit 1. |
| `send` | `--to <jid or phone>`, then `--text <text>` or `--file <path>` (with `--caption`, `--as <image\|video\|audio\|document>`), `--quote <messageId>`, `--at <ISO-8601>` with `--when-online` | `POST /chats/{chatId}/messages` or `POST /chats/{chatId}/media`. A phone number in any format is converted to a jid. | `{"chatId","messageId"}`, or the scheduled record |
| `forward` | `--from <chat>`, `--message <id>`, `--to <chat>` | `POST /chats/{chatId}/forward` | `{"chatId","messageId"}` |
| `location` | `--to`, `--lat`, `--lng`, `--name`, `--address` | `POST /chats/{chatId}/location` | `{"chatId","messageId"}` |
| `poll` | `--to`, `--question`, `--option <text>` (repeat, two to twelve), `--multiple` | `POST /chats/{chatId}/polls` | `{"chatId","messageId"}` |
| `presence` | `--chat` | `GET /chats/{chatId}/presence` | `PresenceInfo` |
| `contact` | `--jid <jid or phone>` | `GET /contacts/{jid}` | `ContactInfo` |
| `chats` | | `GET /chats` | `ChatSummary[]` |
| `messages` | `--chat`, `--q`, `--before`, `--limit` | `GET /chats/{chatId}/messages` | `HistoryRow[]` |
| `search` | `--q`, `--limit` | `GET /messages` | `HistoryRow[]` |
| `scheduled` | | `GET /scheduled` | `ScheduledSend[]` |
| `unschedule` | `--id` | `DELETE /scheduled/{id}` | `{"ok":true}` |
| `history enable` | `--yes` | Prints what history stores, asks, writes `data/history.accepted`. Needed once before `BRIDGE_HISTORY=true`. | |
| `history purge` | `--before <ISO-8601>`, `--yes` | Deletes history older than the instant, straight in the database. | `Removed N message(s)` |
| `edit` | `--chat`, `--message <id>`, `--text` | `PATCH /chats/{chatId}/messages/{messageId}` | `{"ok":true}` |
| `delete` | `--chat`, `--message <id>` | `DELETE /chats/{chatId}/messages/{messageId}` | `{"ok":true}` |
| `react` | `--chat <jid or phone>`, `--message <id>`, `--emoji <emoji>` (👀) | `POST .../reactions` | `{"ok":true}` |
| `unreact` | `--chat`, `--message` | `DELETE .../reactions` | `{"ok":true}` |
| `media` | `--chat`, `--message` | `GET .../media`: downloads if needed. | The `MediaFile` |
| `turns` | | `GET /turns` | The open `TurnSummary[]` |
| `reply` | `--turn <turnId>`, `--text <text>` and/or `--file <path>` (`--caption`, `--as`), `--quote <messageId>`, `--replace-progress` | `POST /turns/{turnId}/reply`; the file goes after the text. | `{"turnId","sentMessageIds"}` |
| `progress` | `--turn <turnId>`, `--text <text>`, `--extend <duration>` | `POST /turns/{turnId}/progress` | `{"turnId","deadline","typingUntil","progressMessageId"?}` |
| `abandon` | `--turn <turnId>`, `--reason <text>`, `--text <text>` | `POST /turns/{turnId}/abandon` | `{"ok":true}` |
| `unlink` | `--yes` (required) | Deletes the saved session so the next `link` starts clean. Without `--yes` it refuses. | `Removed <dir>.` |

Durations on the CLI use the same format as the configuration: `"15 seconds"`, `"10 minutes"`.

### Playbooks

**Answer a turn (manual mode).** Read `TurnOpened.messages` (oldest first, one burst from one chat). Produce one reply for the whole burst. `POST /turns/{turnId}/reply {"text"}`. On `200`, done: the bridge cleared the 👀. On `409 TurnSuperseded`, discard and wait for (or look up) `currentTurnId`. On `409 TurnNotAwaitingReply`, discard. On `502 TurnFailedError`, the reply was not delivered; decide whether a fresh `POST /chats/{chatId}/messages` is appropriate once `GET /health` is `ok`.

**Nothing to say.** `POST /turns/{turnId}/reply {}` completes the turn silently and clears the eye. Do this rather than leaving the turn to time out: a timed-out turn sends the failure text.

**Taking a while.** Within the first minutes, `POST /turns/{turnId}/progress {"text": "Checking…"}`. Typing stays on, the deadline moves, and the user sees a bubble. Answer with `replaceProgress: true` so that bubble becomes the answer, or leave it and add a new one. Repeat progress calls as needed; the hard cap is one hour after the turn opened.

**Cannot do it.** `POST /turns/{turnId}/abandon {"reason": "needs the app", "text": "This needs the app: …"}`. The turn fails with your reason, the eye comes off, the user gets your text quoted on their message.

**Correct yourself.** `PATCH /chats/{chatId}/messages/{messageId} {"text": "..."}` within 15 minutes of sending; `DELETE` within about 2 days. Both only for messages the bridge sent since it started.

**The user edited or deleted something.** Nothing to do: a pending message is corrected or dropped for you, and the turn is superseded or cancelled with an event. If the edit arrives after you answered, `MessageEdited` is your cue to answer again if it matters.

**Remember.** With history on, `GET /chats/{chatId}/messages?limit=30` before answering gives the recent conversation; `?since=<when you last looked>` gives what you missed; `GET /messages?q=dentist` finds it wherever it was said. Read `editedText ?? text`, skip rows with `deletedAt`, and treat `mediaEvictedAt` as "fetch the file again if you need it".

**Schedule a message.** `POST /chats/{chatId}/messages {"text": "Dentist in an hour.", "sendAt": "2026-09-11T09:00:00Z"}` answers `202` with a `scheduledId`; `DELETE /scheduled/{id}` cancels it. Add `"deliverWhenOnline": true` for something that should land when the person is around. Watch `ScheduledSent`, `ScheduledSkipped` and `ScheduledFailed`.

**Pass something on.** `POST /chats/{otherChatId}/forward {"fromChatId", "messageId"}` forwards a message, attachments included, without downloading and re-uploading it.

**Who is this?** `GET /contacts/{jid}` before writing to a number you have not seen: `onWhatsApp: false` means the message can never arrive. `GET /chats/{groupId}` lists a group's members and admins.

**Watch the reactions.** `ReactionReceived` with `onOurMessage: true` is feedback on an answer: a 👍 needs nothing, a ❌ or 👎 is worth a follow-up in the same chat.

**Keep the stream alive.** Open `GET /events?since=<last seq>` (omit `since` the very first time). Parse `data:` frames. Store `seq` from every frame. If no frame arrives for 60 seconds, reconnect. On reconnect the replay fills the gap; if the first replayed `seq` is more than `last seq + 1`, or smaller than it (daemon restart), call `GET /turns` and answer whatever is open.

**Catch up after your own restart.** `GET /turns` lists every turn still waiting. Answer the `awaiting` ones; ignore `batching` and `responding` (their events will follow). Then open the stream.

**Handle an image.** On `TurnOpened`, a message with `kind: "image"` (or `sticker`) usually carries `media.path`: open the file, or read it as bytes. If `media` is missing (download slower than 8 seconds, or automatic downloads disabled), call `GET /chats/{chatId}/messages/{messageId}/media` and use the returned `path`. On `404 MediaUnavailable` or `502 MediaDownloadFailed`, say in the reply that the attachment could not be read.

**Handle a voice note or a document.** `kind: "audio"` (`audio/ogg; codecs=opus`) or `document`. Call `GET .../media`, then transcribe or read the file at `path`. `413 MediaTooLarge` means the file is over the configured limit; tell the user.

**Send an image or a file.** Write it to disk on the bridge machine, then either answer the turn with it (`POST /turns/{turnId}/reply {"text": "...", "attachments": [{"path": "/tmp/q3.png", "caption": "..."}]}`) or send it on its own (`POST /chats/{chatId}/media {"path": "/tmp/q3.png"}`). Use `"kind": "document"` for a PDF, a spreadsheet, or a picture that must arrive uncompressed. From another machine, send `{"data": "<base64>", "mimetype": "image/png"}` instead. On `404 OutboundFileUnreadable` check the path is absolute and readable by the daemon's user; on `413 OutboundTooLarge` shrink the file.

**Say something unprompted.** `POST /chats/{chatId}/messages {"text"}` with the `chatId` of an earlier message. `503 NotConnected` with `state: "connecting"`: wait a minute and retry. With `state: "needs_relink"`: a human must run `npm run link` on the bridge machine; tell them through another channel.

**Escalate to the app.** For anything that only exists as an in-app action (payments, spend approvals, connector authorisations), reply or send: *"This needs the app: open Grok Bot → <the action>."* Never claim the action happened.

**Check the line is alive.** `GET /health` without a token: `200` means messages flow; `503` carries the reason in `status.state` and `status.detail`. `bridge health` does the same from a shell with the exit code.

**Deduplicate across surfaces.** Every message has `chatId` + `messageId`, every turn has `turnId`; key your own dedupe on them so an ask answered in the app is not answered again on WhatsApp.

**Group chats.** Only when `BRIDGE_GROUPS=true`. `isGroup` is true, `chatId` ends in `@g.us`, `senderId` is the participant, `senderName` their push name. A turn opens only when someone @-mentions the bridge (`mentionsMe`) or quotes one of its messages (`quotesMe`); read `context` for what the group was discussing before that, and `GET /chats/{chatId}` for who is in it. If the mention needs no answer, `POST /turns/{turnId}/reply {}`. To address one member in a reply, write `@<digits>` and list their jid in `mentions`. Quote the triggering message (`quoteMessageId`) so the answer reads as a reply in a busy chat.

**Send a place, a card, a vote.** `POST /chats/{chatId}/location {"latitude", "longitude", "name"?}` for a pin; `POST /chats/{chatId}/contacts {"contacts": [{"name", "phone"}]}` for a card the user can save; `POST /chats/{chatId}/polls {"question", "options": [...]}` to ask a multiple-choice question and read the answer from `PollVote` events (`selected` is the voter's current choice, so the last event per voter wins). Inbound, `kind: "location"` and `kind: "contact"` messages carry `location` and `contacts`.

**Several photos.** Put them all in `attachments` and set `"album": true` on the reply, or `POST /chats/{chatId}/album {"items": [...]}`; the user sees one grid instead of a column of bubbles. Mix in a PDF and it goes out as its own bubble.

**Is the user around?** `GET /chats/{chatId}/presence` once; from then on `PresenceChanged` events say when they come online or start typing. Use it to time a nudge, not to answer faster: the bridge already holds the batch window while they type.

## Configuration

Environment variables, with `.env` in the working directory as a fallback (`.env.example` documents all of them).

| Variable | Default | Purpose |
|---|---|---|
| `BRIDGE_DATA_DIR` | `./data` | Session lives in `$BRIDGE_DATA_DIR/auth`, the database in `$BRIDGE_DATA_DIR/bridge.db` |
| `BRIDGE_DATABASE_PATH` | `<data dir>/bridge.db` | Where the SQLite file lives |
| `BRIDGE_HISTORY` | `false` | Record every message; needs `bridge history enable` first |
| `BRIDGE_HOST` / `BRIDGE_PORT` | `127.0.0.1` / `8787` | Where the API listens |
| `BRIDGE_API_TOKEN` | unset | Bearer token for the API |
| `BRIDGE_RESPONDER` | `manual` | `manual`, `http` or `pong` |
| `INTRO_HANDLER_URL` | | Required for `http` |
| `INTRO_HANDLER_TIMEOUT` | `120 seconds` | Per handler call |
| `BRIDGE_BATCH_WINDOW` | `1500 millis` | Batch window per chat |
| `BRIDGE_BATCH_MAX_WAIT` | `5 seconds` | Upper bound on batching per turn |
| `BRIDGE_BATCH_TYPING_WAIT` | `15 seconds` | Upper bound on batching while the sender is still typing |
| `BRIDGE_TURN_TIMEOUT` | `10 minutes` | How long a turn waits for `POST /turns/{id}/reply` |
| `BRIDGE_SEEN_EMOJI` | `👀` | The seen reaction |
| `BRIDGE_SEEN_FALLBACK_TEXT` | unset | Text ack if the reaction fails |
| `BRIDGE_READ_RECEIPTS` | `true` | Mark accepted messages as read (blue ticks, subject to the phone's privacy setting) |
| `BRIDGE_FAILURE_TEXT` | `Something went wrong.` | Quoted reply sent when a turn fails or is abandoned; empty disables |
| `BRIDGE_TYPING_MAX` | `3 minutes` | How long "typing…" is kept alive per turn, re-armed by progress |
| `BRIDGE_GROUPS` | `false` | Handle group chats |
| `BRIDGE_GROUP_TRIGGER` | `mention` | What opens a group turn: `mention` (mentioned or quoted) or `always` (every message) |
| `BRIDGE_ALLOWED_SENDERS` | unset (anyone) | Comma-separated numbers/jids allowed to talk to the line |
| `BRIDGE_OFFLINE_WINDOW` | `0 seconds` | Handle messages replayed after downtime that are younger than this |
| `BRIDGE_TYPING` | `true` | Typing indicator while answering |
| `LOG_LEVEL` / `LOG_FORMAT` / `LOG_BODIES` | `Info` / `pretty` / `false` | Logging |
| `BAILEYS_LOG_LEVEL` | `warn` | The protocol library's own log level; lines are forwarded, redacted, into the bridge log |
| `WA_BROWSER_NAME` | `Intro Bridge` | Name in the phone's Linked devices list |
| `BRIDGE_MEDIA_DIR` | `<data dir>/media` | Where attachments are stored |
| `BRIDGE_MEDIA_AUTO` | `images` | `images`, `all` or `none`: what is downloaded before a turn opens |
| `BRIDGE_MEDIA_MAX_BYTES` | `26214400` | Refuse larger attachments, inbound and outbound |
| `BRIDGE_MEDIA_WAIT` | `8 seconds` | How long a turn waits for an automatic download |
| `BRIDGE_MEDIA_MAX_DISK` | `1073741824` | Disk budget for downloaded files; least recently used go first |
| `BRIDGE_MEDIA_RETENTION` | unset | Optional maximum age for downloaded files |
| `BRIDGE_MEDIA_SWEEP_EVERY` | `1 hour` | How often the retention sweep runs |
| `BRIDGE_HARNESS_SLOW` | `0 seconds` | The pong responder's thinking time (`bridge harness --slow` overrides it) |
| `BRIDGE_URL` | `http://$HOST:$PORT` | Where the CLI client commands find the daemon |

Durations use Effect's format: `"1500 millis"`, `"2 seconds"`, `"10 minutes"`. A value that does not parse stops the daemon at startup with the variable's name.

## Health and failure modes

`GET /health` and `bridge health` (exit code 0 only when `ok`):

```json
{ "state": "ok", "linkedAs": { "jid": "4479...@s.whatsapp.net", "name": "Intro" }, "responder": "manual",
  "version": "0.2.0", "startedAt": "...", "lastEventAt": "...", "lastInboundAt": "...", "lastOutboundAt": "...",
  "reconnects": 2, "lastDisconnect": { "at": "...", "reason": "Connection Terminated (428)" }, "openTurns": 0,
  "databasePath": "/home/box/whatsapp-bridge/data/bridge.db", "history": true }
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

## What happens when things go wrong

The bridge is built so that every failure is either recovered automatically or reported with its reason. The table is the contract.

| Situation | Behaviour |
|---|---|
| Network drops, WhatsApp closes the socket | Reconnect with jittered exponential backoff (1 s up to 60 s). A connection that lived 30 s resets the backoff. "Restart required" closes reconnect at once, at most 3 times in a row. `/health` shows `connecting` with the reason, `reconnects` counts, `lastDisconnect` explains. |
| WhatsApp logs the number out, bans it, rejects the session | `needs_relink` with the reason; the session is moved to `data/auth.invalid-<time>` (last three kept) so `bridge link` starts clean. The daemon keeps serving the API. |
| Socket creation or the connection attempt crashes | Logged with its cause, treated as a reconnect. The supervisor itself is restarted after 5 s if it ever dies. |
| The protocol library logs a warning or error | Forwarded into the bridge log at the same level, redacted (`BAILEYS_LOG_LEVEL`). |
| A phone shows "waiting for this message" | WhatsApp's resend request is answered from an in-memory outbox of the last 1000 sent messages. |
| Two daemons on one session | The second refuses to start: `another bridge process (pid N) holds this session`. Stale locks from crashed processes are taken over. |
| A message cannot be decoded | Logged with the reason, skipped; everything else flows. |
| WhatsApp re-delivers a message | Ignored: one 👀 and one turn per message id, checked in the transport and again in the engine. |
| Messages keep trickling in | The turn opens at `BRIDGE_BATCH_MAX_WAIT` regardless. |
| The responder fails, times out, throws, or the agent abandons the turn | `TurnFailed` with the reason, the eye taken off, typing cleared, the failure text sent as a quoted reply; the worker moves on to the next message. |
| The user edits or deletes a message the bridge is handling | Batching: corrected or dropped in place. Answering: the turn is superseded with the corrected text, or cancelled when nothing is left. Events say which. |
| A quote names a message the bridge does not know | Proactive send: `404 MessageUnknown`. Turn reply: sent without the quote, warning logged. |
| Sending the reply fails | Retried 5 times with backoff (about 30 s, enough for a reconnect); then `TurnFailed`. The message id is fixed before the first attempt, so a send that timed out but went through is never delivered twice. |
| A reply is very long or empty | Text over 4000 characters is split at paragraph, line or sentence boundaries into several messages; blank replies are dropped. |
| An attachment fails to download | The turn still opens (without `media`) after `BRIDGE_MEDIA_WAIT`; the failure is logged; the on-demand endpoint retries (failures are never cached). |
| The database cannot be written | History and state writes are logged and skipped; the daemon keeps handling messages. A database that cannot be opened at all stops startup with the reason. |
| The daemon was down when a scheduled send was due | Sent on startup if under an hour late, otherwise dropped with `ScheduledSkipped`. |
| A file Intro wants to send is missing, unreadable or too big | Refused with `OutboundFileUnreadable` / `OutboundTooLarge` before anything is sent; on a reply the turn stays `awaiting`; in `http` mode the turn fails with the reason. |
| Uploading a file to WhatsApp stalls | The send is capped at 3 minutes and answers `SendFailed`; a turn reply is retried like a text. |
| The 👀 reaction fails | Warning logged; `BRIDGE_SEEN_FALLBACK_TEXT` sends a text instead. |
| Intro's SSE client stalls | Its buffer slides (oldest dropped) instead of growing; reconnect with `?since=` to catch up. |
| A request is malformed | `400 InvalidRequest` naming the field, `404 RouteNotFound`, `415` for a non-JSON body; the daemon is unaffected. |
| A promise inside the protocol library rejects unhandled | Logged with its reason, the daemon keeps running. An uncaught exception is logged and the process exits 1 for the supervisor to restart. |
| Ctrl-C / SIGTERM | In-flight replies finish (that step is uninterruptible), the socket and server close within 5 s, the lock is removed. |
| Bad configuration | The daemon refuses to start and names the variable. |

## Security and privacy

- The session under `data/auth` is the keys to the number. The directory is created with mode 0700 and is git-ignored, as are `.env` files. Never commit or share it.
- The API binds to loopback by default. If you must expose it, set `BRIDGE_API_TOKEN` and put it behind something that terminates TLS.
- `data/bridge.db` is created 0600. With history on it holds every message in plain text; back it up with the same care as the session, and purge what you do not need.
- Sending by `path` reads any file the daemon's user can read and hands it to WhatsApp. That is the point on Intro's own machine; it is one more reason to keep the API on loopback or behind the token, and to run the daemon as a user with nothing to leak.
- The bridge makes no calls to OpenAI, xAI or any model provider. Its only outbound connections are WhatsApp's servers, the WhatsApp Web version lookup at startup, and `INTRO_HANDLER_URL` if you configure one.
- `markOnlineOnConnect` is off so the phone keeps receiving notifications.
- Presence subscriptions are per chat and only for chats that message the line or that the agent asks about; the bridge reports presence to nobody but the agent.

## Development

```sh
npm run typecheck     # tsc, strict
npm test              # vitest: engine (batching, interrupts, edits and deletes, progress, abandon, quoting, typing), API, media in and out, the http responder, extraction
npm run check         # both
npm run build         # dist/ + bin/bridge.js
```

Layout: `src/domain` (ids, message, event and status schemas), `src/whatsapp` (the transport: Baileys layer, session store, message extraction), `src/engine` (health, events, responders, media store, the turn registry and per-chat workers), `src/http` (the `HttpApi` definition, handlers, the typed client), `src/cli.ts`. Tests drive the real engine against an in-memory transport and a scriptable responder under Effect's `TestClock`.

Notes on the Effect v4 APIs and the traps met while building this are in `docs/effect-notes.md`. How Intro should behave on this line is in `docs/intro-integration.md`.
