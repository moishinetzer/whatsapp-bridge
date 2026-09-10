# Changelog

All notable changes to this project are documented here. Intro pins on a tag.

## v0.7.0 — 2026-09-10

Two numbers: the observer, and context as a capability. The design is `docs/spec/observer.md`.

- **The observer.** A second process of the same daemon (`bridge observe enable`, `observe link`, `observe run`) linked to the owner's own number as a read-only device. No send methods exist in that mode; it never marks anything read, never goes online, never reacts. It stores every DM and group both ways, reactions, edits, tombstones, calls (`kind: "call"`), the address book and chat names, and attachments per `BRIDGE_MEDIA_AUTO`, in its own directory, database and port. Backfill is the recent slice WhatsApp pushes at link time, refused before download past `BRIDGE_OBSERVE_BACKFILL_DAYS` (30), 20 chunks or 8 MB each, written in batched transactions; retention `BRIDGE_OBSERVE_RETENTION` (180 days) purged daily; `BRIDGE_OBSERVE_EXCLUDE` chats are never written. A one-time acceptance file gates it; `/status` and a weekly log line keep it visible. `observe health` and `observe purge`.
- **Context is turn-scoped.** History is read only through an open turn: `GET /turns/{id}/context/line/...` (the personal line) and `.../primary/...` (the observer). The engine builds the capability per turn as an Effect service: `OwnerContext` for a DM whose every message is from `BRIDGE_OWNER` (both lines, contacts, primary attachments), `ChatContext` for everything else (that chat on the line, nothing more). It ends with the turn (`409 TurnNotOpen`); reads beyond it are `403 ContextNotAllowed`; a missing observer is `503 ObserverUnavailable` while the line keeps working. `TurnOpened`, `TurnSummary` and `TurnPayload` carry `access`; inbound messages carry `fromOwner`. Every read is logged with its turn id.
- **Removed:** `GET /chats`, `GET /chats/{id}`, `GET /chats/{id}/messages`, `GET /messages`. The CLI's `chats`, `messages` and `search` read the database file directly (`--observer` for the observer's). `GET /contacts/{jid}` stays (a live lookup, not stored context).
- **The access rule.** With `BRIDGE_OWNER` set, the line answers the owner's DM, groups the owner is a member of (from the group's members, cached), and `BRIDGE_ALLOWED_SENDERS`; a listed person unlocks their DM only, a group must be listed as a group. Sends anywhere else are `403 NotAllowlisted`. The owner is recognised by number or by a lid the library maps to it. Without `BRIDGE_OWNER` nothing changes.
- Store: `contacts`, `chats` and `state` tables (migration `0003_observer`), batched inserts, calls as rows, contact merge and search, chat names with precedence (WhatsApp's, then the address book, then the last push name), a shared read side usable over a read-only client.
- The media store now depends on a `MediaSource` (download only), which both transports provide; the download itself is shared code.
- Effect notes: two traps (a layer object memoized by identity across nested provides; read-only `node:sqlite` opens fail as defects).
- Tests: the access rule, the chunk filter, contact mapping, the observer's store side, turn-scoped reads against a real observer database on disk (owner turn, group turn, friend's turn, closed turn, missing observer), sends refused by the rule. 118 in total.
- Event schema stays v1; `access` and `fromOwner` are additive.

## v0.6.0 — 2026-09-10

The long tail: the third and last push from `docs/spec/agent-surface.md`.

- Groups, properly. With `BRIDGE_GROUPS=true` a turn opens only when the bridge is @-mentioned or one of its messages is quoted (`BRIDGE_GROUP_TRIGGER=mention`; `always` for every message). Other group messages are stored with history on, emit no event, and feed a per-group ring: every group `TurnOpened`, `TurnSummary` and `TurnPayload` carries `context`, the last 10 messages before the trigger, oldest first. Inbound messages gain `mentionedIds`, `mentionsMe` and `quotesMe`; `mentions` on `POST /chats/{id}/messages` makes an outbound @-mention real.
- Typing holds the batch. Presence reports keep the batch window open while the sender types, up to `BRIDGE_BATCH_TYPING_WAIT` (15 seconds) from the oldest message, a separate cap from `BRIDGE_BATCH_MAX_WAIT`. The bridge subscribes to a chat's presence on its first message.
- `GET /chats/{id}/presence` returns the last known state (`available`, `unavailable`, `composing`, `recording`, `paused`, `unknown`, with `lastSeenAt` when WhatsApp shares it) and subscribes the chat to `PresenceChanged` events; nothing else emits them.
- Locations and contacts: `location` and `contacts` decoded on inbound messages (pins, live locations, vCards with the first phone number); `POST /chats/{id}/location` and `POST /chats/{id}/contacts` to send them.
- Polls: `POST /chats/{id}/polls {question, options, multiple}`; every vote arrives as a `PollVote` event with the voter's whole current selection, decrypted by the bridge (the library leaves votes encrypted). Votes on a poll sent before a restart need history on.
- Albums: `POST /chats/{id}/album` sends two or more images and videos as one album; `album: true` on `POST /turns/{id}/reply` and in a `HandlerResponse` groups each run of adjacent image and video attachments, documents and audio staying separate. An album item that is not an image or video is `400 OutboundInvalid`.
- CLI: `location`, `poll`, `presence`.
- Tests: mention, location and contact extraction; the group context ring and its cap; typing-aware batching under the test clock, both the hold and the cap; album grouping in the engine and through the API; the new endpoints and their refusals. 99 in total.
- Event schema stays v1; everything new is additive.

## v0.5.0 — 2026-09-10

Memory: the second of three pushes from `docs/spec/agent-surface.md`.

- One SQLite file, `data/bridge.db` (Effect's `SqlClient` over Node's built-in `node:sqlite`, WAL, created 0600), always holds bridge state: what the bridge sent (so edits, deletes, quotes and resends survive restarts), scheduled sends, turn outcomes, and the media cache index.
- History, opt-in: `bridge history enable` prints what gets stored and asks once; with `BRIDGE_HISTORY=true` and no acceptance the daemon refuses to start. Both directions, edits, tombstones on user deletes, reactions, delivery state, attachment paths, the raw message bytes. `GET /chats`, `GET /chats/{chatId}` (with group members), `GET /chats/{chatId}/messages` with `before`, `since`, `limit` and `q`, and `GET /messages?q=` full-text search (FTS5, prefix terms, phrases, snippets). `bridge history purge --before`. Stable `messages_v` and `chats_v` views for `sqlite3`; `GET /status` reports `databasePath` and `history`.
- Media cache: a disk budget, `BRIDGE_MEDIA_MAX_DISK` (1 GB), evicting least recently used first; a use is a download, a turn delivery, a media fetch, or the file appearing in a history or search result. `BRIDGE_MEDIA_RETENTION` is now optional and unset.
- Scheduled sends: `sendAt` and `deliverWhenOnline` on text and file sends answer `202 {scheduledId, sendAt, deliverWhenOnline}`; `GET /scheduled`, `DELETE /scheduled/{id}`; `ScheduledSent`, `ScheduledSkipped` (over an hour late after downtime, or nobody online within 24 hours) and `ScheduledFailed` events.
- `MessageDelivered` and `MessageRead` events for the bridge's own messages, one per participant in groups.
- `POST /chats/{chatId}/forward`, `GET /contacts/{jid}` (push name, phone, picture, on WhatsApp or not), group subject and members on `GET /chats/{chatId}`.
- CLI: `forward`, `contact`, `chats`, `messages`, `search`, `scheduled`, `unschedule`, `history enable`, `history purge`, `send --at --when-online`.
- Tests: the store on a real in-memory SQLite (paging, search, tombstones, purge), the LRU cache, the scheduler under the test clock, and the new endpoints. 87 in total.

## v0.4.0 — 2026-09-10

How the line feels: the first of three pushes from the design session recorded in `docs/spec/agent-surface.md`.

- The eye moves. One message per chat carries the 👀: the previous one loses it before the new one gets it, and it comes off when the turn completes. Every accepted message is also marked read (`BRIDGE_READ_RECEIPTS`, default on).
- Failure text. When a turn fails, times out or is abandoned, the eye comes off and the bridge replies to the newest message, quoting it, with `BRIDGE_FAILURE_TEXT` (default "Something went wrong."; empty disables).
- `POST /turns/{id}/progress {text?, extendBy?}` keeps typing alive, moves the reply deadline (never past one hour after opening) and can send an interim bubble; `replaceProgress: true` on the reply edits that bubble into the answer. `POST /turns/{id}/abandon {reason, text?}` fails a turn deliberately, aborting a running `http` handler call.
- Typing is re-sent every 8 seconds while a turn is open, for at most `BRIDGE_TYPING_MAX` (3 minutes), re-armed by progress.
- Inbound edits and deletes: `MessageEdited` and `MessageDeleted` events; a pending message is corrected or dropped in place while batching, an edit supersedes an open turn with the corrected text, and a delete that empties a turn ends it in the new state `cancelled` with a `TurnCancelled` event.
- Own edits and deletes: `PATCH` and `DELETE /chats/{id}/messages/{messageId}` for messages the bridge sent this run, refused with `409 MessageTooOld` past WhatsApp's windows (15 minutes, about 2 days) and `404 MessageUnknown` otherwise.
- Quoting: `quoteMessageId` on text and media sends and on turn replies (top level for the first bubble, `{text, quoteMessageId}` entries in `replies`, per attachment). Unknown ids fail a proactive send with `404 MessageUnknown`; on a turn reply the bubble goes out unquoted with a warning.
- `ReactionReceived` events for reactions in handled chats, the bridge's own excluded.
- CLI: `edit`, `delete`, `progress`, `abandon`; `--quote` on `send` and `reply`, `--replace-progress` on `reply`.
- Event schema stays v1; the new events and fields are additive.

## v0.3.0 — 2026-09-09

Intro can send images and files.

- `POST /chats/{chatId}/media` sends an image, video, audio clip (or voice note) or document to a chat, from a local `path` or base64 `data`; `kind`, `mimetype` and `fileName` are inferred from the file and can be overridden. `POST /turns/{turnId}/reply` and the `http` responder's `HandlerResponse` take the same objects as `attachments`, sent after the text bubbles; `sentMessageIds` covers them.
- Files are read and size-checked (`BRIDGE_MEDIA_MAX_BYTES`, now both directions) before anything is sent: `404 OutboundFileUnreadable`, `400 OutboundInvalid`, `413 OutboundTooLarge`, and a reply with a bad attachment leaves the turn `awaiting`. Uploads get a 3 minute cap instead of the 30 second one for texts; a failed upload in a turn reply is retried like a text.
- CLI: `send --file <path> [--caption] [--as image|video|audio|document]`, `reply --file`.
- Tests cover the loader, the endpoint, replies with attachments, the ordering of mixed replies in the engine, and (for the first time) the `http` responder through a fake HTTP client.

## v0.2.0 — 2026-09-09

Attachments reach Intro as local files.

- Images and stickers are downloaded automatically when a message arrives, and `TurnOpened` carries `messages[].media` = `{ path, mimetype, size, kind, downloadedAt }` once the download finished within `BRIDGE_MEDIA_WAIT` (default 8 s). The turn opens without it otherwise; the file is still fetched in the background.
- `GET /chats/{chatId}/messages/{messageId}/media` downloads on demand (any kind: video, audio, document too) and returns the same record; `.../media/content` streams the bytes for a remote consumer. `bridge media --chat --message` on the CLI.
- Files live under `<data dir>/media/<chat>/<messageId>.<ext>` with a `.json` sidecar, 0600 in 0700 directories, served across restarts from the sidecar, deleted after `BRIDGE_MEDIA_RETENTION` (default 7 days) by an hourly sweep. `BRIDGE_MEDIA_MAX_BYTES` (default 25 MB) refuses larger attachments before downloading. Expired WhatsApp media URLs are refreshed through a re-upload request.
- Event schema stays v1: the new `media` field is optional.
- `.env.example` values are quoted so the file is safe to `source` from shell scripts.
- Requests that fail validation answer `400 {"_tag":"InvalidRequest","kind","message"}` and unknown routes `404 {"_tag":"RouteNotFound"}` instead of empty bodies.
- `GET /events?since=` rejects anything but a non-negative integer with that 400; before, a bad cursor decoded to NaN and the stream silently delivered nothing.
- README: a complete reference of the tool surface for the agent (every endpoint with exact shapes, the event and object schemas, the error catalogue, the CLI, the responder contracts, and playbooks).

## v0.1.0 — 2026-09-09

First release: the WhatsApp ↔ Intro bridge as a local daemon on a dedicated number.

- Link a dedicated number by QR or pairing code; the session persists on disk (mode 0700) and survives restarts.
- Inbound DMs (groups opt-in) become versioned `MessageReceived` events; echoes, status and protocol noise are filtered.
- Every accepted message gets a 👀 reaction within seconds; it is removed once the reply for its turn is sent.
- Messages from one chat within `BRIDGE_BATCH_WINDOW` form one turn; newer messages interrupt an in-flight turn (`TurnSuperseded`) and a new turn carries everything.
- Three responders: `manual` (Intro answers via `POST /turns/{id}/reply`), `http` (the bridge POSTs turns to `INTRO_HANDLER_URL`), `pong` (the harness).
- Local HTTP API (`/health`, `/status`, `/events` SSE with `?since=` replay, `/chats/...` send/react/unreact/typing, `/turns` list/get/reply) with OpenAPI at `/openapi.json` and docs at `/docs`.
- `needs_relink` detection on logout/ban with the stale session moved aside; automatic reconnect with jittered backoff otherwise.
- CLI: `link` (with `--timeout`), `run`, `harness`, `health`, `send`, `react`, `unreact`, `turns`, `reply`, `unlink`.
- Event schema version 1, with `Ready` (cursor) and `Heartbeat` markers on the SSE stream.
- Robustness: a per-session lock file so two daemons cannot fight over one number; the socket supervisor survives crashes and caps "restart required" storms; the WhatsApp library's own warnings and errors are forwarded, redacted, into the bridge log; an outbox answers WhatsApp's resend requests; a chat worker survives a crashing turn; `BRIDGE_BATCH_MAX_WAIT` bounds batching under a steady trickle; process-level guards log stray promise rejections instead of dying; slow SSE clients drop old events instead of growing memory; constant-time token checks; graceful shutdown with a deadline; every typed error carries a readable message and configuration errors name the variable; outbound message ids are fixed before retries so a timed-out send can never double-deliver; replies longer than 4000 characters are split at paragraph or sentence boundaries and blank replies are dropped.
