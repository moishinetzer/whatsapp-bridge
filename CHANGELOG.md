# Changelog

All notable changes to this project are documented here. Intro pins on a tag.

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
