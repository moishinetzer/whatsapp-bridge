# Changelog

All notable changes to this project are documented here. Intro pins on a tag.

## v0.2.0 — 2026-09-09

Attachments reach Intro as local files.

- Images and stickers are downloaded automatically when a message arrives, and `TurnOpened` carries `messages[].media` = `{ path, mimetype, size, kind, downloadedAt }` once the download finished within `BRIDGE_MEDIA_WAIT` (default 8 s). The turn opens without it otherwise; the file is still fetched in the background.
- `GET /chats/{chatId}/messages/{messageId}/media` downloads on demand (any kind: video, audio, document too) and returns the same record; `.../media/content` streams the bytes for a remote consumer. `bridge media --chat --message` on the CLI.
- Files live under `<data dir>/media/<chat>/<messageId>.<ext>` with a `.json` sidecar, 0600 in 0700 directories, served across restarts from the sidecar, deleted after `BRIDGE_MEDIA_RETENTION` (default 7 days) by an hourly sweep. `BRIDGE_MEDIA_MAX_BYTES` (default 25 MB) refuses larger attachments before downloading. Expired WhatsApp media URLs are refreshed through a re-upload request.
- Event schema stays v1: the new `media` field is optional.
- `.env.example` values are quoted so the file is safe to `source` from shell scripts.

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
