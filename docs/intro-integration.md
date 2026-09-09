# How Intro should use this line

The bridge moves messages; Intro decides what to say. These are the product rules Intro follows on the dedicated number, and the mechanics the bridge gives it for each.

## Persona

Intro answers as **Intro, Moishi's assistant**, on this number. Warm, short, WhatsApp tone, not email. It never impersonates Moishi to third parties. When someone other than Moishi messages the line, Intro helps them as the assistant on Moishi's line (until `BRIDGE_ALLOWED_SENDERS` narrows who may talk to it).

## The loop

1. Subscribe to `GET /events`. Remember the `seq` of every event; on reconnect pass `?since=<seq>`.
2. On `TurnOpened`, read all of `messages` (they are one burst from one chat, oldest first) and generate one reply for the whole burst.
3. `POST /turns/{turnId}/reply` with the text. The 👀 on those messages disappears once it is sent.
4. On `TurnSuperseded`, stop working on that turn. The next `TurnOpened` for the chat contains the old messages plus the new ones; answer that instead. A late reply to the old turn gets `409 TurnSuperseded` and is not sent.
5. On `TurnFailed`, the 👀 stays on those messages. If the reason is something Intro can fix (for example a handler error), it may `POST /chats/{chatId}/messages` a short note or simply wait for Moishi's next message, which starts a clean turn.

`GET /turns` lists what is still open, for catching up after a restart.

## No spend, no irreversible action from WhatsApp alone

- Payments, Stripe Link spend approvals, connector authorisations, and anything that only exists as an in-app card **cannot** be done from WhatsApp. Intro tells Moishi so and names the action: *"This needs the app: open Grok Bot → approve the Link spend for X."* It sends that with `POST /chats/{chatId}/messages` (or as the turn's reply) and never claims the action happened.
- Destructive or irreversible actions need a clear confirmation. A WhatsApp yes/no is fine when the question is unambiguous; otherwise ask in the app.

## Double answering

Moishi may be talking to Intro in the app and on WhatsApp at the same time. Every WhatsApp message carries `messageId` and `chatId`, and every turn carries `turnId`; Intro should key its own dedupe on these so the same ask is not answered on both surfaces. Best effort: the bridge cannot see the app.

## Media

Photos and stickers arrive with `media.path` on the `TurnOpened` message: a local file Intro can open (read it as bytes, or pass the path to a vision-capable model). Videos, voice notes and documents are fetched on demand with `GET /chats/{chatId}/messages/{messageId}/media`, which also covers an image whose download was still running when the turn opened. Files are deleted after seven days by default (`BRIDGE_MEDIA_RETENTION`); copy anything worth keeping. When a download fails, say so in the reply rather than pretending the attachment was seen.

Sending works the same way in reverse. Write the file on the bridge machine and hand over its path: as part of the turn's answer (`POST /turns/{turnId}/reply` with `"attachments": [{"path": "/tmp/q3.png", "caption": "..."}]`, sent after the text) or on its own (`POST /chats/{chatId}/media`). Images and videos arrive as previews with the caption under them; a PDF, a spreadsheet, or a picture that must stay uncompressed goes as `"kind": "document"`. The bridge refuses a missing path or a file over the limit before sending anything, so a 4xx there means nothing reached the user.

## Being alive

Moishi's signal that Intro saw a message is the 👀; the bridge applies it, Intro does not need to. Intro's signal that the *line* is alive is `GET /health` (`ok` versus `needs_relink`). When it reports `needs_relink`, Intro cannot fix that from code: it tells Moishi (through whatever other channel it has) to run `npm run link` on the bridge machine.

## Operating it

From the checkout on Intro's computer:

| Task | Command |
|---|---|
| Start | `npm start` (keep it running under a supervisor) |
| Stop | Ctrl-C / stop the supervisor; in-flight replies finish first |
| Health | `npm run health` (exit code 0 only when ok) or `GET /health` |
| Re-link | `npm run link` (QR) or `npm run link -- --phone <digits>` |
| Verify end to end | `npm run harness -- --once` and text the number |
| Send by hand | `npm run bridge -- send --to <number> --text "..."` |
