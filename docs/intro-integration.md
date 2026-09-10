# How Intro should use this line

The bridge moves messages; Intro decides what to say. These are the product rules Intro follows on the dedicated number, and the mechanics the bridge gives it for each.

## Persona

Intro answers as **Intro, Moishi's assistant**, on this number. Warm, short, WhatsApp tone, not email. It never impersonates Moishi to third parties. When someone other than Moishi messages the line, Intro helps them as the assistant on Moishi's line (until `BRIDGE_ALLOWED_SENDERS` narrows who may talk to it).

## The loop

1. Subscribe to `GET /events`. Remember the `seq` of every event; on reconnect pass `?since=<seq>`.
2. On `TurnOpened`, read all of `messages` (they are one burst from one chat, oldest first) and generate one reply for the whole burst.
3. `POST /turns/{turnId}/reply` with the text. The 👀 on those messages disappears once it is sent.
4. On `TurnSuperseded`, stop working on that turn. The next `TurnOpened` for the chat contains the old messages plus the new ones (or the corrected text, after an edit); answer that instead. A late reply to the old turn gets `409 TurnSuperseded` and is not sent.
5. Taking longer than a minute? `POST /turns/{turnId}/progress {"text": "Checking…"}` keeps typing alive, extends the deadline, and puts a bubble in the chat that the reply can replace with `replaceProgress: true`. Cannot do it at all? `POST /turns/{turnId}/abandon {"reason", "text"}` fails the turn and tells the user in your words.
6. On `TurnFailed`, the eye has already come off and the failure text has gone out; nothing to repair. On `TurnCancelled`, the user deleted the messages; do nothing.
7. `ReactionReceived` on one of Intro's messages is feedback; `MessageEdited` after an answer went out is a cue to answer again if the change matters.
8. A burst is one burst: while Moishi is still typing, the bridge holds the turn open a few seconds longer, so "and also…" arrives in the same `TurnOpened` rather than superseding the answer halfway.

`GET /turns` lists what is still open, for catching up after a restart.

## No spend, no irreversible action from WhatsApp alone

- Payments, Stripe Link spend approvals, connector authorisations, and anything that only exists as an in-app card **cannot** be done from WhatsApp. Intro tells Moishi so and names the action: *"This needs the app: open Grok Bot → approve the Link spend for X."* It sends that with `POST /chats/{chatId}/messages` (or as the turn's reply) and never claims the action happened.
- Destructive or irreversible actions need a clear confirmation. A WhatsApp yes/no is fine when the question is unambiguous; otherwise ask in the app.

## Double answering

Moishi may be talking to Intro in the app and on WhatsApp at the same time. Every WhatsApp message carries `messageId` and `chatId`, and every turn carries `turnId`; Intro should key its own dedupe on these so the same ask is not answered on both surfaces. Best effort: the bridge cannot see the app.

## Media

Photos and stickers arrive with `media.path` on the `TurnOpened` message: a local file Intro can open (read it as bytes, or pass the path to a vision-capable model). Videos, voice notes and documents are fetched on demand with `GET /chats/{chatId}/messages/{messageId}/media`, which also covers an image whose download was still running when the turn opened. Downloaded files live under a disk budget (1 GB, least recently used go first); copy anything worth keeping. When a download fails, say so in the reply rather than pretending the attachment was seen.

Sending works the same way in reverse. Write the file on the bridge machine and hand over its path: as part of the turn's answer (`POST /turns/{turnId}/reply` with `"attachments": [{"path": "/tmp/q3.png", "caption": "..."}]`, sent after the text) or on its own (`POST /chats/{chatId}/media`). Images and videos arrive as previews with the caption under them; a PDF, a spreadsheet, or a picture that must stay uncompressed goes as `"kind": "document"`. Several photos together: `"album": true` on the reply, or `POST /chats/{chatId}/album`, and Moishi sees one grid. The bridge refuses a missing path or a file over the limit before sending anything, so a 4xx there means nothing reached the user.

Beyond files: a location pin (`POST /chats/{chatId}/location`) when the answer is a place, a contact card (`.../contacts`) when it is a person, a poll (`.../polls`) when it is a choice; votes come back as `PollVote` events. Inbound, a shared location or contact arrives decoded on the message (`location`, `contacts`).

## Groups

With `BRIDGE_GROUPS=true` Intro sits in group chats too, and stays quiet until someone @-mentions the line or quotes one of its messages. That `TurnOpened` carries `context`, the last ten messages in the group before the mention, so Intro can answer the actual thread rather than the one line. Reply as the assistant, quote the message that asked (`quoteMessageId`), and @-mention the person you are answering (`mentions`) when several people are talking. A mention that needs no answer (someone saying "ask Intro later") gets `{}`. Intro never speaks in a group unprompted unless Moishi asked it to, in a DM, with a clear message to deliver.

## Memory

With history on (`bridge history enable`, then `BRIDGE_HISTORY=true`), the bridge keeps every message. Before answering something that depends on context, `GET /chats/{chatId}/messages?limit=30`; to find a thing said weeks ago, `GET /messages?q=...`. Everything ever seen can be quoted or forwarded. The SQLite file is on the same machine (`GET /status` names it) and has stable views for `sqlite3` when the API is not enough. Reminders are `sendAt` on a normal send: the bridge holds them, across restarts, and reports `ScheduledSent` or `ScheduledSkipped`.

## Being alive

Moishi's signal that Intro saw a message is the blue ticks plus the 👀 on the newest unanswered message; the bridge applies both, Intro does not need to. For a nudge that should land while Moishi is around, `GET /chats/{chatId}/presence` once and watch `PresenceChanged`, or schedule the send with `deliverWhenOnline`. Correcting an earlier answer is `PATCH /chats/{chatId}/messages/{messageId}` within 15 minutes; quoting the message being answered (`quoteMessageId`) is worth it when several questions are in flight. Intro's signal that the *line* is alive is `GET /health` (`ok` versus `needs_relink`). When it reports `needs_relink`, Intro cannot fix that from code: it tells Moishi (through whatever other channel it has) to run `npm run link` on the bridge machine.

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
