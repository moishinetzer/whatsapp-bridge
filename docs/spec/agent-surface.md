# Agent surface, versions 0.4 to 0.6

The decisions behind the next three releases, settled in one design session on 2026-09-10. Each numbered item is a decision, not a suggestion; the code follows it. Shapes (endpoint paths, field names) are listed where they were fixed; anything not listed is an implementation choice.

## Push one, version 0.4: how the line feels

1. **The eye moves.** Every accepted message gets the seen reaction at once. One message per chat carries it: when a new message is accepted, the reaction is removed from the previous message first, then added to the new one. When a turn completes, the reaction comes off its last message. A message that arrived while the reply was being sent keeps its eye and starts the next turn.
2. **Read receipts.** Every accepted message is marked read on acceptance, in DMs and groups alike, unless `BRIDGE_READ_RECEIPTS=false`. The eye goes only on messages that open a turn. Blue ticks show only if the phone's privacy setting allows them.
3. **Failure text.** When a turn fails or is abandoned, the eye comes off and the bridge replies to the turn's newest message, quoting it, with `BRIDGE_FAILURE_TEXT`. Default "Something went wrong."; an empty value disables it. `POST /turns/{id}/abandon {reason, text?}` fails the turn deliberately; `text` overrides the default for that turn.
4. **Inbound edits and deletes.** `MessageEdited {chatId, messageId, text}` and `MessageDeleted {chatId, messageId}` events. While the turn is still batching, the edit is applied or the message dropped in place. Once the responder is working or the turn is awaiting a reply, an edit supersedes the turn with the corrected text, and a delete that empties the turn ends it in the terminal state `cancelled` with a `TurnCancelled` event and no failure text.
5. **Own edits and deletes.** `PATCH /chats/{id}/messages/{messageId} {text}` and `DELETE /chats/{id}/messages/{messageId}` for the bot's own messages. Refused with `MessageTooOld` past WhatsApp's windows (15 minutes to edit, 2 days to delete for everyone) and with `MessageUnknown` for messages the bridge did not send this run.
6. **Progress.** `POST /turns/{id}/progress {text?, extendBy?}` re-arms the typing indicator, extends the reply deadline (never past one hour after the turn opened), and sends `text` as an interim bubble when given. A reply with `replaceProgress: true` edits the last interim bubble into its first text bubble; otherwise the interim stays.
7. **Typing keep-alive.** While a turn is open the bridge re-sends the typing presence every 8 seconds, for at most `BRIDGE_TYPING_MAX` (3 minutes), re-armed by each progress call.
8. **Quoting.** `quoteMessageId` on `POST /chats/{id}/messages`, on media sends, and on turn replies. On a reply it applies to the first bubble; entries in `replies` may be strings or `{text, quoteMessageId}`; attachments carry their own `quoteMessageId`. Quotable: any message the bridge saw or sent this run, and with history on, anything in the database.
9. **Reactions as events.** `ReactionReceived {chatId, messageId, senderId, emoji, onOurMessage}` for reactions in handled chats; an empty `emoji` means the reaction was removed. A reaction never opens a turn.
10. **Event schema stays v1.** All changes are additive. Consumers ignore tags they do not know.

## Push two, version 0.5: memory

11. **One database.** `data/bridge.db` (SQLite through Effect's `SqlClient` and `SqliteMigrator` over Node's built-in `node:sqlite`, WAL mode) is always created for bridge state: scheduled sends, the sent-message index, turn outcomes, the media cache index. Message content is written only when history is enabled.
12. **History opt-in.** `BRIDGE_HISTORY=true` plus a one-time `bridge history enable`, which prints exactly what will be stored and asks for confirmation, writing `data/history.accepted` (`--yes` for scripts). With the flag set and no acceptance the daemon refuses to start and says why.
13. **What history stores.** Both directions: the decoded record, the raw inbound message JSON (so quoting, forwarding and poll votes work across restarts), reactions, edits, deletes, receipts, and the turn an outbound message answered. No retention. `bridge history purge --before <date> --yes` deletes.
14. **User deletes are tombstones.** A "delete for everyone" keeps the row's id and timestamps, removes the text and media path, and sets `deletedAt`.
15. **Media cache.** Downloaded files live under a disk budget, `BRIDGE_MEDIA_MAX_DISK` (1 GB), evicted least recently used first. A use is anything the bridge can see: the download, delivery on a turn, a fetch through the media endpoints, or the file appearing in a history or search result. The index lives in `bridge.db`; a history row gets `mediaEvictedAt` when its file goes. `BRIDGE_MEDIA_RETENTION` becomes an optional maximum age, unset by default.
16. **History API.** `GET /chats` (chatId, name, isGroup, lastMessageAt, messageCount). `GET /chats/{id}/messages?before=<messageId>&limit=50` (max 500), newest first, both directions, each row with direction, status (sent, delivered, read), editedText, deletedAt, reactions, turnId; `since=<timestamp>` for "what happened since".
17. **Search.** `GET /chats/{id}/messages?q=` and `GET /messages?q=` across chats, SQLite FTS5 with prefix and phrase matching, newest first, `limit` and `before`, each hit with a snippet. Text, captions, edited text and media file names are indexed.
18. **Direct access.** The schema is documented and stable (views survive migrations); `GET /status` reports the database path so the agent can open it read-only with `sqlite3`.
19. **Scheduled sends.** `sendAt` on the text and media endpoints answers `202 {scheduledId, sendAt}`; `GET /scheduled`, `DELETE /scheduled/{id}`. `deliverWhenOnline: true` holds until presence says online, at most 24 hours. Missed while the daemon was down: sent on startup if under one hour late, otherwise dropped with a `ScheduledSkipped` event.
20. **Delivery and read state.** `MessageDelivered` and `MessageRead` events for the bot's own messages; in groups one event per participant with `participantId`.
21. **Forwarding.** `POST /chats/{id}/forward {fromChatId, messageId}`.
22. **Lookups.** `GET /contacts/{jid}` (push name, phone, profile picture URL, on WhatsApp or not) and `GET /chats/{groupId}` (subject, participants, admins).

## Push three, version 0.6: the long tail

23. **Groups.** `BRIDGE_GROUPS=true` with one trigger: a turn opens only when the bot is mentioned or one of its messages is quoted. The agent may still answer `{}` to skip. Other group messages are stored when history is on, emit no event and open no turn. Every group `TurnOpened` carries `context`: the last 10 messages in that group before the trigger, from an in-memory ring, so this works with history off. `mentionsMe` and `mentionedIds` on inbound messages; `mentions` on outbound text.
24. **Presence.** The batch window keeps extending while the user is typing, up to `BRIDGE_BATCH_TYPING_WAIT` (15 seconds), a separate cap from `BRIDGE_BATCH_MAX_WAIT`. `GET /chats/{id}/presence` subscribes and returns the last known state; `PresenceChanged` events flow only for chats the agent asked about. The batching logic subscribes on its own and never emits.
25. **Locations and contacts.** Coordinates and vCard details on inbound messages; `POST /chats/{id}/location` and `POST /chats/{id}/contacts` to send them.
26. **Polls.** `POST /chats/{id}/polls {question, options, multiple}` and a `PollVote` event. Votes on a poll sent before a restart decrypt only with history on; documented.
27. **Albums.** Adjacent images and videos in a reply's attachments become one album when the reply says `album: true`; documents never group. `POST /chats/{id}/album` for proactive sends.

## Out of scope

Link previews (the bridge would have to fetch arbitrary URLs), webhooks (SSE is the only consumer; an `http` handler sees turns only), call handling, rate limits on proactive sends, stats, event filtering, voice-note transcoding. Any of these can be reopened later; none is assumed by the decisions above.
