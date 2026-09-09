import { describe, expect, it } from "vitest"
import { DateTime, Option } from "effect"
import type { WAMessage } from "@whiskeysockets/baileys"
import { extractInbound } from "../src/whatsapp/extract.ts"

const now = DateTime.makeUnsafe(1_700_000_000_000)

const dm = (overrides: Partial<WAMessage> = {}): WAMessage => ({
  key: { remoteJid: "447700900123@s.whatsapp.net", fromMe: false, id: "ABC123" },
  message: { conversation: "hello" },
  messageTimestamp: 1_700_000_100,
  pushName: "Moishi",
  ...overrides,
})

describe("extractInbound", () => {
  it("decodes a plain text DM", () => {
    const result = extractInbound(dm(), now)
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    const { message, key } = result.value
    expect(message.chatId).toBe("447700900123@s.whatsapp.net")
    expect(message.messageId).toBe("ABC123")
    expect(message.senderId).toBe("447700900123@s.whatsapp.net")
    expect(message.senderNumber).toBe("447700900123")
    expect(message.senderName).toBe("Moishi")
    expect(message.isGroup).toBe(false)
    expect(message.kind).toBe("text")
    expect(message.text).toBe("hello")
    expect(message.hasMedia).toBe(false)
    expect(DateTime.toEpochMillis(message.timestamp)).toBe(1_700_000_100_000)
    expect(key.id).toBe("ABC123")
  })

  it("decodes extended text with a quoted message", () => {
    const result = extractInbound(
      dm({ message: { extendedTextMessage: { text: "replying", contextInfo: { stanzaId: "QUOTED1" } } } }),
      now,
    )
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(result.value.message.text).toBe("replying")
    expect(result.value.message.quotedMessageId).toBe("QUOTED1")
  })

  it("unwraps ephemeral wrappers", () => {
    const result = extractInbound(dm({ message: { ephemeralMessage: { message: { conversation: "secret" } } } }), now)
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(result.value.message.text).toBe("secret")
  })

  it("reports media with its caption and mimetype", () => {
    const result = extractInbound(dm({ message: { imageMessage: { caption: "look", mimetype: "image/jpeg" } } }), now)
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(result.value.message.kind).toBe("image")
    expect(result.value.message.hasMedia).toBe(true)
    expect(result.value.message.text).toBe("look")
    expect(result.value.message.mimetype).toBe("image/jpeg")
  })

  it("identifies group messages and their participant", () => {
    const result = extractInbound(
      dm({
        key: { remoteJid: "120363000000000000@g.us", fromMe: false, id: "G1", participant: "447700900999@s.whatsapp.net" },
      }),
      now,
    )
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(result.value.message.isGroup).toBe(true)
    expect(result.value.message.senderId).toBe("447700900999@s.whatsapp.net")
    expect(result.value.message.senderNumber).toBe("447700900999")
  })

  it("ignores our own messages (no reply loops)", () => {
    expect(Option.isNone(extractInbound(dm({ key: { remoteJid: "447700900123@s.whatsapp.net", fromMe: true, id: "MINE" } }), now))).toBe(true)
  })

  it("ignores status broadcasts, protocol messages and reactions", () => {
    expect(Option.isNone(extractInbound(dm({ key: { remoteJid: "status@broadcast", fromMe: false, id: "S" } }), now))).toBe(true)
    expect(Option.isNone(extractInbound(dm({ message: { protocolMessage: { type: 0 } } }), now))).toBe(true)
    expect(Option.isNone(extractInbound(dm({ message: { reactionMessage: { text: "👍", key: {} } } }), now))).toBe(true)
    expect(Option.isNone(extractInbound(dm({ message: null }), now))).toBe(true)
  })

  it("falls back to now when WhatsApp gives no timestamp", () => {
    const result = extractInbound(dm({ messageTimestamp: null }), now)
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(DateTime.toEpochMillis(result.value.message.timestamp)).toBe(DateTime.toEpochMillis(now))
  })
})
