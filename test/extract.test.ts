import { describe, expect, it } from "vitest"
import { DateTime, Option } from "effect"
import type { WAMessage } from "@whiskeysockets/baileys"
import { extractInbound, phoneOfVcard } from "../src/whatsapp/extract.ts"

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

  it("carries who a message mentions", () => {
    const result = extractInbound(
      dm({
        key: { remoteJid: "120363000000000000@g.us", fromMe: false, id: "G2", participant: "447700900999@s.whatsapp.net" },
        message: { extendedTextMessage: { text: "@bridge can you?", contextInfo: { mentionedJid: ["bridge@s.whatsapp.net"] } } },
      }),
      now,
    )
    expect(Option.isSome(result)).toBe(true)
    if (Option.isNone(result)) return
    expect(result.value.message.mentionedIds).toEqual(["bridge@s.whatsapp.net"])
  })

  it("decodes a pin and a live location", () => {
    const pin = extractInbound(
      dm({ message: { locationMessage: { degreesLatitude: 51.5, degreesLongitude: -0.12, name: "Office", address: "1 Main St", url: "https://maps.example/x" } } }),
      now,
    )
    expect(Option.isSome(pin)).toBe(true)
    if (Option.isNone(pin)) return
    expect(pin.value.message.kind).toBe("location")
    expect(pin.value.message.location).toEqual({ latitude: 51.5, longitude: -0.12, name: "Office", address: "1 Main St", url: "https://maps.example/x" })

    const live = extractInbound(dm({ message: { liveLocationMessage: { degreesLatitude: 1, degreesLongitude: 2 } } }), now)
    expect(Option.isSome(live)).toBe(true)
    if (Option.isNone(live)) return
    expect(live.value.message.location).toEqual({ latitude: 1, longitude: 2, live: true })
  })

  it("decodes shared contacts from their vCards", () => {
    const one = extractInbound(
      dm({ message: { contactMessage: { displayName: "Dana", vcard: "BEGIN:VCARD\nFN:Dana\nTEL;type=CELL;waid=447700900555:+44 7700 900555\nEND:VCARD" } } }),
      now,
    )
    expect(Option.isSome(one)).toBe(true)
    if (Option.isNone(one)) return
    expect(one.value.message.kind).toBe("contact")
    expect(one.value.message.contacts).toMatchObject([{ name: "Dana", phone: "447700900555" }])

    const many = extractInbound(
      dm({
        message: {
          contactsArrayMessage: {
            contacts: [
              { vcard: "BEGIN:VCARD\nFN:Ari\nTEL:+1 (555) 010-0001\nEND:VCARD" },
              { displayName: "No Number", vcard: "BEGIN:VCARD\nFN:No Number\nEND:VCARD" },
            ],
          },
        },
      }),
      now,
    )
    expect(Option.isSome(many)).toBe(true)
    if (Option.isNone(many)) return
    expect(many.value.message.contacts?.map((c) => [c.name, c.phone])).toEqual([
      ["Ari", "15550100001"],
      ["No Number", undefined],
    ])
  })
})

describe("phoneOfVcard", () => {
  it("prefers waid, falls back to TEL digits, and gives up cleanly", () => {
    expect(phoneOfVcard("TEL;waid=447700900555:+44 7700 900555")).toBe("447700900555")
    expect(phoneOfVcard("TEL;type=HOME:+44 (0)7700-900555")).toBe("4407700900555")
    expect(phoneOfVcard("FN:Nobody")).toBeUndefined()
  })
})
