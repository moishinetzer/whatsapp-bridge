import { getContentType, isJidBroadcast, isJidGroup, isJidNewsletter, normalizeMessageContent, type WAMessage, type WAMessageKey } from "@whiskeysockets/baileys"
import { DateTime, Option } from "effect"
import { ChatId, MessageId, phoneOfJid } from "../domain/ids.ts"
import { InboundMessage, type MessageKind } from "../domain/message.ts"

export interface Extracted {
  readonly message: InboundMessage
  /** The full key WhatsApp needs to address a reaction at this message. */
  readonly key: WAMessageKey
}

const kindOf = (contentType: string | undefined): MessageKind | undefined => {
  switch (contentType) {
    case "conversation":
    case "extendedTextMessage":
      return "text"
    case "imageMessage":
      return "image"
    case "videoMessage":
    case "ptvMessage":
      return "video"
    case "audioMessage":
      return "audio"
    case "documentMessage":
      return "document"
    case "stickerMessage":
    case "lottieStickerMessage":
      return "sticker"
    case "locationMessage":
    case "liveLocationMessage":
      return "location"
    case "contactMessage":
    case "contactsArrayMessage":
      return "contact"
    case undefined:
    // protocol / bookkeeping payloads are not user messages
    case "protocolMessage":
    case "reactionMessage":
    case "senderKeyDistributionMessage":
    case "pollUpdateMessage":
    case "keepInChatMessage":
    case "messageContextInfo":
      return undefined
    default:
      return "other"
  }
}

const toMillis = (ts: WAMessage["messageTimestamp"]): number | undefined => {
  if (ts === null || ts === undefined) return undefined
  if (typeof ts === "number") return ts * 1000
  if (typeof ts === "object" && "toNumber" in ts && typeof ts.toNumber === "function") return ts.toNumber() * 1000
  return undefined
}

/**
 * Turns a raw WhatsApp message into the bridge's `InboundMessage`, or `None`
 * when it is not something Intro should ever see: our own sends (echoes),
 * status/broadcast/newsletter traffic, protocol bookkeeping, and messages
 * without content.
 */
export const extractInbound = (msg: WAMessage, now: DateTime.Utc): Option.Option<Extracted> => {
  const key = msg.key
  const remoteJid = key.remoteJid ?? undefined
  const id = key.id ?? undefined
  if (!remoteJid || !id) return Option.none()
  if (key.fromMe) return Option.none()
  if (isJidBroadcast(remoteJid) || isJidNewsletter(remoteJid) || remoteJid === "status@broadcast") return Option.none()

  const content = normalizeMessageContent(msg.message)
  if (!content) return Option.none()
  const contentType = getContentType(content)
  const kind = kindOf(contentType)
  if (kind === undefined) return Option.none()

  const text =
    content.conversation ??
    content.extendedTextMessage?.text ??
    content.imageMessage?.caption ??
    content.videoMessage?.caption ??
    content.documentMessage?.caption ??
    undefined

  const mimetype =
    content.imageMessage?.mimetype ??
    content.videoMessage?.mimetype ??
    content.audioMessage?.mimetype ??
    content.documentMessage?.mimetype ??
    content.stickerMessage?.mimetype ??
    undefined

  const hasMedia = kind === "image" || kind === "video" || kind === "audio" || kind === "document" || kind === "sticker"

  const isGroup = isJidGroup(remoteJid) === true
  const participant = key.participant ?? undefined
  const senderId = isGroup ? (participant ?? remoteJid) : remoteJid
  const senderNumber = isGroup
    ? (phoneOfJid(participant) ?? phoneOfJid(key.participantAlt))
    : (phoneOfJid(remoteJid) ?? phoneOfJid(key.remoteJidAlt))

  const quoted = content.extendedTextMessage?.contextInfo?.stanzaId ?? undefined
  const millis = toMillis(msg.messageTimestamp)
  const timestamp = millis === undefined ? now : DateTime.makeUnsafe(millis)

  const message = InboundMessage.make({
    chatId: ChatId.make(remoteJid),
    messageId: MessageId.make(id),
    senderId,
    ...(senderNumber === undefined ? {} : { senderNumber }),
    ...(msg.pushName ? { senderName: msg.pushName } : {}),
    isGroup,
    timestamp,
    kind,
    ...(text === undefined || text === null || text.length === 0 ? {} : { text }),
    hasMedia,
    ...(mimetype ? { mimetype } : {}),
    ...(quoted ? { quotedMessageId: MessageId.make(quoted) } : {}),
  })

  return Option.some({ message, key })
}
