import { Schema } from "effect"
import { randomUUID } from "node:crypto"

/**
 * A WhatsApp chat jid, e.g. `447700900123@s.whatsapp.net` for a DM or
 * `1234567890-1234567890@g.us` for a group. Newer WhatsApp builds may hand out
 * `...@lid` ids; they are still the id you must send to.
 */
export const ChatId = Schema.String.pipe(Schema.check(Schema.isNonEmpty()), Schema.brand("ChatId"))
export type ChatId = typeof ChatId.Type

/** A WhatsApp message id as WhatsApp assigns it. Stable across devices. */
export const MessageId = Schema.String.pipe(Schema.check(Schema.isNonEmpty()), Schema.brand("MessageId"))
export type MessageId = typeof MessageId.Type

/** A bridge-assigned id for one unit of work (a batch of messages waiting for one reply). */
export const TurnId = Schema.String.pipe(Schema.check(Schema.isNonEmpty()), Schema.brand("TurnId"))
export type TurnId = typeof TurnId.Type

export const newTurnId = (): TurnId => TurnId.make(`turn_${randomUUID().replace(/-/g, "").slice(0, 20)}`)

/**
 * Accepts a jid or a phone number in any human format ("+44 7700 900123") and
 * returns the jid WhatsApp expects. Jids are passed through untouched.
 */
export const toChatId = (input: string): ChatId => {
  const trimmed = input.trim()
  if (trimmed.includes("@")) return ChatId.make(trimmed)
  return ChatId.make(`${trimmed.replace(/[^0-9]/g, "")}@s.whatsapp.net`)
}

/** The digits of a phone-number jid, or `undefined` for group / lid ids. */
export const phoneOfJid = (jid: string | undefined | null): string | undefined => {
  if (!jid) return undefined
  const [user, server] = jid.split("@")
  if (server !== "s.whatsapp.net" || !user) return undefined
  return user.split(":")[0]
}
