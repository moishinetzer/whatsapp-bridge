import { Effect } from "effect"

/**
 * The access rule, as a pure decision over probes the transport supplies.
 *
 * Without an owner: the old allowlist, or everyone when it is empty.
 * With an owner: a DM is allowed when it is the owner's or the sender is
 * listed; a group when it is listed as a group or the owner is a member.
 * A listed person unlocks only their DM, never a group.
 */
export interface AccessRule {
  readonly ownerDigits: string | undefined
  readonly allowedSenders: ReadonlyArray<string>
}

export interface Probes<E> {
  /** Is this sender the owner, by number or by a lid that maps to the owner's number? */
  readonly isOwner: (senderId: string, senderNumber: string | undefined) => Effect.Effect<boolean>
  /** Is the owner a member of this group? */
  readonly groupHasOwner: (chatId: string) => Effect.Effect<boolean, E>
}

export const listed = (rule: AccessRule, senderId: string, chatId: string, senderNumber: string | undefined): boolean =>
  rule.allowedSenders.some((s) => s === senderId || s === chatId || (senderNumber !== undefined && s === senderNumber))

/** Whether an inbound message gets through. */
export const messageAllowed = <E>(
  rule: AccessRule,
  message: { readonly chatId: string; readonly isGroup: boolean; readonly senderId: string; readonly senderNumber: string | undefined },
  probes: Probes<E>,
): Effect.Effect<boolean, E> =>
  Effect.gen(function* () {
    if (rule.ownerDigits === undefined) {
      return rule.allowedSenders.length === 0 || listed(rule, message.senderId, message.chatId, message.senderNumber)
    }
    if (!message.isGroup) {
      if (yield* probes.isOwner(message.senderId, message.senderNumber)) return true
      return listed(rule, message.senderId, message.chatId, message.senderNumber)
    }
    if (rule.allowedSenders.includes(message.chatId)) return true
    return yield* probes.groupHasOwner(message.chatId)
  })

/** Whether the bridge may send to a chat: the same rule, seen from the chat's side. */
export const chatAllowed = <E>(
  rule: AccessRule,
  chat: { readonly chatId: string; readonly isGroup: boolean; readonly phone: string | undefined; readonly isOwnerJid: boolean },
  probes: Pick<Probes<E>, "groupHasOwner">,
): Effect.Effect<boolean, E> =>
  Effect.gen(function* () {
    if (rule.ownerDigits === undefined) return true
    if (chat.isGroup) {
      if (rule.allowedSenders.includes(chat.chatId)) return true
      return yield* probes.groupHasOwner(chat.chatId)
    }
    if (chat.phone === rule.ownerDigits || chat.isOwnerJid) return true
    return listed(rule, chat.chatId, chat.chatId, chat.phone)
  })
