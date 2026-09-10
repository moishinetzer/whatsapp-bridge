import { describe, expect, it } from "vitest"
import { Effect } from "effect"
import { chatAllowed, messageAllowed, type AccessRule, type Probes } from "../src/whatsapp/access.ts"

const owner = "447700900123"
const group = "120363000000000000@g.us"
const dm = (n: string) => ({ chatId: `${n}@s.whatsapp.net`, isGroup: false, senderId: `${n}@s.whatsapp.net`, senderNumber: n })
const inGroup = (n: string, chatId = group) => ({ chatId, isGroup: true, senderId: `${n}@s.whatsapp.net`, senderNumber: n })

/** Probes with a fixed view of the world: who the owner is, which groups hold the owner. */
const probes = (ownerGroups: ReadonlyArray<string>): Probes<never> => ({
  isOwner: (_senderId, senderNumber) => Effect.succeed(senderNumber === owner),
  groupHasOwner: (chatId) => Effect.succeed(ownerGroups.includes(chatId)),
})

const run = <A>(e: Effect.Effect<A, never>) => Effect.runSync(e)

describe("access rule: no owner (as before)", () => {
  const open: AccessRule = { ownerDigits: undefined, allowedSenders: [] }
  const listed: AccessRule = { ownerDigits: undefined, allowedSenders: ["447700900555"] }
  it("an empty allowlist lets everyone in; a list lets only the listed", () => {
    expect(run(messageAllowed(open, dm("1"), probes([])))).toBe(true)
    expect(run(messageAllowed(open, inGroup("1"), probes([])))).toBe(true)
    expect(run(messageAllowed(listed, dm("447700900555"), probes([])))).toBe(true)
    expect(run(messageAllowed(listed, dm("1"), probes([])))).toBe(false)
    expect(run(chatAllowed(open, { chatId: "1@s.whatsapp.net", isGroup: false, phone: "1", isOwnerJid: false }, probes([])))).toBe(true)
  })
})

describe("access rule: with an owner", () => {
  const rule: AccessRule = { ownerDigits: owner, allowedSenders: ["447700900555", "999-1@g.us"] }
  it("the owner's DM and a listed person's DM get through; a stranger's does not", () => {
    expect(run(messageAllowed(rule, dm(owner), probes([])))).toBe(true)
    expect(run(messageAllowed(rule, dm("447700900555"), probes([])))).toBe(true)
    expect(run(messageAllowed(rule, dm("447700900777"), probes([])))).toBe(false)
  })
  it("a group gets through when the owner is in it or it is listed as a group; a listed person never unlocks a group", () => {
    expect(run(messageAllowed(rule, inGroup("447700900777"), probes([group])))).toBe(true)
    expect(run(messageAllowed(rule, inGroup("447700900777"), probes([])))).toBe(false)
    expect(run(messageAllowed(rule, inGroup("447700900777", "999-1@g.us"), probes([])))).toBe(true)
    expect(run(messageAllowed(rule, inGroup("447700900555"), probes([])))).toBe(false)
  })
  it("sends follow the same rule, with the owner recognised by number or lid", () => {
    expect(run(chatAllowed(rule, { chatId: `${owner}@s.whatsapp.net`, isGroup: false, phone: owner, isOwnerJid: false }, probes([])))).toBe(true)
    expect(run(chatAllowed(rule, { chatId: "12345@lid", isGroup: false, phone: undefined, isOwnerJid: true }, probes([])))).toBe(true)
    expect(run(chatAllowed(rule, { chatId: "447700900555@s.whatsapp.net", isGroup: false, phone: "447700900555", isOwnerJid: false }, probes([])))).toBe(true)
    expect(run(chatAllowed(rule, { chatId: "447700900777@s.whatsapp.net", isGroup: false, phone: "447700900777", isOwnerJid: false }, probes([])))).toBe(false)
    expect(run(chatAllowed(rule, { chatId: group, isGroup: true, phone: undefined, isOwnerJid: false }, probes([group])))).toBe(true)
    expect(run(chatAllowed(rule, { chatId: group, isGroup: true, phone: undefined, isOwnerJid: false }, probes([])))).toBe(false)
    expect(run(chatAllowed(rule, { chatId: "999-1@g.us", isGroup: true, phone: undefined, isOwnerJid: false }, probes([])))).toBe(true)
  })
  it("a group lookup failure surfaces to the caller", () => {
    const failing: Probes<string> = { isOwner: () => Effect.succeed(false), groupHasOwner: () => Effect.fail("no socket") }
    expect(Effect.runSync(messageAllowed(rule, inGroup("1"), failing).pipe(Effect.flip))).toBe("no socket")
  })
})
