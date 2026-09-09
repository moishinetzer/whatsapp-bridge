import { describe, expect, it } from "vitest"
import { prepareReplies, splitText } from "../src/engine/text.ts"

describe("splitText", () => {
  it("leaves short text alone and drops empty text", () => {
    expect(splitText("hello")).toEqual(["hello"])
    expect(splitText("   ")).toEqual([])
    expect(splitText("")).toEqual([])
  })

  it("prefers paragraph breaks, then lines, then sentences, then spaces", () => {
    const para = `${"a".repeat(30)}\n\n${"b".repeat(30)}`
    expect(splitText(para, 40)).toEqual(["a".repeat(30), "b".repeat(30)])
    const lines = `${"a".repeat(30)}\n${"b".repeat(30)}`
    expect(splitText(lines, 40)).toEqual(["a".repeat(30), "b".repeat(30)])
    const sentences = `${"a".repeat(28)}. ${"b".repeat(30)}`
    expect(splitText(sentences, 40)).toEqual([`${"a".repeat(28)}.`, "b".repeat(30)])
    const words = `${"a".repeat(20)} ${"b".repeat(15)} ${"c".repeat(30)}`
    expect(splitText(words, 40)).toEqual([`${"a".repeat(20)} ${"b".repeat(15)}`, "c".repeat(30)])
  })

  it("hard-cuts text with no break points and never exceeds the limit", () => {
    const chunks = splitText("x".repeat(95), 40)
    expect(chunks.map((c) => c.length)).toEqual([40, 40, 15])
    for (const c of splitText("word ".repeat(2_000), 4_000)) expect(c.length).toBeLessThanOrEqual(4_000)
  })

  it("prepareReplies flattens in order and skips blanks", () => {
    expect(prepareReplies(["one", "", "  ", "two"])).toEqual(["one", "two"])
    expect(prepareReplies([`${"a".repeat(30)}\n\n${"b".repeat(30)}`, "c"], 40)).toEqual(["a".repeat(30), "b".repeat(30), "c"])
  })
})
