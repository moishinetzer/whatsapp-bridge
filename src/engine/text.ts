import { isMedia, type Outgoing } from "../domain/outbound.ts"

/** WhatsApp accepts far more, but long walls of text read badly on a phone and some clients choke past this. */
export const MAX_MESSAGE_CHARS = 4_000

/**
 * Splits a reply into chunks of at most `limit` characters, preferring to cut
 * at paragraph breaks, then line breaks, then sentence ends, then spaces.
 * Empty and whitespace-only inputs produce no chunks.
 */
export const splitText = (text: string, limit: number = MAX_MESSAGE_CHARS): ReadonlyArray<string> => {
  const trimmed = text.trim()
  if (trimmed.length === 0) return []
  if (trimmed.length <= limit) return [trimmed]

  const chunks: Array<string> = []
  let rest = trimmed
  while (rest.length > limit) {
    const window = rest.slice(0, limit)
    const cut = [/\n\n(?!.*\n\n)/s, /\n(?!.*\n)/s, /[.!?](?=\s)(?!.*[.!?]\s)/s, /\s(?!.*\s)/s]
      .map((re) => {
        const m = re.exec(window)
        return m === null ? -1 : m.index + (re.source.startsWith("[.!?]") ? 1 : 0)
      })
      .find((i) => i > limit / 4)
    const at = cut === undefined || cut <= 0 ? limit : cut
    chunks.push(rest.slice(0, at).trim())
    rest = rest.slice(at).trim()
  }
  if (rest.length > 0) chunks.push(rest)
  return chunks.filter((c) => c.length > 0)
}

/** Drops empty replies and splits long ones, in order. */
export const prepareReplies = (replies: ReadonlyArray<string>, limit: number = MAX_MESSAGE_CHARS): ReadonlyArray<string> =>
  replies.flatMap((r) => splitText(r, limit))

/** The same for a mixed reply: text bubbles are cleaned and split (the first chunk keeps the quote), files pass through in place. */
export const prepareOutgoing = (items: ReadonlyArray<Outgoing>, limit: number = MAX_MESSAGE_CHARS): ReadonlyArray<Outgoing> =>
  items.flatMap((item): ReadonlyArray<Outgoing> =>
    isMedia(item)
      ? [item]
      : splitText(item.text, limit).map((text, i) =>
          i === 0 && item.quoteMessageId !== undefined ? { text, quoteMessageId: item.quoteMessageId } : { text },
        ),
  )
