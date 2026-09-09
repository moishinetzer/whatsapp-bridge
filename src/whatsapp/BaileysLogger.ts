import type { ILogger } from "@whiskeysockets/baileys/lib/Utils/logger.js"
import { Effect } from "effect"

export type BaileysLogLevel = "trace" | "debug" | "info" | "warn" | "error" | "silent"

const order: Record<BaileysLogLevel, number> = { trace: 10, debug: 20, info: 30, warn: 40, error: 50, silent: 100 }

export const isBaileysLogLevel = (s: string): s is BaileysLogLevel => s in order

const MAX_STRING = 200

const summariseError = (e: unknown): string => {
  if (e instanceof Error) {
    const code = (e as { output?: { statusCode?: unknown } }).output?.statusCode
    return `${e.name}: ${e.message}${typeof code === "number" ? ` (${code})` : ""}`
  }
  return typeof e === "string" ? e : String(e)
}

/**
 * Keeps only what helps diagnose a problem: primitives (strings truncated),
 * error summaries, and the id/jid of a message key. Message content, binary
 * nodes, buffers and other nested payloads are dropped, so nothing private
 * reaches the logs through the protocol library.
 */
export const redact = (input: unknown): Record<string, unknown> => {
  if (typeof input !== "object" || input === null) return {}
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(input as Record<string, unknown>)) {
    if (value === undefined || value === null) continue
    if (key === "err" || key === "error") {
      out.error = summariseError(value)
      continue
    }
    if (key === "key" && typeof value === "object") {
      const k = value as { id?: unknown; remoteJid?: unknown; fromMe?: unknown }
      if (typeof k.id === "string") out.messageId = k.id
      if (typeof k.remoteJid === "string") out.chatId = k.remoteJid
      if (typeof k.fromMe === "boolean") out.fromMe = k.fromMe
      continue
    }
    if (typeof value === "string") {
      out[key] = value.length > MAX_STRING ? `${value.slice(0, MAX_STRING)}…` : value
    } else if (typeof value === "number" || typeof value === "boolean") {
      out[key] = value
    } else if (value instanceof Uint8Array) {
      out[key] = `<${value.byteLength} bytes>`
    } else if (Array.isArray(value)) {
      out[key] = `<array of ${value.length}>`
    }
    // other objects are dropped on purpose
  }
  return out
}

const effectFor = (level: BaileysLogLevel) => {
  switch (level) {
    case "error":
      return Effect.logError
    case "warn":
      return Effect.logWarning
    case "info":
      return Effect.logInfo
    case "debug":
      return Effect.logDebug
    case "trace":
      return Effect.logTrace
    case "silent":
      return Effect.logTrace
  }
}

/**
 * A pino-shaped logger the protocol library accepts, forwarding into Effect's
 * logger so its warnings and errors show up next to ours, in the same format,
 * redacted. `run` forks the log effect on the bridge's runtime (see
 * `FiberSet.makeRuntime`).
 */
export const makeBaileysLogger = (
  run: (effect: Effect.Effect<void>) => unknown,
  level: BaileysLogLevel,
  bindings: Record<string, unknown> = {},
): ILogger => {
  const threshold = order[level]
  const emit = (at: BaileysLogLevel) => (obj: unknown, msg?: string) => {
    if (order[at] < threshold) return
    const message = typeof obj === "string" && msg === undefined ? obj : (msg ?? "")
    const fields = typeof obj === "string" ? {} : redact(obj)
    run(effectFor(at)(`whatsapp lib: ${message}`).pipe(Effect.annotateLogs({ lib: "baileys", ...bindings, ...fields })))
  }
  return {
    level,
    child: (extra) => makeBaileysLogger(run, level, { ...bindings, ...redact(extra) }),
    trace: emit("trace"),
    debug: emit("debug"),
    info: emit("info"),
    warn: emit("warn"),
    error: emit("error"),
  }
}
