import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { Duration, Effect, Layer } from "effect"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { AuthStore, AuthStoreLive } from "../src/whatsapp/AuthStore.ts"
import { classifyClose } from "../src/whatsapp/Baileys.ts"
import { makeBaileysLogger, redact } from "../src/whatsapp/BaileysLogger.ts"
import { TestConfigLive } from "./support/fakes.ts"

const boom = (statusCode: number, message = "closed") => Object.assign(new Error(message), { output: { statusCode } })

describe("classifyClose", () => {
  it("treats logout, forbidden and bad sessions as terminal", () => {
    expect(classifyClose(boom(401), Duration.zero)).toMatchObject({ _tag: "Terminal", loggedOut: true })
    expect(classifyClose(boom(403), Duration.zero)).toMatchObject({ _tag: "Terminal", loggedOut: false })
    expect(classifyClose(boom(500), Duration.zero)).toMatchObject({ _tag: "Terminal", loggedOut: true })
    expect(classifyClose(boom(411), Duration.zero)).toMatchObject({ _tag: "Terminal", loggedOut: true })
  })

  it("reconnects immediately on restart-required and with backoff otherwise", () => {
    expect(classifyClose(boom(515), Duration.seconds(1))).toMatchObject({ _tag: "Reconnect", immediate: true })
    expect(classifyClose(boom(408, "timed out"), Duration.seconds(1))).toMatchObject({ _tag: "Reconnect", immediate: false })
    expect(classifyClose(boom(428), Duration.seconds(1))).toMatchObject({ _tag: "Reconnect", immediate: false })
    expect(classifyClose(new Error("socket hang up"), Duration.zero)).toMatchObject({ _tag: "Reconnect", immediate: false })
    expect(classifyClose(undefined, Duration.zero)).toMatchObject({ _tag: "Reconnect", immediate: false, detail: "connection closed" })
  })

  it("keeps the reason and the status code in the detail", () => {
    const outcome = classifyClose(boom(408, "timed out"), Duration.seconds(3))
    expect(outcome._tag === "Reconnect" && outcome.detail).toBe("timed out (408)")
    expect(outcome._tag === "Reconnect" && Duration.toSeconds(outcome.uptime)).toBe(3)
  })
})

describe("BaileysLogger", () => {
  it("redacts payloads down to primitives, error summaries and message keys", () => {
    const fields = redact({
      msg: { conversation: "private body" },
      node: { tag: "message", attrs: {} },
      err: boom(428, "Connection Terminated"),
      key: { remoteJid: "447700900123@s.whatsapp.net", id: "ABC", fromMe: false },
      count: 3,
      flag: true,
      long: "x".repeat(500),
      bytes: new Uint8Array(4),
      list: [1, 2, 3],
      nothing: null,
    })
    expect(fields).toEqual({
      error: "Error: Connection Terminated (428)",
      chatId: "447700900123@s.whatsapp.net",
      messageId: "ABC",
      fromMe: false,
      count: 3,
      flag: true,
      long: `${"x".repeat(200)}…`,
      bytes: "<4 bytes>",
      list: "<array of 3>",
    })
    expect(Object.keys(fields)).not.toContain("msg")
    expect(Object.keys(fields)).not.toContain("node")
  })

  it("forwards only at or above the configured level and accepts pino's calling conventions", () => {
    const forwarded: Array<string> = []
    const logger = makeBaileysLogger((effect) => forwarded.push(String(effect)), "warn", { class: "baileys" })
    logger.trace({ a: 1 }, "trace")
    logger.debug("debug")
    logger.info({}, "info")
    logger.warn({ err: new Error("careful") }, "warn")
    logger.error("plain string error")
    const child = logger.child({ class: "child" })
    child.error({}, "from child")
    expect(forwarded.length).toBe(3)
    expect(logger.level).toBe("warn")
  })
})

describe("AuthStore lock", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "bridge-lock-"))
  const layer = AuthStoreLive.pipe(
    Layer.provide([TestConfigLive({ dataDir, authDir: join(dataDir, "auth") }), NodeServices.layer]),
  )

  it.effect("refuses to start while another live process holds the session, and clears on release", () =>
    Effect.gen(function* () {
      const store = yield* AuthStore
      const fs = yield* Effect.service(NodeServicesFileSystem)
      const lockPath = join(dataDir, "bridge.lock")
      // The parent process (vitest) is alive and is not us.
      yield* fs.writeFileString(lockPath, JSON.stringify({ pid: process.ppid, startedAt: "2026-01-01T00:00:00Z" }))
      const refused = yield* Effect.scoped(store.lock).pipe(Effect.flip)
      expect(refused._tag).toBe("AuthStoreError")
      expect(refused.detail).toContain(`pid ${process.ppid}`)
      yield* fs.remove(lockPath)

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* store.lock
          expect(yield* fs.exists(lockPath)).toBe(true)
        }),
      )
      expect(yield* fs.exists(lockPath)).toBe(false)
    }).pipe(Effect.provide(Layer.mergeAll(layer, NodeServices.layer))),
  )

  it.effect("takes over a stale lock left by a dead process", () =>
    Effect.gen(function* () {
      const store = yield* AuthStore
      const fs = yield* Effect.service(NodeServicesFileSystem)
      yield* fs.writeFileString(join(dataDir, "bridge.lock"), JSON.stringify({ pid: 2147483646, startedAt: "2020-01-01T00:00:00Z" }))
      yield* Effect.scoped(store.lock)
      expect(yield* fs.exists(join(dataDir, "bridge.lock"))).toBe(false)
    }).pipe(Effect.provide(Layer.mergeAll(layer, NodeServices.layer))),
  )
})

import { FileSystem } from "effect"
const NodeServicesFileSystem = FileSystem.FileSystem
