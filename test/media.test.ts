import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { DateTime, Duration, Effect, Fiber, FileSystem, Latch, Layer, Option, Ref } from "effect"
import { TestClock } from "effect/testing"
import { mkdtempSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { MediaFile } from "../src/domain/message.ts"
import { MediaStore, MediaStoreLive } from "../src/engine/Media.ts"
import { StoreWithDatabaseLive } from "../src/store/Store.ts"
import { MediaSourceFromWhatsApp } from "../src/whatsapp/MediaSource.ts"
import { FakeWhatsApp, FakeWhatsAppLive, TestConfigLive, chat, jpeg, message } from "./support/fakes.ts"

const storeLayer = (dir: string, extra: Parameters<typeof TestConfigLive>[0] = {}) => {
  const config = TestConfigLive({ mediaDir: dir, ...extra })
  const storage = StoreWithDatabaseLive.pipe(Layer.provide([config, NodeServices.layer]))
  return Layer.mergeAll(
    MediaStoreLive.pipe(Layer.provide([config, MediaSourceFromWhatsApp.pipe(Layer.provide(FakeWhatsAppLive)), storage, NodeServices.layer])),
    FakeWhatsAppLive,
    NodeServices.layer,
  )
}

describe("MediaStore", () => {
  it.effect("downloads once, persists the file and a sidecar with tight permissions", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const m = message("photo", { kind: "image", mimetype: "image/jpeg" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg(128)]]))

      const first = yield* store.get(chat, m.messageId)
      expect(first.path.startsWith(store.dir)).toBe(true)
      expect(first.path.endsWith(`${m.messageId}.jpeg`)).toBe(true)
      expect(first.mimetype).toBe("image/jpeg")
      expect(first.size).toBe(128)
      expect(first.kind).toBe("image")
      expect(statSync(first.path).size).toBe(128)
      expect(statSync(first.path).mode & 0o777).toBe(0o600)
      expect(statSync(first.path.replace(/\.jpeg$/, ".json")).mode & 0o777).toBe(0o600)

      const again = yield* store.get(chat, m.messageId)
      expect(again.path).toBe(first.path)
      expect(yield* Ref.get(wa.downloads)).toBe(1)
    }).pipe(Effect.provide(storeLayer(mkdtempSync(join(tmpdir(), "media-a-"))))),
  )

  it.effect("shares one download between concurrent callers", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const m = message("photo", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg()]]))
      const latch = yield* Latch.make(false)
      yield* Ref.set(wa.mediaGate, Option.some(latch))
      const fibers = yield* Effect.forEach([1, 2, 3], () => store.get(chat, m.messageId).pipe(Effect.forkScoped))
      yield* Effect.yieldNow
      yield* latch.open
      const results = yield* Effect.forEach(fibers, Fiber.join)
      expect(new Set(results.map((r) => r.path)).size).toBe(1)
      expect(yield* Ref.get(wa.downloads)).toBe(1)
    }).pipe(Effect.provide(storeLayer(mkdtempSync(join(tmpdir(), "media-b-"))))),
  )

  it.effect("does not remember failures: a later call retries and succeeds", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const m = message("photo", { kind: "image" })
      const missing = yield* store.get(chat, m.messageId).pipe(Effect.flip)
      expect(missing._tag).toBe("MediaUnavailable")
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg()]]))
      const found = yield* store.get(chat, m.messageId)
      expect(found.size).toBe(64)
      expect(yield* Ref.get(wa.downloads)).toBe(2)
    }).pipe(Effect.provide(storeLayer(mkdtempSync(join(tmpdir(), "media-c-"))))),
  )

  it.effect("refuses attachments over the size limit before writing anything", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const m = message("huge", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[m.messageId, jpeg(2_000)]]))
      const error = yield* store.get(chat, m.messageId).pipe(Effect.flip)
      expect(error._tag).toBe("MediaTooLarge")
      const fs = yield* FileSystem.FileSystem
      expect(yield* fs.exists(join(store.dir, chat))).toBe(false)
    }).pipe(Effect.provide(storeLayer(mkdtempSync(join(tmpdir(), "media-d-")), { mediaMaxBytes: 1_000 }))),
  )

  it.effect("serves a file downloaded by a previous run from its sidecar, without the transport", () =>
    Effect.gen(function* () {
      const dir = mkdtempSync(join(tmpdir(), "media-e-"))
      const m = message("photo", { kind: "image" })
      const before = yield* Effect.gen(function* () {
        const store = yield* MediaStore
        const wa = yield* FakeWhatsApp
        yield* Ref.set(wa.media, new Map([[m.messageId, jpeg()]]))
        return yield* store.get(chat, m.messageId)
      }).pipe(Effect.provide(storeLayer(dir)))
      // A fresh store on the same directory, with a transport that knows nothing.
      const after = yield* Effect.gen(function* () {
        const store = yield* MediaStore
        const wa = yield* FakeWhatsApp
        const file = yield* store.get(chat, m.messageId)
        expect(yield* Ref.get(wa.downloads)).toBe(0)
        return file
      }).pipe(Effect.provide(storeLayer(dir)))
      expect(after.path).toBe(before.path)
      expect(after.size).toBe(before.size)
    }),
  )

  it.effect("sweeps files older than the retention and keeps the rest", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const fs = yield* FileSystem.FileSystem
      const old = message("old", { kind: "image" })
      const fresh = message("fresh", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[old.messageId, jpeg()], [fresh.messageId, jpeg()]]))
      const oldFile = yield* store.get(chat, old.messageId)
      // Move the clock past the retention, then download the fresh one at the new "now".
      yield* TestClock.adjust(Duration.days(8))
      const freshFile = yield* store.get(chat, fresh.messageId)
      const removed = yield* store.sweep
      expect(removed).toBe(1)
      expect(yield* fs.exists(oldFile.path)).toBe(false)
      expect(yield* fs.exists(freshFile.path)).toBe(true)
      // the swept entry is gone from the cache too: asking again re-downloads
      yield* store.get(chat, old.messageId)
      expect(yield* Ref.get(wa.downloads)).toBe(3)
      const record = yield* fs.readFileString(freshFile.path.replace(/\.jpeg$/, ".json"))
      expect(MediaFile.fields.downloadedAt).toBeDefined()
      expect(DateTime.toEpochMillis(freshFile.downloadedAt)).toBeGreaterThan(DateTime.toEpochMillis(oldFile.downloadedAt))
      expect(record).toContain("image/jpeg")
    }).pipe(
      Effect.provide(
        storeLayer(mkdtempSync(join(tmpdir(), "media-f-")), { mediaSweepEvery: Duration.days(365), mediaRetention: Option.some(Duration.days(7)) }),
      ),
    ),
  )

  it.effect("evicts least recently used files once the disk budget is exceeded", () =>
    Effect.gen(function* () {
      const store = yield* MediaStore
      const wa = yield* FakeWhatsApp
      const fs = yield* FileSystem.FileSystem
      const a = message("a", { kind: "image" })
      const b = message("b", { kind: "image" })
      const c = message("c", { kind: "image" })
      yield* Ref.set(wa.media, new Map([[a.messageId, jpeg(100)], [b.messageId, jpeg(100)], [c.messageId, jpeg(100)]]))
      const fa = yield* store.get(chat, a.messageId)
      yield* TestClock.adjust("1 second")
      const fb = yield* store.get(chat, b.messageId)
      yield* TestClock.adjust("1 second")
      // a is used again, so b is now the least recently used.
      yield* store.get(chat, a.messageId)
      yield* TestClock.adjust("1 second")
      const fc = yield* store.get(chat, c.messageId)
      expect((yield* store.usage).bytes).toBe(300)
      const removed = yield* store.sweep
      expect(removed).toBe(1)
      expect(yield* fs.exists(fb.path)).toBe(false)
      expect(yield* fs.exists(fa.path)).toBe(true)
      expect(yield* fs.exists(fc.path)).toBe(true)
      expect(yield* store.usage).toEqual({ bytes: 200, files: 2 })
      // Touching keeps a file alive on the next sweep.
      yield* Ref.set(wa.media, new Map([[b.messageId, jpeg(100)]]))
      yield* TestClock.adjust("1 second")
      yield* store.touch(chat, a.messageId)
      yield* TestClock.adjust("1 second")
      yield* store.get(chat, b.messageId)
      yield* store.sweep
      expect(yield* fs.exists(fc.path)).toBe(false)
      expect(yield* fs.exists(fa.path)).toBe(true)
    }).pipe(Effect.provide(storeLayer(mkdtempSync(join(tmpdir(), "media-g-")), { mediaSweepEvery: Duration.days(365), mediaMaxDisk: 250 }))),
  )
})
