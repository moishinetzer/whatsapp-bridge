import { describe, expect, it } from "@effect/vitest"
import { NodeServices } from "@effect/platform-node"
import { Effect, Layer } from "effect"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Outbound, OutboundLive, kindForMimetype, mimetypeForExtension } from "../src/engine/Outbound.ts"
import { TestConfigLive } from "./support/fakes.ts"

const dir = mkdtempSync(join(tmpdir(), "bridge-outbound-"))
const write = (name: string, bytes: number): string => {
  const file = join(dir, name)
  writeFileSync(file, new Uint8Array(bytes).map((_, i) => i % 256))
  return file
}

const layer = (maxBytes = 1024) => OutboundLive.pipe(Layer.provide([TestConfigLive({ mediaMaxBytes: maxBytes }), NodeServices.layer]))

describe("Outbound.load", () => {
  it.effect("reads a file by path and infers mimetype, kind and file name", () =>
    Effect.gen(function* () {
      const outbound = yield* Outbound
      const image = yield* outbound.load({ path: write("chart.png", 100), caption: "Q3" })
      expect(image).toMatchObject({ kind: "image", mimetype: "image/png", caption: "Q3" })
      expect(image.bytes.byteLength).toBe(100)
      expect(image.fileName).toBeUndefined()

      const pdf = yield* outbound.load({ path: write("report.PDF", 10) })
      expect(pdf).toMatchObject({ kind: "document", mimetype: "application/pdf", fileName: "report.PDF" })

      const unknown = yield* outbound.load({ path: write("blob.xyz", 10), fileName: "renamed.xyz" })
      expect(unknown).toMatchObject({ kind: "document", mimetype: "application/octet-stream", fileName: "renamed.xyz" })

      const asFile = yield* outbound.load({ path: write("photo.jpg", 10), kind: "document" })
      expect(asFile).toMatchObject({ kind: "document", mimetype: "image/jpeg", fileName: "photo.jpg" })

      const voice = yield* outbound.load({ path: write("note.ogg", 10), voiceNote: true, caption: "ignored" })
      expect(voice).toMatchObject({ kind: "audio", mimetype: "audio/ogg; codecs=opus", voiceNote: true })
      expect(voice.fileName).toBeUndefined()
    }).pipe(Effect.provide(layer())),
  )

  it.effect("accepts raw bytes with a mimetype and refuses them without one", () =>
    Effect.gen(function* () {
      const outbound = yield* Outbound
      const media = yield* outbound.load({ data: new Uint8Array([1, 2, 3]), mimetype: "image/jpeg" })
      expect(media).toMatchObject({ kind: "image", mimetype: "image/jpeg" })
      const doc = yield* outbound.load({ data: new Uint8Array([1]), mimetype: "text/csv" })
      expect(doc).toMatchObject({ kind: "document", fileName: "file.csv" })
      const missing = yield* outbound.load({ data: new Uint8Array([1, 2, 3]) }).pipe(Effect.flip)
      expect(missing._tag).toBe("OutboundInvalid")
      const empty = yield* outbound.load({ data: new Uint8Array(), mimetype: "image/png" }).pipe(Effect.flip)
      expect(empty._tag).toBe("OutboundInvalid")
    }).pipe(Effect.provide(layer())),
  )

  it.effect("names a missing path and refuses files over the limit before reading them", () =>
    Effect.gen(function* () {
      const outbound = yield* Outbound
      const missing = yield* outbound.load({ path: join(dir, "nope.png") }).pipe(Effect.flip)
      expect(missing._tag).toBe("OutboundFileUnreadable")
      expect(missing._tag === "OutboundFileUnreadable" && missing.path).toContain("nope.png")
      const directory = yield* outbound.load({ path: dir }).pipe(Effect.flip)
      expect(directory._tag).toBe("OutboundFileUnreadable")
      const big = yield* outbound.load({ path: write("big.png", 2048) }).pipe(Effect.flip)
      expect(big).toMatchObject({ _tag: "OutboundTooLarge", size: 2048, maxBytes: 1024 })
      const bigData = yield* outbound.load({ data: new Uint8Array(2048), mimetype: "image/png" }).pipe(Effect.flip)
      expect(bigData._tag).toBe("OutboundTooLarge")
    }).pipe(Effect.provide(layer())),
  )

  it("maps extensions and mimetypes", () => {
    expect(mimetypeForExtension("JPG")).toBe("image/jpeg")
    expect(mimetypeForExtension("weird")).toBeUndefined()
    expect(kindForMimetype("video/mp4")).toBe("video")
    expect(kindForMimetype("application/pdf")).toBe("document")
    expect(kindForMimetype("AUDIO/mpeg")).toBe("audio")
  })
})
