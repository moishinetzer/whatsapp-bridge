#!/usr/bin/env node
/**
 * SSE wake watcher: GET /events?since=N, queue TurnOpened (and needs_relink)
 * for Intro's poller, optionally POST to WA_WEBHOOK_URL.
 */
import fs from "node:fs"
import path from "node:path"

const base = process.env.BRIDGE_URL ?? "http://127.0.0.1:8787"
const dataDir = process.env.BRIDGE_DATA_DIR ?? "/home/box/whatsapp-bridge/data"
const sinceFile = path.join(dataDir, "events-since.txt")
const pendingFile = path.join(dataDir, "pending-turns.jsonl")
const lastTurnFile = path.join(dataDir, "last-turn.json")
const relinkFlag = path.join(dataDir, "NEEDS_RELINK")
function log(msg) {
  // supervise-wake.sh redirects stdout/stderr to /tmp/wa-wake.log
  process.stdout.write(`${new Date().toISOString()} ${msg}\n`)
}

function ensureDataDir() {
  fs.mkdirSync(dataDir, { recursive: true })
}

function loadSince() {
  try {
    const n = Number(String(fs.readFileSync(sinceFile, "utf8")).trim())
    return Number.isFinite(n) ? n : undefined
  } catch {
    return undefined
  }
}

function saveSince(seq) {
  ensureDataDir()
  fs.writeFileSync(sinceFile, `${seq}\n`)
}

function queueEvent(event) {
  ensureDataDir()
  fs.appendFileSync(pendingFile, `${JSON.stringify(event)}\n`)
  fs.writeFileSync(lastTurnFile, `${JSON.stringify(event, null, 2)}\n`)
}

async function postWebhook(event) {
  const url = process.env.WA_WEBHOOK_URL
  if (!url) return
  const headers = { "content-type": "application/json", accept: "application/json" }
  const raw = process.env.WA_WEBHOOK_HEADER
  if (raw) {
    const colon = raw.indexOf(":")
    if (colon > 0) {
      headers[raw.slice(0, colon).trim()] = raw.slice(colon + 1).trim()
    } else {
      headers.authorization = raw
    }
  }
  try {
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(event),
    })
    log(`webhook POST ${url} -> ${res.status}`)
  } catch (err) {
    log(`webhook error: ${err?.message ?? err}`)
  }
}

async function handleEvent(event) {
  if (typeof event?.seq === "number") {
    saveSince(event.seq)
  }

  if (event._tag === "TurnOpened") {
    queueEvent(event)
    log(`TurnOpened turnId=${event.turnId} chatId=${event.chatId} msgs=${event.messages?.length ?? 0}`)
    await postWebhook(event)
    return
  }

  if (event._tag === "ConnectionChanged" && event.state === "needs_relink") {
    ensureDataDir()
    fs.writeFileSync(relinkFlag, `${JSON.stringify(event)}\n`)
    queueEvent(event)
    log(`ConnectionChanged needs_relink detail=${event.detail ?? ""}`)
    await postWebhook(event)
    return
  }

  if (event._tag === "ConnectionChanged" && event.state === "ok") {
    try {
      if (fs.existsSync(relinkFlag)) fs.unlinkSync(relinkFlag)
    } catch {
      // ignore
    }
  }
}

function bridgeHeaders() {
  const headers = { accept: "text/event-stream" }
  if (process.env.BRIDGE_API_TOKEN) {
    headers.authorization = `Bearer ${process.env.BRIDGE_API_TOKEN}`
  }
  return headers
}

async function listen() {
  let since = loadSince()
  const url = new URL("/events", base)
  if (since !== undefined) url.searchParams.set("since", String(since))
  log(`connecting ${url}`)
  const res = await fetch(url, { headers: bridgeHeaders() })
  if (!res.ok) throw new Error(`events: ${res.status}`)
  if (!res.body) throw new Error("events: no body")
  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return
    buffer += decoder.decode(value, { stream: true })
    let idx
    while ((idx = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, idx)
      buffer = buffer.slice(idx + 2)
      const data = frame
        .split("\n")
        .filter((l) => l.startsWith("data:"))
        .map((l) => l.slice(5).trimStart())
        .join("\n")
      if (!data) continue
      let event
      try {
        event = JSON.parse(data)
      } catch (err) {
        log(`bad SSE JSON: ${err?.message ?? err}`)
        continue
      }
      await handleEvent(event)
    }
  }
}

ensureDataDir()
log(`wa-turn-wake starting dataDir=${dataDir}`)

for (;;) {
  try {
    await listen()
    log("event stream ended")
  } catch (err) {
    log(`event stream dropped: ${err?.message ?? err}`)
  }
  await new Promise((r) => setTimeout(r, 2000))
}
