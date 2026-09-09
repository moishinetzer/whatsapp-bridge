#!/usr/bin/env node
// Minimal "Intro" in manual mode: listen to /events, answer every TurnOpened.
// Plain Node 22, no dependencies. Replace `think()` with the real agent call.
//
//   BRIDGE_URL=http://127.0.0.1:8787 node examples/manual-loop.mjs

const base = process.env.BRIDGE_URL ?? "http://127.0.0.1:8787"
const headers = process.env.BRIDGE_API_TOKEN ? { authorization: `Bearer ${process.env.BRIDGE_API_TOKEN}` } : {}

/** Your agent goes here. It receives the whole batch and returns the reply text (or null for no reply). */
async function think(turn) {
  const last = turn.messages.at(-1)
  return `You said: ${last?.text ?? "(no text)"}`
}

async function reply(turnId, text) {
  const res = await fetch(`${base}/turns/${turnId}/reply`, {
    method: "POST",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify({ text }),
  })
  const body = await res.json()
  if (res.status === 409 && body._tag === "TurnSuperseded") {
    // Newer messages arrived while we were thinking. A TurnOpened for body.currentTurnId
    // has already been (or is about to be) streamed; nothing to do here.
    console.log(`turn ${turnId} superseded by ${body.currentTurnId}`)
    return
  }
  if (!res.ok) throw new Error(`reply failed: ${res.status} ${JSON.stringify(body)}`)
  console.log(`turn ${turnId} answered, sent ${body.sentMessageIds.join(", ")}`)
}

let since = process.env.SINCE ? Number(process.env.SINCE) : undefined

async function listen() {
  const url = new URL("/events", base)
  if (since !== undefined) url.searchParams.set("since", String(since))
  const res = await fetch(url, { headers: { ...headers, accept: "text/event-stream" } })
  if (!res.ok) throw new Error(`events: ${res.status}`)
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
        .map((l) => l.slice(5).trim())
        .join("\n")
      if (!data) continue
      const event = JSON.parse(data)
      since = event.seq
      if (event._tag === "TurnOpened") {
        console.log(`turn ${event.turnId}: ${event.messages.length} message(s) from ${event.chatId}`)
        think(event).then((text) => (text ? reply(event.turnId, text) : null)).catch(console.error)
      } else if (event._tag !== "Heartbeat") {
        console.log(event._tag, JSON.stringify(event))
      }
    }
  }
}

for (;;) {
  try {
    await listen()
  } catch (error) {
    console.error("event stream dropped:", error.message)
  }
  await new Promise((r) => setTimeout(r, 2000))
}
