#!/usr/bin/env node
// Minimal handler for BRIDGE_RESPONDER=http. The bridge POSTs every turn here
// and sends back whatever `text` / `replies` you return. Plain Node, no deps.
//
//   node examples/http-handler.mjs            # listens on :9000
//   INTRO_HANDLER_URL=http://127.0.0.1:9000/turn BRIDGE_RESPONDER=http npm start

import { createServer } from "node:http"

const port = Number(process.env.PORT ?? 9000)

createServer((req, res) => {
  if (req.method !== "POST" || req.url !== "/turn") {
    res.writeHead(404).end()
    return
  }
  let body = ""
  req.on("data", (chunk) => (body += chunk))
  req.on("end", () => {
    const turn = JSON.parse(body)
    const last = turn.messages.at(-1)
    console.log(`turn ${turn.turnId} (${turn.messages.length} msgs)${turn.supersedes ? ` supersedes ${turn.supersedes}` : ""}`)
    // The bridge aborts this request if newer messages supersede the turn; a slow
    // agent should watch `req.on("close")` and stop working.
    const answer = { text: `You said: ${last?.text ?? "(no text)"}` }
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(answer))
  })
}).listen(port, "127.0.0.1", () => console.log(`handler listening on http://127.0.0.1:${port}/turn`))
