#!/usr/bin/env bash
# Keep the SSE turn-wake watcher alive.
cd /home/box/whatsapp-bridge || exit 1
export PATH="/home/box/.local/node/bin:/home/box/.local/bin:${PATH}"
export BRIDGE_DATA_DIR=/home/box/whatsapp-bridge/data

# Webhook secrets live in data/wake.env (do NOT bash-source .env — it has
# Effect duration literals like "1500 millis" that break shell parsing).
if [[ -f "$BRIDGE_DATA_DIR/wake.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$BRIDGE_DATA_DIR/wake.env"
  set +a
fi

mkdir -p "$BRIDGE_DATA_DIR"
LOG=/tmp/wa-wake.log

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG"; }

log "supervise-wake starting (pid $$) webhook=${WA_WEBHOOK_URL:+set}"

while true; do
  log "starting wa-turn-wake.mjs"
  /home/box/.local/node/bin/node /home/box/whatsapp-bridge/scripts/wa-turn-wake.mjs >>"$LOG" 2>&1
  ec=$?
  log "wa-turn-wake.mjs exited ec=$ec — restart in 3s"
  sleep 3
done
