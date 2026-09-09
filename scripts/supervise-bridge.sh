#!/usr/bin/env bash
# Keep the WhatsApp bridge daemon alive. Restart on exit; back off on needs_relink.
cd /home/box/whatsapp-bridge || exit 1
export PATH="/home/box/.local/node/bin:/home/box/.local/bin:${PATH}"
export BRIDGE_DATA_DIR=/home/box/whatsapp-bridge/data
export BRIDGE_GROUPS="${BRIDGE_GROUPS:-true}"

# Load optional .env (BRIDGE_API_TOKEN, etc.) without overriding explicit exports above.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
  export BRIDGE_DATA_DIR=/home/box/whatsapp-bridge/data
  export BRIDGE_GROUPS="${BRIDGE_GROUPS:-true}"
fi

mkdir -p "$BRIDGE_DATA_DIR"
LOG=/tmp/wa-bridge.log
STATUS_URL="http://127.0.0.1:8787/status"

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG"; }

health_state() {
  curl -s --max-time 2 "$STATUS_URL" 2>/dev/null | sed -n 's/.*"state":"\([^"]*\)".*/\1/p' | head -1
}

log "supervise-bridge starting (pid $$)"

while true; do
  state="$(health_state || true)"
  if [[ "$state" == "needs_relink" ]] || [[ -f "$BRIDGE_DATA_DIR/NEEDS_RELINK" && "$state" == "needs_relink" ]]; then
    log "health is needs_relink — sleeping 60s before (re)start"
    sleep 60
    continue
  fi

  # If an orphan bridge is already healthy, wait for it rather than fighting the lock/port.
  if [[ "$state" == "ok" ]]; then
    log "bridge already ok — monitoring (sleep 15)"
    sleep 15
    continue
  fi

  log "starting npm start (prior state=${state:-none})"
  npm start >>"$LOG" 2>&1
  ec=$?
  log "npm start exited ec=$ec"

  state="$(health_state || true)"
  if [[ "$state" == "needs_relink" ]] || [[ -f "$BRIDGE_DATA_DIR/NEEDS_RELINK" ]]; then
    log "post-exit needs_relink (or flag) — backoff 60s"
    sleep 60
  else
    sleep 3
  fi
done
