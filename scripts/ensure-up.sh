#!/usr/bin/env bash
# Ensure bridge + wake supervisors are running; print status summary.
set -euo pipefail
export PATH="/home/box/.local/node/bin:/home/box/.local/bin:${PATH}"
ROOT=/home/box/whatsapp-bridge
DATA="$ROOT/data"
mkdir -p "$DATA" "$ROOT/scripts"
cd "$ROOT"

# Match the real supervisor argv only (not ensure-up.sh / wrapper shells that mention the name).
is_running() {
  local script_base="$1"
  pgrep -f "bash ${ROOT}/scripts/${script_base}" >/dev/null 2>&1 \
    || pgrep -f "bash .*scripts/${script_base}" >/dev/null 2>&1
}

pids_of() {
  local script_base="$1"
  { pgrep -f "bash ${ROOT}/scripts/${script_base}" || true
    pgrep -f "bash .*scripts/${script_base}" || true
  } | sort -u | tr '\n' ' '
}

start_if_missing() {
  local script_base="$1"
  local script="$ROOT/scripts/$script_base"
  if is_running "$script_base"; then
    echo "$script_base already running: $(pids_of "$script_base")"
  else
    nohup bash "$script" >/dev/null 2>&1 &
    local pid=$!
    disown "$pid" 2>/dev/null || true
    echo "started $script_base pid $pid"
    sleep 0.4
  fi
}

start_if_missing "supervise-bridge.sh"
start_if_missing "supervise-wake.sh"

sleep 1

STATUS="$(curl -s --max-time 3 http://127.0.0.1:8787/status 2>/dev/null || echo '{"state":"unreachable"}')"
HEALTH="$(curl -s --max-time 3 http://127.0.0.1:8787/health 2>/dev/null || echo '{"state":"unreachable"}')"

echo "status: $STATUS"
echo "health: $HEALTH"

STATE="$(printf '%s' "$STATUS" | sed -n 's/.*"state":"\([^"]*\)".*/\1/p' | head -1)"
LINKED="$(printf '%s' "$STATUS" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p' | head -1)"
echo "summary: state=${STATE:-unknown} linkedAs=${LINKED:-?} bridge_sup=$(pids_of supervise-bridge.sh) wake_sup=$(pids_of supervise-wake.sh)"

if [[ "$STATE" == "needs_relink" ]]; then
  printf '%s\n' "$STATUS" >"$DATA/NEEDS_RELINK"
  echo "wrote $DATA/NEEDS_RELINK"
elif [[ "$STATE" == "ok" && -f "$DATA/NEEDS_RELINK" ]]; then
  rm -f "$DATA/NEEDS_RELINK"
  echo "cleared NEEDS_RELINK (state=ok)"
fi
