#!/usr/bin/env bash
# Shared helpers for the local-cluster scripts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-python3}"
DATA="$ROOT/data"
LOGS="$DATA/logs"
MARKER="$DATA/current-config"   # remembers which config the running cluster uses

kv() { (cd "$ROOT" && "$PY" -m distributed_key_value_store "$@"); }

node_pattern() { echo "distributed_key_value_store.server.main --id $1 --config"; }

start_node() {
  local id="$1" config
  config="$(cat "$MARKER")"
  mkdir -p "$LOGS"
  (cd "$ROOT" && setsid nohup "$PY" -m distributed_key_value_store.server.main --id "$id" --config "$config" \
      --data-dir "$DATA/$id" >>"$LOGS/$id.log" 2>&1 &)
}

signal_node() {  # signal_node <id> <KILL|STOP|CONT>
  pkill -"$2" -f "$(node_pattern "$1")" 2>/dev/null || true
}
