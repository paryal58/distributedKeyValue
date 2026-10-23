#!/usr/bin/env bash
# Start an N-node cluster as local processes:  scripts/start_cluster.sh [3|5]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
N="${1:-3}"
CONFIG="$ROOT/config/local-$N.json"
[ -f "$CONFIG" ] || (cd "$ROOT" && "$PY" scripts/gen_config.py --nodes "$N" --mode local)

if pgrep -f "distributed_key_value_store.server.main" >/dev/null; then
  echo "a cluster seems to be running already; run scripts/stop_cluster.sh first" >&2; exit 1
fi
mkdir -p "$DATA"
echo "$CONFIG" > "$MARKER"
for i in $(seq 1 "$N"); do start_node "node$i"; done
echo "started $N nodes (logs in data/logs/)"
sleep 1.5
kv status
