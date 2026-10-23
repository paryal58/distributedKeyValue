#!/usr/bin/env bash
# Stop all local nodes.  Add --clean to also delete their data (term, votes, logs).
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
pkill -CONT -f "distributed_key_value_store.server.main" 2>/dev/null || true   # un-freeze paused nodes so they can exit
pkill -TERM -f "distributed_key_value_store.server.main" 2>/dev/null || true
sleep 0.3
pkill -KILL -f "distributed_key_value_store.server.main" 2>/dev/null || true
if [ "${1:-}" = "--clean" ]; then
  rm -rf "$DATA"/node* "$LOGS" "$MARKER"
  echo "stopped and cleaned data"
else
  echo "stopped (data kept; start again to see it recover)"
fi
