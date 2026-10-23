#!/usr/bin/env bash
# Scripted demo: write data, kill the leader, watch the election, verify nothing was lost.
# Good to screen-record (asciinema, or just a terminal capture).
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

"$ROOT/scripts/stop_cluster.sh" --clean >/dev/null
say "Starting a 3-node cluster"
"$ROOT/scripts/start_cluster.sh" 3

say "Writing 5 keys through whichever node is the leader"
for i in 1 2 3 4 5; do kv put "user:$i" "name-$i"; done
kv status

LEADER="$(kv leader)"
say "Hard-killing the leader ($LEADER)"
"$ROOT/scripts/kill_node.sh" "$LEADER"
kv status || true

say "Waiting for the remaining nodes to elect a new leader"
START=$(date +%s.%N)
until kv leader >/dev/null 2>&1 && [ "$(kv leader)" != "$LEADER" ]; do sleep 0.05; done
printf 'new leader elected after %.2fs\n' "$(echo "$(date +%s.%N) - $START" | bc)"
kv status

say "Every acknowledged write must still be there"
for i in 1 2 3 4 5; do printf 'user:%s = ' "$i"; kv get "user:$i"; done

say "The cluster keeps accepting writes with one node down"
kv put after-failover "still works"

say "Restarting $LEADER: it recovers its log from disk and catches up"
"$ROOT/scripts/kill_node.sh" "$LEADER" start
sleep 1.5
kv status
printf 'stale read from %s of a key written while it was down: ' "$LEADER"
kv get after-failover --stale --node "$LEADER"

say "Killing a MAJORITY: the cluster must refuse writes instead of risking inconsistency"
for n in node1 node2 node3; do [ "$n" != "$(kv leader)" ] && "$ROOT/scripts/kill_node.sh" "$n" >/dev/null; done
kv --timeout 2 put should-fail nope || true

"$ROOT/scripts/stop_cluster.sh" >/dev/null
say "Done"
