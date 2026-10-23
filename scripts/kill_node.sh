#!/usr/bin/env bash
# Fault injection for local nodes:
#   scripts/kill_node.sh node2           hard-kill (SIGKILL): a crash
#   scripts/kill_node.sh node2 pause     freeze the process (SIGSTOP): a hung node
#   scripts/kill_node.sh node2 resume    unfreeze it (SIGCONT)
#   scripts/kill_node.sh node2 start     start it again; it recovers from its disk and catches up
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
ID="${1:?usage: kill_node.sh <node id> [kill|pause|resume|start]}"
case "${2:-kill}" in
  kill)   signal_node "$ID" KILL; echo "$ID killed" ;;
  pause)  signal_node "$ID" STOP; echo "$ID paused" ;;
  resume) signal_node "$ID" CONT; echo "$ID resumed" ;;
  start)  start_node "$ID"; echo "$ID started" ;;
  *) echo "unknown action" >&2; exit 1 ;;
esac
