"""Command-line client.

  distributed-key-value-store put name prasanga
  distributed-key-value-store get name [--stale]
  distributed-key-value-store delete name
  distributed-key-value-store status [--watch]
  distributed-key-value-store leader

Cluster is taken from --config (default: $DISTRIBUTED_KEY_VALUE_STORE_CONFIG, with
$CONCLAVE_CONFIG accepted as a legacy alias; otherwise the cluster started by
scripts/start_cluster.sh, else config/local-3.json).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time

from .client import Client, ClusterUnavailable, summarize


def default_config():
    """Resolve the config path, retaining CONCLAVE_CONFIG for existing installations."""
    configured = (os.environ.get("DISTRIBUTED_KEY_VALUE_STORE_CONFIG")
                  or os.environ.get("CONCLAVE_CONFIG"))
    if configured:
        return configured
    marker = os.path.join("data", "current-config")
    if os.path.exists(marker):
        with open(marker) as f:
            path = f.read().strip()
        if os.path.exists(path):
            return path
    return "config/local-3.json"


def _cli_session(config_path):
    identity = hashlib.sha256(os.path.realpath(config_path).encode()).hexdigest()[:24]
    client_id = f"cli-{identity}"
    state_dir = os.path.join(os.path.expanduser("~"), ".distributed-key-value-store")
    state_path = os.path.join(state_dir, f"{identity}.json")
    try:
        with open(state_path) as f:
            saved = json.load(f)
        client_id, seq = saved["client_id"], saved["seq"]
        if not isinstance(client_id, str) or not isinstance(seq, int) or seq < 0:
            raise ValueError(f"invalid CLI session state in {state_path}")
    except FileNotFoundError:
        seq = 0

    def save_sequence(next_seq):
        os.makedirs(state_dir, mode=0o700, exist_ok=True)
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile("w", dir=state_dir, delete=False) as f:
                temp_path = f.name
                json.dump({"client_id": client_id, "seq": next_seq}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_path)
            fd = os.open(state_dir, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    return client_id, seq, save_sequence


def _banner(statuses):
    state, leader, term = summarize(statuses)
    n_up = sum(1 for v in statuses.values() if v)
    if state == "ok":
        return f"cluster: leader={leader} (term {term})"
    if state == "electing":
        return "cluster: no leader yet, election in progress. Waiting a moment..."
    return f"cluster: NO QUORUM, only {n_up} of {len(statuses)} nodes reachable; writes are refused"


def print_status(client):
    statuses = client.status()
    print(f"{'NODE':<8}{'ADDRESS':<18}{'ROLE':<11}{'TERM':>5}{'COMMIT':>8}"
          f"{'APPLIED':>9}{'LOG':>6}{'SNAP':>7}{'KEYS':>6}")
    for nid, st in statuses.items():
        addr = client.nodes[nid]
        if st is None:
            print(f"{nid:<8}{addr:<18}{'DOWN':<11}")
        else:
            print(f"{nid:<8}{addr:<18}{st['role']:<11}{st['term']:>5}{st['commit_index']:>8}"
                  f"{st['last_applied']:>9}{st['log_length']:>6}{st['snapshot_index']:>7}{st['keys']:>6}")
    print(_banner(statuses))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="distributed-key-value-store")
    ap.add_argument("--config", default=default_config())
    ap.add_argument("--timeout", type=float, default=10.0, help="seconds to keep retrying")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put"); p.add_argument("key"); p.add_argument("value")
    g = sub.add_parser("get"); g.add_argument("key"); g.add_argument("--stale", action="store_true")
    g.add_argument("--node", help="with --stale: which node to read from")
    d = sub.add_parser("delete"); d.add_argument("key")
    s = sub.add_parser("status"); s.add_argument("--watch", action="store_true")
    sub.add_parser("leader")
    a = ap.parse_args(argv)

    client_id, initial_seq, save_sequence = _cli_session(a.config)
    client = Client.from_config(a.config, timeout=a.timeout, client_id=client_id,
                                initial_seq=initial_seq, on_sequence=save_sequence)
    try:
        if a.cmd == "put":
            client.put(a.key, a.value)
            print(f"OK (leader: {client.last_leader}, term {client.last_term})")
        elif a.cmd == "get":
            v = client.get(a.key, stale=a.stale, node=a.node)
            if v is None:
                print("(not found)", file=sys.stderr)
                return 1
            print(v)
        elif a.cmd == "delete":
            existed = client.delete(a.key)
            print(f"OK (leader: {client.last_leader}){'' if existed else ' - key did not exist'}")
        elif a.cmd == "status":
            if a.watch:
                while True:
                    print("\033[2J\033[H", end="")
                    print_status(client)
                    time.sleep(0.5)
            print_status(client)
        elif a.cmd == "leader":
            state, leader, _ = summarize(client.status())
            if leader is None:
                print(state, file=sys.stderr)
                return 1
            print(leader)
    except ClusterUnavailable as exc:
        print(f"unavailable: {exc}", file=sys.stderr)
        print(_banner(client.status()), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
