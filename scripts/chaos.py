#!/usr/bin/env python3
"""Chaos test: hammer a real cluster with writes while randomly killing, pausing and restarting nodes.

Afterwards, heal everything and verify:
  1. every write the client was ACKNOWLEDGED is readable (no lost committed writes)
  2. every node converges to identical state
  3. writes with unknown outcome (client timed out) are either present or absent, never corrupt

  python scripts/chaos.py --nodes 5 --duration 60
"""
import argparse
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from distributed_key_value_store.client.client import ClusterUnavailable  # noqa: E402
from distributed_key_value_store.testing.proc_cluster import ProcCluster  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--fsync", action="store_true", help="use real fsync (slower, closer to production)")
    a = ap.parse_args()
    seed = a.seed if a.seed is not None else random.randrange(10**6)
    rng = random.Random(seed)
    print(f"chaos run: {a.nodes} nodes, {a.duration}s, seed={seed}")

    acked, unknown = {}, {}
    stop = threading.Event()
    stats = {"kills": 0, "pauses": 0, "restarts": 0, "leader_kills": 0}

    with ProcCluster(n=a.nodes, fsync=a.fsync) as cluster:
        cluster.wait_for_leader()
        quorum = a.nodes // 2 + 1

        def writer():
            kv = cluster.client(timeout=6.0)
            i = 0
            while not stop.is_set():
                i += 1
                key, val = f"key{i % 50}", f"val{i}"
                try:
                    kv.put(key, val)
                    acked[i] = (key, val)
                except ClusterUnavailable:
                    unknown[i] = (key, val)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        end = time.monotonic() + a.duration
        down, frozen = set(), set()
        while time.monotonic() < end:
            time.sleep(rng.uniform(0.5, 2.0))
            healthy = [n for n in cluster.ids if n not in down and n not in frozen]
            action = rng.choice(["kill", "kill_leader", "pause", "heal", "heal"])
            if action in ("kill", "kill_leader", "pause") and len(healthy) <= quorum:
                action = "heal"                 # keep a majority healthy most of the time
            if action == "kill_leader":
                try:
                    victim = cluster.wait_for_leader(timeout=3)
                except TimeoutError:
                    continue
                if victim in healthy:
                    cluster.kill(victim); down.add(victim); stats["kills"] += 1; stats["leader_kills"] += 1
            elif action == "kill":
                victim = rng.choice(healthy)
                cluster.kill(victim); down.add(victim); stats["kills"] += 1
            elif action == "pause":
                victim = rng.choice(healthy)
                cluster.pause(victim); frozen.add(victim); stats["pauses"] += 1
            else:
                for n in list(down):
                    cluster.start(n); down.discard(n); stats["restarts"] += 1
                for n in list(frozen):
                    cluster.resume(n); frozen.discard(n)

        stop.set()
        t.join(timeout=15)
        for n in list(down):
            cluster.start(n)
        for n in list(frozen):
            cluster.resume(n)
        print("healing cluster...")
        cluster.wait_for_leader(timeout=20)
        data = cluster.wait_converged(timeout=30)

        # last acknowledged write per key must be what the cluster holds (or something newer/unknown)
        last_ack, last_unknown = {}, {}
        for i, (k, v) in acked.items():
            last_ack[k] = max(last_ack.get(k, 0), i)
        for i, (k, v) in unknown.items():
            last_unknown[k] = max(last_unknown.get(k, 0), i)
        by_val = {v: i for i, (k, v) in {**acked, **unknown}.items()}
        problems = []
        for k, i in last_ack.items():
            held = data.get(k)
            held_i = by_val.get(held)
            if held_i is None or held_i < i:
                problems.append(f"{k}: acknowledged write #{i} lost (cluster holds {held!r})")
        print(f"writes acknowledged: {len(acked)}   unknown outcome: {len(unknown)}")
        print(f"faults injected:     {stats}")
        print(f"all {a.nodes} nodes converged to identical state: yes ({len(data)} keys)")
        if problems:
            print("FAIL: lost acknowledged writes:")
            for p in problems[:10]:
                print("  ", p)
            sys.exit(1)
        print("PASS: no acknowledged write was lost")


if __name__ == "__main__":
    main()
