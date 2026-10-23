#!/usr/bin/env python3
"""Measure leader failover: SIGKILL the leader, time (a) until a new leader exists and
(b) until a client write succeeds again.  Repeats and reports median / p95 / max.

  python scripts/bench_failover.py --nodes 3 --trials 20
"""
import argparse
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from distributed_key_value_store.client.client import summarize  # noqa: E402
from distributed_key_value_store.testing.proc_cluster import ProcCluster  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--trials", type=int, default=15)
    ap.add_argument("--fsync", action="store_true")
    a = ap.parse_args()

    elect_ms, write_ms = [], []
    with ProcCluster(n=a.nodes, fsync=a.fsync) as cluster:
        cluster.wait_for_leader()
        kv = cluster.client(timeout=15)
        kv.put("warmup", 1)
        for trial in range(a.trials):
            old = cluster.wait_for_leader()
            old_term = cluster.statuses()[old]["term"]
            probe = cluster.client(timeout=15)
            cluster.kill(old)
            t0 = time.monotonic()
            t_leader = None
            while t_leader is None:
                _, leader, term = summarize(probe.status(timeout=0.2))
                if leader and leader != old and term > old_term:
                    t_leader = time.monotonic() - t0
                time.sleep(0.005)
            probe.put(f"t{trial}", trial)           # first successful write after the crash
            t_write = time.monotonic() - t0
            elect_ms.append(t_leader * 1000)
            write_ms.append(t_write * 1000)
            print(f"trial {trial + 1:>2}: new leader after {t_leader * 1000:6.0f} ms, writes back after {t_write * 1000:6.0f} ms")
            cluster.start(old)                      # bring the old leader back as a follower
            cluster.wait_converged(timeout=20)

    def line(name, xs):
        print(f"{name:<22} median {statistics.median(xs):6.0f} ms   p95 {pct(xs, 95):6.0f} ms   max {max(xs):6.0f} ms")
    print(f"\n{a.nodes}-node cluster, {a.trials} leader kills (election timeout 300-600 ms, heartbeat 100 ms)")
    line("new leader elected:", elect_ms)
    line("writes available again:", write_ms)


if __name__ == "__main__":
    main()
