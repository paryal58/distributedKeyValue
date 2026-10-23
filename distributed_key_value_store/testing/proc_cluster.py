"""Spin up a real multi-process cluster on free local ports, with kill / pause / restart controls.

Used by the integration tests, scripts/chaos.py and scripts/bench_failover.py.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

from ..client.client import Client, summarize

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def free_ports(n):
    socks = []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


class ProcCluster:
    def __init__(self, n=3, workdir=None, election=(0.3, 0.6), heartbeat=0.1, fsync=False,
                 snapshot_threshold=1000, max_client_sessions=10000):
        self.n = n
        self.ids = [f"node{i}" for i in range(1, n + 1)]
        self.workdir = workdir or tempfile.mkdtemp(prefix="distributed-key-value-store-")
        self._own_workdir = workdir is None
        self.fsync = fsync
        ports = free_ports(n)
        self.addrs = {nid: f"127.0.0.1:{p}" for nid, p in zip(self.ids, ports)}
        self.config = {
            "election_timeout_min": election[0], "election_timeout_max": election[1],
            "heartbeat_interval": heartbeat, "request_timeout": 3.0,
            "snapshot_threshold": snapshot_threshold,
            "max_client_sessions": max_client_sessions,
            "nodes": {nid: {"host": "127.0.0.1", "port": p} for nid, p in zip(self.ids, ports)},
        }
        os.makedirs(os.path.join(self.workdir, "logs"), exist_ok=True)
        self.config_path = os.path.join(self.workdir, "cluster.json")
        with open(self.config_path, "w") as f:
            json.dump(self.config, f)
        self.procs: dict = {}
        self.paused: set = set()

    # ---- lifecycle ---------------------------------------------------------
    def start(self, node_id):
        if node_id in self.procs and self.procs[node_id].poll() is None:
            return
        cmd = [sys.executable, "-m", "distributed_key_value_store.server.main", "--id", node_id,
               "--config", self.config_path, "--data-dir", os.path.join(self.workdir, "data", node_id)]
        if not self.fsync:
            cmd.append("--no-fsync")
        log = open(os.path.join(self.workdir, "logs", f"{node_id}.log"), "ab")
        env = dict(os.environ, PYTHONPATH=ROOT + os.pathsep + os.environ.get("PYTHONPATH", ""))
        self.procs[node_id] = subprocess.Popen(cmd, stdout=log, stderr=log, cwd=ROOT, env=env,
                                               start_new_session=True)

    def start_all(self):
        for nid in self.ids:
            self.start(nid)
        return self

    def kill(self, node_id):
        """SIGKILL: a hard crash, no chance to flush or say goodbye."""
        p = self.procs.get(node_id)
        if p and p.poll() is None:
            if node_id in self.paused:
                p.send_signal(signal.SIGCONT)
                self.paused.discard(node_id)
            p.kill()
            p.wait()

    def pause(self, node_id):
        """SIGSTOP: the process freezes (like a hung node or full network blackhole)."""
        p = self.procs.get(node_id)
        if p and p.poll() is None and node_id not in self.paused:
            p.send_signal(signal.SIGSTOP)
            self.paused.add(node_id)

    def resume(self, node_id):
        p = self.procs.get(node_id)
        if p and node_id in self.paused:
            p.send_signal(signal.SIGCONT)
            self.paused.discard(node_id)

    def restart(self, node_id):
        self.kill(node_id)
        self.start(node_id)

    def is_running(self, node_id):
        p = self.procs.get(node_id)
        return bool(p and p.poll() is None and node_id not in self.paused)

    def stop_all(self):
        for nid in self.ids:
            self.kill(nid)

    def cleanup(self):
        self.stop_all()
        if self._own_workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)

    def __enter__(self):
        return self.start_all()

    def __exit__(self, *exc):
        self.cleanup()

    # ---- helpers -----------------------------------------------------------
    def client(self, **kw):
        return Client(self.addrs, **kw)

    def statuses(self, client=None):
        return (client or self.client()).status(timeout=0.3)

    def wait_for_leader(self, timeout=10.0, exclude=(), min_term=0):
        """Block until a leader (not in `exclude`, term >= min_term) is visible. Returns its id."""
        client = self.client()
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            state, leader, term = summarize(client.status(timeout=0.3))
            if state == "ok" and leader not in exclude and term >= min_term:
                return leader
            time.sleep(0.02)
        raise TimeoutError(f"no leader within {timeout}s; statuses={self.statuses(client)}")

    def wait_converged(self, nodes=None, timeout=15.0):
        """Wait until the given nodes (default: all running) report identical key-value state."""
        client = self.client()
        nodes = nodes or [n for n in self.ids if self.is_running(n)]
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            states = [client.debug_state(n) for n in nodes]
            if all(states):
                sigs = {(json.dumps(s["data"], sort_keys=True), s["last_applied"]) for s in states}
                if len(sigs) == 1:
                    return states[0]["data"]
            last = states
            time.sleep(0.1)
        raise TimeoutError(f"nodes did not converge: {last}")
