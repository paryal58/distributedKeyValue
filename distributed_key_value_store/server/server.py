"""Runs a RaftNode: ticks the clock, delivers messages, and lets client requests wait for commits.

One condition variable (wrapping one re-entrant lock) guards the node. HTTP handler threads call
write()/read() and block on the condition until their entry is committed and applied.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict

from ..raft.node import LEADER


class RaftServer:
    RESULT_CACHE = 20000

    def __init__(self, node, transport, tick_interval: float = 0.01):
        self.node = node
        self.transport = transport
        self.tick_interval = tick_interval
        self.cond = threading.Condition(threading.RLock())
        self.results: OrderedDict = OrderedDict()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="raft-ticker", daemon=True)
        node.on_apply = self._on_apply

    # ---- lifecycle -------------------------------------------------------
    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self.transport.close()
        self.node.storage.close()

    def _run(self):
        while not self._stop.is_set():
            with self.cond:
                self.node.tick(time.monotonic())
                self._flush()
                self.cond.notify_all()
            time.sleep(self.tick_interval)

    # ---- internals (call with the lock held) -----------------------------
    def _flush(self):
        for m in self.node.drain():
            self.transport.send(m["to"], m)

    def _on_apply(self, index, entry, result):
        self.results[index] = (entry["term"], result)
        while len(self.results) > self.RESULT_CACHE:
            self.results.popitem(last=False)

    # ---- inbound Raft RPCs ----------------------------------------------
    def deliver(self, msg: dict):
        with self.cond:
            self.node.receive(msg, time.monotonic())
            self._flush()
            self.cond.notify_all()

    # ---- client operations ----------------------------------------------
    def write(self, command: dict, timeout: float):
        """Returns (status, payload). status: ok | not_leader | lost | timeout."""
        deadline = time.monotonic() + timeout
        with self.cond:
            if self.node.role != LEADER:
                return "not_leader", self.node.leader_id
            index, term = self.node.propose(command, time.monotonic())
            self._flush()
            while True:
                if self.node.log.term_at(index) != term:
                    return "lost", None          # our entry was overwritten: definitely NOT applied
                if self.node.last_applied >= index:
                    return "ok", self.results.get(index, (term, {"ok": True}))[1]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return "timeout", None       # outcome unknown: may still commit
                self.cond.wait(remaining)

    def read(self, key: str, timeout: float):
        """Linearizable read via ReadIndex. Returns (status, (found, value))."""
        deadline = time.monotonic() + timeout
        with self.cond:
            token = self.node.begin_read(time.monotonic())
            if token is None:
                return "not_leader", self.node.leader_id
            self._flush()
            while True:
                state = self.node.read_ready(token)
                if state == "ok":
                    return "ok", self.node.sm.get(key)
                if state == "lost":
                    return "lost", None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return "timeout", None
                self.cond.wait(remaining)

    def stale_read(self, key: str):
        """Reads this node's local state with no leadership check; may be arbitrarily stale."""
        with self.cond:
            return self.node.sm.get(key)

    def status(self) -> dict:
        with self.cond:
            return self.node.status()

    def debug_state(self) -> dict:
        with self.cond:
            return {"data": dict(self.node.sm.data), **self.node.status()}
