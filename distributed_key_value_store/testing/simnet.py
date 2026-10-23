"""Deterministic simulated cluster: fake clock, lossy/delayed network, partitions, crashes.

Used by the simulated tests to run thousands of randomized failure scenarios in seconds, and to
check Raft's safety invariants after every step:

  * election safety     - at most one leader per term
  * state machine safety - no two nodes ever apply different commands at the same index
  * leader completeness - a leader's log contains every entry committed in earlier terms
  * log matching        - if two logs agree on (index, term), they agree on everything before it
"""
from __future__ import annotations

import copy
import heapq
import itertools
import random

from ..raft.node import LEADER, RaftNode
from ..raft.storage import MemoryStorage


class SimCluster:
    def __init__(self, n=3, seed=1, election_timeout=(0.15, 0.30), heartbeat=0.05,
                 delay=(0.001, 0.01), drop=0.0, check=True):
        self.rng = random.Random(seed)
        self.now = 0.0
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.election_timeout, self.heartbeat = election_timeout, heartbeat
        self.delay, self.drop = delay, drop
        self.check_enabled = check
        self.storage = {i: MemoryStorage() for i in self.ids}
        self.nodes: dict = {}
        self.down: set = set()
        self.blocked: set = set()
        self._queue: list = []
        self._ctr = itertools.count()
        self._steps = 0
        self.committed: dict = {}       # index -> entry, as applied by any node
        self.leader_of_term: dict = {}  # term -> leader id
        for i in self.ids:
            self._boot(i)

    # ---- lifecycle ------------------------------------------------------
    def _boot(self, i):
        node = RaftNode(
            i, [p for p in self.ids if p != i], self.storage[i],
            election_timeout=self.election_timeout, heartbeat_interval=self.heartbeat,
            rng=random.Random(self.rng.random()), now=self.now,
        )
        node.on_apply = lambda idx, entry, res, _i=i: self._record_apply(_i, idx, entry)
        self.nodes[i] = node
        self.down.discard(i)

    def crash(self, i):
        self.down.add(i)

    def restart(self, i):
        self._boot(i)  # fresh volatile state, same storage

    def partition(self, group_a, group_b):
        for a in group_a:
            for b in group_b:
                self.blocked.add((a, b))
                self.blocked.add((b, a))

    def isolate(self, i):
        self.partition([i], [p for p in self.ids if p != i])

    def heal(self):
        self.blocked.clear()

    # ---- running --------------------------------------------------------
    def _send_all(self, node):
        for msg in node.drain():
            a, b = msg["from"], msg["to"]
            if (a, b) in self.blocked or self.rng.random() < self.drop:
                continue
            t = self.now + self.rng.uniform(*self.delay)
            heapq.heappush(self._queue, (t, next(self._ctr), copy.deepcopy(msg)))

    def step(self, dt=0.005):
        self.now += dt
        self._steps += 1
        while self._queue and self._queue[0][0] <= self.now:
            _, _, msg = heapq.heappop(self._queue)
            to = msg["to"]
            if to in self.down or (msg["from"], to) in self.blocked:
                continue
            self.nodes[to].receive(msg, self.now)
            self._send_all(self.nodes[to])
        for i in self.ids:
            if i not in self.down:
                self.nodes[i].tick(self.now)
                self._send_all(self.nodes[i])
        if self.check_enabled:
            self.check(full=self._steps % 50 == 0)

    def run(self, seconds):
        end = self.now + seconds
        while self.now < end:
            self.step()

    def run_until(self, predicate, limit=10.0):
        end = self.now + limit
        while self.now < end:
            if predicate():
                return True
            self.step()
        return predicate()

    # ---- queries / client ops -------------------------------------------
    def up_nodes(self):
        return [self.nodes[i] for i in self.ids if i not in self.down]

    def leaders(self):
        return [n for n in self.up_nodes() if n.role == LEADER]

    def leader(self):
        ls = self.leaders()
        return max(ls, key=lambda n: n.current_term) if ls else None

    def propose(self, command):
        """Propose on the current leader. Returns (node_id, index, term) or None."""
        node = self.leader()
        if node is None:
            return None
        res = node.propose(command, self.now)
        self._send_all(node)
        return (node.id, *res) if res else None

    def put(self, key, value, client="c1", seq=1):
        return self.propose({"op": "put", "key": key, "value": value, "client_id": client, "seq": seq})

    # ---- invariants -----------------------------------------------------
    def _record_apply(self, node_id, idx, entry):
        prev = self.committed.get(idx)
        if prev is None:
            self.committed[idx] = copy.deepcopy(entry)
        else:
            assert prev == entry, f"state machine safety violated at index {idx}: {prev} vs {entry} ({node_id})"

    def check(self, full=False):
        for i in self.ids:
            if i in self.down:
                continue
            node = self.nodes[i]
            if node.role == LEADER:
                prev = self.leader_of_term.setdefault(node.current_term, i)
                assert prev == i, f"two leaders in term {node.current_term}: {prev} and {i}"
                for idx, entry in self.committed.items():
                    if entry["term"] < node.current_term:
                        assert node.log.term_at(idx) == entry["term"] and node.log.get(idx) == entry, \
                            f"leader completeness violated: {i} (term {node.current_term}) missing committed index {idx}"
        if full:
            self.check_log_matching()

    def check_log_matching(self):
        live = [self.nodes[i] for i in self.ids if i not in self.down]
        for a, b in itertools.combinations(live, 2):
            upto = min(a.log.last_index(), b.log.last_index())
            for idx in range(upto, 0, -1):
                if a.log.term_at(idx) == b.log.term_at(idx):
                    assert a.log.entries[:idx] == b.log.entries[:idx], \
                        f"log matching violated between {a.id} and {b.id} at index {idx}"
                    break

    def converged(self):
        """True when every node is up, has the same log, and has applied all of it."""
        live = [self.nodes[i] for i in self.ids if i not in self.down]
        if len(live) != len(self.ids):
            return False
        first = live[0]
        return all(n.log.entries == first.log.entries and n.last_applied == n.log.last_index()
                   and n.commit_index == first.commit_index for n in live)
