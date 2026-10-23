"""Raft consensus core.

This module is pure logic: it never touches the network or the wall clock.

  * Time is passed in (`tick(now)`, `receive(msg, now)`), so tests can drive it with a fake clock.
  * Messages are plain JSON-able dicts. Handlers append outgoing messages to an outbox; the
    caller collects them with `drain()` and delivers them however it likes (HTTP in production,
    a simulated lossy network in tests).
  * The node is NOT thread-safe. The server wraps every call in one lock.

Follows Figure 2 of the Raft paper, plus: accelerated log backtracking, a no-op entry on
election, check-quorum (a leader that can't reach a majority steps down), and ReadIndex for
linearizable reads.
"""
from __future__ import annotations

import logging
import random
from typing import Callable, Optional

from .log import Log
from .state_machine import KVStateMachine

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers,
        storage,
        state_machine: Optional[KVStateMachine] = None,
        *,
        election_timeout=(0.3, 0.6),
        heartbeat_interval: float = 0.1,
        max_batch: int = 64,
        snapshot_threshold: int = 1000,
        rng: Optional[random.Random] = None,
        now: float = 0.0,
    ):
        self.id = node_id
        self.peers = sorted(peers)
        self.storage = storage
        self.sm = state_machine or KVStateMachine()
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.max_batch = max_batch
        if snapshot_threshold < 1:
            raise ValueError("snapshot_threshold must be positive")
        self.snapshot_threshold = snapshot_threshold
        self.rng = rng or random.Random()
        self.logger = logging.getLogger(f"distributed_key_value_store.raft.{node_id}")

        # persistent state (restored from storage)
        term, voted_for, entries = storage.load()
        self.current_term: int = term
        self.voted_for: Optional[str] = voted_for
        self.snapshot = storage.load_snapshot()
        base_index = storage.log_base_index
        base_term = self.snapshot["last_included_term"] if self.snapshot else 0
        self.log = Log(entries, base_index=base_index, base_term=base_term)

        # volatile state
        snapshot_index = self.snapshot["last_included_index"] if self.snapshot else 0
        self.commit_index = snapshot_index
        self.last_applied = snapshot_index
        if self.snapshot:
            self.sm.restore(self.snapshot["state"])
        self.role = FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes: set = set()

        # leader state
        self.next_index: dict = {}
        self.match_index: dict = {}
        self.peer_ack_seq: dict = {}    # highest heartbeat round each peer has acknowledged
        self.peer_last_ack: dict = {}   # last time each peer answered us (for check-quorum)
        self.hb_seq = 0                 # heartbeat round counter (used by ReadIndex)
        self.noop_index = 0             # index of the no-op appended when we became leader
        self.next_heartbeat = 0.0

        self.election_deadline = 0.0
        self.outbox: list = []
        self.on_apply: Optional[Callable[[int, dict, object], None]] = None
        self._reset_election_timer(now)

    # ------------------------------------------------------------------ helpers
    @property
    def quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def drain(self) -> list:
        out, self.outbox = self.outbox, []
        return out

    def _send(self, to: str, msg: dict) -> None:
        msg["from"] = self.id
        msg["to"] = to
        self.outbox.append(msg)

    def _reset_election_timer(self, now: float) -> None:
        self.election_deadline = now + self.rng.uniform(*self.election_timeout)

    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role,
            "term": self.current_term,
            "leader": self.leader_id,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": len(self.log.entries),
            "last_log_index": self.log.last_index(),
            "snapshot_index": self.log.base_index,
            "keys": len(self.sm.data),
            "client_sessions": len(self.sm.sessions),
            "max_client_sessions": self.sm.max_sessions,
            "peers": self.peers,
        }

    # ------------------------------------------------------------------ clock
    def tick(self, now: float) -> None:
        if self.role == LEADER:
            if self.peers and not self._has_quorum_contact(now):
                self.logger.info("term %d: lost contact with majority, stepping down", self.current_term)
                self._become_follower(self.current_term, now, force_timer=True)
            elif now >= self.next_heartbeat:
                self._broadcast_append(now)
        elif now >= self.election_deadline:
            self._start_election(now)

    def _has_quorum_contact(self, now: float) -> bool:
        window = self.election_timeout[1]
        alive = 1 + sum(1 for p in self.peers if now - self.peer_last_ack.get(p, -1e9) <= window)
        return alive >= self.quorum

    # ------------------------------------------------------------------ role changes
    def _become_follower(self, term: int, now: float, force_timer: bool = False) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.leader_id = None
            self.storage.save_meta(self.current_term, self.voted_for)
        if self.role != FOLLOWER or force_timer:
            if self.role == LEADER:
                self.leader_id = None
            self.role = FOLLOWER
            self._reset_election_timer(now)

    def _start_election(self, now: float) -> None:
        self.role = CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.storage.save_meta(self.current_term, self.voted_for)
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer(now)
        self.logger.info("term %d: election timeout, becoming candidate", self.current_term)
        if len(self.votes) >= self.quorum:  # single-node cluster
            self._become_leader(now)
            return
        for p in self.peers:
            self._send(p, {
                "type": "RequestVote",
                "term": self.current_term,
                "candidate_id": self.id,
                "last_log_index": self.log.last_index(),
                "last_log_term": self.log.last_term(),
            })

    def _become_leader(self, now: float) -> None:
        self.role = LEADER
        self.leader_id = self.id
        for p in self.peers:
            self.next_index[p] = self.log.last_index() + 1
            self.match_index[p] = 0
            self.peer_ack_seq[p] = 0
            self.peer_last_ack[p] = now
        self.logger.info("term %d: became leader", self.current_term)
        # A leader may only commit entries from its own term (Raft section 5.4.2), so append a
        # no-op; once it commits, everything before it is committed too.
        entry = {"term": self.current_term, "command": {"op": "noop"}}
        self.noop_index = self.log.append(entry)
        self.storage.append([entry])
        self._broadcast_append(now)
        self._advance_commit()

    # ------------------------------------------------------------------ replication (leader)
    def _broadcast_append(self, now: float) -> None:
        self.hb_seq += 1
        for p in self.peers:
            self._send_append(p)
        self.next_heartbeat = now + self.heartbeat_interval

    def _send_append(self, peer: str) -> None:
        ni = self.next_index[peer]
        if ni <= self.log.base_index:
            self._send_snapshot(peer)
            return
        prev = ni - 1
        self._send(peer, {
            "type": "AppendEntries",
            "term": self.current_term,
            "leader_id": self.id,
            "prev_log_index": prev,
            "prev_log_term": self.log.term_at(prev),
            "entries": self.log.slice(ni, self.max_batch),
            "leader_commit": self.commit_index,
            "seq": self.hb_seq,
        })

    def _send_snapshot(self, peer: str) -> None:
        if self.snapshot is None:
            raise RuntimeError("compacted log has no snapshot to replicate")
        self._send(peer, {
            "type": "InstallSnapshot",
            "term": self.current_term,
            "leader_id": self.id,
            "seq": self.hb_seq,
            "snapshot": self.snapshot,
        })

    def propose(self, command: dict, now: float):
        """Leader only: append a command and start replicating it.

        Returns (index, term), or None if this node is not the leader. The command is committed
        once last_applied >= index and log.term_at(index) is still the returned term.
        Caller must drain() afterwards.
        """
        if self.role != LEADER:
            return None
        entry = {"term": self.current_term, "command": command}
        index = self.log.append(entry)
        self.storage.append([entry])
        for p in self.peers:
            self._send_append(p)
        self._advance_commit()
        return index, self.current_term

    def _advance_commit(self) -> None:
        for n in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                break  # only entries from the current term commit by counting replicas
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= self.quorum:
                self.commit_index = n
                break
        self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            result = self.sm.apply(entry["command"])
            if self.on_apply:
                self.on_apply(self.last_applied, entry, result)
        if self.last_applied - self.log.base_index >= self.snapshot_threshold:
            self._create_snapshot()

    def _create_snapshot(self) -> None:
        index = self.last_applied
        term = self.log.term_at(index)
        if term is None or index <= self.log.base_index:
            raise RuntimeError("cannot snapshot an unavailable log entry")
        snapshot = {
            "last_included_index": index,
            "last_included_term": term,
            "state": self.sm.snapshot(),
        }
        suffix = self.log.slice(index + 1, self.log.last_index() - index)
        self.storage.save_snapshot(snapshot, suffix)
        self.log.compact(index, term)
        self.snapshot = snapshot

    # ------------------------------------------------------------------ linearizable reads
    def begin_read(self, now: float):
        """Leader only. Starts a ReadIndex read and returns a token for read_ready().

        Caller must drain() afterwards. Returns None if not the leader.
        """
        if self.role != LEADER:
            return None
        read_index = max(self.commit_index, self.noop_index)
        self._broadcast_append(now)  # a fresh heartbeat round proves we're still the leader
        return {"term": self.current_term, "read_index": read_index, "seq": self.hb_seq}

    def read_ready(self, token: dict) -> str:
        """'ok' once it's safe to read local state, 'wait' to check again later, 'lost' if deposed."""
        if self.role != LEADER or self.current_term != token["term"]:
            return "lost"
        acks = 1 + sum(1 for p in self.peers if self.peer_ack_seq.get(p, 0) >= token["seq"])
        if acks >= self.quorum and self.last_applied >= token["read_index"]:
            return "ok"
        return "wait"

    # ------------------------------------------------------------------ message handling
    def receive(self, msg: dict, now: float) -> None:
        if msg["term"] > self.current_term:
            self._become_follower(msg["term"], now)
        handler = {
            "RequestVote": self._on_request_vote,
            "RequestVoteReply": self._on_request_vote_reply,
            "AppendEntries": self._on_append_entries,
            "AppendEntriesReply": self._on_append_entries_reply,
            "InstallSnapshotReply": self._on_install_snapshot_reply,
            "InstallSnapshot": self._on_install_snapshot,
        }.get(msg.get("type"))
        if handler:
            handler(msg, now)

    def _on_request_vote(self, m: dict, now: float) -> None:
        grant = False
        if m["term"] == self.current_term:
            up_to_date = (m["last_log_term"], m["last_log_index"]) >= (
                self.log.last_term(), self.log.last_index())
            if self.voted_for in (None, m["candidate_id"]) and up_to_date:
                grant = True
                self.voted_for = m["candidate_id"]
                self.storage.save_meta(self.current_term, self.voted_for)  # persist BEFORE replying
                self._reset_election_timer(now)
        self.logger.info("term %d: %s vote for %s (voted_for=%s, my last=(t%d,i%d), theirs=(t%d,i%d))",
                         self.current_term, "GRANTED" if grant else "DENIED", m["candidate_id"], self.voted_for,
                         self.log.last_term(), self.log.last_index(), m["last_log_term"], m["last_log_index"])
        self._send(m["from"], {"type": "RequestVoteReply", "term": self.current_term, "granted": grant})

    def _on_request_vote_reply(self, m: dict, now: float) -> None:
        if self.role != CANDIDATE or m["term"] != self.current_term or not m["granted"]:
            return
        self.votes.add(m["from"])
        if len(self.votes) >= self.quorum:
            self._become_leader(now)

    def _on_append_entries(self, m: dict, now: float) -> None:
        reply = {"type": "AppendEntriesReply", "term": self.current_term,
                 "success": False, "seq": m["seq"], "match_index": 0}
        if m["term"] < self.current_term:
            self._send(m["from"], reply)
            return

        # a valid leader for our term exists
        self.role = FOLLOWER
        self.leader_id = m["leader_id"]
        self._reset_election_timer(now)

        prev_i, prev_t = m["prev_log_index"], m["prev_log_term"]
        if prev_i < self.log.base_index:
            reply["conflict_index"] = self.log.base_index + 1
            reply["conflict_term"] = None
            self._send(m["from"], reply)
            return
        if prev_i > self.log.last_index():
            reply["conflict_index"] = self.log.last_index() + 1
            reply["conflict_term"] = None
            self._send(m["from"], reply)
            return
        if self.log.term_at(prev_i) != prev_t:
            ct = self.log.term_at(prev_i)
            reply["conflict_term"] = ct
            reply["conflict_index"] = self.log.first_index_of_term(ct, prev_i)
            self._send(m["from"], reply)
            return

        truncated, appended = False, []
        for offset, entry in enumerate(m["entries"]):
            idx = prev_i + 1 + offset
            if idx <= self.log.last_index():
                if self.log.term_at(idx) == entry["term"]:
                    continue  # already have it (duplicate / reordered RPC): never truncate on a match
                self.log.truncate_from(idx)
                truncated = True
            self.log.append(entry)
            appended.append(entry)
        if truncated:
            self.storage.rewrite(self.log.entries)
        elif appended:
            self.storage.append(appended)  # persist BEFORE acknowledging

        last_new = prev_i + len(m["entries"])
        if m["leader_commit"] > self.commit_index:
            self.commit_index = max(self.commit_index, min(m["leader_commit"], last_new))
            self._apply_committed()

        reply["success"] = True
        reply["match_index"] = last_new
        self._send(m["from"], reply)

    def _on_install_snapshot(self, m: dict, now: float) -> None:
        snapshot = m["snapshot"]
        index = snapshot["last_included_index"]
        term = snapshot["last_included_term"]
        reply = {"type": "InstallSnapshotReply", "term": self.current_term,
                 "seq": m["seq"], "success": False, "match_index": 0}
        if m["term"] < self.current_term:
            self._send(m["from"], reply)
            return

        self.role = FOLLOWER
        self.leader_id = m["leader_id"]
        self._reset_election_timer(now)
        if index <= self.last_applied:
            reply["success"] = True
            reply["match_index"] = max(index, self.log.base_index)
            self._send(m["from"], reply)
            return

        retain_suffix = self.log.term_at(index) == term
        suffix = self.log.slice(index + 1, self.log.last_index() - index) if retain_suffix else []
        old_commit = self.commit_index
        self.storage.save_snapshot(snapshot, suffix)
        self.snapshot = snapshot
        self.log = Log(suffix, base_index=index, base_term=term)
        self.sm.restore(snapshot["state"])
        self.last_applied = index
        self.commit_index = max(index, min(old_commit, self.log.last_index())) if retain_suffix else index
        self._apply_committed()

        reply["success"] = True
        reply["match_index"] = index
        self._send(m["from"], reply)

    def _on_append_entries_reply(self, m: dict, now: float) -> None:
        if self.role != LEADER or m["term"] != self.current_term:
            return
        p = m["from"]
        if p not in self.next_index:
            return
        self.peer_last_ack[p] = now
        self.peer_ack_seq[p] = max(self.peer_ack_seq[p], m["seq"])

        if m["success"]:
            if m["match_index"] > self.match_index[p]:
                self.match_index[p] = m["match_index"]
            self.next_index[p] = self.match_index[p] + 1
            self._advance_commit()
            if self.next_index[p] <= self.log.last_index():
                self._send_append(p)  # more entries to ship (batch limit)
            return

        # rejected: back up next_index using the follower's conflict hint
        ct, ci = m.get("conflict_term"), m["conflict_index"]
        if ct is not None:
            last = self.log.last_index_of_term(ct)
            new_next = last + 1 if last else ci
        else:
            new_next = ci
        new_next = max(1, min(new_next, self.log.last_index() + 1), self.match_index[p] + 1)
        if new_next < self.next_index[p]:
            self.next_index[p] = new_next
            self._send_append(p)
        # otherwise wait for the next heartbeat rather than hot-looping

    def _on_install_snapshot_reply(self, m: dict, now: float) -> None:
        if self.role != LEADER or m["term"] != self.current_term:
            return
        p = m["from"]
        if p not in self.next_index or not m["success"]:
            return
        self.peer_last_ack[p] = now
        self.peer_ack_seq[p] = max(self.peer_ack_seq[p], m["seq"])
        self.match_index[p] = max(self.match_index[p], m["match_index"])
        self.next_index[p] = self.match_index[p] + 1
        self._advance_commit()
        if self.next_index[p] <= self.log.last_index():
            self._send_append(p)
