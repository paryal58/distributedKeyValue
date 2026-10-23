"""The replicated state machine: a dict plus a per-client dedup table.

Every command carries (client_id, seq). If a client retries a request that was already
applied, the cached result is returned instead of applying it twice (exactly-once writes).
Clients must have at most one request in flight, with increasing seq numbers.
"""
from __future__ import annotations

import copy


class KVStateMachine:
    DEFAULT_MAX_SESSIONS = 10000

    def __init__(self, max_sessions: int = DEFAULT_MAX_SESSIONS):
        if max_sessions < 1:
            raise ValueError("max_sessions must be positive")
        self.max_sessions = max_sessions
        self.data: dict = {}
        self.sessions: dict = {}  # client_id -> (last_seq, last_result)

    def apply(self, command: dict):
        op = command.get("op")
        if op == "noop":
            return None
        cid, seq = command.get("client_id"), command.get("seq")
        if cid is not None and seq is not None:
            last = self.sessions.get(cid)
            if last is not None and seq <= last[0]:
                return last[1] if seq == last[0] else {"ok": True, "stale": True}
            if last is None and len(self.sessions) >= self.max_sessions:
                return {"ok": False, "error": "session_limit", "limit": self.max_sessions}
        result = self._execute(command)
        if cid is not None and seq is not None:
            self.sessions[cid] = (seq, result)
        return result

    def snapshot(self) -> dict:
        return {"data": copy.deepcopy(self.data),
                "sessions": {cid: [seq, copy.deepcopy(result)]
                             for cid, (seq, result) in self.sessions.items()}}

    def restore(self, snapshot: dict) -> None:
        data = snapshot["data"]
        sessions = snapshot["sessions"]
        if len(sessions) > self.max_sessions:
            raise ValueError("snapshot exceeds configured client session limit")
        self.data = copy.deepcopy(data)
        self.sessions = {
            cid: (seq, copy.deepcopy(result))
            for cid, (seq, result) in sessions.items()
        }

    def _execute(self, command: dict):
        op, key = command["op"], command["key"]
        if op == "put":
            self.data[key] = command["value"]
            return {"ok": True}
        if op == "delete":
            existed = key in self.data
            self.data.pop(key, None)
            return {"ok": True, "existed": existed}
        raise ValueError(f"unknown op {op!r}")

    def get(self, key):
        """Returns (found, value)."""
        if key in self.data:
            return True, self.data[key]
        return False, None
