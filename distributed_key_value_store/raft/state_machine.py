"""The replicated state machine: a dict plus a per-client dedup table.

Every command carries (client_id, seq). If a client retries a request that was already
applied, the cached result is returned instead of applying it twice (exactly-once writes).
Clients must have at most one request in flight, with increasing seq numbers.
"""
from __future__ import annotations


class KVStateMachine:
    def __init__(self):
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
        result = self._execute(command)
        if cid is not None and seq is not None:
            self.sessions[cid] = (seq, result)
        return result

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
