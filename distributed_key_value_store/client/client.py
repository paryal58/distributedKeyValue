"""Client library: finds the leader, follows redirects, retries safely.

Every write carries (client_id, seq). A retry re-sends the SAME seq, so if the first attempt
actually committed (e.g. the leader died right after), the state machine returns the cached
result instead of applying it twice. A client must have at most one request in flight.
"""
from __future__ import annotations

import http.client
import json
import time
import uuid
from typing import Optional
from urllib.parse import quote


class ClusterUnavailable(Exception):
    """No leader could serve the request before the deadline. For writes the outcome is unknown."""


class Client:
    def __init__(self, nodes: dict, client_id: Optional[str] = None, timeout: float = 10.0,
                 http_timeout: float = 5.0):
        """nodes: {node_id: "host:port"}"""
        self.nodes = dict(nodes)
        self.ids = list(self.nodes)
        self.client_id = client_id or uuid.uuid4().hex[:12]
        self.timeout = timeout
        self.http_timeout = http_timeout
        self.seq = 0
        self.target = self.ids[0]
        self.last_leader: Optional[str] = None
        self.last_term: Optional[int] = None

    @classmethod
    def from_config(cls, path: str, **kw):
        with open(path) as f:
            cfg = json.load(f)
        # clients always reach nodes via localhost: container hostnames aren't resolvable from the host
        host_of = lambda v: "127.0.0.1" if v["host"].startswith("node") else v["host"]
        return cls({k: f"{host_of(v)}:{v['port']}" for k, v in cfg["nodes"].items()}, **kw)

    # ---- low level ------------------------------------------------------
    def _http(self, node_id, method, path, body=None, timeout=None):
        host, port = self.nodes[node_id].rsplit(":", 1)
        conn = http.client.HTTPConnection(host, int(port), timeout=timeout or self.http_timeout)
        try:
            payload = json.dumps(body) if body is not None else None
            headers = {"Content-Type": "application/json"} if payload else {}
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            return resp.status, json.loads(resp.read() or b"{}")
        finally:
            conn.close()

    def _rotate(self):
        self.target = self.ids[(self.ids.index(self.target) + 1) % len(self.ids)]

    def _call(self, method, path, body=None):
        deadline = time.monotonic() + self.timeout
        last = "no attempt made"
        while time.monotonic() < deadline:
            try:
                status, data = self._http(self.target, method, path, body)
            except (OSError, http.client.HTTPException, ValueError) as exc:
                last = f"{self.target}: {exc.__class__.__name__}"
                self._rotate()
                time.sleep(0.05)
                continue
            if status == 421:                                   # not the leader
                leader = data.get("leader")
                if leader in self.nodes and leader != self.target:
                    self.target = leader
                else:
                    self._rotate()
                    time.sleep(0.1)                             # election in progress
                last = f"{self.target}: no leader known"
                continue
            if status in (503, 504):                            # lost leadership / timed out: retry same seq
                last = f"{self.target}: {data.get('error')}"
                time.sleep(0.1)
                continue
            if status >= 400:
                raise RuntimeError(f"{status} {data}")
            self.last_leader, self.last_term = data.get("leader"), data.get("term")
            return data
        raise ClusterUnavailable(f"gave up after {self.timeout}s ({last})")

    # ---- API ------------------------------------------------------------
    def put(self, key: str, value) -> None:
        self.seq += 1
        self._call("PUT", f"/kv/{quote(key, safe='')}",
                   {"value": value, "client_id": self.client_id, "seq": self.seq})

    def delete(self, key: str) -> bool:
        """Returns True if the key existed."""
        self.seq += 1
        data = self._call("DELETE", f"/kv/{quote(key, safe='')}",
                          {"client_id": self.client_id, "seq": self.seq})
        return bool(data["result"].get("existed"))

    def get(self, key: str, stale: bool = False, node: Optional[str] = None):
        """Linearizable read through the leader. stale=True reads one node's local state instead."""
        path = f"/kv/{quote(key, safe='')}"
        if stale:
            status, data = self._http(node or self.target, "GET", path + "?stale=1")
            return data["value"] if data.get("found") else None
        data = self._call("GET", path)
        return data["value"] if data.get("found") else None

    def status(self, timeout: float = 0.5) -> dict:
        """node_id -> /status dict, or None if the node is unreachable."""
        out = {}
        for nid in self.ids:
            try:
                out[nid] = self._http(nid, "GET", "/status", timeout=timeout)[1]
            except (OSError, http.client.HTTPException, ValueError):
                out[nid] = None
        return out

    def debug_state(self, node_id: str, timeout: float = 1.0):
        try:
            return self._http(node_id, "GET", "/debug/state", timeout=timeout)[1]
        except (OSError, http.client.HTTPException, ValueError):
            return None


def summarize(statuses: dict):
    """Returns (state, leader_id, term): state is 'ok', 'electing' or 'no_quorum'."""
    up = {k: v for k, v in statuses.items() if v}
    quorum = len(statuses) // 2 + 1
    if len(up) < quorum:
        return "no_quorum", None, None
    leaders = [v for v in up.values() if v["role"] == "leader"]
    if not leaders:
        return "electing", None, None
    best = max(leaders, key=lambda v: v["term"])   # a deposed leader may linger; trust the highest term
    return "ok", best["id"], best["term"]
