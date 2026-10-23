"""HTTP API.

  POST   /raft           node-to-node Raft messages (internal)
  PUT    /kv/<key>       body: {"value": ..., "client_id": ..., "seq": ...}  (or just the raw value)
  GET    /kv/<key>       linearizable read via the leader; add ?stale=1 for a local, possibly stale read
  DELETE /kv/<key>
  GET    /status         role, term, leader, commit index, ...
  GET    /debug/state    this node's full key-value contents (local, for tests)

Errors: 421 {"error":"not_leader","leader":<id|null>}, 503 leadership_lost, 504 timeout.
"""
from __future__ import annotations

import json
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256
    allow_reuse_address = True


def make_http_server(bind, port, raft_server, request_timeout=3.0):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            # Headers and body go out as two small writes; without NODELAY, Nagle + delayed ACK
            # adds ~40 ms to every request, and the serial per-peer sender threads back up.
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def log_message(self, *args):  # keep the console quiet; Raft events are logged separately
            pass

        # ---- plumbing ----
        def _reply(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def _key(self):
            path = urlparse(self.path).path
            return unquote(path[len("/kv/"):]) if path.startswith("/kv/") and len(path) > 4 else None

        def _outcome(self, status, payload):
            node = raft_server.node
            if status == "ok":
                if isinstance(payload, dict) and payload.get("ok") is False:
                    return self._reply(429, {"error": payload.get("error"),
                                             "limit": payload.get("limit")})
                return self._reply(200, {"ok": True, "result": payload, "leader": node.id, "term": node.current_term})
            if status == "not_leader":
                return self._reply(421, {"error": "not_leader", "leader": payload})
            if status == "lost":
                return self._reply(503, {"error": "leadership_lost", "outcome": "not_applied"})
            return self._reply(504, {"error": "timeout", "outcome": "unknown"})

        # ---- routes ----
        def do_POST(self):
            body = self._read_body()
            if urlparse(self.path).path != "/raft":
                return self._reply(404, {"error": "not_found"})
            try:
                raft_server.deliver(json.loads(body))
            except (ValueError, KeyError):
                return self._reply(400, {"error": "bad_message"})
            self._reply(200, {})

        def do_PUT(self):
            raw = self._read_body()
            key = self._key()
            if key is None:
                return self._reply(404, {"error": "not_found"})
            value, cid, seq = raw.decode(errors="replace"), None, None
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and "value" in parsed:
                    value, cid, seq = parsed["value"], parsed.get("client_id"), parsed.get("seq")
            except ValueError:
                pass
            cmd = {"op": "put", "key": key, "value": value, "client_id": cid, "seq": seq}
            self._outcome(*raft_server.write(cmd, request_timeout))

        def do_DELETE(self):
            raw = self._read_body()
            key = self._key()
            if key is None:
                return self._reply(404, {"error": "not_found"})
            cid, seq = None, None
            try:
                parsed = json.loads(raw) if raw else {}
                cid, seq = parsed.get("client_id"), parsed.get("seq")
            except (ValueError, AttributeError):
                pass
            cmd = {"op": "delete", "key": key, "client_id": cid, "seq": seq}
            self._outcome(*raft_server.write(cmd, request_timeout))

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/status":
                return self._reply(200, raft_server.status())
            if url.path == "/debug/state":
                return self._reply(200, raft_server.debug_state())
            key = self._key()
            if key is None:
                return self._reply(404, {"error": "not_found"})
            node = raft_server.node
            if parse_qs(url.query).get("stale") == ["1"]:
                found, value = raft_server.stale_read(key)
                return self._reply(200, {"ok": True, "found": found, "value": value, "stale": True, "node": node.id})
            status, payload = raft_server.read(key, request_timeout)
            if status == "ok":
                found, value = payload
                return self._reply(200, {"ok": True, "found": found, "value": value,
                                         "leader": node.id, "term": node.current_term})
            self._outcome(status, payload)

    return _Server((bind, port), Handler)
