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
import re
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

MAX_BODY_BYTES = 1024 * 1024
MAX_RAFT_BODY_BYTES = 64 * 1024 * 1024
MAX_KEY_LENGTH = 1024


class _Server(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    request_queue_size = 256
    allow_reuse_address = True


def make_http_server(bind, port, raft_server, request_timeout=3.0):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.settimeout(5.0)
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
            if self.close_connection:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self, max_bytes=MAX_BODY_BYTES) -> Optional[bytes]:
            lengths = self.headers.get_all("Content-Length", [])
            if self.headers.get("Transfer-Encoding") is not None or len(lengths) > 1:
                self.close_connection = True
                self._reply(400, {"error": "invalid_request_framing"})
                return None
            if lengths:
                value = lengths[0].strip()
                if not value.isascii() or not value.isdecimal():
                    self.close_connection = True
                    self._reply(400, {"error": "invalid_content_length"})
                    return None
                length = int(value)
            else:
                length = 0
            if length > max_bytes:
                self.close_connection = True
                self._reply(413, {"error": "request_body_too_large",
                                  "limit": max_bytes})
                return None
            body = self.rfile.read(length) if length else b""
            if len(body) != length:
                self.close_connection = True
                return None
            return body

        def _key(self):
            path = urlparse(self.path).path
            if not path.startswith("/kv/") or len(path) <= 4:
                return None
            encoded = path[len("/kv/"):]
            if re.search(r"%(?![0-9a-fA-F]{2})", encoded):
                raise ValueError("invalid percent encoding")
            key = unquote(encoded, errors="strict")
            if not key or len(key) > MAX_KEY_LENGTH:
                raise ValueError("invalid key length")
            return key

        def _request_key(self):
            try:
                return self._key()
            except (UnicodeDecodeError, ValueError):
                self.close_connection = True
                self._reply(400, {"error": "invalid_key"})
                return None

        @staticmethod
        def _valid_client_metadata(client_id, seq):
            if client_id is None and seq is None:
                return True
            return (isinstance(client_id, str) and 0 < len(client_id) <= 128
                    and isinstance(seq, int) and not isinstance(seq, bool) and seq > 0)

        @staticmethod
        def _is_nonnegative_int(value):
            return isinstance(value, int) and not isinstance(value, bool) and value >= 0

        @classmethod
        def _valid_entry(cls, entry):
            if not isinstance(entry, dict) or not cls._is_nonnegative_int(entry.get("term")):
                return False
            command = entry.get("command")
            if not isinstance(command, dict):
                return False
            op = command.get("op")
            if op == "noop":
                return True
            key = command.get("key")
            if (op not in ("put", "delete") or not isinstance(key, str)
                    or not key or len(key) > MAX_KEY_LENGTH):
                return False
            if op == "put" and "value" not in command:
                return False
            return cls._valid_client_metadata(command.get("client_id"), command.get("seq"))

        @classmethod
        def _valid_snapshot(cls, snapshot, node):
            if not isinstance(snapshot, dict):
                return False
            index = snapshot.get("last_included_index")
            term = snapshot.get("last_included_term")
            state = snapshot.get("state")
            if (not cls._is_nonnegative_int(index) or index == 0
                    or not cls._is_nonnegative_int(term) or not isinstance(state, dict)):
                return False
            data, sessions = state.get("data"), state.get("sessions")
            if not isinstance(data, dict) or not isinstance(sessions, dict):
                return False
            if any(not isinstance(key, str) or not key or len(key) > MAX_KEY_LENGTH
                   for key in data):
                return False
            return all(
                isinstance(client_id, str) and 0 < len(client_id) <= 128
                and isinstance(record, list) and len(record) == 2
                and isinstance(record[0], int) and not isinstance(record[0], bool)
                and record[0] > 0
                for client_id, record in sessions.items()
            ) and len(sessions) <= node.sm.max_sessions

        @classmethod
        def _valid_raft_message(cls, msg, node):
            if not isinstance(msg, dict):
                return False
            msg_type = msg.get("type")
            required = {
                "RequestVote": ("candidate_id", "last_log_index", "last_log_term"),
                "RequestVoteReply": ("granted",),
                "AppendEntries": ("leader_id", "prev_log_index", "prev_log_term",
                                  "entries", "leader_commit", "seq"),
                "AppendEntriesReply": ("success", "seq", "match_index"),
                "InstallSnapshot": ("leader_id", "seq", "snapshot"),
                "InstallSnapshotReply": ("success", "seq", "match_index"),
            }.get(msg_type)
            if (required is None or not isinstance(msg.get("from"), str)
                    or msg["from"] not in node.peers or msg.get("to") != node.id
                    or not cls._is_nonnegative_int(msg.get("term"))
                    or any(field not in msg for field in required)):
                return False
            if msg_type == "RequestVote":
                return (msg["candidate_id"] == msg["from"]
                        and cls._is_nonnegative_int(msg["last_log_index"])
                        and cls._is_nonnegative_int(msg["last_log_term"])
                        and msg["last_log_term"] <= msg["term"])
            if msg_type == "RequestVoteReply":
                return isinstance(msg["granted"], bool)
            if msg_type == "AppendEntries":
                return (msg["leader_id"] == msg["from"]
                        and cls._is_nonnegative_int(msg["prev_log_index"])
                        and cls._is_nonnegative_int(msg["prev_log_term"])
                        and msg["prev_log_term"] <= msg["term"]
                        and cls._is_nonnegative_int(msg["leader_commit"])
                        and cls._is_nonnegative_int(msg["seq"])
                        and isinstance(msg["entries"], list) and len(msg["entries"]) <= 64
                        and all(cls._valid_entry(entry) and entry["term"] <= msg["term"]
                                for entry in msg["entries"])
                        and all(left["term"] <= right["term"]
                                for left, right in zip(
                                    [{"term": msg["prev_log_term"]}] + msg["entries"],
                                    msg["entries"])))
            if msg_type == "AppendEntriesReply":
                if (not isinstance(msg["success"], bool)
                        or not cls._is_nonnegative_int(msg["seq"])
                        or not cls._is_nonnegative_int(msg["match_index"])):
                    return False
                if msg["success"]:
                    return True
                conflict_index = msg.get("conflict_index")
                conflict_term = msg.get("conflict_term")
                return (cls._is_nonnegative_int(conflict_index) and conflict_index > 0
                        and (conflict_term is None
                             or cls._is_nonnegative_int(conflict_term)))
            if msg_type == "InstallSnapshot":
                return (msg["leader_id"] == msg["from"]
                        and cls._is_nonnegative_int(msg["seq"])
                        and cls._valid_snapshot(msg["snapshot"], node)
                        and msg["snapshot"]["last_included_term"] <= msg["term"])
            return (isinstance(msg["success"], bool)
                    and cls._is_nonnegative_int(msg["seq"])
                    and cls._is_nonnegative_int(msg["match_index"]))

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
            body = self._read_body(
                MAX_RAFT_BODY_BYTES if urlparse(self.path).path == "/raft" else MAX_BODY_BYTES)
            if body is None:
                return
            if urlparse(self.path).path != "/raft":
                return self._reply(404, {"error": "not_found"})
            try:
                msg = json.loads(body)
            except (UnicodeDecodeError, ValueError):
                return self._reply(400, {"error": "bad_message"})
            if not self._valid_raft_message(msg, raft_server.node):
                return self._reply(400, {"error": "bad_message"})
            try:
                raft_server.deliver(msg)
            except (ValueError, KeyError, TypeError):
                return self._reply(400, {"error": "bad_message"})
            self._reply(200, {})

        def do_PUT(self):
            raw = self._read_body()
            if raw is None:
                return
            key = self._request_key()
            if key is None:
                if not self.close_connection:
                    return self._reply(404, {"error": "not_found"})
                return
            try:
                value = raw.decode("utf-8")
            except UnicodeDecodeError:
                return self._reply(400, {"error": "invalid_utf8"})
            cid, seq = None, None
            content_type = self.headers.get_content_type()
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and "value" in parsed:
                    value, cid, seq = parsed["value"], parsed.get("client_id"), parsed.get("seq")
                elif content_type == "application/json":
                    return self._reply(400, {"error": "invalid_put_body"})
            except (UnicodeDecodeError, ValueError):
                if content_type == "application/json":
                    return self._reply(400, {"error": "invalid_json"})
            if not self._valid_client_metadata(cid, seq):
                return self._reply(400, {"error": "invalid_client_metadata"})
            cmd = {"op": "put", "key": key, "value": value, "client_id": cid, "seq": seq}
            self._outcome(*raft_server.write(cmd, request_timeout))

        def do_DELETE(self):
            raw = self._read_body()
            if raw is None:
                return
            key = self._request_key()
            if key is None:
                if not self.close_connection:
                    return self._reply(404, {"error": "not_found"})
                return
            cid, seq = None, None
            try:
                parsed = json.loads(raw) if raw else {}
            except (UnicodeDecodeError, ValueError):
                if self.headers.get_content_type() == "application/json":
                    return self._reply(400, {"error": "invalid_json"})
                parsed = {}
            if not isinstance(parsed, dict):
                return self._reply(400, {"error": "invalid_delete_body"})
            cid, seq = parsed.get("client_id"), parsed.get("seq")
            if not self._valid_client_metadata(cid, seq):
                return self._reply(400, {"error": "invalid_client_metadata"})
            cmd = {"op": "delete", "key": key, "client_id": cid, "seq": seq}
            self._outcome(*raft_server.write(cmd, request_timeout))

        def do_GET(self):
            body = self._read_body()
            if body is None:
                return
            if body:
                return self._reply(400, {"error": "unexpected_request_body"})
            url = urlparse(self.path)
            if url.path == "/status":
                return self._reply(200, raft_server.status())
            if url.path == "/debug/state":
                return self._reply(200, raft_server.debug_state())
            key = self._request_key()
            if key is None:
                if not self.close_connection:
                    return self._reply(404, {"error": "not_found"})
                return
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
