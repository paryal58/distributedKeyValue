import http.client
import json
import threading

import pytest

from distributed_key_value_store.raft.state_machine import KVStateMachine
from distributed_key_value_store.server.http_api import MAX_BODY_BYTES, make_http_server


class FakeNode:
    id = "n1"
    peers = ["n2"]
    current_term = 1
    sm = KVStateMachine()


class FakeRaftServer:
    node = FakeNode()

    def __init__(self):
        self.commands = []
        self.messages = []

    def write(self, command, _timeout):
        self.commands.append(command)
        return "ok", {"ok": True}

    def read(self, _key, _timeout):
        return "ok", (False, None)

    def stale_read(self, _key):
        return False, None

    def deliver(self, message):
        self.messages.append(message)

    def status(self):
        return {"id": self.node.id}

    def debug_state(self):
        return {"data": {}}


@pytest.fixture
def http_api():
    raft = FakeRaftServer()
    server = make_http_server("127.0.0.1", 0, raft)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield raft, server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def request(port, method, path, body=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        return response.status, json.loads(response.read())
    finally:
        conn.close()


def test_put_accepts_documented_raw_text_and_json_envelope(http_api):
    raft, port = http_api
    assert request(port, "PUT", "/kv/raw", "Mbappe")[0] == 200
    assert raft.commands[-1]["value"] == "Mbappe"

    payload = json.dumps({"value": {"goals": 3}, "client_id": "client-1", "seq": 1})
    assert request(port, "PUT", "/kv/json", payload,
                   {"Content-Type": "application/json"})[0] == 200
    assert raft.commands[-1]["value"] == {"goals": 3}


@pytest.mark.parametrize("body", ["{", "[]", '{"client_id":"c","seq":true,"value":1}'])
def test_put_rejects_invalid_json_or_client_metadata(http_api, body):
    _, port = http_api
    status, _ = request(port, "PUT", "/kv/key", body,
                        {"Content-Type": "application/json"})
    assert status == 400


def test_rejects_invalid_key_encodings_and_oversized_keys(http_api):
    _, port = http_api
    assert request(port, "GET", "/kv/%GG")[0] == 400
    assert request(port, "GET", f"/kv/{'k' * 1025}")[0] == 400


def test_rejects_oversized_body_before_reading_it(http_api):
    _, port = http_api
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        conn.putrequest("PUT", "/kv/key")
        conn.putheader("Content-Length", str(MAX_BODY_BYTES + 1))
        conn.endheaders()
        response = conn.getresponse()
        assert response.status == 413
        assert response.getheader("Connection") == "close"
    finally:
        conn.close()


def test_rejects_transfer_encoding_and_malformed_raft_messages(http_api):
    raft, port = http_api
    status, _ = request(port, "POST", "/raft", headers={"Transfer-Encoding": "chunked"})
    assert status == 400
    status, _ = request(port, "POST", "/raft", "{", {"Content-Type": "application/json"})
    assert status == 400
    assert raft.messages == []


def test_accepts_valid_peer_message_but_rejects_unconfigured_sender(http_api):
    raft, port = http_api
    message = {
        "type": "AppendEntries", "from": "n2", "to": "n1", "term": 2,
        "leader_id": "n2", "prev_log_index": 0, "prev_log_term": 0,
        "entries": [], "leader_commit": 0, "seq": 1,
    }
    status, _ = request(port, "POST", "/raft", json.dumps(message),
                        {"Content-Type": "application/json"})
    assert status == 200
    assert raft.messages == [message]

    message["from"] = "unknown"
    message["leader_id"] = "unknown"
    assert request(port, "POST", "/raft", json.dumps(message),
                   {"Content-Type": "application/json"})[0] == 400
    assert len(raft.messages) == 1
