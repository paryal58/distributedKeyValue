import threading

import pytest

from distributed_key_value_store.client.client import Client, summarize


def test_concurrent_writes_on_one_client_are_serialized():
    client = Client({"n1": "127.0.0.1:5001"}, client_id="shared-client")
    first_request_started = threading.Event()
    release_first_request = threading.Event()
    second_request_started = threading.Event()
    calls = []
    errors = []

    def fake_call(method, path, body=None):
        calls.append((method, path, body))
        if body["seq"] == 1:
            first_request_started.set()
            assert release_first_request.wait(2)
        else:
            second_request_started.set()
        return {"result": {"ok": True}}

    client._call = fake_call

    def put(key):
        try:
            client.put(key, key)
        except Exception as exc:
            errors.append(exc)

    first = threading.Thread(target=put, args=("first",))
    second = threading.Thread(target=put, args=("second",))
    first.start()
    assert first_request_started.wait(1)
    second.start()

    assert not second_request_started.wait(0.1)
    assert client.seq == 1

    release_first_request.set()
    first.join(2)
    second.join(2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert [(call[1], call[2]["seq"]) for call in calls] == [
        ("/kv/first", 1),
        ("/kv/second", 2),
    ]


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ({}, ("no_quorum", None, None)),
        ({"n1": {"role": "leader", "term": 2}, "n2": None, "n3": None},
         ("no_quorum", None, None)),
        ({"n1": {"role": "follower", "term": 2}, "n2": {"role": "follower", "term": 2},
          "n3": None}, ("electing", None, None)),
        ({"n1": {"id": "n1", "role": "leader", "term": 2},
          "n2": {"id": "n2", "role": "follower", "term": 3},
          "n3": {"id": "n3", "role": "leader", "term": 4}},
         ("ok", "n3", 4)),
    ],
)
def test_summarize_cluster_status_handles_quorum_and_competing_leaders(statuses, expected):
    assert summarize(statuses) == expected


def test_put_escapes_all_reserved_characters_in_key():
    client = Client({"n1": "127.0.0.1:5001"})
    requests = []
    client._call = lambda method, path, body=None: requests.append((method, path, body))

    client.put("team/a b?c#d", "value")

    assert requests[0][0:2] == ("PUT", "/kv/team%2Fa%20b%3Fc%23d")


def test_write_sequence_callback_runs_before_request():
    observed = []
    client = Client({"n1": "127.0.0.1:5001"}, on_sequence=lambda seq: observed.append(
        (seq, client.seq)))
    client._call = lambda method, path, body=None: {"result": {"ok": True}}

    client.put("key", "value")
    assert observed == [(1, 1)]
