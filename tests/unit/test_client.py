import threading

from distributed_key_value_store.client.client import Client


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
