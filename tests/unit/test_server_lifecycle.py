import threading
import time

from distributed_key_value_store.server.server import RaftServer
from distributed_key_value_store.server.transport import HttpTransport


class BlockingNode:
    def __init__(self, events):
        self.events = events
        self.storage = self
        self.on_apply = None
        self.tick_started = threading.Event()
        self.release_tick = threading.Event()

    def tick(self, _now):
        self.tick_started.set()
        self.release_tick.wait(2)
        self.events.append("tick-finished")

    def drain(self):
        return []

    def close(self):
        self.events.append("storage-closed")


class TrackingTransport:
    def __init__(self, events):
        self.events = events

    def send(self, _to, _message):
        pass

    def close(self):
        self.events.append("transport-closed")


def test_stop_joins_ticker_before_transport_and_storage_close():
    events = []
    node = BlockingNode(events)
    raft = RaftServer(node, TrackingTransport(events), tick_interval=0.01)
    raft.start()
    assert node.tick_started.wait(1)

    stopped = threading.Event()
    stop_thread = threading.Thread(target=lambda: (raft.stop(), stopped.set()))
    stop_thread.start()
    time.sleep(0.05)
    assert not stopped.is_set()
    assert events == []

    node.release_tick.set()
    stop_thread.join(2)
    assert not stop_thread.is_alive()
    assert events == ["tick-finished", "transport-closed", "storage-closed"]

    raft.stop()
    assert events == ["tick-finished", "transport-closed", "storage-closed"]


def test_transport_close_joins_sender_without_blocking_on_full_queue(monkeypatch):
    request_started = threading.Event()
    release_request = threading.Event()
    connection_closed = threading.Event()

    class BlockingConnection:
        def __init__(self, _host, _port, timeout):
            self.timeout = timeout

        def request(self, *_args, **_kwargs):
            request_started.set()
            release_request.wait(2)

        def getresponse(self):
            class Response:
                @staticmethod
                def read():
                    return b""

            return Response()

        def close(self):
            connection_closed.set()

    monkeypatch.setattr(
        "distributed_key_value_store.server.transport.http.client.HTTPConnection",
        BlockingConnection,
    )
    transport = HttpTransport({"peer": ("127.0.0.1", 1)}, queue_size=1)
    transport.send("peer", {"type": "first"})
    assert request_started.wait(1)
    transport.send("peer", {"type": "queued"})

    closed = threading.Event()
    close_thread = threading.Thread(target=lambda: (transport.close(), closed.set()))
    close_thread.start()
    time.sleep(0.05)
    assert not closed.is_set()

    release_request.set()
    close_thread.join(2)
    assert not close_thread.is_alive()
    assert closed.is_set()
    assert connection_closed.is_set()
    assert all(not thread.is_alive() for thread in transport._threads)
