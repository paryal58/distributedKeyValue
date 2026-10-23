"""Node-to-node transport: fire-and-forget JSON over HTTP.

Raft tolerates lost, delayed and reordered messages, so sends never block the node and failures
just drop the message. Each peer gets its own queue and sender thread with a keep-alive
connection, so one dead or frozen peer can't slow down traffic to the others.
"""
from __future__ import annotations

import http.client
import json
import queue
import threading
import time

MAX_AGE = 1.0  # drop queued messages older than this; stale heartbeats are worthless


class HttpTransport:
    def __init__(self, peers: dict, connect_timeout: float = 0.5, queue_size: int = 1000):
        """peers: node_id -> (host, port)"""
        self.peers = peers
        self.timeout = connect_timeout
        self._queues = {p: queue.Queue(maxsize=queue_size) for p in peers}
        self._stop = threading.Event()
        self._threads = [
            threading.Thread(target=self._worker, args=(p,), name=f"send-{p}", daemon=True)
            for p in peers
        ]
        for t in self._threads:
            t.start()

    def send(self, to: str, msg: dict) -> None:
        q = self._queues.get(to)
        if q is None:
            return
        item = (time.monotonic(), msg)
        try:
            q.put_nowait(item)
        except queue.Full:
            try:
                q.get_nowait()  # shed the oldest
                q.put_nowait(item)
            except (queue.Empty, queue.Full):
                pass

    def close(self) -> None:
        self._stop.set()
        for q in self._queues.values():
            q.put(None)

    def _worker(self, peer: str) -> None:
        host, port = self.peers[peer]
        q = self._queues[peer]
        conn = None
        while not self._stop.is_set():
            item = q.get()
            if item is None:
                break
            enqueued, msg = item
            if time.monotonic() - enqueued > MAX_AGE:
                continue
            payload = json.dumps(msg)
            sent = False
            for _attempt in range(2):
                fresh = conn is None
                try:
                    if conn is None:
                        conn = http.client.HTTPConnection(host, port, timeout=self.timeout)
                    conn.request("POST", "/raft", body=payload,
                                 headers={"Content-Type": "application/json"})
                    conn.getresponse().read()
                    sent = True
                    break
                except Exception:
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
                    conn = None
                    if fresh:
                        break  # a brand-new connection failed: the peer really is unreachable
                    # otherwise the keep-alive connection was stale (peer restarted): retry once fresh
            if not sent:
                # peer is unreachable: discard the backlog instead of replaying stale messages
                try:
                    while True:
                        q.get_nowait()
                except queue.Empty:
                    pass
                time.sleep(0.02)
