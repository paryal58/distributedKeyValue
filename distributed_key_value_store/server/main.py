"""Start one distributed key-value store node:  python -m distributed_key_value_store.server.main --id node1 --config config/local-3.json"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading

from ..raft.node import RaftNode
from ..raft.storage import FileStorage
from .http_api import make_http_server
from .server import RaftServer
from .transport import HttpTransport


def main(argv=None):
    ap = argparse.ArgumentParser(description="Distributed key-value store: a Raft-replicated node")
    ap.add_argument("--id", required=True, help="this node's id, e.g. node1")
    ap.add_argument("--config", required=True, help="cluster config JSON")
    ap.add_argument("--data-dir", default=None, help="where to persist term/vote/log (default data/<id>)")
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--no-fsync", action="store_true", help="skip fsync (faster, NOT crash-safe; for tests)")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), stream=sys.stderr,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    with open(args.config) as f:
        cfg = json.load(f)
    nodes = cfg["nodes"]
    if args.id not in nodes:
        ap.error(f"{args.id!r} is not in {args.config}")
    peers = {k: (v["host"], v["port"]) for k, v in nodes.items() if k != args.id}

    storage = FileStorage(args.data_dir or os.path.join("data", args.id), fsync=not args.no_fsync)
    node = RaftNode(
        args.id, list(peers), storage,
        election_timeout=(cfg.get("election_timeout_min", 0.3), cfg.get("election_timeout_max", 0.6)),
        heartbeat_interval=cfg.get("heartbeat_interval", 0.1),
        now=__import__("time").monotonic(),
    )
    raft = RaftServer(node, HttpTransport(peers))
    http = make_http_server(args.bind, nodes[args.id]["port"], raft,
                            request_timeout=cfg.get("request_timeout", 3.0))
    raft.start()

    def shutdown(*_):
        threading.Thread(target=http.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logging.getLogger("distributed_key_value_store").info(
        "%s up on port %d (%d-node cluster, log has %d entries, term %d)",
        args.id, nodes[args.id]["port"], len(nodes), node.log.last_index(), node.current_term)
    try:
        http.serve_forever()
    finally:
        raft.stop()


if __name__ == "__main__":
    main()
