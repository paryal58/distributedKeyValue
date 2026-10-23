#!/usr/bin/env python3
"""Generate a cluster config.  e.g.  python scripts/gen_config.py --nodes 5 --mode docker"""
import argparse
import json

ap = argparse.ArgumentParser()
ap.add_argument("--nodes", type=int, default=3)
ap.add_argument("--mode", choices=["local", "docker"], default="local")
ap.add_argument("--base-port", type=int, default=5001)
ap.add_argument("--out", default=None)
a = ap.parse_args()

cfg = {
    "election_timeout_min": 0.3,
    "election_timeout_max": 0.6,
    "heartbeat_interval": 0.1,
    "request_timeout": 3.0,
    "nodes": {
        f"node{i}": {"host": "127.0.0.1" if a.mode == "local" else f"node{i}", "port": a.base_port + i - 1}
        for i in range(1, a.nodes + 1)
    },
}
out = a.out or f"config/{a.mode}-{a.nodes}.json"
with open(out, "w") as f:
    json.dump(cfg, f, indent=2)
    f.write("\n")
print("wrote", out)
