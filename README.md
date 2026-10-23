# Distributed Key-Value Store

A small, fault-tolerant service for storing values by name: save `"Mbappe"` under `"name"` and
retrieve it later. It demonstrates how servers can keep data consistent through failures.

## How it works
Run three or five nodes as a cluster. One node coordinates updates; the others keep copies. An
update is confirmed only after most nodes have saved it, so a single node failure need not lose
confirmed data. If the coordinator stops, the remaining nodes choose another.

It uses Raft to agree on updates and snapshots to speed recovery. This is a learning/demo-scale
implementation, not a managed database or production-hardened service.

## What you can do
- Add, read, and remove values through a command-line tool, Python client, or HTTP API.
- Get consistent reads, or explicitly request a faster local read that may be out of date.
- See which node is coordinating the cluster and whether nodes are reachable.
- Stop and restart nodes to explore failover and recovery.

## Quick start
Requires Python 3.9+. From the project directory:

```bash
pip install -e .
scripts/start_cluster.sh 3
distributed-key-value-store put name Mbappe
distributed-key-value-store get name
distributed-key-value-store status
```

Try `scripts/start_cluster.sh 5` for a five-node cluster. Stop it with `scripts/stop_cluster.sh`.
Use `scripts/kill_node.sh node2 pause` to simulate a stalled node; use `resume` to restore it.
The older `conclave` command and `CONCLAVE_CONFIG` setting remain available.

## Use the HTTP API
Each node accepts requests. If you contact a non-coordinator, it points clients toward the current
coordinator. The `stale=1` option intentionally reads only that node's local copy.

```bash
curl -X PUT localhost:5001/kv/name -d 'Mbappe'
curl localhost:5001/kv/name
curl 'localhost:5001/kv/name?stale=1'
curl -X DELETE localhost:5001/kv/name
curl localhost:5001/status
```

The Python client handles finding the coordinator and retrying requests:

```python
from distributed_key_value_store.client import Client

nodes = {f"node{i}": f"127.0.0.1:{5000 + i}" for i in range(1, 4)}
client = Client(nodes)
client.put("name", "Mbappe")
print(client.get("name"))
```

## Configuration and reliability
Cluster members are listed in `config/local-3.json`. All nodes in a cluster should use the same
configuration. Three nodes can continue with one unavailable; five can continue with two. In
either case, a majority must be available to confirm updates and consistent reads.

Snapshots are created every 1,000 applied operations by default. The store retains deduplication
records for up to 10,000 client sessions; when full, it rejects new sessions rather than forgetting
records that may be needed to safely retry a write. Both limits can be changed in the config with
`snapshot_threshold` and `max_client_sessions`.

This project has no authentication or encrypted transport. Run it only on a trusted network.
Membership is fixed after startup; safely adding or removing nodes is not supported.

## Tests and Docker
```bash
make test-fast  # focused logic and simulated-network tests
make test       # also tests real local server processes
make chaos      # restarts and interrupts nodes while sending updates
docker compose up --build
```

Docker starts three nodes; use `CLUSTER=5 docker compose --profile five up --build` for five.
Node data is stored under `data/docker/`.
