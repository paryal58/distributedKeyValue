# Distributed Key-Value Store

A Python key-value store replicated with Raft consensus. Nodes and clients use only the standard library.

## Features

- Linearizable reads and writes; optional local stale reads.
- Durable Raft log, leader election, quorum replication, and failover.
- Client request deduplication for safe retries.
- Three-node clusters tolerate one failure; five-node clusters tolerate two.

## Quick start

Requires Python 3.9+.

```bash
pip install -e .
scripts/start_cluster.sh 3
distributed-key-value-store put name prasanga
distributed-key-value-store get name
distributed-key-value-store status
```

Use `scripts/start_cluster.sh 5` for five nodes; stop with `scripts/stop_cluster.sh`. Legacy
`conclave` commands and `CONCLAVE_CONFIG` remain supported.

## HTTP API

```bash
curl -X PUT localhost:5001/kv/name -d 'prasanga'
curl localhost:5001/kv/name
curl 'localhost:5001/kv/name?stale=1'  # local, possibly stale read
curl localhost:5001/status
```

## Tests

```bash
make test-fast  # unit and simulated-cluster tests
make test       # all tests, including real-process integration
make chaos      # fault injection against a live cluster
```
