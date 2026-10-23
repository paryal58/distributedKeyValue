.PHONY: test test-fast chaos bench cluster stop demo docker-up docker-up5 docker-down

test:            ## everything: unit + simulated + real-process integration
	python3 -m pytest tests -q

test-fast:       ## no real processes (a few seconds)
	python3 -m pytest tests/unit tests/simulated -q

chaos:           ## random kills/pauses under load; verifies no acknowledged write is lost
	python3 scripts/chaos.py --nodes 5 --duration 60

bench:           ## leader-failover timing
	python3 scripts/bench_failover.py --nodes 3 --trials 20

cluster:         ## 3 local nodes in the background
	scripts/start_cluster.sh 3

stop:
	scripts/stop_cluster.sh

demo:
	scripts/demo.sh

docker-up:
	docker compose up --build

docker-up5:
	CLUSTER=5 docker compose --profile five up --build

docker-down:
	docker compose --profile five down
