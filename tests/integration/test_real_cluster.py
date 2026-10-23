"""Real processes, real sockets, real SIGKILLs."""
import time

import pytest

from distributed_key_value_store.client.client import ClusterUnavailable
from distributed_key_value_store.testing.proc_cluster import ProcCluster

pytestmark = pytest.mark.integration


@pytest.fixture
def cluster():
    with ProcCluster(n=3) as c:
        c.wait_for_leader()
        yield c


def test_put_get_delete(cluster):
    kv = cluster.client()
    kv.put("name", "prasanga")
    assert kv.get("name") == "prasanga"
    assert kv.delete("name") is True
    assert kv.get("name") is None
    assert kv.delete("name") is False


def test_any_node_redirects_to_leader(cluster):
    for nid in cluster.ids:                      # start the client at each node in turn
        kv = cluster.client()
        kv.target = nid
        kv.put(f"via-{nid}", nid)
        assert kv.last_leader is not None
    kv = cluster.client()
    assert all(kv.get(f"via-{n}") == n for n in cluster.ids)


def test_data_replicated_to_every_node(cluster):
    kv = cluster.client()
    for i in range(20):
        kv.put(f"k{i}", i)
    data = cluster.wait_converged()
    assert data == {f"k{i}": i for i in range(20)}


def test_leader_failover_loses_nothing(cluster):
    kv = cluster.client()
    for i in range(30):
        kv.put(f"k{i}", i)
    old = cluster.wait_for_leader()
    cluster.kill(old)
    new = cluster.wait_for_leader(exclude=(old,), timeout=10)
    assert new != old
    assert all(kv.get(f"k{i}") == i for i in range(30))
    kv.put("after", "failover")
    assert kv.get("after") == "failover"


def test_restarted_node_catches_up(cluster):
    kv = cluster.client()
    kv.put("before", 1)
    leader = cluster.wait_for_leader()
    victim = next(n for n in cluster.ids if n != leader)
    cluster.kill(victim)
    for i in range(10):
        kv.put(f"while-down-{i}", i)
    cluster.restart(victim)
    data = cluster.wait_converged(timeout=15)
    assert data["before"] == 1 and data["while-down-9"] == 9


def test_no_quorum_refuses_writes_then_recovers(cluster):
    kv = cluster.client(timeout=2.0)
    kv.put("x", 1)
    leader = cluster.wait_for_leader()
    for n in [n for n in cluster.ids if n != leader]:
        cluster.kill(n)
    with pytest.raises(ClusterUnavailable):
        kv.put("y", 2)                           # a lone node must never accept a write
    for n in cluster.ids:
        cluster.start(n)
    cluster.wait_for_leader(timeout=15)
    kv2 = cluster.client(timeout=10)
    assert kv2.get("x") == 1                     # committed before the outage: survives
    kv2.put("z", 3)
    assert kv2.get("z") == 3
    assert kv2.get("y") in (None, 2)             # unacknowledged write: either outcome is legal


def test_full_cluster_restart_recovers_from_disk():
    with ProcCluster(n=3, fsync=True) as c:
        c.wait_for_leader()
        kv = c.client()
        for i in range(15):
            kv.put(f"k{i}", i)
        c.stop_all()
        for n in c.ids:
            c.start(n)
        c.wait_for_leader(timeout=15)
        kv = c.client(timeout=15)
        assert all(kv.get(f"k{i}") == i for i in range(15))


def test_snapshot_compaction_and_session_limit_survive_restart():
    with ProcCluster(n=3, fsync=True, snapshot_threshold=4,
                     max_client_sessions=2) as c:
        c.wait_for_leader()
        kv = c.client(client_id="snapshot-client", timeout=10)
        victim = next(n for n in c.ids if n != c.wait_for_leader())
        c.kill(victim)
        for i in range(12):
            kv.put(f"snap-{i}", i)
        assert any(st and st["snapshot_index"] >= 4
                   for nid, st in kv.status().items() if nid != victim)

        second = c.client(client_id="second-client", timeout=10)
        second.put("second-session", True)
        with pytest.raises(RuntimeError, match="429"):
            c.client(client_id="third-client", timeout=10).put("rejected", True)

        c.restart(victim)
        assert c.wait_converged(timeout=15)["snap-11"] == 11
        c.stop_all()
        c.start_all()
        c.wait_for_leader(timeout=15)
        assert c.wait_converged(timeout=15)["second-session"] is True
        with pytest.raises(RuntimeError, match="429"):
            c.client(client_id="third-client", timeout=10).put("still-rejected", True)


def test_paused_leader_is_replaced_and_rejoins_as_follower(cluster):
    kv = cluster.client()
    kv.put("a", 1)
    old = cluster.wait_for_leader()
    cluster.pause(old)                           # frozen, not dead
    new = cluster.wait_for_leader(exclude=(old,), timeout=10)
    kv.put("b", 2)
    cluster.resume(old)
    time.sleep(1.0)
    st = cluster.client().status()
    assert st[old]["role"] == "follower" and st[old]["term"] >= st[new]["term"]
    assert cluster.wait_converged()["b"] == 2
