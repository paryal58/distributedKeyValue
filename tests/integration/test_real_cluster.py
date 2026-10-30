"""Real processes, real sockets, real SIGKILLs."""
import json
import time

import pytest

from distributed_key_value_store.client.client import ClusterUnavailable
from distributed_key_value_store.client.cli import main as cli_main
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


def test_python_client_json_value_and_escaped_key_workflow(cluster):
    kv = cluster.client()
    key = "profile/team name/☃"
    value = {"name": "Mbappe", "stats": [1, None, True]}

    kv.put(key, value)
    assert kv.get(key) == value
    assert cluster.wait_converged()[key] == value
    assert kv.get(key, stale=True, node=cluster.ids[0]) == value

    assert kv.delete(key) is True
    assert kv.get(key) is None
    assert kv.delete(key) is False


def test_cli_crud_workflow_persists_write_sequence(cluster, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    config = cluster.config_path

    assert cli_main(["--config", config, "put", "workflow-key", "first"]) == 0
    assert "OK (leader:" in capsys.readouterr().out
    session_files = list((tmp_path / ".distributed-key-value-store").glob("*.json"))
    assert len(session_files) == 1
    assert json.loads(session_files[0].read_text())["seq"] == 1

    assert cli_main(["--config", config, "get", "workflow-key"]) == 0
    assert capsys.readouterr().out == "first\n"

    assert cli_main(["--config", config, "put", "workflow-key", "updated"]) == 0
    capsys.readouterr()
    assert json.loads(session_files[0].read_text())["seq"] == 2
    assert cli_main(["--config", config, "get", "workflow-key"]) == 0
    assert capsys.readouterr().out == "updated\n"

    assert cli_main(["--config", config, "delete", "workflow-key"]) == 0
    assert "OK (leader:" in capsys.readouterr().out
    assert json.loads(session_files[0].read_text())["seq"] == 3
    assert cli_main(["--config", config, "get", "workflow-key"]) == 1
    assert capsys.readouterr().err == "(not found)\n"


def test_python_client_round_trips_falsey_json_values(cluster):
    kv = cluster.client()
    values = {"empty-string": "", "zero": 0, "false": False, "empty-list": [], "empty-object": {}}
    for key, value in values.items():
        kv.put(key, value)
    assert {key: kv.get(key) for key in values} == values
    assert cluster.wait_converged() == values


def test_overwriting_a_key_replicates_the_latest_value(cluster):
    kv = cluster.client()
    kv.put("mutable", "first")
    kv.put("mutable", {"revision": 2})
    assert kv.get("mutable") == {"revision": 2}
    assert cluster.wait_converged() == {"mutable": {"revision": 2}}


def test_deleting_a_missing_key_is_safe_and_does_not_change_data(cluster):
    kv = cluster.client()
    kv.put("retained", "value")
    assert kv.delete("missing") is False
    assert kv.get("retained") == "value"
    assert cluster.wait_converged() == {"retained": "value"}


def test_write_sequence_callback_tracks_put_and_delete(cluster):
    sequences = []
    kv = cluster.client(on_sequence=sequences.append)
    kv.put("sequenced", 1)
    assert kv.delete("sequenced") is True
    assert sequences == [1, 2]
    assert kv.seq == 2
    assert kv.get("sequenced") is None


def test_new_client_can_continue_an_existing_client_session(cluster):
    client_id = "continued-session"
    first = cluster.client(client_id=client_id)
    first.put("before", 1)

    resumed = cluster.client(client_id=client_id, initial_seq=first.seq)
    resumed.put("after", 2)

    assert resumed.seq == 2
    assert resumed.get("before") == 1
    assert resumed.get("after") == 2
    assert cluster.wait_converged() == {"before": 1, "after": 2}


def test_client_can_read_when_initially_targeting_a_follower(cluster):
    kv = cluster.client()
    kv.put("redirected-read", "available")
    leader = cluster.wait_for_leader()
    follower = next(node for node in cluster.ids if node != leader)
    kv.target = follower

    assert kv.get("redirected-read") == "available"
    assert kv.last_leader == leader


def test_cli_leader_and_status_commands_report_cluster(cluster, capsys):
    config = cluster.config_path
    assert cli_main(["--config", config, "leader"]) == 0
    leader_output = capsys.readouterr().out.strip()
    assert leader_output in cluster.ids

    assert cli_main(["--config", config, "status"]) == 0
    status_output = capsys.readouterr().out
    assert "NODE" in status_output
    assert "cluster: leader=" in status_output
    assert leader_output in status_output


def test_offline_follower_catches_up_to_write_and_delete(cluster):
    kv = cluster.client()
    leader = cluster.wait_for_leader()
    follower = next(node for node in cluster.ids if node != leader)
    cluster.kill(follower)

    kv.put("catch-up", "present")
    cluster.restart(follower)
    assert cluster.wait_converged(timeout=15) == {"catch-up": "present"}

    assert kv.delete("catch-up") is True
    assert cluster.wait_converged(timeout=15) == {}


def test_single_node_process_cluster_completes_basic_workflow():
    with ProcCluster(n=1) as single:
        single.wait_for_leader()
        kv = single.client()
        kv.put("solo", {"works": True})
        assert kv.get("solo") == {"works": True}
        assert kv.delete("solo") is True
        assert kv.get("solo") is None


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
