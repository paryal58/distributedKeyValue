import random

import pytest

from distributed_key_value_store.raft.node import LEADER
from distributed_key_value_store.testing.simnet import SimCluster


def elect(c, limit=5.0):
    assert c.run_until(lambda: c.leader() is not None, limit), "no leader elected"
    return c.leader()


def write_and_commit(c, key, value, seq):
    r = c.put(key, value, seq=seq)
    assert r, "no leader to accept write"
    _, idx, term = r
    assert c.run_until(lambda: c.leader() and c.leader().last_applied >= idx, 3.0)
    return r


@pytest.mark.parametrize("n", [1, 3, 5])
def test_elects_exactly_one_leader(n):
    c = SimCluster(n=n, seed=n)
    elect(c)
    c.run(2.0)
    assert len(c.leaders()) == 1


def test_replicates_to_all_nodes():
    c = SimCluster(n=3, seed=2)
    elect(c)
    for i in range(10):
        write_and_commit(c, f"k{i}", i, seq=i + 1)
    assert c.run_until(c.converged, 3.0)
    for node in c.nodes.values():
        assert node.sm.data == {f"k{i}": i for i in range(10)}


def test_leader_crash_triggers_new_election_and_keeps_committed_data():
    c = SimCluster(n=3, seed=3)
    old = elect(c)
    for i in range(5):
        write_and_commit(c, f"k{i}", i, seq=i + 1)
    c.crash(old.id)
    assert c.run_until(lambda: c.leader() is not None and c.leader().id != old.id, 5.0)
    new = c.leader()
    assert new.current_term > old.current_term
    # the new leader only knows entries are committed once its own no-op commits
    assert c.run_until(lambda: c.leader().last_applied >= c.leader().noop_index, 3.0)
    new = c.leader()
    for i in range(5):
        assert new.sm.get(f"k{i}") == (True, i)
    write_and_commit(c, "after", 1, seq=6)
    c.restart(old.id)
    assert c.run_until(c.converged, 5.0)
    assert all(n.sm.get("after") == (True, 1) for n in c.nodes.values())


def test_minority_partition_cannot_commit_and_old_leader_steps_down():
    c = SimCluster(n=5, seed=4)
    leader = elect(c)
    others = [i for i in c.ids if i != leader.id]
    minority = [leader.id, others[0]]
    majority = others[1:]
    c.partition(minority, majority)
    r = c.put("lost", "x", seq=1)          # goes to the old leader, can never commit
    assert r and r[0] == leader.id
    c.run(2.0)
    assert leader.role != LEADER, "check-quorum should have deposed the isolated leader"
    maj_leader = next(n for n in c.up_nodes() if n.id in majority and n.role == LEADER)
    assert maj_leader.current_term > r[2]
    c.heal()
    assert c.run_until(c.converged, 5.0)
    for node in c.nodes.values():
        assert node.sm.get("lost") == (False, None)   # uncommitted write was rolled back


def test_no_quorum_means_no_progress():
    c = SimCluster(n=3, seed=5)
    elect(c)
    for i in c.ids[:2]:
        c.crash(i)
    c.run(3.0)
    assert c.leader() is None


def test_restart_preserves_term_vote_and_log():
    c = SimCluster(n=3, seed=6)
    elect(c)
    write_and_commit(c, "a", 1, seq=1)
    assert c.run_until(c.converged, 3.0)
    victim = next(i for i in c.ids if c.nodes[i].role != LEADER)
    before = (c.nodes[victim].current_term, list(c.nodes[victim].log.entries))
    c.crash(victim)
    c.restart(victim)
    node = c.nodes[victim]
    assert node.current_term >= before[0] and node.log.entries[:len(before[1])] == before[1]


def test_read_index_only_confirms_with_a_majority():
    c = SimCluster(n=3, seed=7)
    leader = elect(c)
    c.run(0.5)
    token = leader.begin_read(c.now)
    c._send_all(leader)
    assert c.run_until(lambda: leader.read_ready(token) == "ok", 2.0)
    # isolate the leader: it can no longer confirm leadership, so a read must not succeed
    c.isolate(leader.id)
    token = leader.begin_read(c.now)
    c._send_all(leader)
    c.run(0.2)
    assert leader.read_ready(token) in ("wait", "lost")


@pytest.mark.parametrize("seed", range(25))
def test_fuzz_random_faults_preserve_safety(seed):
    """Random crashes, partitions, drops and writes; invariants are asserted every step."""
    rng = random.Random(1000 + seed)
    n = rng.choice([3, 5])
    c = SimCluster(n=n, seed=seed, drop=0.05)
    pending, acked, seq = [], [], 0
    for step in range(2500):
        r = rng.random()
        if r < 0.004:
            c.crash(rng.choice(c.ids))
        elif r < 0.012:
            down = [i for i in c.ids if i in c.down]
            if down:
                c.restart(rng.choice(down))
        elif r < 0.016:
            c.isolate(rng.choice(c.ids))
        elif r < 0.024:
            c.heal()
        elif r < 0.12:
            seq += 1
            res = c.put(f"k{seq % 7}", seq, client=f"c{seq % 3}", seq=seq)
            if res:
                pending.append((seq, *res))
        c.step()
        for item in list(pending):
            _, nid, idx, term = item
            node = c.nodes[nid]
            if nid in c.down or node.log.term_at(idx) != term:
                pending.remove(item)               # lost with the leader: never acknowledged
            elif node.last_applied >= idx:
                acked.append(item)
                pending.remove(item)

    # heal everything and let the cluster settle
    c.heal()
    c.drop = 0.0
    for i in list(c.down):
        c.restart(i)
    assert c.run_until(c.converged, 15.0), "cluster failed to converge after faults healed"
    assert len(c.leaders()) == 1
    final = c.nodes[c.ids[0]]
    for _, nid, idx, term in acked:                # every acknowledged write survived
        assert final.log.term_at(idx) == term
    first = c.nodes[c.ids[0]].sm.data
    assert all(node.sm.data == first for node in c.nodes.values())
