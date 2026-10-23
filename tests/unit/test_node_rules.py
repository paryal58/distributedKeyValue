import random

from distributed_key_value_store.raft.node import CANDIDATE, FOLLOWER, LEADER, RaftNode
from distributed_key_value_store.raft.storage import MemoryStorage


def make(node_id="n1", peers=("n2", "n3"), storage=None):
    return RaftNode(node_id, peers, storage or MemoryStorage(), rng=random.Random(7))


def e(term):
    return {"term": term, "command": {"op": "put", "key": "k", "value": term}}


def rv(frm, term, last_index=0, last_term=0):
    return {"type": "RequestVote", "from": frm, "to": "n1", "term": term, "candidate_id": frm,
            "last_log_index": last_index, "last_log_term": last_term}


def ae(term, prev_i, prev_t, entries, commit=0, leader="n2", seq=1):
    return {"type": "AppendEntries", "from": leader, "to": "n1", "term": term, "leader_id": leader,
            "prev_log_index": prev_i, "prev_log_term": prev_t, "entries": entries,
            "leader_commit": commit, "seq": seq}


def reply_of(node):
    out = node.drain()
    assert len(out) == 1
    return out[0]


def test_one_vote_per_term():
    n = make()
    n.receive(rv("n2", 1), 0)
    assert reply_of(n)["granted"] is True
    n.receive(rv("n3", 1), 0)
    assert reply_of(n)["granted"] is False
    n.receive(rv("n2", 1), 0)            # same candidate asking again is fine
    assert reply_of(n)["granted"] is True


def test_rejects_candidate_with_stale_log():
    n = make()
    n.receive(ae(2, 0, 0, [e(1), e(2)]), 0); n.drain()
    n.receive(rv("n3", 3, last_index=5, last_term=1), 0)   # longer, but older last term
    assert reply_of(n)["granted"] is False
    n.receive(rv("n3", 4, last_index=1, last_term=2), 0)   # shorter, but newer last term... 2 > 2? equal term, shorter
    assert reply_of(n)["granted"] is False
    n.receive(rv("n3", 5, last_index=2, last_term=2), 0)
    assert reply_of(n)["granted"] is True


def test_rejects_stale_term_and_steps_down_on_higher():
    n = make()
    n.receive(ae(5, 0, 0, []), 0); n.drain()
    n.receive(rv("n3", 3), 0)
    r = reply_of(n)
    assert r["granted"] is False and r["term"] == 5
    n._start_election(1.0); n.drain()
    assert n.role == CANDIDATE and n.current_term == 6
    n.receive(ae(9, 0, 0, []), 1.1); n.drain()
    assert n.role == FOLLOWER and n.current_term == 9 and n.leader_id == "n2"


def test_vote_and_term_survive_restart():
    st = MemoryStorage()
    n = make(storage=st)
    n.receive(rv("n2", 4), 0); n.drain()
    reborn = make(storage=st)
    assert reborn.current_term == 4 and reborn.voted_for == "n2"
    reborn.receive(rv("n3", 4), 0)
    assert reply_of(reborn)["granted"] is False  # must not vote twice in term 4 after a crash


def test_append_entries_truncates_conflicting_suffix():
    n = make()
    n.receive(ae(1, 0, 0, [e(1), e(1), e(1)]), 0); n.drain()
    n.receive(ae(2, 1, 1, [e(2), e(2)]), 0)
    r = reply_of(n)
    assert r["success"] and r["match_index"] == 3
    assert [x["term"] for x in n.log.entries] == [1, 2, 2]
    assert [x["term"] for x in n.storage.entries] == [1, 2, 2]   # persisted too


def test_stale_duplicate_append_does_not_truncate():
    n = make()
    n.receive(ae(1, 0, 0, [e(1), e(1), e(1)]), 0); n.drain()
    n.receive(ae(1, 0, 0, [e(1)]), 0)     # old, shorter retransmission
    assert reply_of(n)["success"]
    assert n.log.last_index() == 3


def test_append_entries_reports_conflict_hints():
    n = make()
    n.receive(ae(1, 0, 0, [e(1), e(1)]), 0); n.drain()
    n.receive(ae(2, 5, 2, []), 0)
    r = reply_of(n)
    assert not r["success"] and r["conflict_index"] == 3 and r["conflict_term"] is None
    n.receive(ae(3, 2, 2, []), 0)
    r = reply_of(n)
    assert not r["success"] and r["conflict_term"] == 1 and r["conflict_index"] == 1


def test_commit_index_follows_leader_and_applies():
    n = make()
    n.receive(ae(1, 0, 0, [e(1), e(1)], commit=1), 0); n.drain()
    assert n.commit_index == 1 and n.last_applied == 1
    n.receive(ae(1, 2, 1, [], commit=9), 0); n.drain()
    assert n.commit_index == 2


def test_single_node_cluster_elects_itself_and_commits():
    n = RaftNode("solo", [], MemoryStorage(), rng=random.Random(1))
    n.tick(10.0)
    assert n.role == LEADER
    idx, term = n.propose({"op": "put", "key": "a", "value": 1}, 10.0)
    assert n.last_applied >= idx and n.sm.get("a") == (True, 1)


def test_committed_prefix_is_snapshotted_and_restored():
    st = MemoryStorage()
    n = RaftNode("n1", ("n2", "n3"), st, snapshot_threshold=2, rng=random.Random(1))
    n.receive(ae(1, 0, 0, [e(1), e(1)], commit=2), 0)
    n.drain()
    assert n.log.base_index == 2 and n.log.entries == []
    assert n.commit_index == n.last_applied == 2
    assert st.load_snapshot()["last_included_index"] == 2

    reborn = make(storage=st)
    assert reborn.log.base_index == 2
    assert reborn.commit_index == reborn.last_applied == 2
    assert reborn.sm.get("k") == (True, 1)


def test_install_snapshot_restores_state_and_accepts_following_entries():
    n = make()
    snapshot = {
        "last_included_index": 5,
        "last_included_term": 2,
        "state": {"data": {"from-snapshot": "yes"}, "sessions": {}},
    }
    n.receive({"type": "InstallSnapshot", "from": "n2", "to": "n1", "term": 2,
               "leader_id": "n2", "seq": 1, "snapshot": snapshot}, 0)
    reply = reply_of(n)
    assert reply["type"] == "InstallSnapshotReply" and reply["success"]
    assert n.log.base_index == 5 and n.commit_index == n.last_applied == 5
    assert n.sm.get("from-snapshot") == (True, "yes")

    n.receive(ae(2, 5, 2, [e(2)], commit=6), 0)
    assert reply_of(n)["success"]
    assert n.log.last_index() == 6 and n.sm.get("k") == (True, 2)
