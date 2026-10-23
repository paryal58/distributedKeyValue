from distributed_key_value_store.raft.log import Log
from distributed_key_value_store.raft.state_machine import KVStateMachine
from distributed_key_value_store.raft.storage import FileStorage


def e(term, cmd="x"):
    return {"term": term, "command": {"op": "put", "key": cmd, "value": 1}}


def test_log_indexing():
    log = Log()
    assert log.last_index() == 0 and log.last_term() == 0 and log.term_at(0) == 0
    log.append(e(1)); log.append(e(1)); log.append(e(3))
    assert (log.last_index(), log.last_term()) == (3, 3)
    assert log.term_at(2) == 1 and log.term_at(4) is None
    assert log.first_index_of_term(1, 2) == 1
    assert log.last_index_of_term(1) == 2 and log.last_index_of_term(2) == 0 and log.last_index_of_term(3) == 3
    log.truncate_from(2)
    assert log.last_index() == 1


def test_state_machine_put_delete_and_dedup():
    sm = KVStateMachine()
    cmd = {"op": "put", "key": "a", "value": "1", "client_id": "c", "seq": 1}
    assert sm.apply(cmd) == {"ok": True}
    sm.apply({"op": "put", "key": "a", "value": "2", "client_id": "c", "seq": 2})
    assert sm.get("a") == (True, "2")
    # retry of seq 2 with a different value must NOT re-apply
    again = sm.apply({"op": "put", "key": "a", "value": "BAD", "client_id": "c", "seq": 2})
    assert again == {"ok": True} and sm.get("a") == (True, "2")
    assert sm.apply({"op": "delete", "key": "a", "client_id": "c", "seq": 3}) == {"ok": True, "existed": True}
    assert sm.get("a") == (False, None)
    assert sm.apply({"op": "noop"}) is None


def test_file_storage_roundtrip_and_truncation(tmp_path):
    s = FileStorage(str(tmp_path), fsync=False)
    assert s.load() == (0, None, [])
    s.save_meta(3, "n2")
    s.append([e(1), e(2)])
    s.append([e(3)])
    s.close()
    term, voted, entries = FileStorage(str(tmp_path), fsync=False).load()
    assert (term, voted) == (3, "n2") and [x["term"] for x in entries] == [1, 2, 3]
    s2 = FileStorage(str(tmp_path), fsync=False)
    s2.load()
    s2.rewrite(entries[:1])
    s2.append([e(5)])
    s2.close()
    assert [x["term"] for x in FileStorage(str(tmp_path), fsync=False).load()[2]] == [1, 5]


def test_file_storage_tolerates_torn_last_line(tmp_path):
    s = FileStorage(str(tmp_path), fsync=False)
    s.load()
    s.append([e(1), e(2)])
    s.close()
    with open(tmp_path / "log.jsonl", "ab") as f:
        f.write(b'{"term":3,"comm')  # crash mid-write
    entries = FileStorage(str(tmp_path), fsync=False).load()[2]
    assert [x["term"] for x in entries] == [1, 2]
