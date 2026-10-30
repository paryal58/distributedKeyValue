import json

import pytest

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


def test_log_compaction_preserves_absolute_indices():
    log = Log([e(1), e(1), e(2), e(2)])
    assert log.compact(2, 1) == [e(2), e(2)]
    assert log.base_index == 2 and log.last_index() == 4
    assert log.term_at(2) == 1 and log.term_at(1) is None
    assert log.get(3) == e(2) and log.slice(3, 2) == [e(2), e(2)]
    assert log.first_index_of_term(2, 4) == 3
    assert log.last_index_of_term(1) == 2
    assert log.append(e(3)) == 5


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


def test_state_machine_snapshot_and_bounded_sessions():
    sm = KVStateMachine(max_sessions=1)
    command = {"op": "put", "key": "a", "value": "1", "client_id": "c1", "seq": 1}
    assert sm.apply(command) == {"ok": True}
    snapshot = sm.snapshot()
    restored = KVStateMachine(max_sessions=1)
    restored.restore(snapshot)
    assert restored.get("a") == (True, "1")
    assert restored.apply(command) == {"ok": True}
    rejected = restored.apply({"op": "put", "key": "b", "value": "2",
                               "client_id": "c2", "seq": 1})
    assert rejected == {"ok": False, "error": "session_limit", "limit": 1}
    assert restored.get("b") == (False, None)


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
    recovered = FileStorage(str(tmp_path), fsync=False)
    assert [x["term"] for x in recovered.load()[2]] == [1, 2]
    recovered.append([e(3)])
    recovered.close()
    assert [x["term"] for x in FileStorage(str(tmp_path), fsync=False).load()[2]] == [1, 2, 3]


def test_file_storage_rejects_complete_malformed_final_line_without_rewriting(tmp_path):
    storage = FileStorage(str(tmp_path), fsync=False)
    storage.load()
    storage.append([e(1)])
    storage.close()

    with open(tmp_path / "log.jsonl", "ab") as f:
        f.write(b'{"term":2,"command":}\n')
    original = (tmp_path / "log.jsonl").read_bytes()

    try:
        FileStorage(str(tmp_path), fsync=False).load()
    except ValueError as exc:
        assert "invalid JSON in log record at line 3" in str(exc)
    else:
        raise AssertionError("complete malformed final record should fail to load")

    assert (tmp_path / "log.jsonl").read_bytes() == original


def test_file_storage_rejects_malformed_record_in_middle_without_rewriting(tmp_path):
    log_path = tmp_path / "log.jsonl"
    records = [
        b'{"_base_index":0}',
        json.dumps(e(1), separators=(",", ":")).encode(),
        b'{"term":2,"command":}\n',
        json.dumps(e(3), separators=(",", ":")).encode() + b"\n",
    ]
    log_path.write_bytes(b"\n".join(records) + b"\n")
    original = log_path.read_bytes()

    with pytest.raises(ValueError, match="invalid JSON in log record at line 3"):
        FileStorage(str(tmp_path), fsync=False).load()

    assert log_path.read_bytes() == original


def test_file_storage_rewrites_valid_unterminated_record_before_future_appends(tmp_path):
    storage = FileStorage(str(tmp_path), fsync=False)
    storage.load()
    storage.append([e(1)])
    storage.close()

    with open(tmp_path / "log.jsonl", "ab") as f:
        f.write(json.dumps(e(2), separators=(",", ":")).encode())

    recovered = FileStorage(str(tmp_path), fsync=False)
    assert [entry["term"] for entry in recovered.load()[2]] == [1, 2]
    recovered.append([e(3)])
    recovered.close()
    assert [entry["term"] for entry in FileStorage(str(tmp_path), fsync=False).load()[2]] == [1, 2, 3]


def test_file_storage_rejects_snapshot_ahead_of_log_without_rewriting(tmp_path):
    storage = FileStorage(str(tmp_path), fsync=False)
    storage.load()
    storage.append([e(1)])
    storage.close()

    log_path = tmp_path / "log.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    log_before = log_path.read_bytes()
    snapshot = {
        "last_included_index": 2,
        "last_included_term": 1,
        "state": {"data": {}, "sessions": {}},
    }
    snapshot_path.write_text(json.dumps(snapshot))

    with pytest.raises(ValueError, match="durable snapshot is ahead of the persisted log"):
        FileStorage(str(tmp_path), fsync=False).load()

    assert log_path.read_bytes() == log_before
    assert json.loads(snapshot_path.read_text()) == snapshot


def test_file_storage_snapshot_compaction_and_restart(tmp_path):
    storage = FileStorage(str(tmp_path), fsync=False)
    storage.load()
    entries = [e(1), e(1), e(2), e(2)]
    storage.append(entries)
    snapshot = {"last_included_index": 2, "last_included_term": 1,
                "state": {"data": {"a": 1}, "sessions": {}}}
    storage.save_snapshot(snapshot, entries[2:])
    storage.close()

    restored = FileStorage(str(tmp_path), fsync=False)
    assert restored.load() == (0, None, entries[2:])
    assert restored.load_snapshot() == snapshot
    assert restored.log_base_index == 2
    restored.close()


def test_file_storage_recovers_if_crash_happens_between_snapshot_and_log_rewrite(tmp_path):
    storage = FileStorage(str(tmp_path), fsync=False)
    storage.load()
    entries = [e(1), e(1), e(2)]
    storage.append(entries)
    storage.close()
    with open(tmp_path / "snapshot.json", "w") as f:
        json.dump({"last_included_index": 2, "last_included_term": 1,
                   "state": {"data": {"a": 1}, "sessions": {}}}, f)

    recovered = FileStorage(str(tmp_path), fsync=False)
    assert recovered.load()[2] == entries[2:]
    assert recovered.log_base_index == 2
    recovered.close()
