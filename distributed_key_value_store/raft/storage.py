"""Durable storage for the state Raft must persist: currentTerm, votedFor, and the log.

FileStorage layout (one directory per node):
    meta.json   - {"term": int, "voted_for": str|null}, replaced atomically
    snapshot.json - last included index/term and state machine, replaced atomically
    log.jsonl   - a base-index header followed by retained JSON log entries
"""
from __future__ import annotations

import copy
import json
import os


class MemoryStorage:
    """In-memory storage for tests. A 'restart' is a new node built on the same object."""

    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.entries: list[dict] = []
        self.log_base_index = 0
        self.snapshot = None

    def load(self):
        return self.term, self.voted_for, copy.deepcopy(self.entries)

    def load_snapshot(self):
        return copy.deepcopy(self.snapshot)

    def save_meta(self, term, voted_for):
        self.term, self.voted_for = term, voted_for

    def append(self, entries):
        self.entries.extend(copy.deepcopy(entries))

    def rewrite(self, entries, base_index=None):
        if base_index is not None:
            self.log_base_index = base_index
        self.entries = copy.deepcopy(entries)

    def save_snapshot(self, snapshot, entries):
        self.snapshot = copy.deepcopy(snapshot)
        self.rewrite(entries, snapshot["last_included_index"])

    def close(self):
        pass


class FileStorage:
    def __init__(self, directory: str, fsync: bool = True):
        self.dir = directory
        self.fsync = fsync
        os.makedirs(directory, exist_ok=True)
        self.meta_path = os.path.join(directory, "meta.json")
        self.snapshot_path = os.path.join(directory, "snapshot.json")
        self.log_path = os.path.join(directory, "log.jsonl")
        self._log_fh = None

    # ---- loading -------------------------------------------------------
    def load(self):
        term, voted_for = 0, None
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                meta = json.load(f)
            term, voted_for = meta["term"], meta["voted_for"]

        snapshot = None
        if os.path.exists(self.snapshot_path):
            with open(self.snapshot_path) as f:
                snapshot = json.load(f)
        snapshot_index = snapshot["last_included_index"] if snapshot else 0

        entries = []
        base_index = 0
        torn_tail = False
        if os.path.exists(self.log_path):
            with open(self.log_path, "rb") as f:
                lines = f.read().split(b"\n")
            for i, line in enumerate(lines):
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except ValueError:
                    if i >= len(lines) - 2:
                        torn_tail = True
                        break
                    raise
                if i == 0 and isinstance(value, dict) and "_base_index" in value:
                    base_index = value["_base_index"]
                else:
                    entries.append(value)
        if torn_tail:
            self.rewrite(entries, base_index)
        if base_index > snapshot_index:
            raise ValueError("log base index is newer than the durable snapshot")
        if base_index < snapshot_index:
            skip = snapshot_index - base_index
            if skip > len(entries):
                raise ValueError("durable snapshot is ahead of the persisted log")
            entries = entries[skip:]
            base_index = snapshot_index
            self.rewrite(entries, base_index)
        self.log_base_index = base_index
        self.snapshot = snapshot
        return term, voted_for, entries

    def load_snapshot(self):
        return self.snapshot

    # ---- writing -------------------------------------------------------
    def save_meta(self, term, voted_for):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": term, "voted_for": voted_for}, f)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.meta_path)
        self._fsync_dir()

    def append(self, entries):
        fh = self._handle()
        for e in entries:
            fh.write(json.dumps(e, separators=(",", ":")).encode() + b"\n")
        fh.flush()
        if self.fsync:
            os.fsync(fh.fileno())

    def rewrite(self, entries, base_index=None):
        if base_index is not None:
            self.log_base_index = base_index
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None
        tmp = self.log_path + ".tmp"
        with open(tmp, "wb") as f:
            header = {"_base_index": self.log_base_index}
            f.write(json.dumps(header, separators=(",", ":")).encode() + b"\n")
            for e in entries:
                f.write(json.dumps(e, separators=(",", ":")).encode() + b"\n")
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        self._fsync_dir()

    def save_snapshot(self, snapshot, entries):
        tmp = self.snapshot_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snapshot, f, separators=(",", ":"))
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.snapshot_path)
        self._fsync_dir()
        self.rewrite(entries, snapshot["last_included_index"])

    def close(self):
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None

    def _handle(self):
        if self._log_fh is None:
            self._log_fh = open(self.log_path, "ab")
            if os.fstat(self._log_fh.fileno()).st_size == 0:
                header = {"_base_index": self.log_base_index}
                self._log_fh.write(json.dumps(header, separators=(",", ":")).encode() + b"\n")
        return self._log_fh

    def _fsync_dir(self):
        if not self.fsync:
            return
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
