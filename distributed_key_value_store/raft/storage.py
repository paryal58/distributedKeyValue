"""Durable storage for the state Raft must persist: currentTerm, votedFor, and the log.

FileStorage layout (one directory per node):
    meta.json   - {"term": int, "voted_for": str|null}, replaced atomically
    log.jsonl   - one JSON log entry per line, append-only (rewritten on truncation)
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

    def load(self):
        return self.term, self.voted_for, copy.deepcopy(self.entries)

    def save_meta(self, term, voted_for):
        self.term, self.voted_for = term, voted_for

    def append(self, entries):
        self.entries.extend(copy.deepcopy(entries))

    def rewrite(self, entries):
        self.entries = copy.deepcopy(entries)

    def close(self):
        pass


class FileStorage:
    def __init__(self, directory: str, fsync: bool = True):
        self.dir = directory
        self.fsync = fsync
        os.makedirs(directory, exist_ok=True)
        self.meta_path = os.path.join(directory, "meta.json")
        self.log_path = os.path.join(directory, "log.jsonl")
        self._log_fh = None

    # ---- loading -------------------------------------------------------
    def load(self):
        term, voted_for = 0, None
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                meta = json.load(f)
            term, voted_for = meta["term"], meta["voted_for"]

        entries = []
        torn_tail = False
        if os.path.exists(self.log_path):
            with open(self.log_path, "rb") as f:
                lines = f.read().split(b"\n")
            # a trailing empty string is normal; a corrupt *last* line is a torn write
            for i, line in enumerate(lines):
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except ValueError:
                    if i >= len(lines) - 2:
                        torn_tail = True
                        break
                    raise
        if torn_tail:
            self.rewrite(entries)
        return term, voted_for, entries

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

    def rewrite(self, entries):
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None
        tmp = self.log_path + ".tmp"
        with open(tmp, "wb") as f:
            for e in entries:
                f.write(json.dumps(e, separators=(",", ":")).encode() + b"\n")
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        self._fsync_dir()

    def close(self):
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None

    def _handle(self):
        if self._log_fh is None:
            self._log_fh = open(self.log_path, "ab")
        return self._log_fh

    def _fsync_dir(self):
        if not self.fsync:
            return
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
