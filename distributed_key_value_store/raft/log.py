"""The replicated log. Indices are 1-based; index 0 is a virtual entry with term 0."""
from __future__ import annotations


class Log:
    def __init__(self, entries=None, *, base_index: int = 0, base_term: int = 0):
        # each entry: {"term": int, "command": dict}
        self.entries: list[dict] = list(entries or [])
        self.base_index = base_index
        self.base_term = base_term

    def last_index(self) -> int:
        return self.base_index + len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1]["term"] if self.entries else self.base_term

    def term_at(self, index: int):
        """Term of an index, including the compacted boundary; None if unavailable."""
        if index == 0:
            return 0
        if index == self.base_index:
            return self.base_term
        if index < self.base_index or index > self.last_index():
            return None
        return self.entries[index - self.base_index - 1]["term"]

    def get(self, index: int) -> dict:
        if index <= self.base_index or index > self.last_index():
            raise IndexError(f"log index {index} is not retained")
        return self.entries[index - self.base_index - 1]

    def append(self, entry: dict) -> int:
        self.entries.append(entry)
        return self.last_index()

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and everything after it."""
        if index <= self.base_index:
            raise ValueError("cannot truncate a compacted log entry")
        del self.entries[index - self.base_index - 1:]

    def slice(self, start: int, max_entries: int) -> list[dict]:
        start = max(start, self.base_index + 1)
        offset = start - self.base_index - 1
        return self.entries[offset:offset + max_entries]

    def compact(self, index: int, term: int) -> list[dict]:
        """Discard the prefix through index and return the retained suffix."""
        if index < self.base_index or index > self.last_index():
            raise ValueError("snapshot index is outside the retained log")
        if self.term_at(index) != term:
            raise ValueError("snapshot term does not match log")
        del self.entries[:index - self.base_index]
        self.base_index, self.base_term = index, term
        return self.entries

    def first_index_of_term(self, term: int, upto: int) -> int:
        i = upto
        while i > self.base_index + 1 and self.term_at(i - 1) == term:
            i -= 1
        return i

    def last_index_of_term(self, term: int) -> int:
        """Highest index whose entry has `term`, or 0 if the term is absent."""
        i = self.last_index()
        while i > self.base_index:
            t = self.term_at(i)
            if t == term:
                return i
            if t < term:  # terms never decrease along the log
                return self.base_index if self.base_term == term else 0
            i -= 1
        return self.base_index if self.base_term == term else 0
