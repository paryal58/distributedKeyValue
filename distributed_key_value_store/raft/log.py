"""The replicated log. Indices are 1-based; index 0 is a virtual entry with term 0."""
from __future__ import annotations


class Log:
    def __init__(self, entries=None):
        # each entry: {"term": int, "command": dict}
        self.entries: list[dict] = list(entries or [])

    def last_index(self) -> int:
        return len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1]["term"] if self.entries else 0

    def term_at(self, index: int):
        """Term of the entry at `index`, 0 for index 0, None if out of range."""
        if index == 0:
            return 0
        if index < 0 or index > len(self.entries):
            return None
        return self.entries[index - 1]["term"]

    def get(self, index: int) -> dict:
        return self.entries[index - 1]

    def append(self, entry: dict) -> int:
        self.entries.append(entry)
        return len(self.entries)

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and everything after it."""
        del self.entries[index - 1:]

    def slice(self, start: int, max_entries: int) -> list[dict]:
        return self.entries[start - 1:start - 1 + max_entries]

    def first_index_of_term(self, term: int, upto: int) -> int:
        i = upto
        while i > 1 and self.term_at(i - 1) == term:
            i -= 1
        return i

    def last_index_of_term(self, term: int) -> int:
        """Highest index whose entry has `term`, or 0 if the term is absent."""
        i = self.last_index()
        while i >= 1:
            t = self.term_at(i)
            if t == term:
                return i
            if t < term:  # terms never decrease along the log
                return 0
            i -= 1
        return 0
