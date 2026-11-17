"""Id allocation with overflow protection and multi-process striding."""

from __future__ import annotations

import threading

INT_MAX = 2**31 - 1
BIGINT_MAX = 2**63 - 1
LIMITS = {"int": INT_MAX, "bigint": BIGINT_MAX, "varint": BIGINT_MAX}


class IdSpaceExhausted(Exception):
    """Raised when the next id would not fit in the partition key type."""


class IdAllocator:
    """Hands out ``start + offset + k*stride`` for k = 0, 1, 2, ...

    ``offset``/``stride`` let N worker processes of one run share an id range
    without coordination (process i writes ids ≡ i mod N).
    """

    def __init__(self, start: int, *, offset: int = 0, stride: int = 1, limit: int = BIGINT_MAX) -> None:
        if stride < 1:
            raise ValueError("stride must be >= 1")
        self._next = start + offset
        self._stride = stride
        self.limit = limit
        self.start = start
        self.last: int | None = None
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            v = self._next
            if v > self.limit:
                raise IdSpaceExhausted(f"next id {v} exceeds key type limit {self.limit}")
            self._next = v + self._stride
            self.last = v
            return v

    @property
    def high_water(self) -> int:
        """Next id a *resumed* run should start from (last allocated + 1)."""
        return (self.last + 1) if self.last is not None else self.start
