"""Small log-bucketed latency histogram (HdrHistogram-lite).

Buckets grow geometrically by ``RATIO`` from ``MIN_S``; relative error per
bucket is ~5%, which is plenty for p50/p95/p99 reporting. Bucket counts are
plain dicts so they can be shipped cheaply between processes and merged.
"""

from __future__ import annotations

import math
from typing import Iterable, Mapping

MIN_S = 50e-6  # 50 µs
RATIO = 1.1
MAX_INDEX = 200  # 50µs * 1.1**200 ≈ 9.5e3 s — effectively unbounded
_LOG_RATIO = math.log(RATIO)


def bucket_index(seconds: float) -> int:
    if seconds <= MIN_S:
        return 0
    return min(MAX_INDEX, int(math.log(seconds / MIN_S) / _LOG_RATIO) + 1)


def bucket_upper(index: int) -> float:
    """Upper bound (seconds) represented by a bucket."""
    return MIN_S * (RATIO ** index)


class Histogram:
    __slots__ = ("counts", "total", "max_s")

    def __init__(self) -> None:
        self.counts: dict[int, int] = {}
        self.total = 0
        self.max_s = 0.0

    def record(self, seconds: float) -> None:
        i = bucket_index(seconds)
        self.counts[i] = self.counts.get(i, 0) + 1
        self.total += 1
        if seconds > self.max_s:
            self.max_s = seconds

    def merge_counts(self, counts: Mapping[int | str, int], max_s: float = 0.0) -> None:
        for k, v in counts.items():
            i = int(k)
            self.counts[i] = self.counts.get(i, 0) + int(v)
            self.total += int(v)
        if max_s > self.max_s:
            self.max_s = max_s

    def merge(self, other: "Histogram") -> None:
        self.merge_counts(other.counts, other.max_s)

    def percentile(self, p: float) -> float:
        """Return the latency (seconds) at percentile ``p`` in [0, 100]."""
        if self.total == 0:
            return 0.0
        target = max(1, math.ceil(self.total * p / 100.0))
        seen = 0
        for i in sorted(self.counts):
            seen += self.counts[i]
            if seen >= target:
                return min(bucket_upper(i), self.max_s) if self.max_s else bucket_upper(i)
        return self.max_s

    def summary_ms(self, percentiles: Iterable[float] = (50, 95, 99)) -> dict[str, float]:
        out = {f"p{int(p) if float(p).is_integer() else p}": round(self.percentile(p) * 1000, 3) for p in percentiles}
        out["max"] = round(self.max_s * 1000, 3)
        return out

    def reset(self) -> None:
        self.counts = {}
        self.total = 0
        self.max_s = 0.0
