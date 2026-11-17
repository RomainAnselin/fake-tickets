"""Thread-safe per-interval counters, shipped from worker to manager."""

from __future__ import annotations

import threading
from collections import Counter

from .histogram import Histogram


class Recorder:
    """Collects results from driver callbacks (event-loop thread) and the
    worker thread; ``drain`` returns and resets the interval deltas."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset()
        self.total_ok = 0
        self.total_err = 0
        self.last_error: str | None = None

    def _reset(self) -> None:
        self._ok: Counter[str] = Counter()
        self._err: Counter[str] = Counter()
        self._extra: Counter[str] = Counter()
        self._errtypes: Counter[str] = Counter()
        self._hist = Histogram()

    def ok(self, kind: str, latency_s: float) -> None:
        with self._lock:
            self._ok[kind] += 1
            self.total_ok += 1
            self._hist.record(latency_s)

    def err(self, kind: str, latency_s: float, exc: BaseException) -> None:
        name = type(exc).__name__
        with self._lock:
            self._err[kind] += 1
            self.total_err += 1
            self._errtypes[name] += 1
            self.last_error = f"{name}: {exc}"[:500]

    def bump(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._extra[key] += n

    def drain(self) -> dict:
        with self._lock:
            out = {
                "ok": dict(self._ok),
                "err": dict(self._err),
                "extra": dict(self._extra),
                "errtypes": dict(self._errtypes),
                "hist": dict(self._hist.counts),
                "hist_max": self._hist.max_s,
                "last_error": self.last_error,
            }
            self._reset()
            return out
