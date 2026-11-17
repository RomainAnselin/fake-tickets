"""Deadline / token based rate limiter with live re-targeting.

v1 executed a chunk of writes and *then* slept ``chunk / target_ops`` seconds,
so the achieved rate was always ``chunk / (exec_time + sleep)`` < target.

This limiter instead tracks how many permits are *due* since an anchor
instant (``(now - anchor) * rate``) and hands out the difference. The caller
asks for up to ``max_n`` permits and receives how many it may issue right now,
which keeps very high rates (50k+/s) cheap: no per-op sleep.

* ``rate <= 0`` means unlimited (the in-flight cap is the only limiter).
* ``set_rate`` re-anchors so a slider change takes effect immediately and
  never produces a catch-up burst.
* If the consumer falls behind (driver back-pressure), the debt is capped to
  ``burst_s`` worth of permits so we don't hammer the cluster to "catch up".
"""

from __future__ import annotations

import math
import threading
import time
from typing import Callable


class RateLimiter:
    def __init__(
        self,
        rate: float,
        *,
        burst_s: float = 0.1,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._burst_s = burst_s
        self._lock = threading.Lock()
        self._rate = 0.0
        self._anchor = clock()
        self._granted = 0
        self.set_rate(rate)

    @property
    def rate(self) -> float:
        return self._rate

    def set_rate(self, rate: float) -> None:
        with self._lock:
            self._rate = max(0.0, float(rate or 0.0))
            self._anchor = self._clock()
            self._granted = 0

    def _burst(self) -> int:
        return max(1, int(math.ceil(self._rate * self._burst_s)))

    def try_acquire(self, max_n: int) -> tuple[int, float]:
        """Return ``(permits, wait_s)``.

        ``permits`` may be issued immediately. When it is 0, ``wait_s`` is the
        time until the next permit becomes due.
        """
        if max_n <= 0:
            return 0, 0.0
        with self._lock:
            rate = self._rate
            if rate <= 0:
                return max_n, 0.0
            now = self._clock()
            # +1e-9: float rounding must not leave us at 0.99999 permits forever.
            due = int((now - self._anchor) * rate + 1e-9) - self._granted
            burst = self._burst()
            if due > burst:
                # We fell behind: forgive the debt beyond one burst.
                self._anchor = now - burst / rate
                self._granted = 0
                due = burst
            if due >= 1:
                n = min(due, max_n)
                self._granted += n
                return n, 0.0
            next_at = self._anchor + (self._granted + 1) / rate
            return 0, max(50e-6, next_at - now)

    def acquire(self, max_n: int, max_wait: float = 0.05) -> int:
        """Block at most ``max_wait`` seconds; return permits granted (may be 0)."""
        n, wait = self.try_acquire(max_n)
        if n:
            return n
        self._sleep(min(wait, max_wait))
        n, _ = self.try_acquire(max_n)
        return n
