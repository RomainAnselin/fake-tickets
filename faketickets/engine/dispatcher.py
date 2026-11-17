"""Asynchronous request dispatcher with a live-adjustable in-flight cap."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .stats import Recorder


@dataclass(slots=True)
class Op:
    """One CQL request: a (prepared) statement, its bind args and a label."""

    stmt: Any
    args: Any = None
    kind: str = "write"
    # Called with the first page of rows on success (driver event-loop thread).
    on_rows: Optional[Callable[[Any], None]] = None


class Dispatcher:
    def __init__(self, session: Any, recorder: Recorder, concurrency: int, timeout: float | None = None) -> None:
        self._session = session
        self._rec = recorder
        self._cv = threading.Condition()
        self._inflight = 0
        self._concurrency = max(1, int(concurrency))
        self._timeout = timeout

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def concurrency(self) -> int:
        return self._concurrency

    def set_concurrency(self, n: int) -> None:
        with self._cv:
            self._concurrency = max(1, int(n))
            self._cv.notify_all()

    def free_slots(self, wait: float = 0.05) -> int:
        """Number of requests that can be sent now; waits up to ``wait`` for one."""
        with self._cv:
            if self._inflight >= self._concurrency:
                self._cv.wait(wait)
            return max(0, self._concurrency - self._inflight)

    def submit(self, op: Op) -> None:
        with self._cv:
            self._inflight += 1
        t0 = time.perf_counter()
        try:
            if self._timeout is None:
                fut = self._session.execute_async(op.stmt, op.args)
            else:
                fut = self._session.execute_async(op.stmt, op.args, timeout=self._timeout)
        except Exception as exc:  # synchronous failure (e.g. NoHostAvailable)
            self._done_err(exc, op, t0)
            return
        fut.add_callbacks(self._done_ok, self._done_err, callback_args=(op, t0), errback_args=(op, t0))

    def _release(self) -> None:
        with self._cv:
            self._inflight -= 1
            self._cv.notify_all()

    def _done_ok(self, rows: Any, op: Op, t0: float) -> None:
        try:
            self._rec.ok(op.kind, time.perf_counter() - t0)
            if op.on_rows is not None:
                op.on_rows(rows)
        finally:
            self._release()

    def _done_err(self, exc: BaseException, op: Op, t0: float) -> None:
        try:
            self._rec.err(op.kind, time.perf_counter() - t0, exc)
        finally:
            self._release()

    def drain(self, timeout: float = 30.0) -> bool:
        """Wait for all in-flight requests; return False on timeout."""
        deadline = time.monotonic() + timeout
        with self._cv:
            while self._inflight > 0:
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._cv.wait(min(left, 0.2))
        return True
