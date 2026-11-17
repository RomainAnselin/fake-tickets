"""Test-double workloads that need no database (imported by spawned workers too)."""

from __future__ import annotations

from typing import Any, Optional

from faketickets.engine.dispatcher import Op
from faketickets.engine.ids import IdAllocator
from faketickets.workloads.base import Param, RunContext, Workload, rate_params, stop_params


class _Future:
    def add_callbacks(self, callback, errback, callback_args=(), errback_args=()):
        callback([], *callback_args)


class FakeSession:
    """execute_async completes immediately; lets us test pacing & plumbing without Cassandra."""

    def execute_async(self, stmt: Any, args: Any = None, timeout: Any = None) -> _Future:
        return _Future()


class DummyWorkload(Workload):
    name = "dummy"
    title = "Dummy"
    description = "test double"
    uses_ids = True
    needs_session = False
    params = [
        Param("table", "str", "dummy_t", "Table"),
        Param("start_id", "int", -1, "Start id", min=-1),
        Param("fail_setup", "bool", False, "Fail in setup"),
        Param("knob", "int", 1, "Live knob", min=0, max=10, live=True),
    ] + rate_params(200) + stop_params()

    def setup(self, ctx: RunContext) -> None:
        if ctx.params["fail_setup"]:
            raise RuntimeError("boom in setup")
        ctx.session = FakeSession()
        self.ids = IdAllocator(max(0, ctx.params["start_id"]), offset=ctx.proc_index, stride=ctx.proc_count)
        ctx.emit("info", f"dummy ready knob={ctx.params['knob']}")

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        i = self.ids.next()
        ctx.progress["next_id"] = i + 1
        ctx.progress["knob"] = ctx.params["knob"]
        return Op(None, (i,), "write")


class DummyOneShot(Workload):
    name = "dummy_oneshot"
    title = "Dummy one-shot"
    description = "test double"
    kind = "oneshot"
    needs_session = False
    params = [Param("answer", "int", 42, "Answer")]

    def run_once(self, ctx: RunContext) -> dict:
        return {"answer": ctx.params["answer"]}
