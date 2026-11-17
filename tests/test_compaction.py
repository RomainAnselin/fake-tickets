"""Unit tests for compaction options, compaction workloads' op sequencing, counting and progress merge."""

import random
import threading
from collections import Counter
from types import SimpleNamespace

import pytest

from dummy_workloads import FakeSession
from faketickets import workloads
from faketickets.backend.manager import Proc, merge_progress
from faketickets.engine.dispatcher import Dispatcher, Op
from faketickets.engine.ids import IdAllocator
from faketickets.engine.ratelimit import RateLimiter
from faketickets.engine.stats import Recorder
from faketickets.workloads.base import RunContext, Workload
from faketickets.workloads.compaction import OverwriteWorkload, TombstoneWorkload
from faketickets.workloads.tableopts import compaction_map, table_options_cql
from faketickets.worker import run_loop


def test_registry_categories():
    cats = {n: w.category for n, w in workloads.REGISTRY.items()}
    assert {n for n, c in cats.items() if c == "Compaction"} == {"overwrite", "tombstones", "timeseries", "alter_table", "table_stats"}
    assert cats["twcs"] == "Bug repro"
    for w in workloads.REGISTRY.values():
        d = w.describe()
        assert d["category"] and "approx_row_bytes" in d


@pytest.mark.parametrize("strategy,expect", [
    ("stcs", {"class": "SizeTieredCompactionStrategy", "min_threshold": "4", "max_threshold": "32"}),
    ("lcs", {"class": "LeveledCompactionStrategy", "sstable_size_in_mb": "160", "fanout_size": "10"}),
    ("ucs", {"class": "UnifiedCompactionStrategy", "scaling_parameters": "T4", "target_sstable_size": "1024MiB"}),
    ("twcs", {"class": "TimeWindowCompactionStrategy", "compaction_window_unit": "HOURS", "compaction_window_size": "1",
              "timestamp_resolution": "MICROSECONDS"}),
])
def test_compaction_map_per_strategy(strategy, expect):
    p = workloads.get("write").validate({"compaction": strategy})
    m = compaction_map(p)
    assert expect.items() <= m.items()
    assert m["unchecked_tombstone_compaction"] == "false" and m["tombstone_threshold"] == "0.2"
    cql = table_options_cql(p, default_ttl=60)
    assert cql.startswith("compaction = {'class': ") and cql.endswith("gc_grace_seconds = 864000 AND default_time_to_live = 60")


@pytest.mark.parametrize("bad", ["T4'}; DROP TABLE x", "X4", "T4,", "t4", "T12345", ""])
def test_ucs_scaling_parameters_are_strict(bad):
    if bad == "":
        assert workloads.get("write").validate({"compaction": "ucs", "ucs_scaling_parameters": bad})["ucs_scaling_parameters"] == "T4"
        return
    with pytest.raises(ValueError):
        workloads.get("write").validate({"compaction": "ucs", "ucs_scaling_parameters": bad})


def test_stcs_thresholds_cross_checked():
    with pytest.raises(ValueError):
        workloads.get("overwrite").validate({"stcs_min_threshold": 40, "stcs_max_threshold": 32})


def _ctx(params, proc_index=0, proc_count=1):
    return RunContext(session=None, spec=None, params=params, recorder=Recorder(), emit=lambda *a: None,
                      proc_index=proc_index, proc_count=proc_count)


def _tombstones(**over):
    params = workloads.get("tombstones").validate({"start_id": 0, **over})
    ctx = _ctx(params)
    wl = TombstoneWorkload.__new__(TombstoneWorkload)
    wl.ins, wl.del_row, wl.del_range, wl.del_cell, wl.sel = "ins", "del_row", "del_range", "del_cell", "sel"
    wl.rng = random.Random(1)
    wl.ids = IdAllocator(0)
    wl.lag = params["delete_lag"]
    wl.pending, wl.deletes, wl._cb = [], 0, None
    return wl, ctx


def test_tombstone_queue_deletes_precede_writes_and_keep_lag_rows_live():
    wl, ctx = _tombstones(delete_kind="row", delete_lag=10, partitions=3)
    live = set()
    ops = [wl.next_op(ctx) for _ in range(60)]  # ids 0-9 written plainly, then 25 × (delete id-10, write id) for ids 10-34
    for op in ops:
        if op.kind == "write":
            live.add(op.args[1])
        else:
            assert op.stmt == "del_row" and op.args[0] == op.args[1] % 3
            live.discard(op.args[1])
    assert ops[-1].kind == "write" and len(live) == 10 and max(live) == 34


def test_tombstone_range_and_ttl_kinds():
    wl, ctx = _tombstones(delete_kind="range", delete_lag=5, partitions=2, range_every=3)
    ops = [wl.next_op(ctx) for _ in range(40)]
    ranges = [o for o in ops if o.kind == "delete"]
    assert ranges and all(o.stmt == "del_range" and o.args[1] >= 0 for o in ranges)
    wl, ctx = _tombstones(delete_kind="ttl")
    assert all(wl.next_op(ctx).kind == "write" for _ in range(50))


def _overwrite(**over):
    params = workloads.get("overwrite").validate(over)
    wl = OverwriteWorkload.__new__(OverwriteWorkload)
    wl.rng, wl.n, wl.keys = random.Random(2), 0, params["keys"]
    wl.hot = max(1, int(wl.keys * params["hot_keys_pct"] / 100))
    return wl, params


def test_overwrite_key_distributions():
    wl, p = _overwrite(keys=100, distribution="sequential")
    keys = []
    for _ in range(250):
        keys.append(wl.key(_ctx(p)))
        wl.n += 1
    assert keys[:100] == list(range(100)) and keys[100:200] == list(range(100))
    wl, p = _overwrite(keys=10_000, distribution="hotspot", hot_keys_pct=1, hot_ops_pct=90)
    hot = sum(wl.key(_ctx(p)) < 100 for _ in range(20_000)) / 20_000
    assert 0.88 < hot < 0.92


def test_overwrite_sequential_shards_interleave():
    wl0, p = _overwrite(keys=10, distribution="sequential")
    wl1, _ = _overwrite(keys=10, distribution="sequential")
    seen = []
    for _ in range(5):
        seen += [wl0.key(_ctx(p, 0, 2)), wl1.key(_ctx(p, 1, 2))]
        wl0.n += 1
        wl1.n += 1
    assert sorted(seen) == list(range(10))


class _ReadWrite(Workload):
    name, title, description = "rw_t", "rw", "test"
    count_kinds = frozenset({"write"})
    needs_session = False

    def __init__(self):
        self.n = 0

    def next_op(self, ctx):
        self.n += 1
        return Op(None, (self.n,), "read" if self.n % 2 else "write")


def test_count_only_counts_count_kinds():
    ctx = _ctx({})
    rec = ctx.recorder
    disp = Dispatcher(FakeSession(), rec, 64)
    reason = run_loop(_ReadWrite(), ctx, disp, RateLimiter(0), count=100, duration_s=0)
    assert reason == "count_reached" and ctx.progress["counted"] == 100
    snap = rec.drain()
    assert snap["ok"]["write"] == 100 and snap["ok"]["read"] in (100, 101)


def test_merge_progress():
    procs = [Proc(index=0, process=None, conn=None), Proc(index=1, process=None, conn=None)]
    procs[0].progress = {"next_id": 5, "counted": 3, "deletes": 4, "late": 1, "pass": 2, "knob": 7, "cmp_disk_mib": 10}
    procs[1].progress = {"next_id": 6, "counted": 3, "deletes": 6, "late": 2, "pass": 3, "knob": 7, "cmp_disk_mib": 99}
    assert merge_progress(procs) == {"deletes": 10, "late": 3, "pass": 3, "knob": 7, "cmp_disk_mib": 10}
