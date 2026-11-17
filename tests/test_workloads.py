import random
from collections import deque

import pytest

from faketickets import workloads
from faketickets.engine.ids import IdAllocator
from faketickets.models import ConnectionSpec
from faketickets.workloads.base import check_identifier
from faketickets.workloads.datagen import OWNERS, fmt_ms, ticket_row
from faketickets.workloads.twcs import TwcsWorkload


def test_registry_has_the_core_workloads():
    assert set(workloads.REGISTRY) >= {"connectivity", "write", "read", "mixed", "twcs", "datatypes"}
    for w in workloads.REGISTRY.values():
        d = w.describe()
        assert d["params"] is not None and d["title"]
        w.validate({})  # defaults must always be valid


def test_validation_coerces_and_rejects():
    W = workloads.get("write")
    p = W.validate({"rate": "1500", "lwt": "true", "index": "sai"})
    assert p["rate"] == 1500 and p["lwt"] is True and p["index"] == "sai"
    with pytest.raises(ValueError, match="unknown parameter"):
        W.validate({"nope": 1})
    with pytest.raises(ValueError, match="rate"):
        W.validate({"rate": -1})
    with pytest.raises(ValueError, match="index"):
        W.validate({"index": "lucene"})


@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "a.b", "1abc", "", "a" * 49, "tbl'--"])
def test_identifiers_are_strict(bad):
    with pytest.raises(ValueError):
        check_identifier(bad)
    with pytest.raises(ValueError):
        workloads.get("write").validate({"table": bad} if bad else {"table": "ok", "keyspace": "x y"})


def test_unknown_workload():
    with pytest.raises(ValueError, match="unknown workload"):
        workloads.get("nope")


def test_ticket_row_shape_matches_v1():
    rng = random.Random(1)
    i, owner, ticket, ts, notes = ticket_row(10, rng, ts_ms=1_700_000_000_123)
    assert i == 10 and owner in OWNERS and 0 <= ticket <= 100_000
    assert notes.startswith("Python writing data with value 10")
    assert fmt_ms(1_700_000_000_007).endswith(".007")
    odd = ticket_row(11, rng, ts_ms=1_700_000_000_123, payload_bytes=500)[4]
    assert "Odd entries" in odd and len(odd) > 500


def test_connection_spec_validation():
    with pytest.raises(ValueError):
        ConnectionSpec(kind="cassandra")
    with pytest.raises(ValueError, match="Secure Connect Bundle"):
        ConnectionSpec(kind="astra")
    with pytest.raises(ValueError):
        ConnectionSpec(kind="astra", astra_scb_b64="eA==")
    s = ConnectionSpec(contact_points="10.0.0.1; 10.0.0.2", password="pw")
    assert s.contact_points == ["10.0.0.1", "10.0.0.2"]
    assert s.redacted()["password"] == "***"
    with pytest.raises(ValueError):
        ConnectionSpec(contact_points=["h"], keyspace="bad-ks")


class _Ctx:
    def __init__(self, params):
        self.params = params
        self.progress = {}
        self.events = []

    def emit(self, level, msg):
        self.events.append((level, msg))


def _twcs(now, **over):
    params = workloads.get("twcs").validate(over)
    wl = TwcsWorkload.__new__(TwcsWorkload)
    wl.rng = random.Random(3)
    wl.stmt = wl.stmt_ts = "stmt"
    wl.ids = IdAllocator(1000)
    wl.cp_t, wl.cp_id = deque(), deque()
    wl.next_cp = now
    wl.lookback_s = params["overlap_lookback_min"] * 60
    wl.next_overlap = now + params["overlap_every_min"] * 60 if params["overlap_every_min"] > 0 else None
    wl.next_backdate = now + params["backdate_every_min"] * 60 if params["backdate_every_min"] > 0 else None
    wl.overlaps = wl.backdates = 0
    return wl, _Ctx(params)


def test_twcs_overlap_is_wall_clock_scheduled_and_bounded_to_lookback():
    """Simulate 3 h at a *varying* rate: overlaps fire every 30 min regardless of rate,
    and the rewritten id is always within the last 2 h of writes (decision K7)."""
    now = 0.0
    wl, ctx = _twcs(now)
    history = []  # (t, id)
    overlap_ops = []
    t = 0.0
    while t < 3 * 3600:
        rate = 50 if (t // 600) % 2 else 400  # alternate 400/s and 50/s every 10 min
        for _ in range(int(rate)):
            history.append((t, wl.ids.next()))
        for op in wl.tick(ctx, t):
            overlap_ops.append((t, op))
        t += 1.0
    assert [round(x[0]) for x in overlap_ops] == [1800, 3600, 5400, 7200, 9000]
    for t_fire, op in overlap_ops:
        oid = op.args[0]
        assert op.kind == "overlap" and "OVERLAP REWRITE" in op.args[4]
        written_at = next(tt for tt, i in history if i == oid)
        assert t_fire - written_at <= 2 * 3600 + 2
    assert ctx.progress["overlaps"] == 5
    assert sum(1 for lvl, m in ctx.events if m.startswith("OVERLAP")) == 5


def test_twcs_backdated_writes_use_past_timestamp():
    base = 1_800_000_000.0
    wl, ctx = _twcs(base, overlap_every_min=0, backdate_every_min=10, backdate_offset_min=360)
    wl.ids.next()
    ops = []
    for t in range(0, 1300):
        ops += wl.tick(ctx, base + t)
    assert len(ops) == 2 and all(o.kind == "backdated" for o in ops)
    using_ts_us = ops[0].args[5]
    assert using_ts_us == int(base + 600 - 360 * 60) * 1_000_000
    assert ops[0].args[3] * 1000 == using_ts_us  # `time` column matches the write timestamp


def test_twcs_schedule_skips_missed_slots_after_clock_jump():
    wl, ctx = _twcs(0.0, overlap_every_min=0, backdate_every_min=1)
    wl.ids.next()
    ops = []
    for t in (0, 30, 60, 100_000, 100_001, 100_059, 100_060):
        ops += wl.tick(ctx, float(t))
    assert len(ops) == 3  # t=60, the jump at t=100000, and t=100060 — no burst of ~1666 catch-ups
