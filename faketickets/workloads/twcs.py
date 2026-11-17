"""W5 — reproduction of a specific TWCS bug (REPRO2): overlap rewrites + back-dated writes.

Port of v1 ``fake_tickets_twcs_workflow`` (REPRO2) and ``oldrecord.sh`` (v1, tag v1.0). It keeps
v1's exact table options (incl. ``timestamp_resolution = MILLISECONDS``). For general
TWCS / TTL testing use the ``timeseries`` workload instead.

Difference from v1 (decision K7): overlap events are scheduled on the **wall
clock** ("every N minutes") instead of "every target_ops*N*60 ids", and the
lookback pool is resolved from time→id checkpoints. v1's arithmetic drifts
whenever the achieved rate differs from the target, and would be meaningless
with a live rate slider.
"""

from __future__ import annotations

import bisect
import random
from collections import deque
from typing import Optional

from ..db.cluster import ensure_keyspace
from ..engine.dispatcher import Op
from .base import wait_for_table, Param, RunContext, Workload, rate_params, stop_params, monitor_params
from .datagen import fmt_ms, ticket_row
from .tickets import _common_write_params, create_tickets_table, id_limit, prepare_insert, setup_ids


class TwcsWorkload(Workload):
    name = "twcs"
    category = "Bug repro"
    title = "TWCS bug repro (REPRO2)"
    description = (
        "Reproduction of a specific TWCS issue with v1's exact table options: ingestion with default TTL, "
        "an id from the recent past re-written every N minutes (cross-window overlap) and optional "
        "back-dated USING TIMESTAMP writes (oldrecord.sh). For general TWCS testing use Time-series."
    )
    approx_row_bytes = 130
    uses_ids = True
    params = _common_write_params("tickets_twcs") + [
        Param("ttl_seconds", "int", 172800, "Default TTL (s)", "table default_time_to_live (owner's goal: 2 days)", 60, 630_720_000),
        Param("window_hours", "int", 1, "TWCS window (hours)", "", 1, 168),
        Param("gc_grace_seconds", "int", 10800, "gc_grace_seconds", "", 0, 864000, group="advanced"),
        Param("overlap_every_min", "float", 30, "Overlap rewrite every (min)", "0 = off", 0, 10080),
        Param("overlap_lookback_min", "float", 120, "Overlap lookback (min)", "pool of past ids to pick from", 0.01, 10080,
              visible_if={"overlap_every_min": [{"gt": 0}]}),
        Param("backdate_every_min", "float", 0, "Back-dated write every (min)", "0 = off", 0, 10080),
        Param("backdate_offset_min", "float", 360, "Back-date by (min)", "USING TIMESTAMP now - offset", 0.01, 525600,
              visible_if={"backdate_every_min": [{"gt": 0}]}),
    ] + rate_params(1000) + monitor_params() + stop_params()

    CHECKPOINT_EVERY_S = 1.0

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        if ctx.proc_index == 0:
            ensure_keyspace(ctx.session, ctx.spec, ctx.keyspace, create=p["create_keyspace"])
            create_tickets_table(
                ctx,
                id_type=p["id_type"],
                compaction="twcs",
                window_hours=p["window_hours"],
                default_ttl=p["ttl_seconds"],
                gc_grace=p["gc_grace_seconds"],
            )
        else:
            wait_for_table(ctx)
        self.stmt = prepare_insert(ctx)
        self.stmt_ts = prepare_insert(ctx, using_timestamp=True)
        self.rng = random.Random()
        self.ids = setup_ids(ctx, p["start_id"], id_limit(ctx))
        now = ctx.clock()
        self.started = now
        self.cp_t: deque[float] = deque()
        self.cp_id: deque[int] = deque()
        self.next_cp = now
        self.next_overlap = now + p["overlap_every_min"] * 60 if p["overlap_every_min"] > 0 else None
        self.next_backdate = now + p["backdate_every_min"] * 60 if p["backdate_every_min"] > 0 else None
        self.lookback_s = p["overlap_lookback_min"] * 60
        self.overlaps = 0
        self.backdates = 0

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        i = self.ids.next()
        ctx.progress["next_id"] = i + 1
        return Op(self.stmt, ticket_row(i, self.rng, payload_bytes=ctx.params["payload_bytes"]), "write")

    # --- scheduled events ----------------------------------------------------
    def _checkpoint(self, now: float) -> None:
        if self.ids.last is None:
            return
        self.cp_t.append(now)
        self.cp_id.append(self.ids.last)
        horizon = now - self.lookback_s - 60
        while self.cp_t and self.cp_t[0] < horizon:
            self.cp_t.popleft()
            self.cp_id.popleft()

    def pool(self, now: float) -> Optional[tuple[int, int]]:
        """Ids written within the lookback window: [oldest id after now-lookback, last id]."""
        if self.ids.last is None:
            return None
        cutoff = now - self.lookback_s
        if not self.cp_t or self.cp_t[0] > cutoff:
            lo = self.ids.start  # history shorter than the lookback: whole run is eligible (v1 behaviour)
        else:
            k = bisect.bisect_left(list(self.cp_t), cutoff)
            lo = self.cp_id[k] if k < len(self.cp_id) else self.ids.last
        lo = max(self.ids.start, min(lo, self.ids.last))
        return lo, self.ids.last

    def tick(self, ctx: RunContext, now: float) -> list[Op]:
        ops: list[Op] = []
        if now >= self.next_cp:
            self._checkpoint(now)
            self.next_cp = now + self.CHECKPOINT_EVERY_S
        if self.next_overlap is not None and now >= self.next_overlap:
            self.next_overlap = _next(self.next_overlap, ctx.params["overlap_every_min"] * 60, now)
            pool = self.pool(now)
            if pool:
                oid = self.rng.randint(*pool)
                row = list(ticket_row(oid, self.rng))
                row[4] = (
                    f"*** OVERLAP REWRITE | original_id={oid} | rewrite_at_counter={self.ids.last} "
                    f"| owner={row[1]} | written_at={fmt_ms(row[3])} ***"
                )
                ops.append(Op(self.stmt, tuple(row), "overlap"))
                self.overlaps += 1
                ctx.progress["overlaps"] = self.overlaps
                ctx.emit("event", f"OVERLAP id={oid} pool=[{pool[0]},{pool[1]}] trigger_i={self.ids.last} owner={row[1]} ticket={row[2]}")
        if self.next_backdate is not None and now >= self.next_backdate:
            self.next_backdate = _next(self.next_backdate, ctx.params["backdate_every_min"] * 60, now)
            offset_s = ctx.params["backdate_offset_min"] * 60
            ts_ms = int((now - offset_s) * 1000)
            i = self.ids.next()
            row = ticket_row(i, self.rng, ts_ms=ts_ms, tag="*** BACKDATED *** ")
            ops.append(Op(self.stmt_ts, row + (ts_ms * 1000,), "backdated"))
            self.backdates += 1
            ctx.progress["backdated"] = self.backdates
            ctx.emit("event", f"BACKDATED id={i} write_timestamp={fmt_ms(ts_ms)} (now-{ctx.params['backdate_offset_min']}min)")
        return ops


def _next(prev: float, every: float, now: float) -> float:
    """Next fire time; after a clock jump / long stall, skip missed slots instead of bursting."""
    nxt = prev + every
    return nxt if nxt > now else now + every
