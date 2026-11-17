"""Compaction-oriented workloads.

Insert-once rows on unique keys give a compaction strategy nothing to merge.
These workloads create the data shapes that make strategies behave
differently:

* ``overwrite``   — the same keys rewritten over and over (whole row or one column):
                    obsolete versions spread across sstables → space and read amplification.
* ``tombstones``  — queue-style append + delete (row / range / cell / TTL) with reads
                    from the head: tombstone build-up, gc_grace, purge behaviour.
* ``timeseries``  — sensor time-series with TTL, optional mixed TTLs and late
                    (back-dated) writes: TWCS window expiry and the things that block it.
* ``alter_table`` — one-shot ALTER of compaction / gc_grace / default TTL (switch strategy
                    while another run keeps writing).
* ``table_stats`` — one-shot snapshot of table options + per-node compaction view.
"""

from __future__ import annotations

import datetime as dt
import math
import random
import time
from typing import Any, Optional

from ..db.cluster import column_type, ensure_keyspace
from ..engine.dispatcher import Op
from ..engine.ids import BIGINT_MAX
from .base import (
    CONSISTENCY_CHOICES, Param, RunContext, Workload, consistency, monitor_params, rate_params, stop_params,
    table_exists, target_params, wait_for_table,
)
from .datagen import OWNERS, padding, ticket_row
from .monitor import CompactionMonitor
from .tableopts import STRATEGY_CLASS, check_compaction, compaction_params, create_or_reconcile, current_options, table_options_cql
from .tickets import setup_ids, tickets_ddl

_CL = Param("consistency", "choice", "LOCAL_QUORUM", "Consistency", choices=CONSISTENCY_CHOICES, group="advanced")


def _rows_cb(ctx: RunContext):
    rec = ctx.recorder

    def cb(rows: Any) -> None:
        rows = list(rows or [])
        rec.bump("hit" if rows else "miss")
        if len(rows) > 1:
            rec.bump("rows", len(rows))

    return cb


def _prepare(ctx: RunContext, cql: str):
    stmt = ctx.session.prepare(cql)
    stmt.consistency_level = consistency(ctx.params["consistency"])
    return stmt


def _schema(ctx: RunContext, ddl: str, *, default_ttl: Optional[int] = None) -> None:
    if ctx.proc_index == 0:
        ensure_keyspace(ctx.session, ctx.spec, ctx.keyspace, create=ctx.params["create_keyspace"])
        create_or_reconcile(ctx, ddl, default_ttl=default_ttl)
    else:
        wait_for_table(ctx)


# --------------------------------------------------------------------------- overwrite
class OverwriteWorkload(Workload):
    name = "overwrite"
    category = "Compaction"
    title = "Overwrite / update-heavy"
    description = (
        "Rewrites a fixed set of keys again and again (whole row, or only the notes column), "
        "uniformly, on a hot set, or in sequential passes. Obsolete versions pile up across "
        "sstables: compare space and read amplification between STCS, LCS and UCS."
    )
    count_kinds = frozenset({"write"})
    approx_row_bytes = 130
    params = target_params("ft_overwrite") + [
        Param("keys", "int", 100_000, "Distinct keys", "size of the key space being rewritten", 1, 10**10),
        Param("distribution", "choice", "uniform", "Key choice", "sequential = full passes over the key space (each pass rewrites every row)",
              choices=["uniform", "hotspot", "sequential"]),
        Param("hot_keys_pct", "float", 1.0, "Hot keys (% of keys)", "", 0.001, 100, visible_if={"distribution": ["hotspot"]}),
        Param("hot_ops_pct", "float", 90.0, "Ops on hot keys (%)", "", 0, 100, visible_if={"distribution": ["hotspot"]}),
        Param("update_mode", "choice", "full_row", "Update mode",
              "full_row = INSERT every column; one_column = UPDATE notes only (row fragments spread over sstables)",
              choices=["full_row", "one_column"]),
        Param("read_pct", "int", 0, "Read percentage", "point reads by key, same distribution (read latency under amplification)", 0, 100, live=True),
        Param("payload_bytes", "int", 0, "Extra payload bytes", "random filler appended to notes", 0, 1_048_576, group="advanced"),
        _CL,
    ] + compaction_params("stcs") + rate_params(1000) + monitor_params() + stop_params(
        "Number of writes", "0 = no limit (every write overwrites an existing key once the key space is filled)")

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        _schema(ctx, tickets_ddl(ctx, "bigint"))
        ks, t = ctx.keyspace, ctx.table
        if column_type(ctx.session, ks, t, "id") not in ("bigint", "int"):
            raise RuntimeError(f"{ks}.{t} is not a tickets-shaped table")
        self.ins = _prepare(ctx, f"INSERT INTO {ks}.{t} (id, ownedby, ticket, time, updatedat, notes) VALUES (?, ?, ?, ?, ?, ?)")
        self.upd = _prepare(ctx, f"UPDATE {ks}.{t} SET notes = ?, updatedat = ? WHERE id = ?")
        self.sel = _prepare(ctx, f"SELECT id, ownedby, ticket, notes FROM {ks}.{t} WHERE id = ?")
        self.rng = random.Random()
        self.n = 0  # writes issued by this shard
        self.keys = p["keys"]
        self.hot = max(1, int(self.keys * p["hot_keys_pct"] / 100))
        self._cb = _rows_cb(ctx)

    def key(self, ctx: RunContext) -> int:
        d = ctx.params["distribution"]
        if d == "sequential":
            return (self.n * ctx.proc_count + ctx.proc_index) % self.keys
        if d == "hotspot" and self.rng.random() * 100 < ctx.params["hot_ops_pct"]:
            return self.rng.randrange(self.hot)
        if d == "hotspot":
            return self.rng.randrange(self.hot, self.keys) if self.hot < self.keys else self.rng.randrange(self.keys)
        return self.rng.randrange(self.keys)

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        p = ctx.params
        if p["read_pct"] and self.rng.random() * 100 < p["read_pct"]:
            return Op(self.sel, (self.key(ctx),), "read", self._cb)
        k = self.key(ctx)
        self.n += 1
        if p["distribution"] == "sequential":
            ctx.progress["pass"] = (self.n * ctx.proc_count) // self.keys + 1
        now = int(time.time() * 1000)
        if p["update_mode"] == "one_column":
            note = f"update #{self.n} of key {k} at {now}" + (" " + padding(self.rng, p["payload_bytes"]) if p["payload_bytes"] else "")
            return Op(self.upd, (note, now, k), "write")
        i, owner, ticket, ts, notes = ticket_row(k, self.rng, payload_bytes=p["payload_bytes"])
        return Op(self.ins, (i, owner, ticket, ts, ts, notes), "write")


# --------------------------------------------------------------------------- tombstones
class TombstoneWorkload(Workload):
    name = "tombstones"
    category = "Compaction"
    title = "Deletes & tombstones (queue)"
    description = (
        "Queue pattern: rows are appended to N partitions and deleted again once they are "
        "'lag' rows old — as row, range or cell deletes, or by a short TTL. Optional reads "
        "from the head of each partition scan the tombstones (warn / failure thresholds). "
        "Use with a low gc_grace_seconds to watch purging."
    )
    uses_ids = True
    count_kinds = frozenset({"write"})
    approx_row_bytes = 120
    params = target_params("ft_queue") + [
        Param("start_id", "int", -1, "Start sequence", "-1 = resume after the last run on this table (else 0)", -1, 2**62),
        Param("partitions", "int", 10, "Partitions (queues)", "rows are spread round-robin over these partitions", 1, 1_000_000),
        Param("delete_kind", "choice", "row", "Delete as",
              "row = DELETE one row; range = DELETE … WHERE seq < x every N rows; cell = SET payload = null; ttl = rows expire (no DELETE)",
              choices=["row", "range", "cell", "ttl"]),
        Param("delete_lag", "int", 1000, "Live rows kept", "a row is deleted once this many newer rows exist", 0, 10**9,
              visible_if={"delete_kind": ["row", "range", "cell"]}),
        Param("range_every", "int", 100, "Range delete every N rows (per partition)", "", 1, 10**6, visible_if={"delete_kind": ["range"]}),
        Param("row_ttl_seconds", "int", 60, "Row TTL (s)", "", 1, 630_720_000, visible_if={"delete_kind": ["ttl"]}),
        Param("read_pct", "int", 0, "Read percentage", "reads of the oldest rows of a partition — the queue anti-pattern", 0, 100, live=True),
        Param("read_limit", "int", 10, "Rows per read", "", 1, 10_000),
        Param("payload_bytes", "int", 64, "Payload bytes", "", 0, 1_048_576, group="advanced"),
        _CL,
    ] + compaction_params("stcs", gc_grace_default=3600) + rate_params(1000) + monitor_params() + stop_params(
        "Number of records", "0 = no limit (rows appended; deletes and reads are not counted)")

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        ks, t = ctx.keyspace, ctx.table
        _schema(ctx, f"CREATE TABLE IF NOT EXISTS {ks}.{t} (bucket int, seq bigint, payload text, PRIMARY KEY ((bucket), seq))")
        ttl = f" USING TTL {int(p['row_ttl_seconds'])}" if p["delete_kind"] == "ttl" else ""
        self.ins = _prepare(ctx, f"INSERT INTO {ks}.{t} (bucket, seq, payload) VALUES (?, ?, ?){ttl}")
        self.del_row = _prepare(ctx, f"DELETE FROM {ks}.{t} WHERE bucket = ? AND seq = ?")
        self.del_range = _prepare(ctx, f"DELETE FROM {ks}.{t} WHERE bucket = ? AND seq < ?")
        self.del_cell = _prepare(ctx, f"UPDATE {ks}.{t} SET payload = null WHERE bucket = ? AND seq = ?")
        self.sel = _prepare(ctx, f"SELECT seq, payload FROM {ks}.{t} WHERE bucket = ? LIMIT {int(p['read_limit'])}")
        self.rng = random.Random()
        self.ids = setup_ids(ctx, p["start_id"], BIGINT_MAX)
        # with N processes ids are strided: round the lag up so each shard deletes its own rows
        self.lag = math.ceil(p["delete_lag"] / ctx.proc_count) * ctx.proc_count
        self.pending: list[Op] = []
        self.deletes = 0
        self._cb = _rows_cb(ctx)

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        p = ctx.params
        if self.pending:
            return self.pending.pop()
        if p["read_pct"] and self.rng.random() * 100 < p["read_pct"]:
            return Op(self.sel, (self.rng.randrange(p["partitions"]),), "read", self._cb)
        i = self.ids.next()
        ctx.progress["next_id"] = i + 1
        nb = p["partitions"]
        kind = p["delete_kind"]
        old = i - self.lag
        if kind in ("row", "cell") and old >= 0:  # also continues the queue of a resumed run
            self.pending.append(Op(self.del_row if kind == "row" else self.del_cell, (old % nb, old), "delete"))
        elif kind == "range" and old >= 0 and (i // nb) % p["range_every"] == 0:
            self.pending.append(Op(self.del_range, (i % nb, old), "delete"))
        write = Op(self.ins, (i % nb, i, padding(self.rng, p["payload_bytes"]) or f"row {i}"), "write")
        if not self.pending:
            return write
        # issue the delete first and queue the write: a count stop (which follows a write) never
        # leaves a delete un-sent, so exactly `lag` rows stay live
        self.deletes += 1
        ctx.progress["deletes"] = self.deletes
        delete = self.pending.pop()
        self.pending.append(write)
        return delete


# --------------------------------------------------------------------------- timeseries
class TimeseriesWorkload(Workload):
    name = "timeseries"
    category = "Compaction"
    title = "Time-series (TWCS / TTL expiry)"
    description = (
        "Sensor readings in (sensor, time-bucket) partitions with a TTL. Optional mixed TTLs "
        "(some rows live much longer or forever) and late, back-dated writes that make new sstables "
        "overlap older windows — the two classic reasons TWCS cannot drop whole expired sstables."
    )
    count_kinds = frozenset({"write"})
    approx_row_bytes = 90
    params = target_params("ft_timeseries") + [
        Param("sensors", "int", 100, "Sensors", "one partition per sensor per time bucket", 1, 10_000_000),
        Param("bucket_minutes", "int", 60, "Partition time bucket (min)", "bounds partition size", 1, 525_600),
        Param("ttl_mode", "choice", "table_default", "TTL", "table default_time_to_live, or USING TTL on every row",
              choices=["table_default", "per_row", "none"]),
        Param("ttl_seconds", "int", 3600, "TTL (s)", "", 1, 630_720_000, visible_if={"ttl_mode": ["table_default", "per_row"]}),
        Param("long_ttl_pct", "float", 0.0, "Rows with a different TTL (%)", "mixed TTLs keep expired windows alive", 0, 100),
        Param("long_ttl_seconds", "int", 0, "Their TTL (s)", "0 = never expire", 0, 630_720_000, visible_if={"long_ttl_pct": [{"gt": 0}]}),
        Param("late_pct", "float", 0.0, "Late writes (%)", "rows written with a past timestamp (USING TIMESTAMP): flushed with current data, so new sstables overlap older windows", 0, 100),
        Param("late_max_min", "float", 60.0, "Up to (min) late", "", 0.01, 525_600, visible_if={"late_pct": [{"gt": 0}]}),
        Param("read_pct", "int", 0, "Read percentage", "latest readings of a random sensor", 0, 100, live=True),
        Param("read_limit", "int", 100, "Rows per read", "", 1, 10_000),
        Param("payload_bytes", "int", 0, "Payload bytes", "", 0, 1_048_576, group="advanced"),
        _CL,
    ] + compaction_params("twcs", gc_grace_default=600, twcs_unit="MINUTES", twcs_size=5) + rate_params(1000) + monitor_params() + stop_params(
        "Number of records", "0 = no limit (readings written)")

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        ks, t = ctx.keyspace, ctx.table
        _schema(
            ctx,
            f"CREATE TABLE IF NOT EXISTS {ks}.{t} (sensor int, bucket bigint, ts timestamp, seq bigint, value double, payload text, "
            "PRIMARY KEY ((sensor, bucket), ts, seq)) WITH CLUSTERING ORDER BY (ts DESC, seq DESC)",
            default_ttl=p["ttl_seconds"] if p["ttl_mode"] == "table_default" else 0,
        )
        cols = f"INSERT INTO {ks}.{t} (sensor, bucket, ts, seq, value, payload) VALUES (?, ?, ?, ?, ?, ?)"
        self.ins = _prepare(ctx, cols + " USING TIMESTAMP ?")
        self.ins_ttl = _prepare(ctx, cols + " USING TTL ? AND TIMESTAMP ?")
        self.sel = _prepare(ctx, f"SELECT ts, value FROM {ks}.{t} WHERE sensor = ? AND bucket = ? LIMIT {int(p['read_limit'])}")
        self.rng = random.Random()
        self.n = 0
        self.late = 0
        self.bucket_s = p["bucket_minutes"] * 60
        self._cb = _rows_cb(ctx)

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        p, r = ctx.params, self.rng
        now = time.time()
        if p["read_pct"] and r.random() * 100 < p["read_pct"]:
            return Op(self.sel, (r.randrange(p["sensors"]), int(now // self.bucket_s)), "read", self._cb)
        ts = now
        if p["late_pct"] and r.random() * 100 < p["late_pct"]:
            ts = now - r.uniform(0, p["late_max_min"] * 60)
            self.late += 1
            ctx.progress["late"] = self.late
        seq = self.n * ctx.proc_count + ctx.proc_index
        self.n += 1
        sensor = r.randrange(p["sensors"])
        row = (sensor, int(ts // self.bucket_s), int(ts * 1000), seq, r.gauss(20.0, 5.0),
               padding(r, p["payload_bytes"]) if p["payload_bytes"] else "")
        wts = int(ts * 1_000_000)
        if p["long_ttl_pct"] and r.random() * 100 < p["long_ttl_pct"]:
            return Op(self.ins_ttl, row + (int(p["long_ttl_seconds"]), wts), "write")  # TTL 0 = never expires
        if p["ttl_mode"] == "per_row":
            return Op(self.ins_ttl, row + (int(p["ttl_seconds"]), wts), "write")
        return Op(self.ins, row + (wts,), "write")


# --------------------------------------------------------------------------- one-shots
def _opts_summary(o: dict[str, Any]) -> dict[str, Any]:
    return {k: o.get(k) for k in ("compaction", "gc_grace_seconds", "default_time_to_live")}


class AlterTableWorkload(Workload):
    name = "alter_table"
    category = "Compaction"
    title = "Change compaction (ALTER TABLE)"
    kind = "oneshot"
    description = (
        "Switches an existing table to another compaction strategy / gc_grace_seconds / default TTL. "
        "Start it while a write workload is running to watch the strategy change under load."
    )
    params = [
        Param("keyspace", "str", "", "Keyspace", "empty = the connection's default keyspace"),
        Param("table", "str", "fake_tickets", "Table"),
        Param("default_ttl", "int", -1, "default_time_to_live (s)", "-1 = leave unchanged, 0 = none", -1, 630_720_000),
    ] + [q for q in compaction_params("lcs") if q.name != "existing_table"]

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def run_once(self, ctx: RunContext) -> dict[str, Any]:
        p = ctx.params
        if not table_exists(ctx.session, ctx.keyspace, ctx.table):
            raise RuntimeError(f"{ctx.keyspace}.{ctx.table} does not exist")
        before = _opts_summary(current_options(ctx))
        cql = f"ALTER TABLE {ctx.keyspace}.{ctx.table} WITH " + table_options_cql(p, default_ttl=p["default_ttl"] if p["default_ttl"] >= 0 else None)
        ctx.session.execute(cql, timeout=60)
        ctx.emit("info", cql)
        after = _opts_summary(current_options(ctx))
        return {"table": f"{ctx.keyspace}.{ctx.table}", "before": before, "after": after,
                "note": f"existing sstables are rewritten by {STRATEGY_CLASS[p['compaction']]} in the background"}


class TableStatsWorkload(Workload):
    name = "table_stats"
    category = "Compaction"
    title = "Inspect table (compaction view)"
    kind = "oneshot"
    description = (
        "Table options plus, per node, disk usage, largest partition and sstable, tombstones per "
        "read, running compactions and recent compaction history (Cassandra 4.1+ virtual tables)."
    )
    params = [
        Param("keyspace", "str", "", "Keyspace", "empty = the connection's default keyspace"),
        Param("table", "str", "fake_tickets", "Table"),
        Param("history_hours", "float", 24.0, "Compaction history (hours)", "", 0.01, 24 * 30),
    ]

    def run_once(self, ctx: RunContext) -> dict[str, Any]:
        if not table_exists(ctx.session, ctx.keyspace, ctx.table):
            raise RuntimeError(f"{ctx.keyspace}.{ctx.table} does not exist")
        opts = _opts_summary(current_options(ctx))
        since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=ctx.params["history_hours"])
        mon = CompactionMonitor(ctx.session, ctx.keyspace, ctx.table, ctx.emit, since=since, max_events_per_sample=20)
        return {"table": f"{ctx.keyspace}.{ctx.table}", "options": opts, "cluster_view": mon.sample()}
