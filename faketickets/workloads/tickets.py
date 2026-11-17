"""W2 write / W3 read / W4 mixed on the v1 ``fake_tickets`` table shape."""

from __future__ import annotations

import random
from typing import Any, Optional

from ..db.cluster import column_type, ensure_keyspace
from ..engine.dispatcher import Op
from ..engine.ids import LIMITS, IdAllocator
from .base import wait_for_table, CONSISTENCY_CHOICES, Param, RunContext, Workload, consistency, rate_params, stop_params, target_params, monitor_params
from .datagen import OWNERS, ticket_row
from .tableopts import check_compaction, compaction_params, create_or_reconcile

# v1's exact table options — used only by the TWCS bug-reproduction workload.
COMPACTION = {
    "stcs": "{'class': 'SizeTieredCompactionStrategy'}",
    "lcs": "{'class': 'LeveledCompactionStrategy'}",
    "ucs": "{'class': 'UnifiedCompactionStrategy'}",
    "twcs": (
        "{{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '{w}', "
        "'compaction_window_unit': 'HOURS', 'timestamp_resolution': 'MILLISECONDS', "
        "'unchecked_tombstone_compaction': 'true'}}"
    ),
}


def create_tickets_table(
    ctx: RunContext,
    *,
    id_type: str = "bigint",
    compaction: str = "stcs",
    window_hours: int = 1,
    default_ttl: int = 0,
    gc_grace: Optional[int] = None,
) -> None:
    ks, t = ctx.keyspace, ctx.table
    comp = COMPACTION[compaction]
    opts = [f"compaction = {comp.format(w=window_hours) if compaction == 'twcs' else comp}"]
    if default_ttl:
        opts.append(f"default_time_to_live = {int(default_ttl)}")
    if gc_grace is not None:
        opts.append(f"gc_grace_seconds = {int(gc_grace)}")
    ctx.session.execute(
        f"CREATE TABLE IF NOT EXISTS {ks}.{t} ("
        f"id {id_type}, ownedby text, ticket int, time timestamp, updatedat timestamp, notes text, "
        f"PRIMARY KEY (id)) WITH " + " AND ".join(opts)
    )


def tickets_ddl(ctx: RunContext, id_type: str = "bigint") -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {ctx.keyspace}.{ctx.table} ("
        f"id {id_type}, ownedby text, ticket int, time timestamp, updatedat timestamp, notes text, PRIMARY KEY (id))"
    )


def id_limit(ctx: RunContext) -> int:
    """Key type of an existing table decides the overflow limit (v1 tables are ``int``)."""
    ctype = column_type(ctx.session, ctx.keyspace, ctx.table, "id")
    if ctype not in LIMITS:
        raise RuntimeError(f"{ctx.keyspace}.{ctx.table}.id has type {ctype!r}; expected int or bigint")
    if ctype == "int":
        ctx.emit("warning", f"{ctx.table}.id is 'int': run will stop cleanly at id {LIMITS['int']}")
    return LIMITS[ctype]


def create_index(ctx: RunContext, index: str) -> None:
    ks, t = ctx.keyspace, ctx.table
    if index == "none":
        return
    if index == "sai":
        cql = f"CREATE CUSTOM INDEX IF NOT EXISTS {t}_ownedby_sai ON {ks}.{t} (ownedby) USING 'StorageAttachedIndex'"
    elif index == "2i":
        cql = f"CREATE INDEX IF NOT EXISTS {t}_ownedby_2i ON {ks}.{t} (ownedby)"
    elif index == "solr":
        if ctx.spec.kind != "dse":
            raise RuntimeError("Solr search indexes require a DSE connection")
        cql = f"CREATE SEARCH INDEX IF NOT EXISTS ON {ks}.{t} WITH COLUMNS ownedby, notes"
    else:
        raise ValueError(index)
    ctx.emit("info", f"creating {index} index on {t}(ownedby)")
    ctx.session.execute(cql, timeout=120)


def prepare_insert(ctx: RunContext, *, ttl: Optional[int] = None, lwt: bool = False, using_timestamp: bool = False) -> Any:
    using = []
    if ttl:
        using.append(f"TTL {int(ttl)}")
    if using_timestamp:
        using.append("TIMESTAMP ?")
    cql = f"INSERT INTO {ctx.keyspace}.{ctx.table} (id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?)"
    if lwt:
        cql += " IF NOT EXISTS"
    if using:
        cql += " USING " + " AND ".join(using)
    stmt = ctx.session.prepare(cql)
    stmt.consistency_level = consistency(ctx.params.get("consistency", "LOCAL_QUORUM"))
    if lwt:
        stmt.serial_consistency_level = consistency("LOCAL_SERIAL")
    return stmt


def setup_ids(ctx: RunContext, start: int, limit: int) -> IdAllocator:
    count = int(ctx.params.get("count") or 0)
    if count and max(0, start) + count - 1 > limit:
        raise RuntimeError(
            f"{count:,} records from id {max(0, start):,} would pass the id limit {limit:,} "
            f"(only {max(0, limit - max(0, start) + 1):,} ids left); lower the count or use a bigint table"
        )
    ids = IdAllocator(max(0, start), offset=ctx.proc_index, stride=ctx.proc_count, limit=limit)
    ctx.ids = ids
    ctx.progress["next_id"] = ids.high_water
    return ids


def _common_write_params(table: str) -> list[Param]:
    return target_params(table) + [
        Param("start_id", "int", -1, "Start id", "-1 = resume after the last run on this table (else 0)", -1, 2**63 - 1),
        Param("id_type", "choice", "bigint", "Id type (new tables)", "v1 used int, which overflows at 2,147,483,647", choices=["bigint", "int"], group="advanced"),
        Param("payload_bytes", "int", 0, "Extra payload bytes", "random filler appended to notes", 0, 1_048_576, group="advanced"),
        Param("consistency", "choice", "LOCAL_QUORUM", "Consistency", choices=CONSISTENCY_CHOICES, group="advanced"),
    ]


class WriteWorkload(Workload):
    name = "write"
    title = "Write fake tickets"
    description = (
        "Inserts synthetic tickets (id, ownedby, ticket, time, notes) with increasing ids. "
        "Optional TTL, compaction strategy, SAI / 2i / Solr index on ownedby, and LWT."
    )
    uses_ids = True
    approx_row_bytes = 130
    params = _common_write_params("fake_tickets") + [
        Param("ttl_mode", "choice", "none", "TTL", "none, table default_time_to_live, or USING TTL per row", choices=["none", "table_default", "per_row"]),
        Param("ttl_seconds", "int", 86400, "TTL seconds", "", 1, 630_720_000, visible_if={"ttl_mode": ["table_default", "per_row"]}),
        Param("index", "choice", "none", "Index on ownedby", "solr requires DSE", choices=["none", "sai", "2i", "solr"]),
        Param("lwt", "bool", False, "Lightweight transactions", "INSERT ... IF NOT EXISTS (LOCAL_SERIAL)", group="advanced"),
    ] + compaction_params("stcs") + rate_params(1000) + monitor_params() + stop_params()

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        if ctx.proc_index == 0:
            ensure_keyspace(ctx.session, ctx.spec, ctx.keyspace, create=p["create_keyspace"])
            create_or_reconcile(ctx, tickets_ddl(ctx, p["id_type"]),
                                default_ttl=p["ttl_seconds"] if p["ttl_mode"] == "table_default" else None)
            create_index(ctx, p["index"])
        else:
            wait_for_table(ctx)
        self.stmt = prepare_insert(ctx, ttl=p["ttl_seconds"] if p["ttl_mode"] == "per_row" else None, lwt=p["lwt"])
        self.rng = random.Random()
        self.ids = setup_ids(ctx, p["start_id"], id_limit(ctx))

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        i = self.ids.next()
        ctx.progress["next_id"] = i + 1
        return Op(self.stmt, ticket_row(i, self.rng, payload_bytes=ctx.params["payload_bytes"]), "write")


class ReadWorkload(Workload):
    name = "read"
    title = "Read fake tickets"
    description = (
        "Random point reads by id over an existing range (hit / miss counted), or index "
        "lookups by ownedby (SAI / 2i) or Solr query (DSE)."
    )
    params = target_params("fake_tickets") + [
        Param("mode", "choice", "by_id", "Read mode", "by_owner needs an SAI or 2i index; solr needs DSE Search", choices=["by_id", "by_owner", "solr"]),
        Param("id_min", "int", 0, "Lowest id", "", 0, 2**63 - 1, visible_if={"mode": ["by_id"]}),
        Param("id_max", "int", -1, "Highest id", "-1 = last id written by previous runs, else probed", -1, 2**63 - 1, visible_if={"mode": ["by_id"]}),
        Param("limit", "int", 10, "Rows per index query", "", 1, 10000, visible_if={"mode": ["by_owner", "solr"]}),
        Param("consistency", "choice", "LOCAL_QUORUM", "Consistency", choices=CONSISTENCY_CHOICES, group="advanced"),
    ] + rate_params(1000) + monitor_params() + stop_params("Number of reads", "0 = no limit")

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        ks, t = ctx.keyspace, ctx.table
        if column_type(ctx.session, ks, t, "id") is None:
            raise RuntimeError(f"{ks}.{t} does not exist — run a write workload first")
        cl = consistency(p["consistency"])
        if p["mode"] == "by_id":
            self.stmt = ctx.session.prepare(f"SELECT id, ownedby, ticket, time, notes FROM {ks}.{t} WHERE id = ?")
        elif p["mode"] == "by_owner":
            self.stmt = ctx.session.prepare(f"SELECT id, ownedby, ticket FROM {ks}.{t} WHERE ownedby = ? LIMIT {p['limit']}")
        else:
            self.stmt = ctx.session.prepare(f"SELECT id, ownedby, ticket FROM {ks}.{t} WHERE solr_query = ? LIMIT {p['limit']}")
        self.stmt.consistency_level = cl
        self.rng = random.Random()
        if p["mode"] == "by_id":
            self.lo = p["id_min"]
            self.hi = p["id_max"]
            if self.hi < 0:
                self.hi = probe_max_id(ctx, self.lo)
                if self.hi is None:
                    raise RuntimeError(f"id {self.lo} not found in {ks}.{t}; set id_min / id_max explicitly")
                ctx.emit("info", f"probed id range [{self.lo}, {self.hi}]")
            if self.hi < self.lo:
                raise RuntimeError("id_max < id_min")

    def _on_rows(self, ctx: RunContext):
        rec = ctx.recorder

        def cb(rows: Any) -> None:
            n = len(rows) if rows is not None else 0
            rec.bump("hit" if n else "miss")
            if n > 1:
                rec.bump("rows", n)

        return cb

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        mode = ctx.params["mode"]
        if mode == "by_id":
            args: tuple = (self.rng.randint(self.lo, self.hi),)
        elif mode == "by_owner":
            args = (self.rng.choice(OWNERS),)
        else:
            args = ('{"q": "ownedby:%s"}' % self.rng.choice(OWNERS),)
        if not hasattr(self, "_cb"):
            self._cb = self._on_rows(ctx)
        return Op(self.stmt, args, "read", self._cb)


def probe_max_id(ctx: RunContext, lo: int) -> Optional[int]:
    """Find the highest contiguous id >= lo with exponential + binary search (~2·log2 N reads)."""
    stmt = ctx.session.prepare(f"SELECT id FROM {ctx.keyspace}.{ctx.table} WHERE id = ?")

    def exists(i: int) -> bool:
        return ctx.session.execute(stmt, (i,)).one() is not None

    if not exists(lo):
        return None
    step, good = 1, lo
    bad = None
    while bad is None:
        cand = good + step
        if cand > 2**63 - 1 or not exists(cand):
            bad = cand
        else:
            good, step = cand, step * 2
    while bad - good > 1:
        mid = (good + bad) // 2
        if exists(mid):
            good = mid
        else:
            bad = mid
    return good


class MixedWorkload(Workload):
    name = "mixed"
    title = "Mixed read / write"
    description = (
        "Writes new tickets and reads back random ids from the range written so far "
        "(plus any pre-existing range from 'read_from'), at a configurable read percentage."
    )
    uses_ids = True
    count_kinds = frozenset({"write"})
    approx_row_bytes = 130
    params = _common_write_params("fake_tickets") + [
        Param("read_pct", "int", 50, "Read percentage", "0 = write only, 100 = read only", 0, 100, live=True),
        Param("read_from", "int", -1, "Read ids from", "-1 = this run's start id", -1, 2**63 - 1, group="advanced"),
    ] + compaction_params("stcs") + rate_params(1000) + monitor_params() + stop_params("Number of records", "0 = no limit (rows inserted; reads are not counted)")

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        check_compaction(params)

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        if ctx.proc_index == 0:
            ensure_keyspace(ctx.session, ctx.spec, ctx.keyspace, create=p["create_keyspace"])
            create_or_reconcile(ctx, tickets_ddl(ctx, p["id_type"]))
        else:
            wait_for_table(ctx)
        self.ins = prepare_insert(ctx)
        self.sel = ctx.session.prepare(f"SELECT id, ownedby, ticket, time, notes FROM {ctx.keyspace}.{ctx.table} WHERE id = ?")
        self.sel.consistency_level = consistency(p["consistency"])
        self.rng = random.Random()
        self.ids = setup_ids(ctx, p["start_id"], id_limit(ctx))
        self.read_from = p["read_from"] if p["read_from"] >= 0 else max(0, p["start_id"])
        rec = ctx.recorder
        self._cb = lambda rows: rec.bump("hit" if rows else "miss")

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        last = self.ids.last
        if last is not None and last >= self.read_from and self.rng.random() * 100 < ctx.params["read_pct"]:
            return Op(self.sel, (self.rng.randint(self.read_from, last),), "read", self._cb)
        i = self.ids.next()
        ctx.progress["next_id"] = i + 1
        return Op(self.ins, ticket_row(i, self.rng, payload_bytes=ctx.params["payload_bytes"]), "write")
