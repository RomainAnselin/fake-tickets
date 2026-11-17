"""Workload contract, parameter schema and shared helpers."""

from __future__ import annotations

import re
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, ClassVar, Optional

from cassandra import ConsistencyLevel

from ..engine.dispatcher import Op
from ..engine.ids import IdAllocator
from ..engine.stats import Recorder

_IDENT = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,47}$")

CONSISTENCY_CHOICES = ["LOCAL_ONE", "LOCAL_QUORUM", "QUORUM", "ONE", "TWO", "THREE", "EACH_QUORUM", "ALL"]


def check_identifier(value: str, what: str = "identifier") -> str:
    """CQL identifiers end up in DDL strings: only allow plain, unquoted names."""
    if not isinstance(value, str) or not _IDENT.match(value):
        raise ValueError(f"invalid {what} {value!r}: use letters, digits, underscore; start with a letter; max 48 chars")
    return value


def consistency(name: str) -> int:
    return getattr(ConsistencyLevel, name)


@dataclass
class Param:
    name: str
    type: str  # int | float | bool | str | choice
    default: Any
    label: str = ""
    help: str = ""
    min: Optional[float] = None
    max: Optional[float] = None
    choices: Optional[list[str]] = None
    group: str = "workload"  # workload | compaction | rate | stop | advanced
    visible_if: Optional[dict[str, list[Any]]] = None
    live: bool = False  # can be changed while the run is active
    pattern: Optional[str] = None  # str params: full-match regex (values may end up in DDL)

    def coerce(self, raw: Any) -> Any:
        if raw is None or raw == "":
            return self.default
        try:
            if self.type == "int":
                v: Any = int(float(raw))
            elif self.type == "float":
                v = float(raw)
            elif self.type == "bool":
                v = raw if isinstance(raw, bool) else str(raw).lower() in ("1", "true", "yes", "on")
            else:
                v = str(raw).strip()
        except (TypeError, ValueError):
            raise ValueError(f"{self.name}: expected {self.type}, got {raw!r}") from None
        if self.type in ("int", "float"):
            if self.min is not None and v < self.min:
                raise ValueError(f"{self.name}: must be >= {self.min:g}")
            if self.max is not None and v > self.max:
                raise ValueError(f"{self.name}: must be <= {self.max:g}")
        if self.type == "choice" and v not in (self.choices or []):
            raise ValueError(f"{self.name}: must be one of {self.choices}")
        if self.pattern and not re.fullmatch(self.pattern, v):
            raise ValueError(f"{self.name}: {v!r} does not match {self.pattern}")
        return v

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


def rate_params(default_rate: int = 1000) -> list[Param]:
    return [
        Param("rate", "int", default_rate, "Target rate (ops/s)", "0 = unlimited (bounded by concurrency)", 0, 1_000_000, group="rate", live=True),
        Param("concurrency", "int", 64, "Max in-flight requests", "per worker process", 1, 4096, group="rate", live=True),
        Param("processes", "int", 1, "Worker processes", "fan out a run over N processes for rates above ~5-10k ops/s", 1, 16, group="rate"),
    ]


def stop_params(count_label: str = "Number of records", count_help: str = "0 = no limit (rows inserted)") -> list[Param]:
    return [
        Param("count", "int", 0, count_label, count_help, 0, 10**12, group="stop"),
        Param("duration_s", "int", 0, "Stop after N seconds", "0 = run until stopped", 0, 365 * 86400, group="stop"),
    ]


def monitor_params(default_s: int = 10) -> list[Param]:
    return [
        Param("monitor_every_s", "int", default_s, "Compaction monitor every (s)",
              "0 = off. Polls system_views (disk usage, partition/sstable size, tombstones per read, "
              "running compactions) and compaction_history on every node — Cassandra 4.1+", 0, 3600, group="advanced"),
    ]


def target_params(table_default: str) -> list[Param]:
    return [
        Param("keyspace", "str", "", "Keyspace", "empty = the connection's default keyspace"),
        Param("table", "str", table_default, "Table"),
        Param("create_keyspace", "bool", True, "Create keyspace if missing", "NetworkTopologyStrategy, RF from the connection (ignored on Astra)", group="advanced"),
    ]


@dataclass
class RunContext:
    """What a workload sees inside its worker process."""

    session: Any
    spec: Any  # ConnectionSpec
    params: dict[str, Any]
    recorder: Recorder
    emit: Callable[[str, str], None]  # (level, message) -> run event log
    proc_index: int = 0
    proc_count: int = 1
    clock: Callable[[], float] = time.time
    stop_event: threading.Event = field(default_factory=threading.Event)
    ids: Optional[IdAllocator] = None
    progress: dict[str, Any] = field(default_factory=dict)

    @property
    def keyspace(self) -> str:
        return self.params.get("keyspace") or self.spec.keyspace

    @property
    def table(self) -> str:
        return self.params["table"]


def table_exists(session, keyspace: str, table: str) -> bool:
    ks = session.cluster.metadata.keyspaces.get(keyspace)
    return bool(ks and table in ks.tables)


def wait_for_table(ctx: "RunContext", timeout: float = 60.0) -> None:
    """Shards other than #0 wait for shard 0 to create the schema (avoids concurrent DDL)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not ctx.stop_event.is_set():
        try:
            ctx.session.cluster.refresh_schema_metadata(max_schema_agreement_wait=5)
        except Exception:
            pass
        if table_exists(ctx.session, ctx.keyspace, ctx.table):
            # system_schema can list the table a moment before the node serves it:
            # only return once a real query against it succeeds.
            try:
                ctx.session.execute(f"SELECT * FROM {ctx.keyspace}.{ctx.table} LIMIT 1")
                return
            except Exception:
                pass
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {ctx.keyspace}.{ctx.table} to be created by shard 0")


class Workload:
    name: ClassVar[str]
    title: ClassVar[str]
    description: ClassVar[str]
    kind: ClassVar[str] = "continuous"  # or "oneshot"
    category: ClassVar[str] = "Data"  # UI grouping: Data | Compaction | Bug repro
    uses_ids: ClassVar[bool] = False  # writes monotonically increasing ids (resume cursor)
    needs_session: ClassVar[bool] = True  # False: the worker does not open a CQL session (test doubles)
    params: ClassVar[list[Param]] = []
    # Op kinds that count towards ``count`` (None = every op from next_op).
    count_kinds: ClassVar[Optional[frozenset[str]]] = None
    # Rough size of one written row, used by the UI to estimate data volume (0 = unknown).
    approx_row_bytes: ClassVar[int] = 0

    # --- schema ---------------------------------------------------------
    @classmethod
    def describe(cls) -> dict[str, Any]:
        return {
            "name": cls.name,
            "title": cls.title,
            "description": cls.description,
            "kind": cls.kind,
            "category": cls.category,
            "uses_ids": cls.uses_ids,
            "count_kinds": sorted(cls.count_kinds) if cls.count_kinds else None,
            "approx_row_bytes": cls.approx_row_bytes,
            "params": [p.to_dict() for p in cls.params],
        }

    @classmethod
    def validate(cls, raw: dict[str, Any]) -> dict[str, Any]:
        known = {p.name: p for p in cls.params}
        unknown = set(raw) - set(known)
        if unknown:
            raise ValueError(f"unknown parameter(s): {', '.join(sorted(unknown))}")
        out = {name: p.coerce(raw.get(name)) for name, p in known.items()}
        for key in ("keyspace", "table", "index_name"):
            if key in out and out[key]:
                check_identifier(out[key], key)
        cls.check(out)
        return out

    @classmethod
    def check(cls, params: dict[str, Any]) -> None:
        """Cross-field validation hook."""

    # --- execution --------------------------------------------------------
    def setup(self, ctx: RunContext) -> None:  # schema + prepared statements
        pass

    def next_op(self, ctx: RunContext) -> Optional[Op]:  # continuous workloads
        raise NotImplementedError

    def tick(self, ctx: RunContext, now: float) -> list[Op]:
        """Called ~5x/s from the dispatch loop; may return extra ops (scheduled events)."""
        return []

    def run_once(self, ctx: RunContext) -> dict[str, Any]:  # oneshot workloads
        raise NotImplementedError
