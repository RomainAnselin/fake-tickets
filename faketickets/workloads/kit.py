"""Everything a workload needs, in one import.

    from .kit import *          # inside faketickets/workloads/

Guide: docs/04-ADDING-A-WORKLOAD.md. The built-in compaction workloads use the
same helpers.
"""

from __future__ import annotations

from typing import Any, Optional

from ..db.cluster import ensure_keyspace
from ..engine.dispatcher import Op
from .base import (
    CONSISTENCY_CHOICES, Param, RunContext, Workload, consistency, monitor_params, rate_params, stop_params,
    target_params, wait_for_table,
)
from .datagen import OWNERS, padding
from .tableopts import check_compaction, compaction_params, create_or_reconcile

__all__ = [
    "Any", "Optional", "Op", "Param", "RunContext", "Workload", "OWNERS", "padding",
    "target_params", "compaction_params", "rate_params", "monitor_params", "stop_params", "consistency_param",
    "check_compaction", "create_table", "prepare", "count_hits",
]


def consistency_param(default: str = "LOCAL_QUORUM") -> Param:
    """The ``consistency`` parameter that :func:`prepare` applies to every statement."""
    return Param("consistency", "choice", default, "Consistency", choices=CONSISTENCY_CHOICES, group="advanced")


def create_table(ctx: RunContext, create_prefix: str, *, default_ttl: Optional[int] = None) -> None:
    """Create the keyspace and table, or reconcile an existing table.

    ``create_prefix`` is ``CREATE TABLE IF NOT EXISTS ks.t (...)``, optionally
    followed by ``WITH CLUSTERING ORDER BY (...)``. The compaction options the
    user chose (``compaction_params()``) and ``gc_grace_seconds`` (plus
    ``default_ttl`` when given) are appended. Only worker 0 runs DDL; the
    others wait until the table is queryable.
    """
    if ctx.proc_index == 0:
        ensure_keyspace(ctx.session, ctx.spec, ctx.keyspace, create=ctx.params["create_keyspace"])
        create_or_reconcile(ctx, create_prefix, default_ttl=default_ttl)
    else:
        wait_for_table(ctx)


def prepare(ctx: RunContext, cql: str):
    """Prepare a statement with the run's ``consistency`` parameter."""
    stmt = ctx.session.prepare(cql)
    stmt.consistency_level = consistency(ctx.params["consistency"])
    return stmt


def count_hits(ctx: RunContext):
    """``Op.on_rows`` callback for reads: counts ``hit`` / ``miss`` (and ``rows`` when >1) in the run stats."""
    rec = ctx.recorder

    def cb(rows: Any) -> None:
        rows = list(rows or [])
        rec.bump("hit" if rows else "miss")
        if len(rows) > 1:
            rec.bump("rows", len(rows))

    return cb
