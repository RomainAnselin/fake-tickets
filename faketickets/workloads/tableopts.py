"""Compaction strategy + table options shared by every workload that creates a table.

Everything here ends up in DDL strings, so every value is either a whitelisted
choice, a number, or matches a strict pattern (see ``Param.pattern``).
"""

from __future__ import annotations

from typing import Any, Optional

from .base import Param, RunContext, table_exists

STRATEGY_CLASS = {
    "stcs": "SizeTieredCompactionStrategy",
    "lcs": "LeveledCompactionStrategy",
    "ucs": "UnifiedCompactionStrategy",
    "twcs": "TimeWindowCompactionStrategy",
}
UCS_SCALING_PATTERN = r"^[TLN][0-9]{0,3}(,[TLN][0-9]{0,3}){0,15}$"


def compaction_params(
    default: str = "stcs",
    *,
    gc_grace_default: int = 864000,
    twcs_unit: str = "HOURS",
    twcs_size: int = 1,
    strategies: Optional[list[str]] = None,
) -> list[Param]:
    """Strategy choice + per-strategy sub-options, rendered as the UI's "Table & compaction" box."""
    strategies = strategies or list(STRATEGY_CLASS)
    g = "compaction"

    def only(*s: str) -> dict[str, list[Any]]:
        return {"compaction": list(s)}

    return [
        Param("compaction", "choice", default, "Compaction strategy", "applied when the table is created (see 'If the table exists')", choices=strategies, group=g),
        # STCS
        Param("stcs_min_threshold", "int", 4, "STCS min_threshold", "similar-sized sstables needed to trigger a compaction", 2, 64, group=g, visible_if=only("stcs")),
        Param("stcs_max_threshold", "int", 32, "STCS max_threshold", "max sstables per compaction", 2, 256, group=g, visible_if=only("stcs")),
        # LCS
        Param("lcs_sstable_size_mb", "int", 160, "LCS sstable_size_in_mb", "small values = many levels quickly (good for tests)", 1, 10240, group=g, visible_if=only("lcs")),
        Param("lcs_fanout_size", "int", 10, "LCS fanout_size", "", 2, 100, group=g, visible_if=only("lcs")),
        # UCS (Cassandra 5)
        Param("ucs_scaling_parameters", "str", "T4", "UCS scaling_parameters", "T4 ≈ STCS-like, L10 ≈ LCS-like, N = in between; comma list per level",
              group=g, visible_if=only("ucs"), pattern=UCS_SCALING_PATTERN),
        Param("ucs_target_sstable_size_mb", "int", 1024, "UCS target_sstable_size (MiB)", "", 1, 1_048_576, group=g, visible_if=only("ucs")),
        # TWCS
        Param("twcs_window_unit", "choice", twcs_unit, "TWCS window unit", "", choices=["MINUTES", "HOURS", "DAYS"], group=g, visible_if=only("twcs")),
        Param("twcs_window_size", "int", twcs_size, "TWCS window size", "aim for ~20–30 windows over the TTL", 1, 10000, group=g, visible_if=only("twcs")),
        Param("twcs_timestamp_resolution", "choice", "MICROSECONDS", "TWCS timestamp_resolution",
              "must match the write timestamps (drivers write microseconds)", choices=["MICROSECONDS", "MILLISECONDS"], group=g, visible_if=only("twcs")),
        # all strategies
        Param("gc_grace_seconds", "int", gc_grace_default, "gc_grace_seconds", "tombstones are purgeable after this (default 10 days)", 0, 10 * 365 * 86400, group=g),
        Param("tombstone_threshold", "float", 0.2, "tombstone_threshold", "droppable-tombstone ratio that triggers a single-sstable compaction", 0.0, 1.0, group=g),
        Param("unchecked_tombstone_compaction", "bool", False, "unchecked_tombstone_compaction", "allow single-sstable tombstone compactions without overlap checks", group=g),
        Param("existing_table", "choice", "keep", "If the table exists", "keep = leave its settings alone; alter = ALTER TABLE to the settings above",
              choices=["keep", "alter"], group=g),
    ]


def compaction_map(p: dict[str, Any]) -> dict[str, str]:
    s = p["compaction"]
    m: dict[str, str] = {"class": STRATEGY_CLASS[s]}
    if s == "stcs":
        m["min_threshold"] = str(int(p["stcs_min_threshold"]))
        m["max_threshold"] = str(int(p["stcs_max_threshold"]))
    elif s == "lcs":
        m["sstable_size_in_mb"] = str(int(p["lcs_sstable_size_mb"]))
        m["fanout_size"] = str(int(p["lcs_fanout_size"]))
    elif s == "ucs":
        m["scaling_parameters"] = p["ucs_scaling_parameters"]
        m["target_sstable_size"] = f"{int(p['ucs_target_sstable_size_mb'])}MiB"
    elif s == "twcs":
        m["compaction_window_unit"] = p["twcs_window_unit"]
        m["compaction_window_size"] = str(int(p["twcs_window_size"]))
        m["timestamp_resolution"] = p["twcs_timestamp_resolution"]
    m["tombstone_threshold"] = repr(float(p["tombstone_threshold"]))
    m["unchecked_tombstone_compaction"] = "true" if p["unchecked_tombstone_compaction"] else "false"
    return m


def check_compaction(p: dict[str, Any]) -> None:
    if p.get("compaction") == "stcs" and p["stcs_min_threshold"] > p["stcs_max_threshold"]:
        raise ValueError("stcs_min_threshold must be <= stcs_max_threshold")


def table_options_cql(p: dict[str, Any], *, default_ttl: Optional[int] = None) -> str:
    """``compaction = {...} AND gc_grace_seconds = N [AND default_time_to_live = N]``."""
    comp = ", ".join(f"'{k}': '{v}'" for k, v in compaction_map(p).items())
    parts = [f"compaction = {{{comp}}}", f"gc_grace_seconds = {int(p['gc_grace_seconds'])}"]
    if default_ttl is not None:
        parts.append(f"default_time_to_live = {int(default_ttl)}")
    return " AND ".join(parts)


def current_options(ctx: RunContext) -> dict[str, Any]:
    ctx.session.cluster.refresh_table_metadata(ctx.keyspace, ctx.table)
    return dict(ctx.session.cluster.metadata.keyspaces[ctx.keyspace].tables[ctx.table].options)


def create_or_reconcile(ctx: RunContext, create_prefix: str, *, default_ttl: Optional[int] = None) -> None:
    """CREATE the table with the chosen options, or reconcile an existing one.

    ``CREATE TABLE IF NOT EXISTS`` silently ignores new options on an existing
    table, so the user is told (keep) or the table is altered (alter).
    """
    p = ctx.params
    opts = table_options_cql(p, default_ttl=default_ttl)
    if not table_exists(ctx.session, ctx.keyspace, ctx.table):
        joiner = " AND " if " WITH " in create_prefix.upper() else " WITH "
        ctx.session.execute(f"{create_prefix}{joiner}{opts}", timeout=60)
        ctx.emit("info", f"created {ctx.keyspace}.{ctx.table} with {STRATEGY_CLASS[p['compaction']]}")
        return
    cur = current_options(ctx)
    cur_class = str(cur.get("compaction", {}).get("class", "")).rsplit(".", 1)[-1]
    want_class = STRATEGY_CLASS[p["compaction"]]
    differs = cur_class != want_class or int(cur.get("gc_grace_seconds", -1)) != int(p["gc_grace_seconds"])
    if p.get("existing_table") == "alter":
        ctx.session.execute(f"ALTER TABLE {ctx.keyspace}.{ctx.table} WITH {opts}", timeout=60)
        ctx.emit("info", f"altered existing {ctx.table}: {cur_class} → {want_class}, gc_grace_seconds={p['gc_grace_seconds']}")
    elif differs:
        ctx.emit("warning", f"{ctx.table} already exists with {cur_class}, gc_grace_seconds={cur.get('gc_grace_seconds')}; "
                            "keeping it (set 'If the table exists' = alter to apply the chosen settings)")
