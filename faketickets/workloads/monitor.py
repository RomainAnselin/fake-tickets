"""Compaction / table health monitor over CQL virtual tables (Cassandra 4.1+ / 5.0).

Virtual tables are node-local, so every view is queried on every live host and
aggregated. Views that a cluster does not have (Astra, DSE 6.8, Cassandra < 4.1)
are skipped after the first failure, with a single info event.

Keys written to ``ctx.progress`` (all prefixed ``cmp_``):
  disk_mib, max_partition_mib, max_sstable_mib, tombstones_p99, tombstones_max,
  active (running compactions on the table), active_pct, done (compactions since
  the run started), bytes_in, bytes_out.
"""

from __future__ import annotations

import datetime as dt
import threading
from typing import Any, Callable, Optional

VIEWS = {
    "disk_usage": "SELECT mebibytes FROM system_views.disk_usage WHERE keyspace_name = %s AND table_name = %s",
    "max_partition_size": "SELECT mebibytes FROM system_views.max_partition_size WHERE keyspace_name = %s AND table_name = %s",
    "max_sstable_size": "SELECT mebibytes FROM system_views.max_sstable_size WHERE keyspace_name = %s AND table_name = %s",
    "tombstones_per_read": "SELECT p99th, max FROM system_views.tombstones_per_read WHERE keyspace_name = %s AND table_name = %s",
    "sstable_tasks": "SELECT kind, progress, total, sstables FROM system_views.sstable_tasks WHERE keyspace_name = %s AND table_name = %s",
    "compaction_history": (
        "SELECT id, compacted_at, bytes_in, bytes_out, rows_merged FROM system.compaction_history "
        "WHERE keyspace_name = %s AND columnfamily_name = %s AND compacted_at > %s ALLOW FILTERING"
    ),
}


class CompactionMonitor:
    def __init__(self, session: Any, keyspace: str, table: str, emit: Callable[[str, str], None],
                 since: Optional[dt.datetime] = None, max_events_per_sample: int = 10) -> None:
        self.session, self.ks, self.table, self.emit = session, keyspace, table, emit
        self.since = since or (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5))
        self.disabled: set[str] = set()
        self.seen: set[Any] = set()
        self.max_events = max_events_per_sample
        self.totals = {"done": 0, "bytes_in": 0, "bytes_out": 0}
        self._thread: Optional[threading.Thread] = None

    def _q(self, view: str, host: Any, *extra: Any) -> list:
        if view in self.disabled:
            return []
        try:
            return list(self.session.execute(VIEWS[view], (self.ks, self.table, *extra), host=host, timeout=10))
        except Exception as exc:  # view missing on this cluster / host down
            msg = str(exc)
            if "unconfigured table" in msg or "does not exist" in msg or "Unauthorized" in msg or "Invalid" in msg:
                self.disabled.add(view)
                self.emit("info", f"compaction monitor: {view} not available on this cluster ({type(exc).__name__})")
            return []

    def sample(self) -> dict[str, Any]:
        out: dict[str, Any] = {"disk_mib": 0, "max_partition_mib": 0, "max_sstable_mib": 0,
                               "tombstones_p99": 0.0, "tombstones_max": 0.0, "active": 0, "active_pct": None}
        prog = total = 0
        new_events: list[str] = []
        hosts = [h for h in self.session.cluster.metadata.all_hosts() if h.is_up is not False]
        for h in hosts:
            for r in self._q("disk_usage", h):
                out["disk_mib"] += int(r.mebibytes or 0)
            for r in self._q("max_partition_size", h):
                out["max_partition_mib"] = max(out["max_partition_mib"], int(r.mebibytes or 0))
            for r in self._q("max_sstable_size", h):
                out["max_sstable_mib"] = max(out["max_sstable_mib"], int(r.mebibytes or 0))
            for r in self._q("tombstones_per_read", h):
                out["tombstones_p99"] = max(out["tombstones_p99"], float(r.p99th or 0))
                out["tombstones_max"] = max(out["tombstones_max"], float(r.max or 0))
            for r in self._q("sstable_tasks", h):
                if str(r.kind).lower() == "compaction":
                    out["active"] += 1
                    prog += int(r.progress or 0)
                    total += int(r.total or 0)
            for r in self._q("compaction_history", h, self.since):
                key = (str(h.endpoint), r.id)
                if key in self.seen:
                    continue
                self.seen.add(key)
                self.totals["done"] += 1
                self.totals["bytes_in"] += int(r.bytes_in or 0)
                self.totals["bytes_out"] += int(r.bytes_out or 0)
                if len(new_events) < self.max_events:
                    merged = sum((r.rows_merged or {}).values())
                    ratio = f" ({(r.bytes_out / r.bytes_in - 1) * 100:+.0f}% size)" if r.bytes_in else ""
                    new_events.append(f"compaction on {h.address}: {_mib(r.bytes_in)} → {_mib(r.bytes_out)}{ratio}, {merged:,} partitions merged")
        if total:
            out["active_pct"] = round(prog / total * 100, 1)
        out.update(self.totals)
        for e in new_events:
            self.emit("event", e)
        return out

    def start(self, every_s: float, stop: threading.Event, progress: dict[str, Any]) -> None:
        def loop() -> None:
            while not stop.wait(every_s):
                self.publish(progress)

        self._thread = threading.Thread(target=loop, name="compaction-monitor", daemon=True)
        self._thread.start()

    def publish(self, progress: dict[str, Any]) -> None:
        try:
            for k, v in self.sample().items():
                progress[f"cmp_{k}"] = v
        except Exception as exc:  # never let monitoring kill a run
            self.emit("warning", f"compaction monitor sample failed: {exc}")


def _mib(b: Optional[int]) -> str:
    b = int(b or 0)
    return f"{b / 1048576:.1f} MiB" if b >= 104858 else f"{b / 1024:.0f} KiB"
