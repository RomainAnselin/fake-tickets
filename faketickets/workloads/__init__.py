"""Workload registry: the backend lists and runs what is registered here."""

from __future__ import annotations

from .base import Workload
from .compaction import AlterTableWorkload, OverwriteWorkload, TableStatsWorkload, TimeseriesWorkload, TombstoneWorkload
from .connectivity import ConnectivityWorkload
from .datatypes import DatatypesWorkload
from .tickets import MixedWorkload, ReadWorkload, WriteWorkload
from .twcs import TwcsWorkload

REGISTRY: dict[str, type[Workload]] = {
    w.name: w
    for w in (
        ConnectivityWorkload, WriteWorkload, ReadWorkload, MixedWorkload,
        OverwriteWorkload, TombstoneWorkload, TimeseriesWorkload, DatatypesWorkload,
        AlterTableWorkload, TableStatsWorkload, TwcsWorkload,
    )
}


def get(name: str) -> type[Workload]:
    try:
        return REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown workload {name!r}; available: {', '.join(REGISTRY)}") from None


def register(cls: type[Workload]) -> type[Workload]:
    """Decorator for extra workloads (e.g. test doubles)."""
    REGISTRY[cls.name] = cls
    return cls
