"""W1 — connectivity / version check (v1: cassqueries.simple_test_query)."""

from __future__ import annotations

import statistics
import time
from typing import Any

from ..db.cluster import local_info
from .base import Param, RunContext, Workload


class ConnectivityWorkload(Workload):
    name = "connectivity"
    title = "Connectivity test"
    description = (
        "Connects, reads release_version (and dse_version) from system.local, lists the "
        "topology and measures round-trip latency. One-shot."
    )
    kind = "oneshot"
    params = [
        Param("pings", "int", 20, "Latency probes", "number of 'SELECT now() FROM system.local'", 1, 1000),
    ]

    def run_once(self, ctx: RunContext) -> dict[str, Any]:
        session = ctx.session
        cluster = session.cluster
        info = local_info(session)
        lat = []
        for _ in range(ctx.params["pings"]):
            t0 = time.perf_counter()
            session.execute("SELECT now() FROM system.local")
            dt = time.perf_counter() - t0
            lat.append(dt * 1000)
            ctx.recorder.ok("read", dt)
        hosts = [
            {
                "address": str(h.endpoint),
                "datacenter": h.datacenter,
                "rack": h.rack,
                "up": bool(h.is_up),
                "release_version": str(h.release_version) if getattr(h, "release_version", None) else None,
            }
            for h in cluster.metadata.all_hosts()
        ]
        keyspaces = sorted(k for k in cluster.metadata.keyspaces if not k.startswith(("system", "dse_", "solr_admin")))
        product = "DSE " + info["dse_version"] if info.get("dse_version") else "Apache Cassandra " + str(info.get("release_version"))
        if ctx.spec.kind == "astra":
            product = f"Astra DB (CQL {info.get('release_version')})"
        result = {
            "product": product,
            **info,
            "protocol_version": cluster.protocol_version,
            "hosts": hosts,
            "keyspaces": keyspaces,
            "default_keyspace_exists": ctx.spec.keyspace in cluster.metadata.keyspaces,
            "latency_ms": {
                "min": round(min(lat), 3),
                "p50": round(statistics.median(lat), 3),
                "max": round(max(lat), 3),
            },
        }
        ctx.emit("info", f"connected to {product}, {len(hosts)} host(s), p50 {result['latency_ms']['p50']} ms")
        return result
