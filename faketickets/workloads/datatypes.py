"""W6 — wide-partition table exercising most CQL data types."""

from __future__ import annotations

import datetime as dt
import ipaddress
import random
import uuid
from decimal import Decimal
from typing import Optional

from cassandra.util import Duration, uuid_from_time

from ..db.cluster import ensure_keyspace
from ..engine.dispatcher import Op
from ..engine.ids import BIGINT_MAX
from .base import wait_for_table, CONSISTENCY_CHOICES, Param, RunContext, Workload, consistency, rate_params, stop_params, target_params, monitor_params
from .datagen import OWNERS, padding
from .tickets import setup_ids

CITIES = ["Paris", "Lyon", "Austin", "Dublin", "Pune", "Sydney", "Toronto", "Berlin"]
TAGS = ["alpha", "beta", "gamma", "delta", "kappa", "sigma", "omega", "zeta", "eta", "theta"]
NS = uuid.UUID("6f1c2a52-6a3b-4f4e-9a55-2f1d7c1f7e10")


class DatatypesWorkload(Workload):
    name = "datatypes"
    title = "Datatypes (wide partitions)"
    description = (
        "Device events table: uuid partition key, timeuuid clustering, and columns of type "
        "bigint, decimal, double, float, varint, smallint, tinyint, boolean, inet, date, time, "
        "duration, blob, set, list, map, tuple and a UDT. Rows are grouped N per partition."
    )
    uses_ids = True
    approx_row_bytes = 700
    params = target_params("fake_events") + [
        Param("start_id", "int", -1, "Start sequence", "-1 = resume after the last run on this table (else 0)", -1, 2**62),
        Param("rows_per_partition", "int", 100, "Rows per partition", "controls partition width", 1, 10_000_000),
        Param("collection_size", "int", 5, "Collection size", "elements per set / list / map", 0, 1000),
        Param("blob_bytes", "int", 256, "Blob size (bytes)", "", 0, 1_048_576),
        Param("ttl_seconds", "int", 0, "TTL (s)", "0 = no TTL", 0, 630_720_000),
        Param("consistency", "choice", "LOCAL_QUORUM", "Consistency", choices=CONSISTENCY_CHOICES, group="advanced"),
    ] + rate_params(500) + monitor_params() + stop_params()

    def setup(self, ctx: RunContext) -> None:
        p = ctx.params
        s, ks, t = ctx.session, ctx.keyspace, ctx.table
        if ctx.proc_index == 0:
            ensure_keyspace(s, ctx.spec, ks, create=p["create_keyspace"])
            s.execute(
                f"CREATE TYPE IF NOT EXISTS {ks}.ft_address (street text, city text, zip text, geo frozen<tuple<double, double>>)"
            )
            s.execute(
                f"CREATE TABLE IF NOT EXISTS {ks}.{t} ("
                "device_id uuid, event_time timeuuid, seq bigint, owner text, "
                "reading double, ratio float, amount decimal, big varint, small smallint, tiny tinyint, "
                "ok boolean, src_ip inet, event_date date, event_tod time, elapsed duration, payload blob, "
                "tags set<text>, samples list<int>, attrs map<text, int>, "
                "home frozen<ft_address>, location tuple<double, double>, "
                "PRIMARY KEY ((device_id), event_time)) WITH CLUSTERING ORDER BY (event_time DESC)"
            )
        else:
            wait_for_table(ctx)
        cols = ("device_id, event_time, seq, owner, reading, ratio, amount, big, small, tiny, ok, src_ip, "
                "event_date, event_tod, elapsed, payload, tags, samples, attrs, home, location")
        cql = f"INSERT INTO {ks}.{t} ({cols}) VALUES ({', '.join('?' * len(cols.split(', ')))})"
        if p["ttl_seconds"]:
            cql += f" USING TTL {p['ttl_seconds']}"
        self.stmt = s.prepare(cql)
        self.stmt.consistency_level = consistency(p["consistency"])
        self.rng = random.Random()
        self.ids = setup_ids(ctx, p["start_id"], BIGINT_MAX)

    def row(self, seq: int, ctx: RunContext) -> tuple:
        r, p = self.rng, ctx.params
        n = p["collection_size"]
        device = uuid.uuid5(NS, f"{ctx.keyspace}.{ctx.table}:{seq // p['rows_per_partition']}")
        now = dt.datetime.now(dt.timezone.utc)
        lat, lon = round(r.uniform(-90, 90), 6), round(r.uniform(-180, 180), 6)
        return (
            device,
            uuid_from_time(now.timestamp() + r.random() * 1e-6),
            seq,
            r.choice(OWNERS),
            r.gauss(20.0, 5.0),
            r.random(),
            Decimal(r.randint(0, 10**9)) / Decimal(100),
            r.getrandbits(96) * (-1 if r.random() < 0.5 else 1),
            r.randint(-32768, 32767),
            r.randint(-128, 127),
            r.random() < 0.5,
            str(ipaddress.IPv4Address(r.getrandbits(32))) if r.random() < 0.8 else str(ipaddress.IPv6Address(r.getrandbits(128))),
            now.date(),
            now.time().replace(tzinfo=None),
            Duration(0, r.randint(0, 30), r.randint(0, 86_400) * 10**9),
            padding(r, p["blob_bytes"]).encode() if p["blob_bytes"] else b"",
            set(r.sample(TAGS, min(n, len(TAGS)))),
            [r.randint(0, 1000) for _ in range(n)],
            {f"k{j}": r.randint(0, 10**6) for j in range(n)},
            (f"{r.randint(1, 999)} Main St", r.choice(CITIES), f"{r.randint(10000, 99999)}", (lat, lon)),
            (lat, lon),
        )

    def next_op(self, ctx: RunContext) -> Optional[Op]:
        seq = self.ids.next()
        ctx.progress["next_id"] = seq + 1
        return Op(self.stmt, self.row(seq, ctx), "write")
