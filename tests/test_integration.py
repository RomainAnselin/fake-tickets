"""End-to-end against a live Cassandra (FT_IT_CONTACT_POINTS=host[:port]).

    podman run -d --name ft-cass -p 19042:9042 -e MAX_HEAP_SIZE=640M -e HEAP_NEWSIZE=128M cassandra:5.0
    FT_IT_CONTACT_POINTS=127.0.0.1:19042 pytest -m integration
"""

import multiprocessing as mp
import os
import time
import uuid

import pytest

from faketickets import workloads
from faketickets.db.cluster import connect
from faketickets.engine.ids import INT_MAX
from faketickets.models import ConnectionSpec
from faketickets.worker import worker_main

pytestmark = pytest.mark.integration

KS = "ft_it"


def spec() -> dict:
    hp = os.environ.get("FT_IT_CONTACT_POINTS", "127.0.0.1:9042")
    host, _, port = hp.partition(":")
    return {"kind": "cassandra", "contact_points": [host], "port": int(port or 9042), "keyspace": KS, "replication_factor": 1}


@pytest.fixture(scope="module")
def session():
    cluster, s = connect(ConnectionSpec(**spec()))
    s.execute(f"CREATE KEYSPACE IF NOT EXISTS {KS} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    yield s
    cluster.shutdown()


def run(workload: str, params: dict, *, seconds: float = 0, procs: int = 1, timeout: float = 60):
    """Run a workload through real worker processes; return (messages, stats-per-proc)."""
    cls = workloads.get(workload)
    p = cls.validate(params)
    if p.get("start_id") == -1:
        p["start_id"] = 0
    ctx = mp.get_context("spawn")
    pipes = []
    for i in range(procs):
        a, b = ctx.Pipe()
        job = {"run_id": "it", "proc_index": i, "proc_count": procs, "workload_ref": f"{cls.__module__}:{cls.__qualname__}",
               "params": p, "spec": spec(), "rate": p.get("rate", 0), "concurrency": p.get("concurrency", 64)}
        pr = ctx.Process(target=worker_main, args=(b, job), daemon=True)
        pr.start()
        b.close()
        pipes.append((a, pr))
    msgs = []
    t0 = time.time()
    stopped = False
    done = 0
    while done < procs and time.time() - t0 < timeout:
        if seconds and not stopped and time.time() - t0 >= seconds:
            for a, _ in pipes:
                a.send({"type": "stop"})
            stopped = True
        for a, _ in pipes:
            while a.poll(0.05):
                try:
                    m = a.recv()
                except EOFError:
                    break
                msgs.append(m)
                done += m["type"] == "done"
    for _, pr in pipes:
        pr.join(10)
    assert done == procs, f"workers did not finish: {[m for m in msgs if m['type'] != 'stats'][-5:]}"
    return msgs


def errors(msgs):
    return [m for m in msgs if m["type"] == "error"]


def total_ok(msgs, kind=None):
    return sum(v for m in msgs if m["type"] == "stats" for k, v in m["ok"].items() if kind in (None, k))


def test_connectivity():
    msgs = run("connectivity", {})
    res = next(m["data"] for m in msgs if m["type"] == "result")
    assert res["release_version"] and res["hosts"] and res["latency_ms"]["p50"] > 0


def test_write_rate_is_accurate_and_rows_exist(session):
    t = "w_" + uuid.uuid4().hex[:8]
    msgs = run("write", {"table": t, "rate": 1500, "duration_s": 4, "start_id": 0})
    assert not errors(msgs)
    n = total_ok(msgs, "write")
    assert 1500 * 4 * 0.9 <= n <= 1500 * 4 * 1.1, n
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == n
    assert any(m["type"] == "done" and m["reason"] == "duration_reached" for m in msgs)
    # read workload probes the id range and hits every time
    msgs = run("read", {"table": t, "rate": 500, "count": 300})
    stats = [m for m in msgs if m["type"] == "stats"]
    assert sum(m["extra"].get("hit", 0) for m in stats) == 300 and not errors(msgs)


def test_write_options_ttl_sai_twcs_two_processes(session):
    t = "o_" + uuid.uuid4().hex[:8]
    msgs = run("write", {"table": t, "rate": 1000, "count": 2000, "processes": 2, "compaction": "twcs", "ttl_mode": "per_row",
                         "ttl_seconds": 3600, "index": "sai", "start_id": 0}, procs=2)
    assert not errors(msgs), errors(msgs)
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 2000
    ttl = session.execute(f"SELECT TTL(notes) FROM {KS}.{t} WHERE id = 7").one()[0]
    assert 3500 < ttl <= 3600
    rows = session.execute(f"SELECT id FROM {KS}.{t} WHERE ownedby = 'Romain' LIMIT 5").all()
    assert rows  # SAI index query works
    session.cluster.refresh_table_metadata(KS, t)
    meta = session.cluster.metadata.keyspaces[KS].tables[t]
    assert "TimeWindowCompactionStrategy" in meta.options["compaction"]["class"]  # noqa


def test_int_table_stops_cleanly_at_overflow(session):
    t = "i_" + uuid.uuid4().hex[:8]
    session.execute(f"CREATE TABLE {KS}.{t} (id int PRIMARY KEY, ownedby text, ticket int, time timestamp, updatedat timestamp, notes text)")
    msgs = run("write", {"table": t, "rate": 0, "start_id": INT_MAX - 9})
    assert any(m["type"] == "done" and m["reason"] == "id_space_exhausted" for m in msgs)
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 10


def test_mixed_and_datatypes(session):
    t = "m_" + uuid.uuid4().hex[:8]
    msgs = run("mixed", {"table": t, "rate": 1000, "duration_s": 2, "read_pct": 30, "start_id": 0})
    assert not errors(msgs) and total_ok(msgs, "read") > 300 and total_ok(msgs, "write") > 1000
    t = "d_" + uuid.uuid4().hex[:8]
    msgs = run("datatypes", {"table": t, "rate": 500, "count": 250, "rows_per_partition": 50, "start_id": 0})
    assert not errors(msgs), errors(msgs)
    parts = session.execute(f"SELECT DISTINCT device_id FROM {KS}.{t}").all()
    assert len(parts) == 5
    row = session.execute(f"SELECT tags, attrs, home, elapsed, amount FROM {KS}.{t} LIMIT 1").one()
    assert row.home.city and row.attrs and row.elapsed is not None


def test_twcs_overlap_and_backdated(session):
    t = "t_" + uuid.uuid4().hex[:8]
    msgs = run("twcs", {"table": t, "rate": 500, "start_id": 0, "ttl_seconds": 7200, "overlap_every_min": 0.02,
                        "overlap_lookback_min": 1, "backdate_every_min": 0.03, "backdate_offset_min": 360}, seconds=4)
    assert not errors(msgs)
    events = [m["msg"] for m in msgs if m["type"] == "event"]
    overlaps = [e for e in events if e.startswith("OVERLAP")]
    backdated = [e for e in events if e.startswith("BACKDATED")]
    assert len(overlaps) >= 2 and backdated
    session.cluster.refresh_table_metadata(KS, t)
    oid = int(overlaps[0].split("id=")[1].split()[0])
    notes = session.execute(f"SELECT notes FROM {KS}.{t} WHERE id = %s", (oid,)).one()[0]
    assert "OVERLAP REWRITE" in notes  # (or a later overlap of the same id)
    bid = int(backdated[0].split("id=")[1].split()[0])
    wt = session.execute(f"SELECT WRITETIME(notes) FROM {KS}.{t} WHERE id = %s", (bid,)).one()[0]
    assert abs(wt / 1e6 - (time.time() - 6 * 3600)) < 60
    meta = session.cluster.metadata.keyspaces[KS].tables[t]
    assert meta.options["default_time_to_live"] == 7200


@pytest.mark.parametrize(
    "opts",
    [
        {"lwt": True},
        {"lwt": True, "ttl_mode": "per_row", "ttl_seconds": 600},
        {"index": "2i"},
        {"compaction": "lcs"},
        {"compaction": "ucs"},
        {"ttl_mode": "table_default", "ttl_seconds": 900},
        {"payload_bytes": 2048, "consistency": "ONE"},
    ],
    ids=lambda o: "-".join(f"{k}={v}" for k, v in o.items()),
)
def test_write_option_matrix(session, opts):
    t = "x_" + uuid.uuid4().hex[:8]
    msgs = run("write", {"table": t, "rate": 800, "count": 400, "start_id": 0, **opts})
    assert not errors(msgs), errors(msgs)
    assert total_ok(msgs, "write") == 400
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 400
    session.cluster.refresh_table_metadata(KS, t)  # schema event from the worker's DDL may not have arrived yet
    meta = session.cluster.metadata.keyspaces[KS].tables[t]
    if opts.get("ttl_mode") == "table_default":
        assert meta.options["default_time_to_live"] == 900
    if opts.get("ttl_mode") == "per_row":
        assert 0 < session.execute(f"SELECT TTL(notes) FROM {KS}.{t} WHERE id = 3").one()[0] <= 600
    if "compaction" in opts:
        assert {"lcs": "Leveled", "ucs": "Unified"}[opts["compaction"]] in meta.options["compaction"]["class"]
    if opts.get("index") == "2i":
        msgs = run("read", {"table": t, "mode": "by_owner", "rate": 200, "count": 50})
        assert not errors(msgs), errors(msgs)
        stats = [m for m in msgs if m["type"] == "stats"]
        assert sum(m["extra"].get("hit", 0) for m in stats) > 0


# ----------------------------------------------------------------------------- compaction workloads
def last_progress(msgs, key, proc=0):
    vals = [m["progress"].get(key) for m in msgs if m["type"] == "stats" and m.get("proc") == proc and key in m.get("progress", {})]
    return vals[-1] if vals else None


def options(session, t):
    session.cluster.refresh_table_metadata(KS, t)
    return session.cluster.metadata.keyspaces[KS].tables[t].options


def test_count_is_records_written_not_reads(session):
    t = "c_" + uuid.uuid4().hex[:8]
    msgs = run("mixed", {"table": t, "rate": 2000, "count": 500, "read_pct": 50, "start_id": 0})
    assert not errors(msgs)
    assert total_ok(msgs, "write") == 500 and total_ok(msgs, "read") > 100
    assert any(m["type"] == "done" and m["reason"] == "count_reached" for m in msgs)
    assert last_progress(msgs, "counted") == 500


def test_count_past_int_limit_is_rejected_up_front(session):
    t = "j_" + uuid.uuid4().hex[:8]
    session.execute(f"CREATE TABLE {KS}.{t} (id int PRIMARY KEY, ownedby text, ticket int, time timestamp, updatedat timestamp, notes text)")
    msgs = run("write", {"table": t, "rate": 0, "start_id": INT_MAX - 9, "count": 100})
    assert errors(msgs) and "id limit" in errors(msgs)[0]["error"]
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 0


def test_bigint_ids_beyond_int_range(session):
    t = "b_" + uuid.uuid4().hex[:8]
    msgs = run("write", {"table": t, "rate": 0, "start_id": INT_MAX + 1, "count": 50})
    assert not errors(msgs)
    assert session.execute(f"SELECT id FROM {KS}.{t} WHERE id = %s", (INT_MAX + 50,)).one()


def test_overwrite_sequential_rewrites_keyspace_lcs(session):
    t = "ow_" + uuid.uuid4().hex[:8]
    msgs = run("overwrite", {"table": t, "keys": 500, "distribution": "sequential", "count": 2000, "rate": 2000,
                             "compaction": "lcs", "lcs_sstable_size_mb": 1, "monitor_every_s": 1})
    assert not errors(msgs), errors(msgs)
    assert total_ok(msgs, "write") == 2000
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 500  # 4 passes over 500 keys
    assert last_progress(msgs, "pass") >= 4
    o = options(session, t)
    assert "Leveled" in o["compaction"]["class"] and o["compaction"]["sstable_size_in_mb"] == "1"
    assert last_progress(msgs, "cmp_disk_mib") is not None  # monitor published (Cassandra 5 virtual tables)


def test_overwrite_one_column_hotspot_with_reads_ucs(session):
    t = "oh_" + uuid.uuid4().hex[:8]
    msgs = run("overwrite", {"table": t, "keys": 1000, "distribution": "hotspot", "hot_keys_pct": 1, "hot_ops_pct": 90,
                             "update_mode": "one_column", "read_pct": 30, "count": 1000, "rate": 2000,
                             "compaction": "ucs", "ucs_scaling_parameters": "L10,T4"})
    assert not errors(msgs), errors(msgs)
    n = session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0]
    assert 10 <= n < 1000  # ≥ 90 % of writes went to the 10 hot keys
    assert sum(m["extra"].get("hit", 0) for m in msgs if m["type"] == "stats") > 0
    assert options(session, t)["compaction"]["scaling_parameters"] == "L10,T4"


@pytest.mark.parametrize("kind", ["row", "range", "cell", "ttl"])
def test_tombstones_queue(session, kind):
    t = f"q{kind}_" + uuid.uuid4().hex[:6]
    msgs = run("tombstones", {"table": t, "partitions": 5, "delete_kind": kind, "delete_lag": 100, "range_every": 10,
                              "row_ttl_seconds": 60, "count": 1000, "rate": 3000, "start_id": 0, "gc_grace_seconds": 0,
                              "read_pct": 10})
    assert not errors(msgs), errors(msgs)
    assert total_ok(msgs, "write") == 1000
    live = session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0]
    first = session.execute(f"SELECT payload FROM {KS}.{t} WHERE bucket = 0 AND seq = 0").one()
    last = session.execute(f"SELECT payload FROM {KS}.{t} WHERE bucket = %s AND seq = 999", (999 % 5,)).one()
    assert last and last.payload
    if kind == "row":
        assert live == 100 and first is None
    elif kind == "range":
        assert 100 <= live < 300 and first is None
    elif kind == "cell":
        assert live == 1000 and first.payload is None
    else:
        assert live == 1000 and 0 < session.execute(f"SELECT TTL(payload) FROM {KS}.{t} WHERE bucket = 0 AND seq = 0").one()[0] <= 60
    assert options(session, t)["gc_grace_seconds"] == 0


def test_timeseries_mixed_ttl_and_late_writes(session):
    t = "ts_" + uuid.uuid4().hex[:8]
    msgs = run("timeseries", {"table": t, "sensors": 10, "count": 2000, "rate": 3000, "long_ttl_pct": 20,
                              "long_ttl_seconds": 0, "late_pct": 10, "late_max_min": 120, "read_pct": 10})
    assert not errors(msgs), errors(msgs)
    assert session.execute(f"SELECT count(*) FROM {KS}.{t}").one()[0] == 2000
    o = options(session, t)
    assert o["default_time_to_live"] == 3600 and o["gc_grace_seconds"] == 600
    c = o["compaction"]
    assert "TimeWindow" in c["class"] and c["compaction_window_unit"] == "MINUTES" and c["compaction_window_size"] == "5"
    ttls = [r[0] for r in session.execute(f"SELECT TTL(value) FROM {KS}.{t}")]
    forever = sum(1 for x in ttls if x is None)
    assert 200 < forever < 600, forever  # ~20 % never expire
    assert last_progress(msgs, "late") > 100
    oldest = min(r[0] for r in session.execute(f"SELECT WRITETIME(value) FROM {KS}.{t}"))
    assert time.time() - oldest / 1e6 > 30 * 60  # some rows back-dated by > 30 min


def test_existing_table_keep_warns_then_alter_table_switches(session):
    t = "al_" + uuid.uuid4().hex[:8]
    assert not errors(run("write", {"table": t, "count": 100, "rate": 0, "start_id": 0}))
    msgs = run("write", {"table": t, "count": 10, "rate": 0, "compaction": "lcs"})
    assert any(m["type"] == "event" and "keeping it" in m["msg"] for m in msgs)
    assert "SizeTiered" in options(session, t)["compaction"]["class"]
    msgs = run("alter_table", {"table": t, "compaction": "twcs", "twcs_window_unit": "MINUTES", "twcs_window_size": 10,
                               "gc_grace_seconds": 300, "default_ttl": 7200})
    res = next(m["data"] for m in msgs if m["type"] == "result")
    assert "SizeTiered" in res["before"]["compaction"]["class"] and "TimeWindow" in res["after"]["compaction"]["class"]
    o = options(session, t)
    assert o["gc_grace_seconds"] == 300 and o["default_time_to_live"] == 7200
    msgs = run("write", {"table": t, "count": 10, "rate": 0, "compaction": "stcs", "existing_table": "alter"})
    assert not errors(msgs) and "SizeTiered" in options(session, t)["compaction"]["class"]


def test_table_stats(session):
    t = "st_" + uuid.uuid4().hex[:8]
    assert not errors(run("write", {"table": t, "count": 200, "rate": 0, "start_id": 0}))
    msgs = run("table_stats", {"table": t})
    assert not errors(msgs), errors(msgs)
    res = next(m["data"] for m in msgs if m["type"] == "result")
    view = res["cluster_view"]
    assert {"disk_mib", "max_partition_mib", "tombstones_p99", "active", "done"} <= set(view)
    assert res["options"]["compaction"]["class"].endswith("SizeTieredCompactionStrategy")
