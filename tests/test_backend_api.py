"""Backend API + run manager, using DB-less dummy workloads in real spawned processes."""

import time

import pytest
from fastapi.testclient import TestClient

import dummy_workloads
from faketickets import workloads
from faketickets.backend.app import create_app
from faketickets.backend.manager import RunManager
from faketickets.backend.store import RunStore
from faketickets.config import Settings

workloads.register(dummy_workloads.DummyWorkload)
workloads.register(dummy_workloads.DummyOneShot)

CONN = {"kind": "cassandra", "contact_points": ["127.0.0.1"], "keyspace": "ks", "password": "pw"}
AUTH = {"Authorization": "Bearer t0ken"}


@pytest.fixture
def client(tmp_path):
    settings = Settings(role="backend", data_dir=str(tmp_path), backend_token="t0ken", max_runs=3, max_processes_per_run=4)
    with TestClient(create_app(settings)) as c:
        yield c


def wait_for(c, rid, pred, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = c.get(f"/api/v1/runs/{rid}", headers=AUTH).json()
        if pred(d):
            return d
        time.sleep(0.2)
    raise AssertionError(f"timeout; last={d}")


def start(c, **params):
    body = {"workload": params.pop("workload", "dummy"), "params": params, "connection": CONN,
            "connection_ref": "conn-1", "connection_name": "lab", "owner": "alice"}
    return c.post("/api/v1/runs", json=body, headers=AUTH)


def test_auth_required(client):
    assert client.get("/api/v1/workloads").status_code == 401
    assert client.get("/api/v1/workloads", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.get("/healthz").status_code == 200
    names = [w["name"] for w in client.get("/api/v1/workloads", headers=AUTH).json()]
    assert {"connectivity", "write", "read", "mixed", "twcs", "datatypes"} <= set(names)


def test_run_lifecycle_live_update_and_cursor(client):
    r = start(client, rate=300, processes=2, start_id=1000)
    assert r.status_code == 201, r.text
    rid = r.json()["id"]
    d = wait_for(client, rid, lambda d: d["state"] == "running" and len(d["series"]) >= 3 and d["series"][-1]["rate"] > 0)
    assert 240 <= d["series"][-1]["rate"] <= 360, d["series"][-3:]
    # live rate + live param change
    p = client.patch(f"/api/v1/runs/{rid}", json={"rate": 1000, "params": {"knob": 7}}, headers=AUTH)
    assert p.status_code == 200, p.text
    d = wait_for(client, rid, lambda d: d["series"] and d["series"][-1]["rate"] > 850 and d["progress"].get("knob") == 7)
    assert d["series"][-1]["target"] == 1000
    assert client.patch(f"/api/v1/runs/{rid}", json={"params": {"table": "x"}}, headers=AUTH).status_code == 422
    # stop
    client.post(f"/api/v1/runs/{rid}/stop", headers=AUTH)
    d = wait_for(client, rid, lambda d: d["state"] == "stopped")
    assert d["total_ok"] > 0 and d["next_id"] > 1000
    msgs = " ".join(e["msg"] for e in d["events"])
    assert "updated rate=1000" in msgs and "dummy ready" in msgs
    # resume cursor: start_id=-1 continues after the previous run on the same target
    cur = client.get("/api/v1/cursor", params={"connection_ref": "conn-1", "keyspace": "ks", "table": "dummy_t"}, headers=AUTH).json()
    assert cur["next_id"] == d["next_id"]
    r2 = start(client, rate=100, count=10).json()
    assert r2["params"]["start_id"] == d["next_id"]
    d2 = wait_for(client, r2["id"], lambda d: d["state"] == "finished")
    assert d2["reason"] == "count_reached" and d2["total_ok"] == 10
    # delete
    assert client.delete(f"/api/v1/runs/{rid}", headers=AUTH).status_code == 204
    assert client.get(f"/api/v1/runs/{rid}", headers=AUTH).status_code == 404


def test_duration_stop_and_oneshot_result(client):
    rid = start(client, rate=50, duration_s=1).json()["id"]
    d = wait_for(client, rid, lambda d: d["state"] == "finished")
    assert d["reason"] == "duration_reached" and 30 <= d["total_ok"] <= 70
    rid = start(client, workload="dummy_oneshot", answer=7).json()["id"]
    d = wait_for(client, rid, lambda d: d["state"] == "finished")
    assert d["result"] == {"answer": 7}


def test_failure_is_reported(client):
    rid = start(client, fail_setup=True).json()["id"]
    d = wait_for(client, rid, lambda d: d["state"] == "failed")
    assert "boom in setup" in d["error"]


def test_validation_and_limits(client):
    assert start(client, rate=-3).status_code == 422
    assert start(client, processes=9).status_code == 422
    assert start(client, nope=1).status_code == 422
    bad = client.post("/api/v1/runs", json={"workload": "dummy", "connection": {"kind": "cassandra"}}, headers=AUTH)
    assert bad.status_code == 422
    assert client.post("/api/v1/runs", json={"workload": "zzz", "connection": CONN}, headers=AUTH).status_code == 404
    ids = [start(client, rate=10).json()["id"] for _ in range(3)]
    assert start(client, rate=10).status_code == 429  # max_runs=3
    for rid in ids:
        client.post(f"/api/v1/runs/{rid}/stop", headers=AUTH)
    for rid in ids:
        wait_for(client, rid, lambda d: d["state"] == "stopped")


def test_metrics(client):
    rid = start(client, rate=100).json()["id"]
    wait_for(client, rid, lambda d: d["total_ok"] > 0)
    text = client.get("/metrics", headers=AUTH).text
    assert "ft_runs_active 1" in text and f'run="{rid}"' in text and "ft_run_latency_seconds" in text
    client.post(f"/api/v1/runs/{rid}/stop", headers=AUTH)
    wait_for(client, rid, lambda d: d["state"] == "stopped")


def test_restart_marks_active_runs_interrupted(tmp_path):
    store = RunStore(str(tmp_path / "b.db"))
    store.upsert("abc", workload="write", params={}, state="running", created_at=time.time(), next_id=42,
                 connection_ref="c", target="ks.t")
    mgr = RunManager(Settings(data_dir=str(tmp_path)), store)
    try:
        rec = store.get("abc")
        assert rec["state"] == "interrupted" and rec["reason"] == "backend_restart"
        assert store.cursor("c", "ks.t") == 42
    finally:
        mgr.shutdown()


def test_secrets_never_persisted(client, tmp_path):
    rid = start(client, rate=10, count=5).json()["id"]
    wait_for(client, rid, lambda d: d["state"] == "finished")
    raw = (tmp_path / "backend.db").read_bytes()
    assert b"pw" not in raw.replace(b"pwd", b"")  # connection password is never stored by the backend


def test_shutdown_marks_running_runs_interrupted(tmp_path):
    store = RunStore(str(tmp_path / "b.db"))
    mgr = RunManager(Settings(data_dir=str(tmp_path)), store)
    run = mgr.start({"workload": "dummy", "params": {"rate": 100}, "connection": CONN, "connection_ref": "c"})
    deadline = time.time() + 15
    while run.state != "running" and time.time() < deadline:
        time.sleep(0.1)
    mgr.shutdown()
    rec = store.get(run.id)
    assert rec["state"] == "interrupted" and rec["reason"] == "backend_shutdown"
    assert rec["next_id"] is not None and rec["next_id"] > 0


def test_max_rate_ceiling_applies_to_rated_workloads_only(tmp_path):
    settings = Settings(role="backend", data_dir=str(tmp_path), backend_token="t0ken", max_rate=500)
    with TestClient(create_app(settings)) as c:
        assert start(c, rate=0).status_code == 422          # unlimited not allowed under a ceiling
        assert start(c, rate=501).status_code == 422
        rid = start(c, workload="dummy_oneshot").json()["id"]  # no rate param: unaffected
        assert wait_for(c, rid, lambda d: d["state"] == "finished")["result"] == {"answer": 42}
        rid = start(c, rate=400).json()["id"]
        assert c.patch(f"/api/v1/runs/{rid}", json={"rate": 900}, headers=AUTH).status_code == 422
        c.post(f"/api/v1/runs/{rid}/stop", headers=AUTH)
        wait_for(c, rid, lambda d: d["state"] == "stopped")


def test_finished_runs_are_evicted_from_memory_but_kept_in_store(tmp_path, monkeypatch):
    import faketickets.backend.manager as m

    monkeypatch.setattr(m, "KEEP_FINISHED_IN_MEMORY", 2)
    settings = Settings(role="backend", data_dir=str(tmp_path), backend_token="t0ken")
    with TestClient(create_app(settings)) as c:
        ids = []
        for _ in range(4):
            rid = start(c, workload="dummy_oneshot").json()["id"]
            wait_for(c, rid, lambda d: d["state"] == "finished")
            ids.append(rid)
        listed = {r["id"] for r in c.get("/api/v1/runs", headers=AUTH).json()}
        assert set(ids) <= listed  # still listed (from SQLite)
        old = c.get(f"/api/v1/runs/{ids[0]}", headers=AUTH).json()
        assert old["state"] == "finished" and old["result"] == {"answer": 42}
