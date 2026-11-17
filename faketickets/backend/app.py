"""Backend HTTP API (called by the frontend, or by scripts / CI with the bearer token)."""

from __future__ import annotations

import hmac
import os
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, ValidationError

from .. import __version__, workloads
from ..config import Settings
from ..models import ConnectionSpec
from .manager import RunError, RunManager
from .store import RunStore


class RunRequest(BaseModel):
    workload: str
    params: dict[str, Any] = Field(default_factory=dict)
    connection: dict[str, Any]
    connection_ref: Optional[str] = None
    connection_name: Optional[str] = None
    owner: Optional[str] = None
    label: Optional[str] = None


class RunPatch(BaseModel):
    rate: Optional[float] = None
    concurrency: Optional[int] = None
    params: Optional[dict[str, Any]] = None


def create_app(settings: Settings, manager: Optional[RunManager] = None) -> FastAPI:
    state: dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if manager is None:
            store = RunStore(os.path.join(settings.data_dir, "backend.db"))
            state["mgr"] = RunManager(settings, store)
        else:
            state["mgr"] = manager
        yield
        state["mgr"].shutdown()

    app = FastAPI(title="fake-tickets backend", version=__version__, lifespan=lifespan)

    def mgr() -> RunManager:
        return state["mgr"]

    def auth(request: Request) -> None:
        if not settings.backend_token:
            return
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else ""
        if not hmac.compare_digest(token.encode(), settings.backend_token.encode()):
            raise HTTPException(401, "invalid or missing bearer token")

    @app.exception_handler(RunError)
    async def _run_error(_: Request, exc: RunError):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "version": __version__}

    @app.get("/readyz")
    def readyz():
        if "mgr" not in state:
            raise HTTPException(503, "starting")
        return {"status": "ready"}

    @app.get("/api/v1/workloads", dependencies=[Depends(auth)])
    def list_workloads():
        return [w.describe() for w in workloads.REGISTRY.values()]

    @app.post("/api/v1/runs", status_code=201, dependencies=[Depends(auth)])
    def create_run(req: RunRequest):
        try:
            spec = ConnectionSpec(**req.connection)
        except ValidationError as exc:
            raise HTTPException(422, f"invalid connection: {exc.errors()[0].get('msg')}") from None
        try:
            workloads.get(req.workload)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from None
        run = mgr().start(
            {
                **req.model_dump(exclude={"connection"}),
                "connection": spec.model_dump(),
                "connection_summary": spec.summary(),
            }
        )
        return mgr().view(run)

    @app.get("/api/v1/runs", dependencies=[Depends(auth)])
    def list_runs(owner: Optional[str] = None):
        return mgr().list_views(owner)

    @app.get("/api/v1/runs/{run_id}", dependencies=[Depends(auth)])
    def get_run(run_id: str, since: Optional[float] = None):
        run = mgr().runs.get(run_id)
        if run is not None:
            return mgr().view(run, series_since=since)
        rec = mgr().store.get(run_id)
        if rec is None:
            raise HTTPException(404, "run not found")
        rec.update(series=[], events=mgr().store.events(run_id), rate=0.0)
        return rec

    @app.patch("/api/v1/runs/{run_id}", dependencies=[Depends(auth)])
    def patch_run(run_id: str, body: RunPatch):
        run = mgr().update(run_id, rate=body.rate, concurrency=body.concurrency, params=body.params)
        return mgr().view(run, detail=False)

    @app.post("/api/v1/runs/{run_id}/stop", dependencies=[Depends(auth)])
    def stop_run(run_id: str):
        return mgr().view(mgr().stop(run_id), detail=False)

    @app.delete("/api/v1/runs/{run_id}", status_code=204, dependencies=[Depends(auth)])
    def delete_run(run_id: str):
        mgr().delete(run_id)

    @app.get("/api/v1/cursor", dependencies=[Depends(auth)])
    def cursor(connection_ref: str, keyspace: str, table: str):
        return {"next_id": mgr().store.cursor(connection_ref, f"{keyspace}.{table}")}

    # Unauthenticated on purpose (Prometheus scraping); exposes run ids / rates only, no secrets.
    # The NetworkPolicy restricts who can reach the backend at all.
    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        return render_metrics(mgr())

    return app


def _lbl(**kv: Any) -> str:
    return "{" + ",".join(f'{k}="{str(v).replace(chr(34), "")}"' for k, v in kv.items()) + "}"


def render_metrics(mgr: RunManager) -> str:
    """Prometheus text exposition (no client library needed)."""
    lines = [
        "# HELP ft_runs_active Active runs.",
        "# TYPE ft_runs_active gauge",
        f"ft_runs_active {sum(1 for r in mgr.runs.values() if r.active)}",
        "# HELP ft_run_ops_total Completed operations per run.",
        "# TYPE ft_run_ops_total counter",
    ]
    runs = list(mgr.runs.values())
    for r in runs:
        lines.append(f"ft_run_ops_total{_lbl(run=r.id, workload=r.workload, result='ok')} {r.total_ok}")
        lines.append(f"ft_run_ops_total{_lbl(run=r.id, workload=r.workload, result='error')} {r.total_err}")
    lines += ["# HELP ft_run_rate Achieved ops/s (last second).", "# TYPE ft_run_rate gauge"]
    for r in runs:
        if r.active:
            rate = r.series[-1]["rate"] if r.series else 0
            lines.append(f"ft_run_rate{_lbl(run=r.id, workload=r.workload)} {rate}")
            lines.append(f"ft_run_rate_target{_lbl(run=r.id, workload=r.workload)} {r.rate}")
    lines += ["# HELP ft_run_latency_seconds Latency quantiles since run start.", "# TYPE ft_run_latency_seconds gauge"]
    for r in runs:
        for q in (50, 95, 99):
            lines.append(
                f"ft_run_latency_seconds{_lbl(run=r.id, workload=r.workload, quantile=q / 100)} {r.hist_total.percentile(q):.6f}"
            )
    return "\n".join(lines) + "\n"
