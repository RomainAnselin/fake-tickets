"""Frontend: serves the UI, owns the connection vault, proxies runs to the backend."""

from __future__ import annotations

import base64
import json
import logging
import os
import zipfile
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Any, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError

from .. import __version__
from ..config import Settings
from .auth import AuthError, authenticate
from .backend_client import BackendClient, BackendError
from .crypto import SecretBox, load_or_create_master_key
from .store import FrontStore

log = logging.getLogger(__name__)
STATIC = os.path.join(os.path.dirname(__file__), "static")
COOKIE = "ft_session"
CSRF_HEADER = "x-requested-with"


class LoginBody(BaseModel):
    username: str
    password: str


class StartRun(BaseModel):
    connection_id: int
    workload: str
    params: dict[str, Any] = Field(default_factory=dict)
    label: Optional[str] = None


class PatchRun(BaseModel):
    rate: Optional[float] = None
    concurrency: Optional[int] = None
    params: Optional[dict[str, Any]] = None


def create_app(settings: Settings, backend: Optional[BackendClient] = None, store: Optional[FrontStore] = None) -> FastAPI:
    st: dict[str, Any] = {}
    secure_cookie = settings.cookie_secure if settings.cookie_secure is not None else settings.tls.enabled

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if store is None:
            key, generated = load_or_create_master_key(settings.master_key, settings.data_dir)
            if generated:
                log.warning("FT_MASTER_KEY not set: generated %s/master.key — back it up or set FT_MASTER_KEY", settings.data_dir)
            st["store"] = FrontStore(os.path.join(settings.data_dir, "frontend.db"), SecretBox(key))
        else:
            st["store"] = store
        if settings.auth_mode == "local" and settings.admin_user and settings.admin_password:
            st["store"].upsert_user(settings.admin_user, settings.admin_password)
        if settings.auth_mode not in ("none", "local", "ldap"):
            raise RuntimeError(f"FT_AUTH_MODE must be none, local or ldap (got {settings.auth_mode!r})")
        st["backend"] = backend or BackendClient(settings)
        yield
        st["backend"].close()

    app = FastAPI(title="fake-tickets", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        # CSRF: state-changing API calls must carry a custom header (cannot be sent cross-site
        # without CORS preflight); the session cookie is also SameSite=Strict.
        if request.url.path.startswith("/api/") and request.method not in ("GET", "HEAD", "OPTIONS"):
            if request.headers.get(CSRF_HEADER) != "fake-tickets":
                return JSONResponse({"detail": "missing X-Requested-With header"}, status_code=403)
        resp = await call_next(request)
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("Referrer-Policy", "no-referrer")
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'",
        )
        if secure_cookie:
            resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000")
        return resp

    @app.exception_handler(BackendError)
    async def _backend_error(_: Request, exc: BackendError):
        return JSONResponse({"detail": exc.detail}, status_code=exc.status)

    def store_() -> FrontStore:
        return st["store"]

    def be() -> BackendClient:
        return st["backend"]

    def current_user(request: Request) -> str:
        if settings.auth_mode == "none":
            return request.headers.get("x-forwarded-user") or "anonymous"
        user = store_().session_user(request.cookies.get(COOKIE))
        if not user:
            raise HTTPException(401, "login required")
        return user

    # ---------------------------------------------------------------- health
    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "version": __version__}

    @app.get("/readyz")
    def readyz():
        try:
            be().request("GET", "/healthz")
        except BackendError as exc:
            raise HTTPException(503, f"backend not reachable: {exc}") from None
        return {"status": "ready"}

    # ---------------------------------------------------------------- auth
    @app.get("/api/me")
    def me(request: Request):
        try:
            user = current_user(request)
        except HTTPException:
            user = None
        return {"user": user, "auth_mode": settings.auth_mode, "version": __version__}

    @app.post("/api/login")
    def login(body: LoginBody, response: Response):
        try:
            user = authenticate(settings, store_(), body.username, body.password)
        except AuthError as exc:
            log.info("login failed for %r: %s", body.username[:64], exc)
            raise HTTPException(401, str(exc) if "unavailable" in str(exc) or "disabled" in str(exc) else "invalid credentials") from None
        token = store_().create_session(user, settings.session_ttl_hours * 3600)
        response.set_cookie(COOKIE, token, httponly=True, secure=secure_cookie, samesite="strict",
                            max_age=settings.session_ttl_hours * 3600, path="/")
        return {"user": user}

    @app.post("/api/logout")
    def logout(request: Request, response: Response):
        store_().delete_session(request.cookies.get(COOKIE))
        response.delete_cookie(COOKIE, path="/")
        return {"ok": True}

    # ---------------------------------------------------------------- connections
    @app.get("/api/connections")
    def list_connections(user: str = Depends(current_user)):
        return store_().list_connections()

    async def _read_form(data: str, scb: Optional[UploadFile]) -> tuple[str, dict[str, Any], Optional[str]]:
        try:
            payload = json.loads(data)
        except ValueError:
            raise HTTPException(422, "data must be JSON") from None
        name = str(payload.pop("name", "")).strip()
        filename = None
        if scb is not None and scb.filename:
            raw = await scb.read(settings.max_upload_mb * 1024 * 1024 + 1)
            if len(raw) > settings.max_upload_mb * 1024 * 1024:
                raise HTTPException(413, f"bundle larger than {settings.max_upload_mb} MB")
            try:
                with zipfile.ZipFile(BytesIO(raw)) as z:
                    names = set(z.namelist())
            except zipfile.BadZipFile:
                raise HTTPException(422, "secure connect bundle must be a .zip file") from None
            if "config.json" not in names:
                raise HTTPException(422, "this zip does not look like an Astra secure connect bundle (no config.json)")
            payload["astra_scb_b64"] = base64.b64encode(raw).decode()
            filename = os.path.basename(scb.filename)[:200]
        return name, payload, filename

    @app.post("/api/connections", status_code=201)
    async def create_connection(data: str = Form(...), scb: Optional[UploadFile] = File(None), user: str = Depends(current_user)):
        name, payload, filename = await _read_form(data, scb)
        try:
            return store_().save_connection(name, payload, user, scb_filename=filename)
        except (ValueError, ValidationError) as exc:
            raise HTTPException(422, _msg(exc)) from None

    @app.put("/api/connections/{cid}")
    async def update_connection(cid: int, data: str = Form(...), scb: Optional[UploadFile] = File(None), user: str = Depends(current_user)):
        name, payload, filename = await _read_form(data, scb)
        try:
            return store_().save_connection(name, payload, user, cid=cid, scb_filename=filename)
        except KeyError:
            raise HTTPException(404, "connection not found") from None
        except (ValueError, ValidationError) as exc:
            raise HTTPException(422, _msg(exc)) from None

    @app.delete("/api/connections/{cid}", status_code=204)
    def delete_connection(cid: int, user: str = Depends(current_user)):
        store_().delete_connection(cid)

    # ---------------------------------------------------------------- workloads & runs
    @app.get("/api/workloads")
    def workloads(user: str = Depends(current_user)):
        return be().request("GET", "/api/v1/workloads")

    @app.get("/api/runs")
    def runs(user: str = Depends(current_user)):
        return be().request("GET", "/api/v1/runs")

    @app.post("/api/runs", status_code=201)
    def start_run(body: StartRun, user: str = Depends(current_user)):
        try:
            spec, pub = store_().connection_spec(body.connection_id)
        except KeyError:
            raise HTTPException(404, "connection not found") from None
        return be().request(
            "POST",
            "/api/v1/runs",
            json={
                "workload": body.workload,
                "params": body.params,
                "connection": spec.model_dump(),
                "connection_ref": f"conn-{pub['uid']}",
                "connection_name": pub["name"],
                "owner": user,
                "label": body.label,
            },
        )

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str, since: Optional[float] = None, user: str = Depends(current_user)):
        return be().request("GET", f"/api/v1/runs/{_rid(run_id)}", params={"since": since} if since else None)

    @app.patch("/api/runs/{run_id}")
    def patch_run(run_id: str, body: PatchRun, user: str = Depends(current_user)):
        return be().request("PATCH", f"/api/v1/runs/{_rid(run_id)}", json=body.model_dump(exclude_none=True))

    @app.post("/api/runs/{run_id}/stop")
    def stop_run(run_id: str, user: str = Depends(current_user)):
        return be().request("POST", f"/api/v1/runs/{_rid(run_id)}/stop")

    @app.delete("/api/runs/{run_id}", status_code=204)
    def delete_run(run_id: str, user: str = Depends(current_user)):
        be().request("DELETE", f"/api/v1/runs/{_rid(run_id)}")

    @app.get("/api/cursor")
    def cursor(connection_id: int, table: str, keyspace: str = "", user: str = Depends(current_user)):
        conn = store_().get_connection(connection_id)
        if conn is None:
            raise HTTPException(404, "connection not found")
        return be().request(
            "GET", "/api/v1/cursor",
            params={"connection_ref": f"conn-{conn['uid']}", "keyspace": keyspace or conn["keyspace"], "table": table},
        )

    # ---------------------------------------------------------------- UI
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(os.path.join(STATIC, "index.html"), headers={"Cache-Control": "no-cache"})

    return app


def _rid(run_id: str) -> str:
    if not run_id.isalnum() or len(run_id) > 32:
        raise HTTPException(404, "run not found")
    return run_id


def _msg(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        e = exc.errors()[0]
        loc = ".".join(str(x) for x in e.get("loc", ()) if x != "__root__")
        return f"{loc}: {e.get('msg')}" if loc else str(e.get("msg"))
    return str(exc)
