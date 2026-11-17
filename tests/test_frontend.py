"""Frontend: crypto, store, auth, CSRF, proxying (backend mocked with httpx.MockTransport)."""

import io
import json
import zipfile

import httpx
import pytest
from fastapi.testclient import TestClient

from faketickets.config import Settings
from faketickets.frontend.app import create_app
from faketickets.frontend.backend_client import BackendClient
from faketickets.frontend.crypto import SecretBox, hash_password, verify_password
from faketickets.frontend.store import FrontStore

KEY = "k" * 32
H = {"X-Requested-With": "fake-tickets"}


# ---------------------------------------------------------------- crypto
def test_secretbox_roundtrip_salt_and_tamper():
    box = SecretBox(KEY)
    a = box.seal({"password": "pw"}, "aad-1")
    b = box.seal({"password": "pw"}, "aad-1")
    assert a != b and a[1:17] != b[1:17]  # fresh salt (and nonce) per record
    assert box.open(a, "aad-1") == {"password": "pw"}
    with pytest.raises(Exception):
        box.open(a, "aad-2")  # bound to its record
    with pytest.raises(Exception):
        box.open(a[:-1] + bytes([a[-1] ^ 1]), "aad-1")
    with pytest.raises(Exception):
        SecretBox("x" * 32).open(a, "aad-1")
    with pytest.raises(ValueError):
        SecretBox("short")


def test_password_hashing():
    h = hash_password("hunter2")
    assert h.startswith("scrypt$") and "hunter2" not in h
    assert verify_password("hunter2", h) and not verify_password("hunter3", h)
    assert hash_password("hunter2") != h


# ---------------------------------------------------------------- store
def test_store_keeps_secrets_encrypted_and_preserves_on_update(tmp_path):
    db = tmp_path / "f.db"
    st = FrontStore(str(db), SecretBox(KEY))
    c = st.save_connection("lab", {"kind": "cassandra", "contact_points": "10.0.0.5", "password": "Sup3rSecret!"}, "alice")
    assert c["has_secret"] == {"password": True} and "password" not in c
    assert b"Sup3rSecret!" not in db.read_bytes()
    st.save_connection("lab2", {"kind": "cassandra", "contact_points": "10.0.0.6", "username": "u"}, "alice", cid=c["id"])
    spec, pub = st.connection_spec(c["id"])
    assert spec.password == "Sup3rSecret!" and spec.contact_points == ["10.0.0.6"] and pub["name"] == "lab2"
    with pytest.raises(ValueError, match="already exists"):
        st.save_connection("lab2", {"kind": "cassandra", "contact_points": "x"}, "bob")


def test_sessions_and_users(tmp_path):
    st = FrontStore(str(tmp_path / "f.db"), SecretBox(KEY))
    st.upsert_user("admin", "pw-123456")
    assert st.check_user("admin", "pw-123456") and not st.check_user("admin", "nope") and not st.check_user("ghost", "x")
    tok = st.create_session("admin", 60)
    assert st.session_user(tok) == "admin"
    st.delete_session(tok)
    assert st.session_user(tok) is None
    assert st.session_user(st.create_session("admin", -1)) is None  # expired


# ---------------------------------------------------------------- app
class FakeBackend:
    def __init__(self):
        self.calls = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body, dict(request.headers)))
        if request.url.path == "/api/v1/workloads":
            return httpx.Response(200, json=[{"name": "write", "params": []}])
        if request.url.path == "/api/v1/runs" and request.method == "POST":
            return httpx.Response(201, json={"id": "abc123", "state": "starting"})
        if request.url.path == "/api/v1/cursor":
            return httpx.Response(200, json={"next_id": 77})
        if request.url.path == "/healthz":
            return httpx.Response(200, json={"status": "ok"})
        return httpx.Response(404, json={"detail": "nope"})


def make_client(tmp_path, **over):
    fb = FakeBackend()
    settings = Settings(role="frontend", data_dir=str(tmp_path), backend_url="http://backend", backend_token="tok", **over)
    backend = BackendClient(settings, transport=httpx.MockTransport(fb.handler))
    store = FrontStore(str(tmp_path / "f.db"), SecretBox(KEY))
    return TestClient(create_app(settings, backend=backend, store=store)), fb, store


def scb_zip(with_config=True):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        if with_config:
            z.writestr("config.json", "{}")
        z.writestr("ca.crt", "x")
    return buf.getvalue()


def test_csrf_and_security_headers(tmp_path):
    c, _, _ = make_client(tmp_path)
    with c:
        assert c.post("/api/connections", data={"data": "{}"}).status_code == 403
        r = c.get("/")
        assert r.status_code == 200 and "default-src 'self'" in r.headers["content-security-policy"]
        assert r.headers["x-frame-options"] == "DENY"


def test_connection_crud_and_run_proxy(tmp_path):
    c, fb, _ = make_client(tmp_path)
    with c:
        data = {"name": "lab", "kind": "cassandra", "contact_points": "10.0.0.1", "keyspace": "ks", "password": "pw!"}
        r = c.post("/api/connections", data={"data": json.dumps(data)}, headers=H)
        assert r.status_code == 201, r.text
        cid = r.json()["id"]
        assert "pw!" not in r.text
        listed = c.get("/api/connections").json()
        assert listed[0]["name"] == "lab" and "pw!" not in json.dumps(listed)
        r = c.post("/api/runs", json={"connection_id": cid, "workload": "write", "params": {"rate": 5}}, headers=H)
        assert r.status_code == 201 and r.json()["id"] == "abc123"
        method, path, body, headers = fb.calls[-1]
        assert path == "/api/v1/runs" and headers["authorization"] == "Bearer tok"
        assert body["connection"]["password"] == "pw!" and body["owner"] == "anonymous"
        assert body["connection_ref"].startswith("conn-") and body["connection_name"] == "lab"
        assert c.get(f"/api/cursor?connection_id={cid}&table=t").json() == {"next_id": 77}
        assert c.get("/api/runs/../../etc").status_code == 404
        assert c.delete(f"/api/connections/{cid}", headers=H).status_code == 204
        assert c.post("/api/runs", json={"connection_id": cid, "workload": "write"}, headers=H).status_code == 404


def test_astra_scb_upload(tmp_path):
    c, _, store = make_client(tmp_path)
    with c:
        data = {"name": "astra", "kind": "astra", "keyspace": "ks", "astra_token": "AstraCS:abc"}
        bad = c.post("/api/connections", data={"data": json.dumps(data)}, files={"scb": ("b.zip", scb_zip(False), "application/zip")}, headers=H)
        assert bad.status_code == 422 and "config.json" in bad.text
        notzip = c.post("/api/connections", data={"data": json.dumps(data)}, files={"scb": ("b.zip", b"hello", "application/zip")}, headers=H)
        assert notzip.status_code == 422
        r = c.post("/api/connections", data={"data": json.dumps(data)}, files={"scb": ("secure-connect-db.zip", scb_zip(), "application/zip")}, headers=H)
        assert r.status_code == 201, r.text
        pub = r.json()
        assert pub["scb_filename"] == "secure-connect-db.zip" and pub["has_secret"]["astra_scb_b64"]
        # edit without re-uploading keeps the bundle
        r = c.put(f"/api/connections/{pub['id']}", data={"data": json.dumps({**data, "name": "astra2", "astra_token": ""})}, headers=H)
        assert r.status_code == 200, r.text
        spec, pub2 = store.connection_spec(pub["id"])
        assert spec.astra_token == "AstraCS:abc" and spec.astra_scb_b64 and pub2["scb_filename"] == "secure-connect-db.zip"


def test_local_login_flow(tmp_path):
    c, _, _ = make_client(tmp_path, auth_mode="local", admin_user="admin", admin_password="correct horse")
    with c:
        assert c.get("/api/connections").status_code == 401
        assert c.get("/api/me").json() == {"user": None, "auth_mode": "local", "version": c.get("/api/me").json()["version"]}
        assert c.post("/api/login", json={"username": "admin", "password": "wrong"}, headers=H).status_code == 401
        r = c.post("/api/login", json={"username": "admin", "password": "correct horse"}, headers=H)
        assert r.status_code == 200
        cookie = r.headers["set-cookie"].lower()
        assert "httponly" in cookie and "samesite=strict" in cookie
        assert c.get("/api/me").json()["user"] == "admin"
        assert c.get("/api/connections").status_code == 200
        c.post("/api/logout", headers=H)
        assert c.get("/api/connections").status_code == 401


def test_backend_down_is_502(tmp_path):
    settings = Settings(role="frontend", data_dir=str(tmp_path), backend_url="http://127.0.0.1:9")
    store = FrontStore(str(tmp_path / "f.db"), SecretBox(KEY))
    with TestClient(create_app(settings, store=store)) as c:
        assert c.get("/api/workloads").status_code == 502
        assert c.get("/readyz").status_code == 503
