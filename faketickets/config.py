"""Environment-driven settings (12-factor / Kubernetes friendly).

Every secret setting ``FT_X`` can also be given as ``FT_X_FILE`` pointing at a
file (k8s Secret volume), which takes precedence.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    path = os.environ.get(name + "_FILE")
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _bool(name: str, default: bool) -> bool:
    v = _env(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    v = _env(name)
    return default if v is None else int(v)


@dataclass
class TLSSettings:
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None  # CA bundle used to verify peers (backend, LDAPS)
    client_auth: bool = False  # require client certs (mTLS) on this listener
    reload_interval_s: int = 60

    @property
    def enabled(self) -> bool:
        return bool(self.cert_file and self.key_file)


@dataclass
class Settings:
    role: str = "backend"
    host: str = "0.0.0.0"
    port: int = 8444
    data_dir: str = "/data"
    log_level: str = "INFO"
    tls: TLSSettings = field(default_factory=TLSSettings)
    # backend
    backend_token: Optional[str] = None
    max_runs: int = 8
    max_processes_per_run: int = 8
    max_rate: int = 0  # 0 = no ceiling
    # frontend
    backend_url: str = "http://127.0.0.1:8444"
    master_key: Optional[str] = None
    auth_mode: str = "none"  # none | local | ldap
    session_ttl_hours: int = 12
    cookie_secure: Optional[bool] = None
    admin_user: Optional[str] = None
    admin_password: Optional[str] = None
    max_upload_mb: int = 20
    # ldap
    ldap_url: Optional[str] = None
    ldap_user_dn_template: Optional[str] = None  # e.g. uid={username},ou=people,dc=home,dc=lab
    ldap_base_dn: Optional[str] = None
    ldap_user_filter: str = "(uid={username})"
    ldap_bind_dn: Optional[str] = None
    ldap_bind_password: Optional[str] = None
    ldap_required_group: Optional[str] = None  # DN; checked via memberOf or group member attribute
    ldap_start_tls: bool = False
    ldap_ca_file: Optional[str] = None

    @classmethod
    def from_env(cls, role: str) -> "Settings":
        default_port = 8443 if role == "frontend" else 8444
        tls = TLSSettings(
            cert_file=_env("FT_TLS_CERT_FILE"),
            key_file=_env("FT_TLS_KEY_FILE"),
            ca_file=_env("FT_TLS_CA_FILE"),
            client_auth=_bool("FT_TLS_CLIENT_AUTH", False),
            reload_interval_s=_int("FT_TLS_RELOAD_INTERVAL", 60),
        )
        cookie_secure = _env("FT_COOKIE_SECURE")
        return cls(
            role=role,
            host=_env("FT_HOST", "0.0.0.0") or "0.0.0.0",
            port=_int("FT_PORT", default_port),
            data_dir=_env("FT_DATA_DIR", "/data") or "/data",
            log_level=(_env("FT_LOG_LEVEL", "INFO") or "INFO").upper(),
            tls=tls,
            backend_token=_env("FT_BACKEND_TOKEN"),
            max_runs=_int("FT_MAX_RUNS", 8),
            max_processes_per_run=_int("FT_MAX_PROCESSES_PER_RUN", 8),
            max_rate=_int("FT_MAX_RATE", 0),
            backend_url=_env("FT_BACKEND_URL", "http://127.0.0.1:8444") or "",
            master_key=_env("FT_MASTER_KEY"),
            auth_mode=(_env("FT_AUTH_MODE", "none") or "none").lower(),
            session_ttl_hours=_int("FT_SESSION_TTL_HOURS", 12),
            cookie_secure=None if cookie_secure is None else cookie_secure.lower() in ("1", "true", "yes"),
            admin_user=_env("FT_ADMIN_USER"),
            admin_password=_env("FT_ADMIN_PASSWORD"),
            max_upload_mb=_int("FT_MAX_UPLOAD_MB", 20),
            ldap_url=_env("FT_LDAP_URL"),
            ldap_user_dn_template=_env("FT_LDAP_USER_DN_TEMPLATE"),
            ldap_base_dn=_env("FT_LDAP_BASE_DN"),
            ldap_user_filter=_env("FT_LDAP_USER_FILTER", "(uid={username})") or "(uid={username})",
            ldap_bind_dn=_env("FT_LDAP_BIND_DN"),
            ldap_bind_password=_env("FT_LDAP_BIND_PASSWORD"),
            ldap_required_group=_env("FT_LDAP_REQUIRED_GROUP"),
            ldap_start_tls=_bool("FT_LDAP_START_TLS", False),
            ldap_ca_file=_env("FT_LDAP_CA_FILE") or _env("FT_TLS_CA_FILE"),
        )
