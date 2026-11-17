"""HTTP client from the frontend to the backend (bearer token, CA-verified TLS)."""

from __future__ import annotations

import ssl
from typing import Any, Optional

import httpx

from ..config import Settings


class BackendError(Exception):
    def __init__(self, status: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status = status
        self.detail = detail


class BackendClient:
    def __init__(self, settings: Settings, transport: Optional[httpx.BaseTransport] = None) -> None:
        verify: Any = True
        if settings.backend_url.startswith("https") and settings.tls.ca_file:
            verify = ssl.create_default_context(cafile=settings.tls.ca_file)
            if settings.tls.cert_file and settings.tls.key_file:
                # Present our own (step-issued) cert too, so the backend can require mTLS.
                verify.load_cert_chain(settings.tls.cert_file, settings.tls.key_file)
        headers = {"Authorization": f"Bearer {settings.backend_token}"} if settings.backend_token else {}
        self._c = httpx.Client(base_url=settings.backend_url.rstrip("/"), headers=headers, timeout=15.0,
                               verify=verify, transport=transport)

    def request(self, method: str, path: str, **kw: Any) -> Any:
        try:
            r = self._c.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise BackendError(502, f"backend unreachable: {exc}") from None
        if r.status_code >= 400:
            try:
                detail = r.json().get("detail", r.text)
            except ValueError:
                detail = r.text
            raise BackendError(r.status_code, detail)
        return r.json() if r.content else None

    def close(self) -> None:
        self._c.close()
