"""uvicorn launcher with HTTPS certificate hot-reload.

smallstep leaf certificates are short-lived (24 h by default) and get renewed
in place — by cert-manager/step-issuer (Secret volume) or by a
``step ca renew --daemon`` sidecar. We watch the files and reload the cert
chain into the *live* SSLContext: new TLS handshakes pick it up without a
restart.
"""

from __future__ import annotations

import hashlib
import logging
import ssl
import threading
from typing import Optional

import uvicorn

from .config import Settings, TLSSettings

log = logging.getLogger(__name__)


def _digest(*paths: Optional[str]) -> str:
    h = hashlib.sha256()
    for p in paths:
        if p:
            with open(p, "rb") as fh:
                h.update(fh.read())
    return h.hexdigest()


class CertReloader(threading.Thread):
    def __init__(self, ctx: ssl.SSLContext, tls: TLSSettings) -> None:
        super().__init__(name="tls-reloader", daemon=True)
        self.ctx, self.tls = ctx, tls
        self._stop = threading.Event()
        self._last = _digest(tls.cert_file, tls.key_file, tls.ca_file)

    def check(self) -> bool:
        try:
            cur = _digest(self.tls.cert_file, self.tls.key_file, self.tls.ca_file)
        except OSError as exc:  # mid-rotation: files briefly missing
            log.warning("TLS files unreadable, keeping current cert: %s", exc)
            return False
        if cur == self._last:
            return False
        try:
            self.ctx.load_cert_chain(self.tls.cert_file, self.tls.key_file)
            if self.tls.ca_file:
                self.ctx.load_verify_locations(self.tls.ca_file)
        except (ssl.SSLError, OSError) as exc:
            log.error("TLS reload failed, keeping current cert: %s", exc)
            return False
        self._last = cur
        log.info("TLS certificate reloaded from %s", self.tls.cert_file)
        return True

    def run(self) -> None:
        while not self._stop.wait(self.tls.reload_interval_s):
            self.check()

    def stop(self) -> None:
        self._stop.set()


def serve(app, settings: Settings) -> None:
    tls = settings.tls
    kwargs = {}
    if tls.enabled:
        kwargs.update(ssl_certfile=tls.cert_file, ssl_keyfile=tls.key_file)
        if tls.ca_file:
            kwargs["ssl_ca_certs"] = tls.ca_file
        if tls.client_auth:
            kwargs["ssl_cert_reqs"] = ssl.CERT_REQUIRED
    config = uvicorn.Config(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_graceful_shutdown=30,
        **kwargs,
    )
    config.load()
    reloader = None
    if tls.enabled and config.ssl is not None:
        reloader = CertReloader(config.ssl, tls)
        reloader.start()
    log.info("%s listening on %s://%s:%s", settings.role, "https" if tls.enabled else "http", settings.host, settings.port)
    try:
        uvicorn.Server(config).run()
    finally:
        if reloader:
            reloader.stop()
