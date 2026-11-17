"""Optional login: ``FT_AUTH_MODE = none | local | ldap``."""

from __future__ import annotations

import logging
import ssl
from typing import Optional

from ..config import Settings

log = logging.getLogger(__name__)


class AuthError(Exception):
    pass


def ldap_authenticate(settings: Settings, username: str, password: str, *, server=None) -> str:
    """Bind as the user; optionally require group membership. Returns the user's DN.

    Two ways to find the DN:
      * ``FT_LDAP_USER_DN_TEMPLATE`` e.g. ``uid={username},ou=people,dc=home,dc=lab`` (no search), or
      * search ``FT_LDAP_BASE_DN`` with ``FT_LDAP_USER_FILTER`` (service bind via
        ``FT_LDAP_BIND_DN``/``FT_LDAP_BIND_PASSWORD``, else anonymous search).
    """
    from ldap3 import BASE, SUBTREE, Connection, Server, Tls
    from ldap3.core.exceptions import LDAPException
    from ldap3.utils.conv import escape_filter_chars
    from ldap3.utils.dn import escape_rdn

    if not username or not password:
        # An empty password would be an *unauthenticated* simple bind that many servers accept.
        raise AuthError("username and password are required")
    if server is None:
        if not settings.ldap_url:
            raise AuthError("FT_LDAP_URL is not configured")
        tls = Tls(ca_certs_file=settings.ldap_ca_file, validate=ssl.CERT_REQUIRED) if settings.ldap_ca_file else Tls(validate=ssl.CERT_REQUIRED)
        server = Server(settings.ldap_url, use_ssl=settings.ldap_url.lower().startswith("ldaps://"), tls=tls, connect_timeout=5)

    def conn(user: Optional[str], pw: Optional[str]) -> "Connection":
        c = Connection(server, user=user, password=pw, receive_timeout=10, raise_exceptions=False)
        if settings.ldap_start_tls:
            c.open()
            c.start_tls()
        return c

    try:
        if settings.ldap_user_dn_template:
            user_dn = settings.ldap_user_dn_template.format(username=escape_rdn(username))
        else:
            if not settings.ldap_base_dn:
                raise AuthError("set FT_LDAP_USER_DN_TEMPLATE or FT_LDAP_BASE_DN")
            svc = conn(settings.ldap_bind_dn, settings.ldap_bind_password)
            if not svc.bind():
                log.error("LDAP service bind failed: %s", svc.result.get("description"))
                raise AuthError("directory unavailable")
            flt = settings.ldap_user_filter.format(username=escape_filter_chars(username))
            svc.search(settings.ldap_base_dn, flt, SUBTREE, attributes=[], size_limit=2)
            entries = list(svc.entries)
            svc.unbind()
            if len(entries) != 1:
                raise AuthError("invalid credentials")
            user_dn = entries[0].entry_dn
        user_conn = conn(user_dn, password)
        if not user_conn.bind():
            raise AuthError("invalid credentials")
        if settings.ldap_required_group:
            dn_e = escape_filter_chars(user_dn)
            flt = f"(|(member={dn_e})(uniqueMember={dn_e})(memberUid={escape_filter_chars(username)}))"
            user_conn.search(settings.ldap_required_group, flt, BASE, attributes=[])
            if not user_conn.entries:
                user_conn.unbind()
                raise AuthError("not a member of the required group")
        user_conn.unbind()
        return user_dn
    except LDAPException as exc:
        log.error("LDAP error: %s", exc)
        raise AuthError("directory unavailable") from None


def authenticate(settings: Settings, store, username: str, password: str) -> str:
    """Return the canonical username or raise AuthError."""
    mode = settings.auth_mode
    username = (username or "").strip()
    if mode == "local":
        if not store.check_user(username, password):
            raise AuthError("invalid credentials")
        return username
    if mode == "ldap":
        ldap_authenticate(settings, username, password)
        return username
    raise AuthError(f"login is disabled (FT_AUTH_MODE={mode})")
