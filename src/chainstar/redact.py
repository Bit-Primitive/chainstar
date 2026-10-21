"""Keep secrets out of anything chainstar prints or writes.

Hosted RPC providers put API keys in the URL path (`/v2/<key>`), the query string
(`?apikey=`) or userinfo, so for display we keep only scheme, host and port.
Warehouse targets carry passwords, tokens and keys, which are replaced outright.
"""

import re
from collections.abc import Iterable
from urllib.parse import urlsplit

# user:password@ in any URL-like string, e.g. a ClickHouse or Postgres URL in a
# driver's error message.
_USERINFO_PASSWORD = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s]+):[^@/\s]+@")


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    if not parts.scheme or not parts.hostname:
        return "<redacted>"
    host = parts.hostname
    if parts.port:
        host = f"{host}:{parts.port}"
    hidden = parts.path not in ("", "/") or parts.query or parts.fragment or parts.username
    return f"{parts.scheme}://{host}{'/<redacted>' if hidden else ''}"


def redact_text(text: str, secrets: Iterable[str]) -> str:
    """Replace every occurrence of each secret URL in `text` with its redacted form."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, redact_url(secret))
    return text


def redact_values(text: str, secrets: Iterable[str]) -> str:
    """Replace every occurrence of each secret value (password, token, key) with <redacted>,
    and the password part of any user:password@ in a URL."""
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, "<redacted>")
    return _USERINFO_PASSWORD.sub(r"\g<scheme>:<redacted>@", text)
