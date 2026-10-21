"""Keep secrets out of anything chainstar prints or writes.

Hosted RPC providers put API keys in the URL path (`/v2/<key>`), the query string
(`?apikey=`) or userinfo, so for display we keep only scheme, host and port.
"""

from collections.abc import Iterable
from urllib.parse import urlsplit


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
