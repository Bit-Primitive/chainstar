"""One plain sentence of what to do next, for the failures people commonly hit.

The error itself says what happened; the hint says what to try. Hints are found by
the error's type and wording, because most of these come from DuckDB, Postgres or
the network rather than from chainstar.
"""

ISSUES_URL = "https://github.com/Bit-Primitive/chainstar/issues"

RATE_LIMIT_HINT = (
    "the RPC endpoint is limiting requests: wait a minute and run the same command again "
    "(finished chunks are kept), lower --requests-per-second, or use your own RPC URL"
)

# (words in the lowercased error message, hint). The first match wins.
_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("rate limited", "http 429", "too many requests"), RATE_LIMIT_HINT),
    (
        ("could not reach",),
        "check your internet connection and the RPC URL; a free public endpoint may also be "
        "down for a while, so try again later or use your own RPC URL",
    ),
    (
        ("password authentication failed",),
        "check the user name in the postgres URL and put the password in the PGPASSWORD "
        "environment variable",
    ),
    (
        ("connection refused", "could not connect to server", "could not translate host"),
        "check that Postgres is running and that the host and port in --target are right",
    ),
    (
        ("could not set lock", "conflicting lock"),
        "another program has the DuckDB file open (the duckdb tool, a notebook, a SQL editor); "
        "close it and try again",
    ),
    (
        ("permission denied", "read-only file system"),
        "choose a folder you can write to, with --target, --data-dir or --output",
    ),
    (("no space left",), "free up disk space, or choose another disk with --data-dir and --target"),
    (
        ("failed to download extension",),
        "DuckDB downloads its postgres and excel extensions the first time they are used; "
        "check your internet connection, or use the Docker image, which includes them",
    ),
)


def describe(error: BaseException) -> str:
    if isinstance(error, OSError) and error.filename:
        return f"could not use {error.filename}: {error.strerror or error}"
    return str(error) or type(error).__name__


def hint_for(error: BaseException) -> str | None:
    text = describe(error).lower()
    if isinstance(error, PermissionError):
        text += " permission denied"
    for words, hint in _HINTS:
        if any(word in text for word in words):
            return hint
    return None


def bug_report_hint() -> str:
    return f"this looks like a bug: run again with --debug and report it at {ISSUES_URL}"
