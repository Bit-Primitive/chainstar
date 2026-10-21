"""Warehouse targets: where the raw and modelled tables are written."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, urlsplit


class TargetError(ValueError):
    pass


@dataclass(frozen=True)
class DuckDBTarget:
    path: Path

    def describe(self) -> str:
        return f"duckdb:{self.path}"


@dataclass(frozen=True)
class PostgresTarget:
    host: str
    port: int
    user: str
    dbname: str
    # Excluded from repr so a stray print or traceback never shows it.
    password: str = field(default="", repr=False)

    def describe(self) -> str:
        return f"postgres:postgresql://{self.user}@{self.host}:{self.port}/{self.dbname}"

    def libpq_dsn(self) -> str:
        """Key/value connection string for DuckDB's postgres extension (libpq syntax)."""
        parts = {
            "host": self.host,
            "port": str(self.port),
            "user": self.user,
            "dbname": self.dbname,
            "password": self.password,
        }
        return " ".join(f"{key}={_libpq_quote(value)}" for key, value in parts.items() if value)


Target = DuckDBTarget | PostgresTarget

TARGET_HELP = "duckdb:PATH or postgres:postgresql://USER[:PASSWORD]@HOST[:PORT]/DBNAME"


def parse_target(spec: str) -> Target:
    kind, sep, rest = spec.partition(":")
    if not sep or not rest:
        raise TargetError(f"target must look like {TARGET_HELP}")
    kind = kind.strip().lower()
    if kind == "duckdb":
        return DuckDBTarget(path=Path(rest).expanduser())
    if kind in ("postgres", "postgresql"):
        return _parse_postgres_url(rest)
    raise TargetError(f"unknown target type {kind!r}; expected {TARGET_HELP}")


def _parse_postgres_url(url: str) -> PostgresTarget:
    parts = urlsplit(url)
    if parts.scheme not in ("postgres", "postgresql"):
        raise TargetError(f"postgres target must be a postgresql:// URL; expected {TARGET_HELP}")
    dbname = parts.path.lstrip("/")
    if not parts.hostname or not parts.username or not dbname:
        raise TargetError(f"postgres URL needs a user, host and database; expected {TARGET_HELP}")
    # Same fallback libpq uses, so a password can stay out of shell history and config files.
    password = unquote(parts.password) if parts.password else os.environ.get("PGPASSWORD", "")
    return PostgresTarget(
        host=parts.hostname,
        port=parts.port or 5432,
        user=unquote(parts.username),
        dbname=unquote(dbname),
        password=password,
    )


def _libpq_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
