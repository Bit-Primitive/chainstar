"""Warehouse targets: where the raw and modelled tables are written.

Credentials never come from the target itself when they can be avoided: each
target reads its password, token or keys from an environment variable or the
service's own standard credential mechanism, and describe() never shows them.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit


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


# MotherDuck reads its token from this variable (or motherduck_token), as the
# DuckDB motherduck extension and dbt-duckdb both do.
MOTHERDUCK_TOKEN_ENVS = ("motherduck_token", "MOTHERDUCK_TOKEN")


@dataclass(frozen=True)
class MotherDuckTarget:
    """A MotherDuck database, reached through DuckDB's `md:` paths."""

    database: str

    def describe(self) -> str:
        return f"motherduck:{self.database}"

    @property
    def path(self) -> str:
        return f"md:{self.database}"


@dataclass(frozen=True)
class S3Options:
    """Where an s3:// lake lives. Keys are not here: they come from the environment."""

    endpoint: str | None = None
    region: str | None = None
    url_style: str | None = None
    use_ssl: bool = True


@dataclass(frozen=True)
class LakeTarget:
    """A Parquet data lake on local disk or S3-compatible storage.

    The star schema is built in a local DuckDB file (`engine`), exactly as for a
    DuckDB target, and published to `location` as partitioned Parquet after each
    successful build. See lake.py.
    """

    location: str
    engine: Path
    s3: S3Options | None = None

    def describe(self) -> str:
        return f"parquet:{self.location}"

    @property
    def remote(self) -> bool:
        return self.s3 is not None

    def path(self, *parts: str) -> str:
        return "/".join([self.location.rstrip("/"), *parts])


CLICKHOUSE_PASSWORD_ENV = "CLICKHOUSE_PASSWORD"


@dataclass(frozen=True)
class ClickHouseTarget:
    host: str
    port: int
    user: str
    secure: bool
    password: str = field(default="", repr=False)

    def describe(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"clickhouse:{scheme}://{self.user}@{self.host}:{self.port}"


@dataclass(frozen=True)
class BigQueryTarget:
    """A Google Cloud project; chainstar's schemas become datasets in it.

    Credentials are Google's Application Default Credentials: the file named by
    GOOGLE_APPLICATION_CREDENTIALS, `gcloud auth application-default login`, or
    the metadata server on Google Cloud.
    """

    project: str
    location: str | None = None

    def describe(self) -> str:
        return f"bigquery:{self.project}" + (f"?location={self.location}" if self.location else "")


SNOWFLAKE_PASSWORD_ENV = "SNOWFLAKE_PASSWORD"
SNOWFLAKE_KEY_PATH_ENV = "SNOWFLAKE_PRIVATE_KEY_PATH"
SNOWFLAKE_KEY_PASSPHRASE_ENV = "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"


@dataclass(frozen=True)
class SnowflakeTarget:
    account: str
    user: str
    database: str
    warehouse: str | None = None
    role: str | None = None
    # e.g. externalbrowser; None means a password or key pair from the environment.
    authenticator: str | None = None
    password: str = field(default="", repr=False)
    private_key_path: str | None = None
    private_key_passphrase: str = field(default="", repr=False)

    def describe(self) -> str:
        return f"snowflake:{self.user}@{self.account}/{self.database}"


Target = (
    DuckDBTarget
    | PostgresTarget
    | MotherDuckTarget
    | LakeTarget
    | ClickHouseTarget
    | BigQueryTarget
    | SnowflakeTarget
)

# Targets whose tables are built and read through a DuckDB connection.
DUCKDB_FAMILY = (DuckDBTarget, PostgresTarget, MotherDuckTarget, LakeTarget)

TARGET_HELP = (
    "duckdb:PATH, postgres:postgresql://USER@HOST:PORT/DB, motherduck:DB, "
    "parquet:PATH or parquet:s3://BUCKET/PREFIX, clickhouse:http://USER@HOST:8123, "
    "bigquery:PROJECT, or snowflake:USER@ACCOUNT/DB?warehouse=WH (see the README's Targets)"
)
# Default engine file of a Parquet lake, next to where chainstar runs.
DEFAULT_LAKE_ENGINE = "lake-engine.duckdb"


def parse_target(spec: str) -> Target:
    kind, sep, rest = spec.partition(":")
    if not sep or not rest:
        raise TargetError(f"target must look like one of: {TARGET_HELP}")
    kind = kind.strip().lower()
    if kind == "duckdb":
        if rest.startswith("md:"):
            return _parse_motherduck(rest.removeprefix("md:"))
        return DuckDBTarget(path=Path(rest).expanduser())
    if kind in ("postgres", "postgresql"):
        return _parse_postgres_url(rest)
    if kind in ("motherduck", "md"):
        return _parse_motherduck(rest)
    if kind == "parquet":
        return _parse_lake(rest)
    if kind == "clickhouse":
        return _parse_clickhouse(rest)
    if kind == "bigquery":
        return _parse_bigquery(rest)
    if kind == "snowflake":
        return _parse_snowflake(rest)
    raise TargetError(f"unknown target type {kind!r}; expected one of: {TARGET_HELP}")


def _parse_postgres_url(url: str) -> PostgresTarget:
    parts = urlsplit(url)
    if parts.scheme not in ("postgres", "postgresql"):
        raise TargetError(
            "postgres target must be a postgresql:// URL, like "
            "postgres:postgresql://USER@HOST:PORT/DB"
        )
    dbname = parts.path.lstrip("/")
    if not parts.hostname or not parts.username or not dbname:
        raise TargetError(
            "postgres URL needs a user, host and database, like "
            "postgres:postgresql://USER@HOST:PORT/DB"
        )
    # Same fallback libpq uses, so a password can stay out of shell history and config files.
    password = unquote(parts.password) if parts.password else os.environ.get("PGPASSWORD", "")
    return PostgresTarget(
        host=parts.hostname,
        port=parts.port or 5432,
        user=unquote(parts.username),
        dbname=unquote(dbname),
        password=password,
    )


def _parse_motherduck(database: str) -> MotherDuckTarget:
    database = database.strip()
    if not database or "?" in database or "/" in database:
        raise TargetError(
            "a MotherDuck target names one database, like motherduck:my_db; "
            "the token comes from the motherduck_token environment variable"
        )
    return MotherDuckTarget(database)


_LAKE_OPTIONS = {"endpoint", "region", "url_style", "use_ssl", "engine"}


def _parse_lake(location: str) -> LakeTarget:
    path, _, query = location.partition("?")
    options = {key: values[-1] for key, values in parse_qs(query, keep_blank_values=True).items()}
    unknown = set(options) - _LAKE_OPTIONS
    if unknown:
        raise TargetError(
            f"unknown parquet target option(s) {', '.join(sorted(unknown))}; "
            f"known: {', '.join(sorted(_LAKE_OPTIONS))}"
        )
    engine = Path(options.get("engine") or DEFAULT_LAKE_ENGINE).expanduser()
    scheme = path.split("://", 1)[0].lower() if "://" in path else ""
    if not scheme:
        if not path:
            raise TargetError("a parquet target needs a folder, like parquet:./lake")
        s3_only = set(options) - {"engine"}
        if s3_only:
            raise TargetError(f"{', '.join(sorted(s3_only))} apply only to s3:// lakes")
        return LakeTarget(location=str(Path(path).expanduser().resolve()), engine=engine)
    if scheme != "s3":
        raise TargetError(
            f"{scheme}:// lakes are not supported; use a local folder or s3:// (AWS S3, "
            "Cloudflare R2, MinIO and other S3-compatible storage, with ?endpoint=)"
        )
    bucket = urlsplit(path).netloc
    if not bucket:
        raise TargetError("an s3 lake needs a bucket, like parquet:s3://my-bucket/chainstar")
    use_ssl = options.get("use_ssl", "true").lower()
    if use_ssl not in ("true", "false"):
        raise TargetError(f"use_ssl must be true or false, got {use_ssl!r}")
    url_style = options.get("url_style")
    if url_style not in (None, "path", "vhost"):
        raise TargetError(f"url_style must be path or vhost, got {url_style!r}")
    endpoint = options.get("endpoint") or os.environ.get("AWS_ENDPOINT_URL") or None
    region = options.get("region") or os.environ.get("AWS_REGION")
    region = region or os.environ.get("AWS_DEFAULT_REGION") or None
    if endpoint and "://" in endpoint:
        # DuckDB takes the host only; the scheme says whether to use TLS.
        scheme_part, _, endpoint = endpoint.partition("://")
        if "use_ssl" not in options:
            use_ssl = "true" if scheme_part.lower() == "https" else "false"
    return LakeTarget(
        location=path.rstrip("/"),
        engine=engine,
        s3=S3Options(
            endpoint=endpoint.rstrip("/") if endpoint else None,
            region=region,
            url_style=url_style,
            use_ssl=use_ssl == "true",
        ),
    )


def _parse_clickhouse(url: str) -> ClickHouseTarget:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise TargetError(
            "clickhouse target must be an http:// or https:// URL to the HTTP interface, "
            "like clickhouse:http://USER@HOST:8123"
        )
    if not parts.hostname:
        raise TargetError("clickhouse URL needs a host, like clickhouse:http://USER@HOST:8123")
    if parts.path not in ("", "/"):
        raise TargetError(
            "clickhouse URL takes no database: chainstar writes to its own databases "
            "(chainstar, chainstar_raw, chainstar_staging, chainstar_state)"
        )
    secure = parts.scheme == "https"
    password = (
        unquote(parts.password) if parts.password else os.environ.get(CLICKHOUSE_PASSWORD_ENV, "")
    )
    return ClickHouseTarget(
        host=parts.hostname,
        port=parts.port or (8443 if secure else 8123),
        user=unquote(parts.username) if parts.username else "default",
        secure=secure,
        password=password,
    )


def _parse_bigquery(rest: str) -> BigQueryTarget:
    project, _, query = rest.removeprefix("//").partition("?")
    project = project.strip().strip("/")
    options = {key: values[-1] for key, values in parse_qs(query).items()}
    unknown = set(options) - {"location"}
    if not project or "/" in project or unknown:
        raise TargetError(
            "a BigQuery target names a project, like bigquery:my-project or "
            "bigquery:my-project?location=EU"
        )
    return BigQueryTarget(project=project, location=options.get("location"))


_SNOWFLAKE_OPTIONS = {"warehouse", "role", "authenticator"}


def _parse_snowflake(rest: str) -> SnowflakeTarget:
    parts = urlsplit("snowflake://" + rest.removeprefix("//").removeprefix("snowflake://"))
    database = parts.path.strip("/")
    if not parts.username or not parts.hostname or not database or "/" in database:
        raise TargetError(
            "a Snowflake target looks like snowflake:USER@ACCOUNT/DATABASE?warehouse=WH; "
            "the password comes from SNOWFLAKE_PASSWORD, or a key pair from "
            "SNOWFLAKE_PRIVATE_KEY_PATH"
        )
    if parts.password:
        raise TargetError(
            "keep the Snowflake password out of the target; set SNOWFLAKE_PASSWORD instead"
        )
    options = {key: values[-1] for key, values in parse_qs(parts.query).items()}
    unknown = set(options) - _SNOWFLAKE_OPTIONS
    if unknown:
        raise TargetError(
            f"unknown snowflake target option(s) {', '.join(sorted(unknown))}; "
            f"known: {', '.join(sorted(_SNOWFLAKE_OPTIONS))}"
        )
    return SnowflakeTarget(
        # urlsplit lowercases the host; Snowflake account identifiers are not case-sensitive.
        account=parts.hostname,
        user=unquote(parts.username),
        database=unquote(database),
        warehouse=options.get("warehouse"),
        role=options.get("role"),
        authenticator=options.get("authenticator"),
        password=os.environ.get(SNOWFLAKE_PASSWORD_ENV, ""),
        private_key_path=os.environ.get(SNOWFLAKE_KEY_PATH_ENV) or None,
        private_key_passphrase=os.environ.get(SNOWFLAKE_KEY_PASSPHRASE_ENV, ""),
    )


# Environment variables that hold warehouse credentials, scrubbed from every error
# message and log line whatever the target.
CREDENTIAL_ENVS = (
    "PGPASSWORD",
    CLICKHOUSE_PASSWORD_ENV,
    SNOWFLAKE_PASSWORD_ENV,
    SNOWFLAKE_KEY_PASSPHRASE_ENV,
    *MOTHERDUCK_TOKEN_ENVS,
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


def credential_secrets(target: Target | None = None) -> list[str]:
    """Every credential value in the environment, and those the target carries."""
    secrets = [os.environ.get(name, "") for name in CREDENTIAL_ENVS]
    if target is not None:
        secrets += [
            getattr(target, "password", ""),
            getattr(target, "private_key_passphrase", ""),
        ]
    # A one-character "secret" would scrub every occurrence of that character.
    return [secret for secret in dict.fromkeys(secrets) if len(secret) > 3]


def _libpq_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
