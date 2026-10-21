"""A Parquet data lake: the star schema and sync progress as partitioned Parquet files.

Where the tables live, on local disk or S3-compatible storage (AWS S3, Cloudflare
R2, MinIO), with the Hive layout DuckDB, Spark, Athena and Trino read:

    LAKE/chainstar/fct_transfers/chain=base/date_day=2026-10-21/data_0.parquet
    LAKE/chainstar/fct_fees/chain=base/date_day=2026-10-21/data_0.parquet
    LAKE/chainstar/dim_wallets/chain=base/data_0.parquet     (and dim_tokens, dim_contracts)
    LAKE/chainstar/dim_dates/data_0.parquet
    LAKE/chainstar_state/sync_progress/data_0.parquet
    LAKE/_chainstar/published.parquet                        (what was published, below)

How it is built: exactly as a DuckDB target, in a local DuckDB file (the
engine), then published. dbt-duckdb's external materialization writes Parquet
directly but rebuilds each table in full on every run and cannot be
incremental; building in the engine keeps the incremental models, the
transactional progress record and reorganisation rollbacks identical to the
DuckDB target, and the lake is a copy of tables already built and tested.

Publishing is incremental too. Each partition's row count and a hash of its rows
are kept in `_chainstar/published.parquet`; a publish rewrites only partitions
whose rows changed, and empties partitions that no longer exist (after a
rollback, say). On local disk those files are deleted; object stores are
written through DuckDB, which cannot delete, so there the file is replaced by
an empty one with the same columns, which every reader treats as no rows. The
manifest is written last, so a publish that stops half-way is redone in full
by the next one. Each file is replaced in one PUT, but a reader listing the
lake while a publish runs can see some partitions new and others not yet.

S3 credentials come from the environment, as every AWS tool reads them:
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN), else the AWS
credential chain (shared config and credentials files, SSO, instance roles).
"""

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.load import Warehouse, _ident, _quote, attach
from chainstar.model import MART_SCHEMA, MART_TABLES
from chainstar.state import STATE_SCHEMA, STATE_TABLE
from chainstar.targets import LakeTarget

# Hive partition columns per table. Facts by chain and day, so a query for one
# chain and a date range reads only those files; dimensions by chain.
PARTITIONS: dict[str, tuple[str, ...]] = {
    "fct_transfers": ("chain", "date_day"),
    "fct_fees": ("chain", "date_day"),
    "dim_wallets": ("chain",),
    "dim_tokens": ("chain",),
    "dim_contracts": ("chain",),
    "dim_dates": (),
}
assert set(PARTITIONS) == set(MART_TABLES)
DATA_FILE = "data_0.parquet"
MANIFEST = "_chainstar/published.parquet"
# What Hive and Spark call a partition whose value is NULL.
NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class LakeError(RuntimeError):
    pass


@dataclass
class Published:
    written: int = 0
    emptied: int = 0
    unchanged: int = 0
    rows: int = 0

    def describe(self) -> str:
        text = (
            f"{self.written:,} partitions written ({self.rows:,} rows), "
            f"{self.unchanged:,} unchanged"
        )
        if self.emptied:
            text += f", {self.emptied:,} emptied"
        return text


def configure(connection: duckdb.DuckDBPyConnection, target: LakeTarget) -> None:
    """Let DuckDB read and write an s3:// lake: the httpfs extension and an S3 secret."""
    if target.s3 is None:
        return
    connection.execute("INSTALL httpfs; LOAD httpfs;")
    options = ["TYPE s3"]
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if key_id and secret:
        options += [
            "PROVIDER config",
            f"KEY_ID {_quote(key_id)}",
            f"SECRET {_quote(secret)}",
        ]
        if token := os.environ.get("AWS_SESSION_TOKEN"):
            options.append(f"SESSION_TOKEN {_quote(token)}")
    else:
        # Shared config and credentials files, SSO, instance and container roles.
        connection.execute("INSTALL aws; LOAD aws;")
        options.append("PROVIDER credential_chain")
    s3 = target.s3
    if s3.endpoint:
        options.append(f"ENDPOINT {_quote(s3.endpoint)}")
        # S3-compatible stores (MinIO, R2) usually want bucket names in the path.
        options.append(f"URL_STYLE {_quote(s3.url_style or 'path')}")
    elif s3.url_style:
        options.append(f"URL_STYLE {_quote(s3.url_style)}")
    if s3.region:
        options.append(f"REGION {_quote(s3.region)}")
    options.append(f"USE_SSL {'true' if s3.use_ssl else 'false'}")
    bucket = target.location.split("/")[2]
    options.append(f"SCOPE {_quote('s3://' + bucket)}")
    connection.execute(f"CREATE OR REPLACE TEMPORARY SECRET chainstar_lake ({', '.join(options)})")


def publish(target: LakeTarget) -> Published:
    """Copy the engine's star schema and progress to the lake; only what changed."""
    result = Published()
    with attach(target) as warehouse:
        connection = warehouse.connection
        configure(connection, target)
        previous = _read_manifest(connection, target)
        entries: list[tuple[str, str, int, str, str]] = []
        for table, keys in PARTITIONS.items():
            if not warehouse.has_table(MART_SCHEMA, table):
                continue
            reference = warehouse.table(MART_SCHEMA, table)
            columns = _columns(connection, reference)
            current = _fingerprints(connection, reference, keys)
            for partition, (values, rows, fingerprint) in current.items():
                entries.append((table, partition, rows, fingerprint, columns))
                if previous.get((table, partition)) == fingerprint:
                    result.unchanged += 1
                    continue
                _write_partition(connection, target, reference, table, keys, partition, values)
                result.written += 1
                result.rows += rows
            for (old_table, partition), fingerprint in previous.items():
                if old_table != table or partition in current or fingerprint == _EMPTY:
                    continue
                _empty_partition(connection, target, reference, table, keys, partition)
                entries.append((table, partition, 0, _EMPTY, columns))
                result.emptied += 1
        if warehouse.has_table(STATE_SCHEMA, STATE_TABLE):
            _copy(
                connection,
                f"SELECT * FROM {warehouse.table(STATE_SCHEMA, STATE_TABLE)} ORDER BY chain",
                [],
                target,
                f"{STATE_SCHEMA}/{STATE_TABLE}/{DATA_FILE}",
            )
        _write_manifest(connection, target, entries)
    return result


# The fingerprint of a partition that was emptied.
_EMPTY = "0:0"


def _columns(connection: duckdb.DuckDBPyConnection, reference: str) -> str:
    """The table's columns and types, in order, as JSON for the manifest."""
    described = connection.execute(f"DESCRIBE SELECT * FROM {reference}").fetchall()
    return json.dumps([[name, column_type] for name, column_type, *_ in described])


def _fingerprints(
    connection: duckdb.DuckDBPyConnection, reference: str, keys: tuple[str, ...]
) -> dict[str, tuple[tuple, int, str]]:
    """Per partition: its key values, row count, and count plus a sum of row hashes."""
    key_list = "".join(f"{_ident(key)}, " for key in keys)
    group = f" GROUP BY {', '.join(_ident(key) for key in keys)}" if keys else ""
    rows = connection.execute(
        f"SELECT {key_list}count(*), CAST(sum(hash(chainstar_row)) AS VARCHAR) "
        f"FROM {reference} AS chainstar_row{group}"
    ).fetchall()
    found = {}
    for row in rows:
        values, count, hashes = tuple(row[: len(keys)]), int(row[-2]), row[-1]
        if count:
            found[_partition_path(keys, values)] = (values, count, f"{count}:{hashes}")
    return found


def _partition_path(keys: tuple[str, ...], values: tuple) -> str:
    return "/".join(
        f"{key}={NULL_PARTITION if value is None else value}"
        for key, value in zip(keys, values, strict=True)
    )


def _where(keys: tuple[str, ...]) -> str:
    if not keys:
        return "true"
    return " AND ".join(f"{_ident(key)} IS NOT DISTINCT FROM ?" for key in keys)


def _without_keys(keys: tuple[str, ...]) -> str:
    return f"* EXCLUDE ({', '.join(_ident(key) for key in keys)})" if keys else "*"


def _file(table: str, partition: str) -> str:
    return "/".join(part for part in (MART_SCHEMA, table, partition, DATA_FILE) if part)


def _write_partition(connection, target, reference, table, keys, partition, values) -> None:
    # Partition columns are in the path, not the file, as Hive, Spark and Athena expect.
    _copy(
        connection,
        f"SELECT {_without_keys(keys)} FROM {reference} WHERE {_where(keys)}",
        list(values),
        target,
        _file(table, partition),
    )


def _empty_partition(connection, target, reference, table, keys, partition) -> None:
    if not target.remote:
        path = Path(target.path(_file(table, partition)))
        path.unlink(missing_ok=True)
        # Remove the partition's now-empty folders, up to the table's.
        folder, top = path.parent, Path(target.path(MART_SCHEMA, table))
        while folder != top and folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
            folder = folder.parent
        return
    _copy(
        connection,
        f"SELECT {_without_keys(keys)} FROM {reference} LIMIT 0",
        [],
        target,
        _file(table, partition),
    )


def _copy(connection, query: str, params: list, target: LakeTarget, relative: str) -> None:
    destination = target.path(relative)
    if not target.remote:
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({query}) TO {_quote(destination)} (FORMAT parquet, COMPRESSION zstd)", params
    )


def _read_manifest(
    connection: duckdb.DuckDBPyConnection, target: LakeTarget
) -> dict[tuple[str, str], str]:
    location = target.path(MANIFEST)
    if not _exists(connection, location):
        return {}
    rows = connection.execute(
        f"SELECT table_name, partition, fingerprint FROM read_parquet({_quote(location)})"
    ).fetchall()
    return {(table, partition): fingerprint for table, partition, fingerprint in rows}


def _write_manifest(connection, target: LakeTarget, entries: list) -> None:
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE chainstar_manifest (table_name VARCHAR, partition VARCHAR, "
        "rows BIGINT, fingerprint VARCHAR, columns VARCHAR)"
    )
    if entries:
        connection.executemany("INSERT INTO chainstar_manifest VALUES (?, ?, ?, ?, ?)", entries)
    _copy(
        connection,
        "SELECT * FROM chainstar_manifest ORDER BY table_name, partition",
        [],
        target,
        MANIFEST,
    )


def _exists(connection: duckdb.DuckDBPyConnection, location: str) -> bool:
    """Whether a file, or any file matching a glob pattern, exists (on S3: by listing)."""
    if not any(char in location for char in "*?["):
        # glob() returns a plain path as it is, without looking; a one-character
        # class makes it a pattern, so the file is really listed.
        location = f"{location[:-1]}[{location[-1]}]"
    row = connection.execute("SELECT count(*) FROM glob(?)", [location]).fetchone()
    return bool(row and row[0])


@contextmanager
def open_published(target: LakeTarget) -> Iterator[Warehouse]:
    """The lake as read by status and export: views over its files, in memory.

    Columns come back in the order and types the engine built them, partition
    columns included, so an export from the lake matches one from DuckDB.
    """
    connection = duckdb.connect()
    try:
        configure(connection, target)
        for schema in (MART_SCHEMA, STATE_SCHEMA):
            connection.execute(f"CREATE SCHEMA {_ident(schema)}")
        tables = _published_tables(connection, target)
        for table, columns in tables.items():
            _create_view(connection, target, table, columns)
        state = target.path(f"{STATE_SCHEMA}/{STATE_TABLE}/{DATA_FILE}")
        if _exists(connection, state):
            connection.execute(
                f"CREATE VIEW {_ident(STATE_SCHEMA)}.{_ident(STATE_TABLE)} AS "
                f"SELECT * FROM read_parquet({_quote(state)})"
            )
        yield Warehouse(connection, "memory")
    finally:
        connection.close()


def _published_tables(connection, target: LakeTarget) -> dict[str, list[list[str]]]:
    location = target.path(MANIFEST)
    if not _exists(connection, location):
        return {}
    rows = connection.execute(
        f"SELECT DISTINCT table_name, columns FROM read_parquet({_quote(location)})"
    ).fetchall()
    return {table: json.loads(columns) for table, columns in rows}


def _create_view(connection, target: LakeTarget, table: str, columns: list[list[str]]) -> None:
    keys = PARTITIONS[table]
    folder = target.path(MART_SCHEMA, table)
    pattern = f"{folder}/**/*.parquet" if keys else f"{folder}/{DATA_FILE}"
    select = ", ".join(
        f"CAST({_ident(name)} AS {kind}) AS {_ident(name)}" for name, kind in columns
    )
    if _exists(connection, pattern):
        # Partition values are read as text and cast below to the engine's types.
        hive = "true, hive_types_autocast = false" if keys else "false"
        source = (
            f"read_parquet({_quote(pattern)}, hive_partitioning = {hive}, union_by_name = true)"
        )
        # A partition whose value is NULL is stored as NULL_PARTITION.
        replacements = ", ".join(
            f"nullif({_ident(key)}, {_quote(NULL_PARTITION)}) AS {_ident(key)}" for key in keys
        )
        if replacements:
            source = f"(SELECT * REPLACE ({replacements}) FROM {source})"
    else:
        # Every partition was emptied: the table exists, with no rows.
        source = f"(SELECT {', '.join(f'NULL AS {_ident(name)}' for name, _ in columns)} LIMIT 0)"
    connection.execute(
        f"CREATE VIEW {_ident(MART_SCHEMA)}.{_ident(table)} AS SELECT {select} FROM {source}"
    )
