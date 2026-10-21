"""ClickHouse, BigQuery and Snowflake: warehouses reached through their own drivers.

Each offers the operations of load.Warehouse, so loading, sync progress,
rollbacks, status and export work unchanged. SQL is written with `?`
placeholders, as for DuckDB, and each driver binds them its own way.

Raw rows are never inserted row by row. DuckDB reads, casts and de-duplicates the
chunk files as for every target, writes the rows to one Parquet file per table,
and the warehouse's bulk loader takes that file:

- ClickHouse: `INSERT INTO ... FORMAT Parquet` over its HTTP interface.
- BigQuery: a load job with the file as its source (source format PARQUET).
- Snowflake: PUT to the table's stage, then COPY INTO with MATCH_BY_COLUMN_NAME.

ClickHouse and BigQuery have no transaction spanning several statements, so
progress is written last and as a new version; see state.py. Snowflake has
transactions, and a sync batch commits its rows and progress together there as
on DuckDB and Postgres.

The drivers are optional: `pip install 'chainstar[clickhouse]'` (or bigquery,
snowflake). Credentials come from the environment, never from chainstar.yml.
"""

import re
import tempfile
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import duckdb

from chainstar.load import LoadError, Param, _ident
from chainstar.targets import BigQueryTarget, ClickHouseTarget, SnowflakeTarget, Target

EXTRAS = {ClickHouseTarget: "clickhouse", BigQueryTarget: "bigquery", SnowflakeTarget: "snowflake"}
_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)


class MissingDriver(LoadError):
    pass


def _missing(target: Target, error: ImportError) -> MissingDriver:
    extra = EXTRAS[type(target)]
    return MissingDriver(
        f"{target.describe()} needs chainstar's {extra} extra ({error.name} is not installed): "
        f"pip install 'chainstar[{extra}]', or uv tool install 'chainstar[{extra}]'; "
        "the Docker image includes it"
    )


@contextmanager
def attach_remote(target: Target) -> Iterator["RemoteWarehouse"]:
    kinds = {
        ClickHouseTarget: ClickHouseWarehouse,
        BigQueryTarget: BigQueryWarehouse,
        SnowflakeTarget: SnowflakeWarehouse,
    }
    kind = kinds.get(type(target))
    if kind is None:
        raise LoadError(f"unsupported target {target!r}")
    warehouse = kind(target)
    try:
        yield warehouse
    finally:
        warehouse.close()


class RemoteWarehouse:
    """What every remote warehouse shares: a local DuckDB for files, and bulk loads."""

    via_duckdb = False
    postgres = False
    transactional = False
    dialect = ""

    def __init__(self, target: Target) -> None:
        self.target = target
        self._scratch = tempfile.TemporaryDirectory(prefix="chainstar-load-")
        # Reads chunk files and holds fetched rows; never the warehouse itself.
        self.local = duckdb.connect()
        try:
            self._connect()
        except BaseException:
            self.close()
            raise

    def _connect(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self.local.close()
        self._scratch.cleanup()

    def scratch_file(self, suffix: str = ".parquet") -> Path:
        return Path(self._scratch.name) / f"{uuid.uuid4().hex}{suffix}"

    def insert_select(self, schema: str, name: str, select: str, params: Sequence[Any]) -> int:
        """Run the SELECT in DuckDB over local files and bulk-load its rows; return how many."""
        path = self.scratch_file()
        try:
            row = self.local.execute(
                f"COPY ({select}) TO '{path}' (FORMAT parquet)", list(params)
            ).fetchone()
            rows = int(row[0]) if row else 0
            if rows:
                self._upload(schema, name, path)
            return rows
        finally:
            path.unlink(missing_ok=True)

    def _upload(self, schema: str, name: str, path: Path) -> None:
        raise NotImplementedError

    def fetch_into(self, name: str, sql: str, params: Sequence[Any]) -> None:
        """Run a SELECT in the warehouse and keep its rows in local table `name`.

        Column names are lowercased (Snowflake returns them in upper case) and
        times with a zone become naive UTC, as every warehouse stores them.
        """
        source = self._fetch(sql, params)
        columns = self.local.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
        select = []
        for column, column_type, *_ in columns:
            expression = _ident(column)
            if column_type.upper().startswith("TIMESTAMP WITH TIME ZONE"):
                expression = f"timezone('UTC', {expression})"
            select.append(f"{expression} AS {_ident(column.lower())}")
        self.local.execute(
            f"CREATE OR REPLACE TEMP TABLE {_ident(name)} AS "
            f"SELECT {', '.join(select)} FROM {source}"
        )

    def _fetch(self, sql: str, params: Sequence[Any]) -> str:
        """Fetch the rows; return a DuckDB FROM-clause that reads them."""
        raise NotImplementedError

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def _plain(params: Sequence[Any]) -> list[Any]:
    return [param.value if isinstance(param, Param) else param for param in params]


def _key_columns(columns: Sequence[tuple[str, str]]) -> list[str]:
    """The columns a raw or state table is ordered or clustered by: chain, then block."""
    names = [name for name, _ in columns]
    key = [next((n for n in ("chain_id", "chain") if n in names), None)]
    key.append(next((n for n in ("block_number", "slot") if n in names), None))
    return [name for name in key if name]


class ClickHouseWarehouse(RemoteWarehouse):
    """ClickHouse over HTTP with clickhouse-connect, the driver dbt-clickhouse uses."""

    dialect = "clickhouse"
    # Lightweight DELETE waits until the rows are gone on every replica, so a
    # following INSERT or SELECT never sees them; join_use_nulls gives SQL's NULLs
    # for unmatched outer-join rows instead of empty values.
    SETTINGS: ClassVar[dict[str, int]] = {
        "lightweight_deletes_sync": 2,
        "mutations_sync": 2,
        "join_use_nulls": 1,
    }

    def _connect(self) -> None:
        try:
            import clickhouse_connect
        except ImportError as error:
            raise _missing(self.target, error) from None
        target: ClickHouseTarget = self.target  # type: ignore[assignment]
        self.client = clickhouse_connect.get_client(
            host=target.host,
            port=target.port,
            username=target.user,
            password=target.password,
            secure=target.secure,
            settings=self.SETTINGS,
        )

    def close(self) -> None:
        client = getattr(self, "client", None)
        if client is not None:
            client.close()
        super().close()

    def quote(self, name: str) -> str:
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"

    def table(self, schema: str, name: str) -> str:
        # ClickHouse has databases, not schemas: chainstar_raw is a database.
        return f"{self.quote(schema)}.{self.quote(name)}"

    def has_table(self, schema: str, name: str) -> bool:
        rows = self.query(
            "SELECT count() FROM system.tables WHERE database = ? AND name = ?", [schema, name]
        )
        return bool(rows and rows[0][0])

    def _bind(self, sql: str, params: Sequence[Any]) -> tuple[str, list[Any] | None]:
        if not params:
            return sql, None
        values = []
        for value in _plain(params):
            if isinstance(value, datetime):
                # The driver's own formatting drops microseconds.
                value = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            values.append(value)
        # Client-side binding with %s placeholders; a literal % is written %%.
        return sql.replace("%", "%%").replace("?", "%s"), values

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        sql, values = self._bind(sql, params)
        if sql.lstrip().split(None, 1)[0].upper() in ("SELECT", "WITH"):
            return [tuple(row) for row in self.client.query(sql, parameters=values).result_rows]
        self.client.command(sql, parameters=values)
        return []

    def create_schema(self, schema: str) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.quote(schema)}")

    def create_table(self, schema: str, name: str, columns: Sequence[tuple[str, str]]) -> None:
        ddl = ", ".join(f"{self.quote(c)} Nullable({_clickhouse_type(t)})" for c, t in columns)
        key = ", ".join(self.quote(column) for column in _key_columns(columns))
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self.table(schema, name)} ({ddl}) "
            f"ENGINE = MergeTree ORDER BY ({key}) SETTINGS allow_nullable_key = 1"
        )

    def delete(self, table: str, where: str, params: Sequence[Any]) -> int:
        rows = self.query(f"SELECT count() FROM {table} WHERE {where}", params)
        count = int(rows[0][0]) if rows else 0
        if count:
            # A lightweight delete: the rows are hidden at once and dropped at the next merge.
            self.query(f"DELETE FROM {table} WHERE {where}", params)
        return count

    def _upload(self, schema: str, name: str, path: Path) -> None:
        columns = [
            row[0] for row in self.local.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
        ]
        with path.open("rb") as handle:
            self.client.raw_insert(
                self.table(schema, name), column_names=columns, insert_block=handle, fmt="Parquet"
            )

    def _fetch(self, sql: str, params: Sequence[Any]) -> str:
        sql, values = self._bind(sql, params)
        path = self.scratch_file()
        with (
            self.client.raw_stream(sql, parameters=values, fmt="Parquet") as stream,
            path.open("wb") as handle,
        ):
            for chunk in stream:
                handle.write(chunk)
        return f"read_parquet('{path}')"


def _clickhouse_type(sql_type: str) -> str:
    sql_type = sql_type.upper()
    if match := _DECIMAL.match(sql_type):
        return f"Decimal({match[1]}, {match[2]})"
    return {
        "BIGINT": "Int64",
        "VARCHAR": "String",
        "BOOLEAN": "Bool",
        "TIMESTAMP": "DateTime64(6, 'UTC')",
    }[sql_type]


class BigQueryWarehouse(RemoteWarehouse):
    """BigQuery with google-cloud-bigquery and Application Default Credentials."""

    dialect = "bigquery"

    def _connect(self) -> None:
        try:
            from google.cloud import bigquery
        except ImportError as error:
            raise _missing(self.target, error) from None
        target: BigQueryTarget = self.target  # type: ignore[assignment]
        self.bigquery = bigquery
        self.client = bigquery.Client(project=target.project, location=target.location)

    def close(self) -> None:
        client = getattr(self, "client", None)
        if client is not None:
            client.close()
        super().close()

    def quote(self, name: str) -> str:
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"

    def table(self, schema: str, name: str) -> str:
        # Each chainstar schema is a dataset of the project.
        return f"{self.quote(self.target.project)}.{self.quote(schema)}.{self.quote(name)}"

    def table_id(self, schema: str, name: str) -> str:
        return f"{self.target.project}.{schema}.{name}"

    def has_table(self, schema: str, name: str) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self.client.get_table(self.table_id(schema, name))
        except NotFound:
            return False
        return True

    def _job(self, sql: str, params: Sequence[Any]):
        config = self.bigquery.QueryJobConfig(
            query_parameters=[self._parameter(param) for param in params]
        )
        job = self.client.query(sql, job_config=config)
        job.result()
        return job

    def _parameter(self, param: Any):
        if isinstance(param, Param):
            kind = _bigquery_type(param.sql_type).split("(")[0]
            return self.bigquery.ScalarQueryParameter(None, kind, param.value)
        if isinstance(param, bool):
            kind = "BOOL"
        elif isinstance(param, int):
            kind = "INT64"
        elif isinstance(param, Decimal):
            kind = "NUMERIC"
        elif isinstance(param, datetime):
            kind = "DATETIME"
        else:
            kind = "STRING"
        return self.bigquery.ScalarQueryParameter(None, kind, param)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        job = self._job(sql, params)
        if job.statement_type not in (None, "SELECT"):
            return []
        return [tuple(row.values()) for row in job.result()]

    def create_schema(self, schema: str) -> None:
        dataset = self.bigquery.Dataset(f"{self.target.project}.{schema}")
        if self.target.location:
            dataset.location = self.target.location
        self.client.create_dataset(dataset, exists_ok=True)

    def create_table(self, schema: str, name: str, columns: Sequence[tuple[str, str]]) -> None:
        ddl = ", ".join(f"{self.quote(c)} {_bigquery_type(t)}" for c, t in columns)
        key = _key_columns(columns)
        cluster = f" CLUSTER BY {', '.join(self.quote(c) for c in key)}" if key else ""
        self._job(f"CREATE TABLE IF NOT EXISTS {self.table(schema, name)} ({ddl}){cluster}", [])

    def delete(self, table: str, where: str, params: Sequence[Any]) -> int:
        job = self._job(f"DELETE FROM {table} WHERE {where}", params)
        return int(job.num_dml_affected_rows or 0)

    def _upload(self, schema: str, name: str, path: Path) -> None:
        bigquery = self.bigquery
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # DECIMAL(20,0) fits NUMERIC; anything wider would become BIGNUMERIC.
            decimal_target_types=["NUMERIC", "BIGNUMERIC"],
        )
        with path.open("rb") as handle:
            job = self.client.load_table_from_file(
                handle, self.table_id(schema, name), job_config=config
            )
        job.result()

    def _fetch(self, sql: str, params: Sequence[Any]) -> str:
        table = narrow_decimals(self._job(sql, params).to_arrow())
        name = f"fetched_{uuid.uuid4().hex}"
        self.local.register(name, table)
        return _ident(name)


def narrow_decimals(table):
    """BIGNUMERIC arrives as a 76-digit Arrow decimal, wider than DuckDB's 38.

    chainstar's BIGNUMERIC columns hold whole numbers of at most 38 digits
    (DECIMAL(38,0) elsewhere), so they become that; any other value stays exact
    as text.
    """
    import pyarrow as pa

    columns = []
    for column, field in zip(table.columns, table.schema, strict=True):
        if pa.types.is_decimal256(field.type):
            try:
                column = column.cast(pa.decimal128(38, 0))
            except pa.ArrowInvalid:
                column = column.cast(pa.string())
        columns.append(column)
    return pa.table(columns, names=table.column_names)


def _bigquery_type(sql_type: str) -> str:
    sql_type = sql_type.upper()
    if match := _DECIMAL.match(sql_type):
        return (
            f"NUMERIC({match[1]}, {match[2]})"
            if int(match[1]) - int(match[2]) <= 29
            else (f"BIGNUMERIC({match[1]}, {match[2]})")
        )
    # DATETIME, not TIMESTAMP: chainstar's times are naive UTC.
    return {"BIGINT": "INT64", "VARCHAR": "STRING", "BOOLEAN": "BOOL", "TIMESTAMP": "DATETIME"}[
        sql_type
    ]


class SnowflakeWarehouse(RemoteWarehouse):
    """Snowflake with snowflake-connector-python, the driver dbt-snowflake uses.

    Identifiers are upper case, as Snowflake folds the unquoted names dbt writes.
    """

    dialect = "snowflake"
    transactional = True

    def _connect(self) -> None:
        try:
            import snowflake.connector
        except ImportError as error:
            raise _missing(self.target, error) from None
        target: SnowflakeTarget = self.target  # type: ignore[assignment]
        options: dict[str, Any] = {
            "account": target.account,
            "user": target.user,
            "database": target.database,
            "warehouse": target.warehouse,
            "role": target.role,
            # Server-side binding of ? placeholders.
            "paramstyle": "qmark",
            "application": "chainstar",
        }
        if target.authenticator:
            options["authenticator"] = target.authenticator
        elif target.private_key_path:
            options["private_key_file"] = target.private_key_path
            if target.private_key_passphrase:
                options["private_key_file_pwd"] = target.private_key_passphrase
        else:
            options["password"] = target.password
        self.connection = snowflake.connector.connect(
            **{key: value for key, value in options.items() if value is not None}
        )

    def close(self) -> None:
        connection = getattr(self, "connection", None)
        if connection is not None:
            connection.close()
        super().close()

    def quote(self, name: str) -> str:
        return '"' + name.upper().replace('"', '""') + '"'

    def table(self, schema: str, name: str) -> str:
        return f"{self.quote(self.target.database)}.{self.quote(schema)}.{self.quote(name)}"

    def has_table(self, schema: str, name: str) -> bool:
        rows = self.query(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema.upper(), name.upper()],
        )
        return bool(rows and rows[0][0])

    def _execute(self, sql: str, params: Sequence[Any]):
        values = []
        for value in _plain(params):
            # qmark binding needs the Snowflake type of a datetime.
            values.append(("TIMESTAMP_NTZ", value) if isinstance(value, datetime) else value)
        cursor = self.connection.cursor()
        cursor.execute(sql, values or None)
        return cursor

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        with self._execute(sql, params) as cursor:
            if cursor.description is None:
                return []
            return [tuple(row) for row in cursor.fetchall()]

    def create_schema(self, schema: str) -> None:
        self.query(
            f"CREATE SCHEMA IF NOT EXISTS {self.quote(self.target.database)}.{self.quote(schema)}"
        )

    def create_table(self, schema: str, name: str, columns: Sequence[tuple[str, str]]) -> None:
        ddl = ", ".join(f"{self.quote(c)} {_snowflake_type(t)}" for c, t in columns)
        self.query(f"CREATE TABLE IF NOT EXISTS {self.table(schema, name)} ({ddl})")

    def delete(self, table: str, where: str, params: Sequence[Any]) -> int:
        with self._execute(f"DELETE FROM {table} WHERE {where}", params) as cursor:
            return int(cursor.rowcount or 0)

    def _upload(self, schema: str, name: str, path: Path) -> None:
        stage = f"@{self.quote(self.target.database)}.{self.quote(schema)}.%{self.quote(name)}"
        self.query(f"PUT 'file://{path}' {stage} AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        # FORCE: COPY skips a file name it loaded before; every upload's name is new,
        # and FORCE makes sure a repeated load is never silently skipped.
        self.query(
            f"COPY INTO {self.table(schema, name)} FROM {stage} FILES = ('{path.name}') "
            "FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            "FORCE = TRUE PURGE = TRUE"
        )

    def _fetch(self, sql: str, params: Sequence[Any]) -> str:
        with self._execute(sql, params) as cursor:
            table = cursor.fetch_arrow_all(force_return_table=True)
        name = f"fetched_{uuid.uuid4().hex}"
        self.local.register(name, table)
        return _ident(name)

    def begin(self) -> None:
        self.query("BEGIN")

    def commit(self) -> None:
        self.query("COMMIT")

    def rollback(self) -> None:
        self.query("ROLLBACK")


def _snowflake_type(sql_type: str) -> str:
    sql_type = sql_type.upper()
    if match := _DECIMAL.match(sql_type):
        return f"NUMBER({match[1]}, {match[2]})"
    return {
        "BIGINT": "BIGINT",
        "VARCHAR": "VARCHAR",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP_NTZ",
    }[sql_type]
