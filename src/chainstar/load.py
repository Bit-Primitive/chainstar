"""The load stage: copy cryo's Parquet files into raw tables in the target warehouse.

Both targets are written through DuckDB: a DuckDB file directly, and Postgres
through DuckDB's postgres extension. One code path means the raw tables have the
same columns and types in either warehouse.

Each run replaces the chain's rows with everything currently in its data
directory. That makes loads idempotent, lets Ethereum and Base share tables, and
lets ranges accumulate across runs. Overlapping ranges are de-duplicated here.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.chains import Chain
from chainstar.datasets import ALL_DATASETS, Dataset, parquet_files
from chainstar.targets import DuckDBTarget, PostgresTarget, Target

RAW_SCHEMA = "chainstar_raw"


class LoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class Warehouse:
    """A DuckDB connection plus the catalog name under which the target is reachable."""

    connection: duckdb.DuckDBPyConnection
    catalog: str

    def table(self, schema: str, name: str) -> str:
        return f"{_ident(self.catalog)}.{_ident(schema)}.{_ident(name)}"


@contextmanager
def attach(target: Target) -> Iterator[Warehouse]:
    if isinstance(target, DuckDBTarget):
        target.path.parent.mkdir(parents=True, exist_ok=True)
        # Connect to the file itself rather than ATTACH it: dbt-duckdb keeps the file
        # open in this process after a build, and DuckDB refuses to attach a file
        # twice but will share an already-open database with a plain connect.
        connection = duckdb.connect(str(target.path))
        catalog = connection.execute("SELECT current_database()").fetchone()[0]
    elif isinstance(target, PostgresTarget):
        connection = duckdb.connect()
        try:
            connection.execute("INSTALL postgres; LOAD postgres;")
            connection.execute(f"ATTACH {_quote(target.libpq_dsn())} AS warehouse (TYPE postgres)")
        except duckdb.Error as error:
            connection.close()
            raise LoadError(_without_password(str(error), target)) from None
        catalog = "warehouse"
    else:
        raise LoadError(f"unsupported target {target!r}")
    try:
        yield Warehouse(connection, catalog)
    except duckdb.Error as error:
        if isinstance(target, PostgresTarget):
            raise LoadError(_without_password(str(error), target)) from None
        raise
    finally:
        # Close before dbt runs: DuckDB allows only one writer per database file.
        connection.close()


def _without_password(message: str, target: PostgresTarget) -> str:
    """DuckDB quotes the whole connection string, password included, in its errors."""
    message = message.replace(target.libpq_dsn(), target.describe())
    if target.password:
        message = message.replace(target.password, "<redacted>")
    return message


def load_raw(target: Target, data_dir: Path, chain: Chain) -> dict[str, int]:
    """Load every dataset for `chain` and return the chain's row count per raw table."""
    counts: dict[str, int] = {}
    with attach(target) as warehouse:
        warehouse.connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {_ident(warehouse.catalog)}.{_ident(RAW_SCHEMA)}"
        )
        for dataset in ALL_DATASETS:
            files = parquet_files(data_dir, chain.name, dataset)
            counts[dataset.name] = _load_dataset(warehouse, dataset, files, chain)
    return counts


def _load_dataset(warehouse: Warehouse, dataset: Dataset, files: list[Path], chain: Chain) -> int:
    connection = warehouse.connection
    table = warehouse.table(RAW_SCHEMA, dataset.name)
    column_ddl = ", ".join(f"{_ident(name)} {sql_type}" for name, sql_type in dataset.columns)
    connection.execute(f"CREATE TABLE IF NOT EXISTS {table} ({column_ddl})")
    if files:
        _check_chain_id(connection, dataset, files, chain)

    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DELETE FROM {table} WHERE chain_id = ?", [chain.chain_id])
        if files:
            # Explicit casts pin the raw schema: if cryo ever changes a column,
            # the load fails loudly here instead of drifting downstream.
            select_list = ", ".join(
                f"CAST({_ident(name)} AS {sql_type})" for name, sql_type in dataset.columns
            )
            connection.execute(
                f"INSERT INTO {table} SELECT DISTINCT {select_list} FROM read_parquet(?)",
                [[str(f) for f in files]],
            )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    row = connection.execute(
        f"SELECT count(*) FROM {table} WHERE chain_id = ?", [chain.chain_id]
    ).fetchone()
    return int(row[0]) if row else 0


def _check_chain_id(
    connection: duckdb.DuckDBPyConnection, dataset: Dataset, files: list[Path], chain: Chain
) -> None:
    """Refuse to load files from a different chain than the directory claims.

    That happens if a data directory is reused with the wrong RPC URL, and loading
    it would label one chain's data as another's.
    """
    found = {
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT chain_id FROM read_parquet(?)", [[str(f) for f in files]]
        ).fetchall()
    }
    unexpected = found - {chain.chain_id}
    if unexpected:
        raise LoadError(
            f"{dataset.name} files for {chain.name} contain chain id(s) "
            f"{sorted(unexpected)}, expected {chain.chain_id}"
        )


def table_row_counts(target: Target, schema: str, tables: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with attach(target) as warehouse:
        for table in tables:
            row = warehouse.connection.execute(
                f"SELECT count(*) FROM {warehouse.table(schema, table)}"
            ).fetchone()
            counts[table] = int(row[0]) if row else 0
    return counts


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
