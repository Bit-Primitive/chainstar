"""The load stage: copy extracted Parquet files into raw tables in the target warehouse.

Both targets are written through DuckDB: a DuckDB file directly, and Postgres
through DuckDB's postgres extension. One code path means the raw tables have the
same columns and types in either warehouse.

`chainstar run` replaces the chain's rows with everything currently in its data
directory. That makes loads idempotent, lets Ethereum and Base share tables, and
lets ranges accumulate across runs. `chainstar sync` instead replaces only the
blocks it just extracted (load_range), inside a transaction that also records
its progress. Overlapping chunk files are de-duplicated either way.

EVM rows are told apart by chain id; Bitcoin and Solana rows, which have none,
by chain name.
"""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.chains import EVM, Chain
from chainstar.config import BlockRange
from chainstar.datasets import (
    ALL_DATASETS,
    BLOCK_TIME_COLUMN,
    ERC20_METADATA,
    Dataset,
    blocks_dataset,
    coverage_dataset,
    datasets_for,
    parquet_files,
    ranged_datasets_for,
)
from chainstar.targets import DuckDBTarget, PostgresTarget, Target

RAW_SCHEMA = "chainstar_raw"
# cryo's chunk file names end in <first>_to_<last>.parquet, both ends inclusive,
# after an optional label: base__erc20_transfers__from__52896690_to_52896699.parquet
_CHUNK_SPAN = re.compile(r"__(\d+)_to_(\d+)\.parquet$")


class LoadError(RuntimeError):
    pass


@dataclass(frozen=True)
class Warehouse:
    """A DuckDB connection plus the catalog name under which the target is reachable."""

    connection: duckdb.DuckDBPyConnection
    catalog: str
    postgres: bool = False

    def table(self, schema: str, name: str) -> str:
        return f"{_ident(self.catalog)}.{_ident(schema)}.{_ident(name)}"

    def has_table(self, schema: str, name: str) -> bool:
        row = self.connection.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            [self.catalog, schema, name],
        ).fetchone()
        return bool(row and row[0])


@contextmanager
def attach(target: Target, *, read_only: bool = False) -> Iterator[Warehouse]:
    """Open the target. read_only applies to DuckDB files, so a reader such as
    `chainstar status` never takes the file's write lock."""
    if isinstance(target, DuckDBTarget):
        target.path.parent.mkdir(parents=True, exist_ok=True)
        # Connect to the file itself rather than ATTACH it: dbt-duckdb keeps the file
        # open in this process after a build, and DuckDB refuses to attach a file
        # twice but will share an already-open database with a plain connect.
        connection = duckdb.connect(str(target.path), read_only=read_only)
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
        yield Warehouse(connection, catalog, postgres=isinstance(target, PostgresTarget))
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
        prepare_raw(warehouse)
        for dataset in datasets_for(chain):
            files = parquet_files(data_dir, chain.name, dataset)
            counts[dataset.name] = _load_dataset(warehouse, dataset, files, chain)
    return counts


def prepare_raw(warehouse: Warehouse) -> None:
    """Create the raw schema and every raw table.

    Every raw table exists after any load, so the dbt sources for chains not
    loaded yet resolve to empty tables.
    """
    warehouse.connection.execute(
        f"CREATE SCHEMA IF NOT EXISTS {_ident(warehouse.catalog)}.{_ident(RAW_SCHEMA)}"
    )
    for dataset in ALL_DATASETS:
        _create_table(warehouse, dataset)


def index_raw_tables(warehouse: Warehouse) -> None:
    """On Postgres, index each raw table by chain and block.

    A sync reads, replaces and rolls back block ranges; without these indexes each
    of those would scan the whole table. DuckDB needs none: it skips row groups by
    their min/max statistics, and an index there would slow every insert.
    """
    if not warehouse.postgres:
        return
    for dataset in ALL_DATASETS:
        column = "chain_id" if dataset.family == EVM else "chain"
        statement = (
            f"CREATE INDEX IF NOT EXISTS {_ident(f'{dataset.name}_chain_block')} "
            f"ON {_ident(RAW_SCHEMA)}.{_ident(dataset.name)} "
            f"({column}, {_ident(dataset.block_column)})"
        )
        warehouse.connection.execute("CALL postgres_execute(?, ?)", [warehouse.catalog, statement])


@contextmanager
def transaction(warehouse: Warehouse) -> Iterator[None]:
    """Commit everything in the block together, or nothing (also on Ctrl-C)."""
    warehouse.connection.execute("BEGIN TRANSACTION")
    try:
        yield
    except BaseException:
        warehouse.connection.execute("ROLLBACK")
        raise
    warehouse.connection.execute("COMMIT")


def chunk_span(path: Path) -> tuple[int, int] | None:
    """The first and last block a chunk file covers, from its name."""
    match = _CHUNK_SPAN.search(path.name)
    return (int(match[1]), int(match[2])) if match else None


def overlapping_files(
    data_dir: Path, chain: Chain, dataset: Dataset, first: int, last: int | None = None
) -> list[Path]:
    """Chunk files with any block in [first, last] (last None: no upper end).

    A file whose name carries no span is included; rows are filtered by block anyway.
    """
    files = []
    for path in parquet_files(data_dir, chain.name, dataset):
        span = chunk_span(path)
        if span is None or (span[1] >= first and (last is None or span[0] <= last)):
            files.append(path)
    return files


def load_range(
    warehouse: Warehouse, data_dir: Path, chain: Chain, blocks: BlockRange
) -> dict[str, int]:
    """Replace the chain's raw rows in `blocks` with what its chunk files hold there.

    Runs inside the caller's transaction, so the caller can record its progress in
    the same commit. Only files that overlap the range are read. Token metadata
    is not tied to blocks and is small, so it is replaced in full. Returns the
    rows now in the range per dataset (metadata: all of the chain's).
    """
    connection = warehouse.connection
    column, value = _identity(chain)
    counts: dict[str, int] = {}
    for dataset in ranged_datasets_for(chain):
        table = warehouse.table(RAW_SCHEMA, dataset.name)
        block = _ident(dataset.block_column)
        connection.execute(
            f"DELETE FROM {table} WHERE {_ident(column)} = ? AND {block} >= ? AND {block} < ?",
            [value, blocks.start, blocks.end],
        )
        files = overlapping_files(data_dir, chain, dataset, blocks.start, blocks.end - 1)
        counts[dataset.name] = 0
        if files:
            _check_identity(connection, dataset, files, chain)
            row = connection.execute(
                f"INSERT INTO {table} {_select_distinct(dataset)} "
                f"WHERE {block} >= ? AND {block} < ?",
                [[str(f) for f in files], blocks.start, blocks.end],
            ).fetchone()
            counts[dataset.name] = int(row[0]) if row else 0
    if chain.family == EVM:
        table = warehouse.table(RAW_SCHEMA, ERC20_METADATA.name)
        files = parquet_files(data_dir, chain.name, ERC20_METADATA)
        connection.execute(f"DELETE FROM {table} WHERE {_ident(column)} = ?", [value])
        counts[ERC20_METADATA.name] = 0
        if files:
            _check_identity(connection, ERC20_METADATA, files, chain)
            row = connection.execute(
                f"INSERT INTO {table} {_select_distinct(ERC20_METADATA)}",
                [[str(f) for f in files]],
            ).fetchone()
            counts[ERC20_METADATA.name] = int(row[0]) if row else 0
    return counts


def delete_from_block(warehouse: Warehouse, chain: Chain, first_block: int) -> int:
    """Delete the chain's raw rows at and above `first_block`; return how many.

    Token metadata stays: it belongs to no block and still describes the tokens.
    """
    column, value = _identity(chain)
    deleted = 0
    for dataset in ranged_datasets_for(chain):
        row = warehouse.connection.execute(
            f"DELETE FROM {warehouse.table(RAW_SCHEMA, dataset.name)} "
            f"WHERE {_ident(column)} = ? AND {_ident(dataset.block_column)} >= ?",
            [value, first_block],
        ).fetchone()
        deleted += int(row[0]) if row else 0
    return deleted


@dataclass(frozen=True)
class StoredBlock:
    number: int
    hash: str
    # Unix seconds; None when the node gave no time (Solana sometimes).
    time: int | None


def stored_blocks(
    warehouse: Warehouse, chain: Chain, at_or_below: int, limit: int
) -> list[StoredBlock]:
    """The chain's newest raw blocks at or below a block, newest first."""
    dataset = blocks_dataset(chain)
    column, value = _identity(chain)
    number = _ident(dataset.block_column)
    rows = warehouse.connection.execute(
        f"SELECT {number}, block_hash, {_ident(BLOCK_TIME_COLUMN[chain.family])} "
        f"FROM {warehouse.table(RAW_SCHEMA, dataset.name)} "
        f"WHERE {_ident(column)} = ? AND {number} <= ? ORDER BY {number} DESC LIMIT ?",
        [value, at_or_below, limit],
    ).fetchall()
    return [StoredBlock(int(n), str(h), None if t is None else int(t)) for n, h, t in rows]


def held_ranges(data_dir: Path, chain: Chain) -> list[tuple[int, int]]:
    """The blocks (Solana: slots) a load puts in the warehouse, as merged inclusive spans.

    A load takes every chunk on disk for the chain, not only the range just
    extracted, so earlier runs' blocks are included; this says which those are.
    """
    spans = sorted(
        span
        for path in parquet_files(data_dir, chain.name, coverage_dataset(chain))
        if (span := chunk_span(path))
    )
    merged: list[tuple[int, int]] = []
    for first, last in spans:
        if merged and first <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], last))
        else:
            merged.append((first, last))
    return merged


def _create_table(warehouse: Warehouse, dataset: Dataset) -> None:
    column_ddl = ", ".join(f"{_ident(name)} {sql_type}" for name, sql_type in dataset.columns)
    warehouse.connection.execute(
        f"CREATE TABLE IF NOT EXISTS {warehouse.table(RAW_SCHEMA, dataset.name)} ({column_ddl})"
    )


def _identity(chain: Chain) -> tuple[str, int | str]:
    """The raw column and value that mark a row as belonging to `chain`."""
    if chain.chain_id is not None:
        return "chain_id", chain.chain_id
    return "chain", chain.name


def _load_dataset(warehouse: Warehouse, dataset: Dataset, files: list[Path], chain: Chain) -> int:
    connection = warehouse.connection
    table = warehouse.table(RAW_SCHEMA, dataset.name)
    column, value = _identity(chain)
    if files:
        _check_identity(connection, dataset, files, chain)

    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DELETE FROM {table} WHERE {_ident(column)} = ?", [value])
        if files:
            connection.execute(
                f"INSERT INTO {table} {_select_distinct(dataset)}", [[str(f) for f in files]]
            )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    row = connection.execute(
        f"SELECT count(*) FROM {table} WHERE {_ident(column)} = ?", [value]
    ).fetchone()
    return int(row[0]) if row else 0


def _select_distinct(dataset: Dataset) -> str:
    """SELECT DISTINCT of a dataset's columns from read_parquet(?), the file list.

    Explicit casts pin the raw schema: if cryo ever changes a column, the load
    fails loudly here instead of drifting downstream.
    """
    select_list = ", ".join(
        f"CAST({_ident(name)} AS {sql_type})" for name, sql_type in dataset.columns
    )
    return f"SELECT DISTINCT {select_list} FROM read_parquet(?)"


def _check_identity(
    connection: duckdb.DuckDBPyConnection, dataset: Dataset, files: list[Path], chain: Chain
) -> None:
    """Refuse to load files from a different chain than the directory claims.

    That happens if a data directory is reused with the wrong RPC URL, or files are
    copied between chain directories, and loading them would label one chain's
    data as another's.
    """
    column, value = _identity(chain)
    found = {
        row[0]
        for row in connection.execute(
            f"SELECT DISTINCT {_ident(column)} FROM read_parquet(?)", [[str(f) for f in files]]
        ).fetchall()
    }
    unexpected = found - {value}
    if unexpected:
        label = "chain id(s)" if column == "chain_id" else "chain(s)"
        raise LoadError(
            f"{dataset.name} files for {chain.name} contain {label} "
            f"{sorted(unexpected)}, expected {value}"
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
