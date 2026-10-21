"""`chainstar export`: star-schema tables to CSV, Excel or Parquet files.

Exports run through DuckDB, as loads do, so one query works on a DuckDB file,
Postgres and MotherDuck. A Parquet lake is read from its published files. On
ClickHouse, BigQuery and Snowflake the warehouse applies the filters and DuckDB
sorts and writes the rows it returns. Excel files are written by DuckDB's `excel`
extension, which DuckDB downloads from its extension repository the first time,
like the `postgres` extension; no extra Python package is needed.

`amount_raw` stays text in every format, so 256-bit token amounts keep every
digit; `amount` is a number for sums and charts. `amount_usd` is the amount at
the token's hourly on-chain USD price, with the price, how it was derived, its
confidence and the hour it is for beside it, so each value can be checked.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.load import attach_reader
from chainstar.model import MART_SCHEMA, MART_TABLES
from chainstar.targets import DuckDBTarget, Target

FORMATS = ("csv", "xlsx", "parquet")
# The curated view: one row per transfer with its token's symbol and name.
TRANSFERS_VIEW = "transfers"
TABLES = (TRANSFERS_VIEW, *MART_TABLES)
# An Excel sheet holds 1,048,576 rows, one of which is the header.
EXCEL_MAX_ROWS = 1_048_575
_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


class ExportError(ValueError):
    pass


@dataclass(frozen=True)
class Filters:
    """Columns that --wallet and --token match in each table; None when it does not apply."""

    wallet: tuple[str, ...] | None
    token: str | tuple[str, ...] | None
    chain: bool = True


TABLE_FILTERS: dict[str, Filters] = {
    TRANSFERS_VIEW: Filters(wallet=("from_address", "to_address"), token="token_address"),
    "fct_transfers": Filters(wallet=("from_address", "to_address"), token="token_address"),
    "fct_fees": Filters(wallet=("from_address", "to_address"), token=None),
    "fct_swaps": Filters(
        wallet=("trader", "sender", "recipient"), token=("token_in_address", "token_out_address")
    ),
    "fct_token_prices": Filters(wallet=None, token="token_address"),
    "dim_wallets": Filters(wallet=("address",), token=None),
    "dim_tokens": Filters(wallet=None, token="token_address"),
    "dim_contracts": Filters(wallet=None, token="contract_address"),
    "dim_pools": Filters(wallet=None, token=("token0_address", "token1_address")),
    "dim_dates": Filters(wallet=None, token=None, chain=False),
}


@dataclass(frozen=True)
class ExportRequest:
    table: str
    output: Path
    format: str
    chain: str | None = None
    wallet: str | None = None
    token: str | None = None
    limit: int | None = None


def normalize_address(address: str) -> str:
    """EVM addresses are stored lowercase, so a checksummed one is lowercased.

    Bitcoin and Solana addresses are case-sensitive and kept as typed.
    """
    address = address.strip()
    return address.lower() if _EVM_ADDRESS.match(address) else address


def resolve_format(output: Path | None, format_name: str | None, table: str) -> tuple[Path, str]:
    """Pick the format from --format or the file extension, and a default file name."""
    suffix = output.suffix.lower().lstrip(".") if output else ""
    chosen = (format_name or suffix or "csv").lower()
    if chosen not in FORMATS:
        raise ExportError(f"unknown format {chosen!r}; choose one of: {', '.join(FORMATS)}")
    if output is None:
        return Path(f"{table}.{chosen}"), chosen
    if suffix and suffix != chosen:
        raise ExportError(f"--format {chosen} does not match the file name {output.name}")
    return output, chosen


def export(target: Target, request: ExportRequest) -> int:
    """Write the export file and return its row count."""
    if request.table not in TABLES:
        raise ExportError(f"unknown table {request.table!r}; choose one of: {', '.join(TABLES)}")
    if isinstance(target, DuckDBTarget) and not target.path.exists():
        # Connecting would create an empty file; say what to do instead.
        raise ExportError(
            f"there is no warehouse at {target.path}; run `chainstar quickstart` or "
            "`chainstar run` first, or point --target at your warehouse"
        )
    request.output.parent.mkdir(parents=True, exist_ok=True)
    with attach_reader(target, read_only=False) as warehouse:
        if warehouse.via_duckdb:
            connection = warehouse.connection
            query, params = build_query(warehouse.table, request)
        else:
            connection = warehouse.local
            query, params = _fetched_query(warehouse, request)
        try:
            if request.format == "xlsx":
                _check_excel_size(connection, query, params)
                connection.execute("INSTALL excel; LOAD excel;")
            options = {
                "csv": "FORMAT csv, HEADER true",
                "xlsx": "FORMAT xlsx, HEADER true",
                "parquet": "FORMAT parquet",
            }[request.format]
            connection.execute(
                f"COPY ({query}) TO {_quote(str(request.output))} ({options})", params
            )
            row = connection.execute(f"SELECT count(*) FROM ({query})", params).fetchone()
        except duckdb.CatalogException:
            raise ExportError(_no_tables(target)) from None
    return int(row[0]) if row else 0


def _no_tables(target: Target) -> str:
    return (
        f"the warehouse {target.describe()} has no star-schema tables yet; "
        "run `chainstar quickstart` or `chainstar run` first"
    )


@dataclass(frozen=True)
class QueryParts:
    # SELECT of the table's rows, as the warehouse runs it.
    source: str
    conditions: list[str]
    params: list
    # Applied by DuckDB: the columns to keep and the order.
    columns: str
    order: str
    limit: int | None


def build_query(table_name, request: ExportRequest) -> tuple[str, list]:
    """The SELECT for `request`, with its filters as parameters.

    `table_name(schema, name)` returns the quoted table reference for the target.
    """
    parts = query_parts(table_name, request)
    query = f"SELECT {parts.columns} FROM ({parts.source}) AS rows"
    if parts.conditions:
        query += " WHERE " + " AND ".join(parts.conditions)
    return query + _order_and_limit(parts), parts.params


def _order_and_limit(parts: QueryParts) -> str:
    clause = f" ORDER BY {parts.order}"
    if parts.limit is not None:
        clause += f" LIMIT {int(parts.limit)}"
    return clause


def query_parts(table_name, request: ExportRequest) -> QueryParts:
    filters = TABLE_FILTERS[request.table]
    if request.table == TRANSFERS_VIEW:
        transfers = table_name(MART_SCHEMA, "fct_transfers")
        tokens = table_name(MART_SCHEMA, "dim_tokens")
        source = (
            "SELECT t.block_timestamp, t.chain, t.block_number, t.transaction_hash, "
            "t.transfer_position, t.token_address, k.symbol AS token_symbol, "
            "k.name AS token_name, t.from_address, t.to_address, t.amount, t.amount_raw, "
            "t.amount_usd, t.usd_price, t.price_method, t.price_confidence, "
            "t.price_bucket_start, t.transaction_index "
            f"FROM {transfers} AS t LEFT JOIN {tokens} AS k USING (token_id)"
        )
        # transfer_position is text such as "12" or "3.1"; compare it as numbers.
        order = (
            "chain, block_number, transaction_index, "
            "list_transform(string_split(transfer_position, '.'), x -> CAST(x AS INTEGER))"
        )
        columns = "* EXCLUDE (transaction_index)"
    else:
        source = f"SELECT * FROM {table_name(MART_SCHEMA, request.table)}"
        order = "1"
        columns = "*"

    conditions: list[str] = []
    params: list = []
    if request.chain:
        if not filters.chain:
            raise ExportError(f"--chain does not apply to {request.table}")
        conditions.append("chain = ?")
        params.append(request.chain.strip().lower())
    if request.wallet:
        if not filters.wallet:
            raise ExportError(f"--wallet does not apply to {request.table}")
        conditions.append(_any_equals(filters.wallet))
        params += [normalize_address(request.wallet)] * len(filters.wallet)
    if request.token:
        if not filters.token:
            raise ExportError(f"--token does not apply to {request.table}")
        token_columns = (filters.token,) if isinstance(filters.token, str) else filters.token
        conditions.append(_any_equals(token_columns))
        params += [normalize_address(request.token)] * len(token_columns)
    if request.limit is not None and request.limit < 1:
        raise ExportError(f"--limit must be at least 1, got {request.limit}")
    return QueryParts(source, conditions, params, columns, order, request.limit)


def _any_equals(columns: tuple[str, ...]) -> str:
    """One parameter matched against several columns. Written as ORs: ClickHouse
    accepts `? IN (a, b)` only with constants on the right."""
    return "(" + " OR ".join(f"{column} = ?" for column in columns) + ")"


# The local table holding a remote warehouse's filtered rows.
FETCHED = "chainstar_export"


def _fetched_query(warehouse, request: ExportRequest) -> tuple[str, list]:
    """Filter in the warehouse, then order and pick columns in DuckDB.

    The order compares transfer positions as lists of numbers, which only DuckDB
    has a portable way to say, so the warehouse returns the matching rows and
    DuckDB sorts them.
    """
    parts = query_parts(warehouse.table, request)
    query = f"SELECT * FROM ({parts.source}) AS filtered"
    if parts.conditions:
        query += " WHERE " + " AND ".join(parts.conditions)
    if not warehouse.has_table(
        MART_SCHEMA, "fct_transfers" if request.table == TRANSFERS_VIEW else request.table
    ):
        raise ExportError(_no_tables(warehouse.target))
    warehouse.fetch_into(FETCHED, query, parts.params)
    return f"SELECT {parts.columns} FROM {FETCHED}{_order_and_limit(parts)}", []


def _check_excel_size(connection: duckdb.DuckDBPyConnection, query: str, params: list) -> None:
    row = connection.execute(f"SELECT count(*) FROM ({query})", params).fetchone()
    rows = int(row[0]) if row else 0
    if rows > EXCEL_MAX_ROWS:
        raise ExportError(
            f"{rows:,} rows do not fit in an Excel sheet (at most {EXCEL_MAX_ROWS:,} rows); "
            "narrow the export with --chain, --wallet, --token or --limit, "
            "or export to .csv or .parquet, which have no row limit"
        )


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
