"""`chainstar export`: star-schema tables to CSV, Excel or Parquet files.

Exports run through DuckDB for both targets, as loads do, so one query works on a
DuckDB file and on Postgres. Excel files are written by DuckDB's `excel`
extension, which DuckDB downloads from its extension repository the first time,
like the `postgres` extension; no extra Python package is needed.

`amount_raw` stays text in every format, so 256-bit token amounts keep every
digit; `amount` is a number for sums and charts.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.load import attach
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
    token: str | None
    chain: bool = True


TABLE_FILTERS: dict[str, Filters] = {
    TRANSFERS_VIEW: Filters(wallet=("from_address", "to_address"), token="token_address"),
    "fct_transfers": Filters(wallet=("from_address", "to_address"), token="token_address"),
    "fct_fees": Filters(wallet=("from_address", "to_address"), token=None),
    "dim_wallets": Filters(wallet=("address",), token=None),
    "dim_tokens": Filters(wallet=None, token="token_address"),
    "dim_contracts": Filters(wallet=None, token="contract_address"),
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
    with attach(target) as warehouse:
        connection = warehouse.connection
        query, params = build_query(warehouse.table, request)
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
            raise ExportError(
                f"the warehouse {target.describe()} has no star-schema tables yet; "
                "run `chainstar quickstart` or `chainstar run` first"
            ) from None
    return int(row[0]) if row else 0


def build_query(table_name, request: ExportRequest) -> tuple[str, list]:
    """The SELECT for `request`, with its filters as parameters.

    `table_name(schema, name)` returns the quoted table reference for the target.
    """
    filters = TABLE_FILTERS[request.table]
    if request.table == TRANSFERS_VIEW:
        transfers = table_name(MART_SCHEMA, "fct_transfers")
        tokens = table_name(MART_SCHEMA, "dim_tokens")
        source = (
            "SELECT t.block_timestamp, t.chain, t.block_number, t.transaction_hash, "
            "t.transfer_position, t.token_address, k.symbol AS token_symbol, "
            "k.name AS token_name, t.from_address, t.to_address, t.amount, t.amount_raw, "
            "t.transaction_index "
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
        conditions.append("? IN (" + ", ".join(filters.wallet) + ")")
        params.append(normalize_address(request.wallet))
    if request.token:
        if not filters.token:
            raise ExportError(f"--token does not apply to {request.table}")
        conditions.append(f"{filters.token} = ?")
        params.append(normalize_address(request.token))

    query = f"SELECT {columns} FROM ({source}) AS rows"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += f" ORDER BY {order}"
    if request.limit is not None:
        if request.limit < 1:
            raise ExportError(f"--limit must be at least 1, got {request.limit}")
        query += f" LIMIT {int(request.limit)}"
    return query, params


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
