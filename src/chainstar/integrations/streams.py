"""The streams the dlt and Airbyte plug-ins emit: the star-schema tables, their keys and columns.

Columns and types are as the bundled dbt project builds them in DuckDB, the
working warehouse the plug-ins use (see feed.py); a test builds the star schema
and checks these against it, so a change to the models cannot drift silently.
Every stream also carries two columns of its own:

- `_chainstar_generation`: the change number, per stream, of the run that last
  emitted the row; increasing, and the cursor of incremental loads;
- `_chainstar_deleted`: true on a row that left the star schema (after a
  reorganisation rollback, for example), so the destination deletes its key.
"""

from dataclasses import dataclass

GENERATION_COLUMN = "_chainstar_generation"
DELETED_COLUMN = "_chainstar_deleted"


@dataclass(frozen=True)
class Stream:
    name: str
    primary_key: str
    # (column, DuckDB type), in table order.
    columns: tuple[tuple[str, str], ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def column_type(self, column: str) -> str:
        return dict(self.columns)[column]


STREAM_LIST = (
    Stream(
        "fct_transfers",
        "transfer_id",
        (
            ("transfer_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("block_number", "BIGINT"),
            ("block_timestamp", "TIMESTAMP"),
            ("date_day", "DATE"),
            ("transaction_hash", "VARCHAR"),
            ("transaction_index", "BIGINT"),
            ("transfer_position", "VARCHAR"),
            ("log_index", "BIGINT"),
            ("token_id", "VARCHAR"),
            ("from_wallet_id", "VARCHAR"),
            ("to_wallet_id", "VARCHAR"),
            ("token_address", "VARCHAR"),
            ("from_address", "VARCHAR"),
            ("to_address", "VARCHAR"),
            ("input_address_count", "BIGINT"),
            ("from_token_account", "VARCHAR"),
            ("to_token_account", "VARCHAR"),
            ("script_type", "VARCHAR"),
            ("amount_raw", "VARCHAR"),
            ("amount", "DOUBLE"),
            ("amount_usd", "DOUBLE"),
            ("usd_price", "DOUBLE"),
            ("price_method", "VARCHAR"),
            ("price_confidence", "VARCHAR"),
            ("price_bucket_start", "TIMESTAMP"),
        ),
    ),
    Stream(
        "fct_fees",
        "fee_id",
        (
            ("fee_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("block_number", "BIGINT"),
            ("block_timestamp", "TIMESTAMP"),
            ("date_day", "DATE"),
            ("transaction_hash", "VARCHAR"),
            ("transaction_index", "BIGINT"),
            ("payer_wallet_id", "VARCHAR"),
            ("from_address", "VARCHAR"),
            ("to_address", "VARCHAR"),
            ("transaction_type", "BIGINT"),
            ("success", "BOOLEAN"),
            ("is_vote", "BOOLEAN"),
            ("gas_limit", "BIGINT"),
            ("gas_used", "BIGINT"),
            ("effective_gas_price_wei", "DECIMAL(20,0)"),
            ("base_fee_per_gas_wei", "DECIMAL(20,0)"),
            ("execution_fee_wei", "DECIMAL(38,0)"),
            ("fee_raw", "DECIMAL(38,0)"),
            ("native_symbol", "VARCHAR"),
            ("fee_native", "DOUBLE"),
            ("execution_fee_native", "DOUBLE"),
            ("fee_usd", "DOUBLE"),
            ("native_usd_price", "DOUBLE"),
            ("price_method", "VARCHAR"),
            ("price_confidence", "VARCHAR"),
            ("price_bucket_start", "TIMESTAMP"),
        ),
    ),
    Stream(
        "fct_swaps",
        "swap_id",
        (
            ("swap_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("block_number", "BIGINT"),
            ("block_timestamp", "TIMESTAMP"),
            ("date_day", "DATE"),
            ("transaction_hash", "VARCHAR"),
            ("transaction_index", "BIGINT"),
            ("swap_position", "VARCHAR"),
            ("log_index", "BIGINT"),
            ("venue", "VARCHAR"),
            ("swap_method", "VARCHAR"),
            ("pool_id", "VARCHAR"),
            ("pool_address", "VARCHAR"),
            ("pool_verified", "BOOLEAN"),
            ("trader", "VARCHAR"),
            ("sender", "VARCHAR"),
            ("recipient", "VARCHAR"),
            ("token_in_id", "VARCHAR"),
            ("token_out_id", "VARCHAR"),
            ("token_in_address", "VARCHAR"),
            ("token_out_address", "VARCHAR"),
            ("amount_in_raw", "VARCHAR"),
            ("amount_out_raw", "VARCHAR"),
            ("amount_in", "DOUBLE"),
            ("amount_out", "DOUBLE"),
            ("usd_value", "DOUBLE"),
            ("usd_value_side", "VARCHAR"),
            ("price_method", "VARCHAR"),
            ("price_confidence", "VARCHAR"),
            ("price_bucket_start", "TIMESTAMP"),
        ),
    ),
    Stream(
        "fct_token_prices",
        "price_id",
        (
            ("price_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("token_address", "VARCHAR"),
            ("token_id", "VARCHAR"),
            ("granularity", "VARCHAR"),
            ("bucket_start", "TIMESTAMP"),
            ("date_day", "DATE"),
            ("bucket_start_unix", "BIGINT"),
            ("usd_price", "DOUBLE"),
            ("observed_price", "DOUBLE"),
            ("price_low", "DOUBLE"),
            ("price_high", "DOUBLE"),
            ("price_method", "VARCHAR"),
            ("confidence", "VARCHAR"),
            ("observations", "BIGINT"),
            ("rejected_observations", "BIGINT"),
            ("volume_usd", "DOUBLE"),
            ("sources", "BIGINT"),
            ("last_block_number", "BIGINT"),
        ),
    ),
    Stream(
        "dim_wallets",
        "wallet_id",
        (
            ("wallet_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("address", "VARCHAR"),
            ("is_zero_address", "BOOLEAN"),
            ("first_seen_block", "BIGINT"),
            ("first_seen_at", "TIMESTAMP"),
            ("last_seen_block", "BIGINT"),
            ("last_seen_at", "TIMESTAMP"),
            ("transfers_out", "BIGINT"),
            ("transfers_in", "BIGINT"),
            ("transactions_sent", "BIGINT"),
        ),
    ),
    Stream(
        "dim_tokens",
        "token_id",
        (
            ("token_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("token_address", "VARCHAR"),
            ("token_standard", "VARCHAR"),
            ("name", "VARCHAR"),
            ("symbol", "VARCHAR"),
            ("decimals", "BIGINT"),
            ("metadata_block_number", "BIGINT"),
            ("has_metadata", "BOOLEAN"),
            ("first_seen_block", "BIGINT"),
            ("last_seen_block", "BIGINT"),
        ),
    ),
    Stream(
        "dim_contracts",
        "contract_id",
        (
            ("contract_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("contract_address", "VARCHAR"),
            ("contract_type", "VARCHAR"),
            ("first_seen_block", "BIGINT"),
            ("last_seen_block", "BIGINT"),
            ("transfer_events", "BIGINT"),
        ),
    ),
    Stream(
        "dim_pools",
        "pool_id",
        (
            ("pool_id", "VARCHAR"),
            ("chain", "VARCHAR"),
            ("pool_address", "VARCHAR"),
            ("venue", "VARCHAR"),
            ("event_family", "VARCHAR"),
            ("factory_address", "VARCHAR"),
            ("factory_confirms", "BOOLEAN"),
            ("is_verified", "BOOLEAN"),
            ("token0_address", "VARCHAR"),
            ("token1_address", "VARCHAR"),
            ("token0_id", "VARCHAR"),
            ("token1_id", "VARCHAR"),
            ("fee", "BIGINT"),
            ("tick_spacing", "BIGINT"),
            ("is_stable", "BOOLEAN"),
            ("metadata_block_number", "BIGINT"),
            ("first_seen_block", "BIGINT"),
            ("last_seen_block", "BIGINT"),
            ("swap_events", "BIGINT"),
        ),
    ),
    Stream(
        "dim_dates",
        "date_day",
        (
            ("date_day", "DATE"),
            ("year", "INTEGER"),
            ("quarter", "INTEGER"),
            ("month", "INTEGER"),
            ("day_of_month", "INTEGER"),
            ("iso_day_of_week", "INTEGER"),
            ("iso_week", "INTEGER"),
            ("is_weekend", "BOOLEAN"),
        ),
    ),
)

STREAMS: dict[str, Stream] = {stream.name: stream for stream in STREAM_LIST}


class UnknownStreamError(ValueError):
    pass


def select_streams(names) -> tuple[Stream, ...]:
    """The named streams in table order; all of them when `names` is empty or None."""
    if not names:
        return STREAM_LIST
    unknown = [name for name in names if name not in STREAMS]
    if unknown:
        raise UnknownStreamError(
            f"unknown table(s) {', '.join(map(repr, unknown))}; choose from: {', '.join(STREAMS)}"
        )
    return tuple(stream for stream in STREAM_LIST if stream.name in set(names))
