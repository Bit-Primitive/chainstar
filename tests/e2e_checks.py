"""Assertions shared by the DuckDB and Postgres end-to-end tests."""

from pathlib import Path

from chainstar.chains import get_chain
from chainstar.load import attach, load_raw
from chainstar.model import MART_SCHEMA, build_models
from chainstar.targets import Target

# A USDT transfer in the fixture slice: raw value 26240000 with USDT's 6 decimals
# is 26.24 USDT.
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"


def load_and_build(target: Target, data_dir: Path, work_dir: Path) -> None:
    load_raw(target, data_dir, get_chain("ethereum"))
    result = build_models(target, work_dir)
    assert result.failures == [], result.failures
    assert result.models == 10
    assert result.tests_passed > 40


def fetch(target: Target, sql: str) -> list[tuple]:
    with attach(target) as warehouse:
        schema = f'"{warehouse.catalog}".{MART_SCHEMA}'
        return warehouse.connection.execute(sql.format(schema=schema)).fetchall()


def check_star_schema(target: Target) -> None:
    counts = dict(
        fetch(
            target,
            "SELECT 'fct_transfers', count(*) FROM {schema}.fct_transfers "
            "UNION ALL SELECT 'fct_fees', count(*) FROM {schema}.fct_fees "
            "UNION ALL SELECT 'dim_tokens', count(*) FROM {schema}.dim_tokens "
            "UNION ALL SELECT 'dim_contracts', count(*) FROM {schema}.dim_contracts "
            "UNION ALL SELECT 'dim_dates', count(*) FROM {schema}.dim_dates",
        )
    )
    assert counts == {
        "fct_transfers": 562,
        "fct_fees": 50,
        "dim_tokens": 11,
        "dim_contracts": 11,
        "dim_dates": 1,
    }

    usdt = fetch(
        target,
        "SELECT t.amount_raw, t.amount, d.symbol, d.decimals FROM {schema}.fct_transfers t "
        "JOIN {schema}.dim_tokens d ON d.token_id = t.token_id "
        f"WHERE t.token_address = '{USDT}' AND t.amount_raw = '26240000'",
    )
    assert len(usdt) == 1
    amount_raw, amount, symbol, decimals = usdt[0]
    assert (amount_raw, symbol, decimals) == ("26240000", "USDT", 6)
    assert abs(amount - 26.24) < 1e-9

    # Every fee equals gas_used x effective price exactly.
    mismatched = fetch(
        target,
        "SELECT count(*) FROM {schema}.fct_fees "
        "WHERE execution_fee_wei <> CAST(gas_used AS DECIMAL(38,0)) * effective_gas_price_wei",
    )
    assert mismatched == [(0,)]

    # Wallet counts are plain bigints. A bare sum() of integers is HUGEINT in
    # DuckDB, which Parquet and most BI tools can only represent as a float.
    types = fetch(
        target,
        "SELECT column_name, lower(data_type) FROM information_schema.columns "
        "WHERE table_schema = 'chainstar' AND table_name = 'dim_wallets' "
        "AND column_name IN ('transfers_out', 'transfers_in', 'transactions_sent')",
    )
    assert sorted(types) == [
        ("transactions_sent", "bigint"),
        ("transfers_in", "bigint"),
        ("transfers_out", "bigint"),
    ], types
