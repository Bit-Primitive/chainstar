"""Assertions shared by the DuckDB and Postgres end-to-end tests."""

from collections.abc import Sequence
from pathlib import Path

from chainstar.chains import SUPPORTED_CHAINS, get_chain
from chainstar.load import attach, load_raw
from chainstar.model import MART_SCHEMA, build_models
from chainstar.targets import Target

# A USDT transfer in the fixture slice: raw value 26240000 with USDT's 6 decimals
# is 26.24 USDT.
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
# Staging, intermediate and mart models in the bundled dbt project.
MODELS = 30


def load_and_build(
    target: Target, data_dir: Path, work_dir: Path, chains: Sequence[str] = ("ethereum",)
) -> None:
    for chain in chains:
        load_raw(target, data_dir, get_chain(chain))
    # As `chainstar run` does: the load replaced the chain's raw rows, so rebuild.
    result = build_models(target, work_dir, full_refresh=True)
    assert result.failures == [], result.failures
    assert result.models == MODELS
    assert result.tests_passed > 40


ALL_CHAINS = tuple(SUPPORTED_CHAINS)


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


def _per_chain(target: Target, table: str) -> dict[str, int]:
    return dict(fetch(target, f"SELECT chain, count(*) FROM {{schema}}.{table} GROUP BY chain"))


def check_all_chains(target: Target) -> None:
    """All four chains in one warehouse, with each chain's semantics intact."""
    assert _per_chain(target, "fct_transfers") == {
        "ethereum": 562,
        "base": 62,
        "bitcoin": 13,  # every output of the 6 fixture transactions
        "solana": 12,  # 20 transfer instructions, 8 of them in failed transactions
    }
    assert _per_chain(target, "fct_fees") == {
        "ethereum": 50,
        "base": 50,
        "bitcoin": 5,  # 6 transactions minus the coinbase
        "solana": 12,  # every transaction, failed and vote ones included
    }
    assert _per_chain(target, "dim_tokens") == {
        "ethereum": 11,
        "base": 12,
        "bitcoin": 1,
        "solana": 7,
    }
    assert _per_chain(target, "dim_contracts") == {"ethereum": 11, "base": 12}
    assert _per_chain(target, "dim_wallets") == {
        "ethereum": 581,
        "base": 85,
        "bitcoin": 16,
        "solana": 20,
    }
    assert fetch(target, "SELECT count(*) FROM {schema}.dim_dates") == [(1,)]

    # Keys carry the chain, so no two chains collide.
    for table, key in (("fct_transfers", "transfer_id"), ("dim_wallets", "wallet_id")):
        prefixed = fetch(
            target,
            f"SELECT count(*) FROM {{schema}}.{table} WHERE {key} NOT LIKE chain || ':%'",
        )
        assert prefixed == [(0,)], table

    # Native assets.
    natives = fetch(
        target,
        "SELECT chain, symbol, decimals, token_standard FROM {schema}.dim_tokens "
        "WHERE token_address = 'native' ORDER BY chain",
    )
    assert natives == [("bitcoin", "BTC", 8, "native"), ("solana", "SOL", 9, "native")]

    # Bitcoin fees are inputs minus outputs, exactly as the node reports them.
    bitcoin_fees = fetch(
        target,
        "SELECT sum(fee_raw), count(payer_wallet_id) FROM {schema}.fct_fees "
        "WHERE chain = 'bitcoin'",
    )
    assert bitcoin_fees == [(9900 + 4264 + 1605 + 1204 + 920, 4)]  # one tx has two funders
    # A sender is named only when one address funded the transaction; the coinbase
    # has none; OP_RETURN outputs have no recipient.
    bitcoin = fetch(
        target,
        "SELECT input_address_count, count(*), count(from_address), count(to_address) "
        "FROM {schema}.fct_transfers WHERE chain = 'bitcoin' "
        "GROUP BY input_address_count ORDER BY input_address_count",
    )
    assert bitcoin == [(0, 2, 0, 1), (1, 9, 9, 8), (2, 2, 0, 2)]
    op_return = fetch(
        target,
        "SELECT count(*), sum(amount) FROM {schema}.fct_transfers "
        "WHERE chain = 'bitcoin' AND script_type = 'nulldata' AND to_wallet_id IS NULL",
    )
    assert op_return == [(2, 0)]

    # Solana: failed transactions pay fees but move nothing; votes are flagged.
    solana_fees = fetch(
        target,
        "SELECT count(*) FILTER (WHERE NOT success), count(*) FILTER (WHERE is_vote), "
        "sum(fee_raw) FROM {schema}.fct_fees WHERE chain = 'solana'",
    )
    assert solana_fees == [(4, 2, 73124)]
    moved_by_failed = fetch(
        target,
        "SELECT count(*) FROM {schema}.fct_transfers t JOIN {schema}.fct_fees f "
        "ON f.chain = t.chain AND f.transaction_hash = t.transaction_hash "
        "WHERE t.chain = 'solana' AND NOT f.success",
    )
    assert moved_by_failed == [(0,)]
    spl = fetch(
        target,
        "SELECT count(*), count(name), count(symbol), count(decimals) FROM {schema}.dim_tokens "
        "WHERE chain = 'solana' AND token_standard <> 'native'",
    )
    assert spl == [(6, 0, 0, 6)]  # decimals from the data; no names or symbols on-chain
    usdc = fetch(
        target,
        "SELECT amount_raw, amount FROM {schema}.fct_transfers WHERE chain = 'solana' "
        "AND token_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'",
    )
    assert len(usdc) == 1
    assert usdc[0][0] == "12568210"
    assert abs(usdc[0][1] - 12.56821) < 1e-9
