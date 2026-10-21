"""Load the fixture Parquet files into DuckDB and run the real dbt project on them."""

from pathlib import Path

import duckdb

from chainstar.chains import get_chain
from chainstar.datasets import ERC20_METADATA, parquet_files
from chainstar.load import load_raw
from chainstar.model import DBT_PROJECT_DIR, build_models
from chainstar.targets import DuckDBTarget
from e2e_checks import (
    ALL_CHAINS,
    USDT,
    check_all_chains,
    check_star_schema,
    fetch,
    load_and_build,
)


def test_build_star_schema_on_duckdb(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_and_build(target, data_dir, tmp_path / "dbt")
    check_star_schema(target)


def test_rebuild_is_idempotent(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_and_build(target, data_dir, tmp_path / "dbt")
    load_and_build(target, data_dir, tmp_path / "dbt")
    check_star_schema(target)


def test_token_without_metadata_keeps_its_transfers(data_dir: Path, tmp_path: Path):
    # Simulate a contract that answered none of name/symbol/decimals.
    metadata_file = parquet_files(data_dir, "ethereum", ERC20_METADATA)[0]
    with duckdb.connect() as connection:
        connection.execute(
            f"COPY (SELECT * FROM read_parquet('{metadata_file}') WHERE erc20 <> '{USDT}') "
            f"TO '{metadata_file}.tmp' (FORMAT parquet)"
        )
    Path(f"{metadata_file}.tmp").replace(metadata_file)

    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_and_build(target, data_dir, tmp_path / "dbt")

    token = fetch(
        target,
        f"SELECT has_metadata, decimals FROM {{schema}}.dim_tokens WHERE token_address = '{USDT}'",
    )
    assert token == [(False, None)]
    amounts = fetch(
        target,
        "SELECT count(*), count(amount) FROM {schema}.fct_transfers "
        f"WHERE token_address = '{USDT}'",
    )
    transfers, with_amount = amounts[0]
    assert transfers > 0
    assert with_amount == 0


def test_two_chains_share_one_warehouse(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_raw(target, data_dir, get_chain("base"))
    load_and_build(target, data_dir, tmp_path / "dbt")  # loads Ethereum, then builds

    per_chain = fetch(
        target,
        "SELECT chain, count(*) FROM {schema}.fct_transfers GROUP BY chain ORDER BY chain",
    )
    assert per_chain == [("base", 62), ("ethereum", 562)]
    # OP-stack deposit transactions (type 126) pay no L2 execution fee.
    deposits = fetch(
        target,
        "SELECT count(*), sum(execution_fee_wei) FROM {schema}.fct_fees "
        "WHERE chain = 'base' AND transaction_type = 126",
    )
    assert deposits[0][0] > 0
    assert deposits[0][1] == 0
    # A rebuild after reloading Base alone keeps both chains.
    load_raw(target, data_dir, get_chain("base"))
    assert build_models(target, tmp_path / "dbt").failures == []
    assert fetch(target, "SELECT count(*) FROM {schema}.fct_fees") == [(100,)]


def test_four_chains_share_one_warehouse(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_and_build(target, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    check_all_chains(target)
    # Reloading one chain and rebuilding leaves the others as they were.
    load_and_build(target, data_dir, tmp_path / "dbt", chains=("solana",))
    check_all_chains(target)


def test_relative_work_dir_stays_out_of_the_package(data_dir: Path, tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = DuckDBTarget(Path("warehouse.duckdb"))
    load_raw(target, data_dir, get_chain("ethereum"))
    assert build_models(target, Path("data/.dbt")).failures == []
    assert (tmp_path / "data" / ".dbt" / "target" / "manifest.json").exists()
    assert not (DBT_PROJECT_DIR / "data").exists()


def test_wallet_shaped_data_builds(data_dir: Path, tmp_path: Path):
    # `chainstar wallet` on an EVM chain saves transfers and their blocks, but no
    # transactions; the star schema must still build and pass its tests.
    for path in (data_dir / "base" / "transactions").glob("*.parquet"):
        path.unlink()
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_raw(target, data_dir, get_chain("base"))
    assert build_models(target, tmp_path / "dbt").failures == []
    assert fetch(target, "SELECT count(*) FROM {schema}.fct_transfers") == [(62,)]
    assert fetch(target, "SELECT count(*) FROM {schema}.fct_fees") == [(0,)]
