import shutil
from pathlib import Path

import duckdb
import pytest

from chainstar.chains import get_chain
from chainstar.config import BlockRange
from chainstar.datasets import BLOCKS, SOLANA_BLOCKS, TRANSACTIONS, dataset_dir, parquet_files
from chainstar.load import (
    LoadError,
    attach,
    delete_from_block,
    load_range,
    load_raw,
    overlapping_files,
    prepare_raw,
    stored_blocks,
    transaction,
)
from chainstar.targets import DuckDBTarget, parse_target

ETHEREUM = get_chain("ethereum")
BASE = get_chain("base")


def _raw_count(path: Path, table: str) -> int:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(f"SELECT count(*) FROM chainstar_raw.{table}").fetchone()[0]


def test_load_counts_and_types(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    counts = load_raw(target, data_dir, ETHEREUM)
    assert counts == {"blocks": 2, "transactions": 50, "erc20_transfers": 562, "erc20_metadata": 11}
    with duckdb.connect(str(target.path), read_only=True) as connection:
        types = dict(
            connection.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'chainstar_raw' AND table_name = 'transactions'"
            ).fetchall()
        )
    assert types["gas_price"] == "DECIMAL(20,0)"
    assert types["value_string"] == "VARCHAR"


def test_reload_is_idempotent_and_dedupes_overlapping_files(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    load_raw(target, data_dir, ETHEREUM)
    # An overlapping re-extraction leaves the same rows in a second file.
    original = parquet_files(data_dir, "ethereum", TRANSACTIONS)[0]
    shutil.copy(original, original.with_name("overlap.parquet"))
    counts = load_raw(target, data_dir, ETHEREUM)
    assert counts["transactions"] == 50
    assert _raw_count(target.path, "transactions") == 50


def test_chains_share_tables_without_clobbering(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    load_raw(target, data_dir, ETHEREUM)
    base_counts = load_raw(target, data_dir, BASE)
    assert base_counts["erc20_transfers"] == 62
    # Reloading one chain must leave the other chain's rows alone.
    load_raw(target, data_dir, ETHEREUM)
    assert _raw_count(target.path, "erc20_transfers") == 562 + 62


def test_chain_without_files_gets_empty_tables(data_dir: Path, tmp_path: Path):
    shutil.rmtree(data_dir / "base")
    counts = load_raw(DuckDBTarget(tmp_path / "w.duckdb"), data_dir, BASE)
    assert set(counts.values()) == {0}


def test_refuses_files_from_another_chain(data_dir: Path, tmp_path: Path):
    wrong = dataset_dir(data_dir, "base", BLOCKS)
    shutil.copy(parquet_files(data_dir, "ethereum", BLOCKS)[0], wrong / "blocks.parquet")
    with pytest.raises(LoadError, match="expected 8453"):
        load_raw(DuckDBTarget(tmp_path / "w.duckdb"), data_dir, BASE)


def test_postgres_connection_error_hides_password():
    # Port 1 on localhost refuses connections; DuckDB's raw error would include the DSN.
    target = parse_target("postgres:postgresql://ana:hunter2@127.0.0.1:1/dw")
    with pytest.raises(LoadError) as raised, attach(target):
        pass
    assert "hunter2" not in str(raised.value)
    assert "ana@127.0.0.1:1/dw" in str(raised.value)


def test_json_rpc_chains_load_by_chain_name(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    assert load_raw(target, data_dir, get_chain("bitcoin")) == {
        "bitcoin_blocks": 1,
        "bitcoin_transactions": 6,
        "bitcoin_inputs": 9,
        "bitcoin_outputs": 13,
    }
    assert load_raw(target, data_dir, get_chain("solana")) == {
        "solana_blocks": 2,
        "solana_transactions": 12,
        "solana_transfers": 20,
    }
    # Every raw table exists after any load, so dbt sources always resolve.
    assert _raw_count(target.path, "erc20_transfers") == 0


def test_refuses_another_chains_files_by_name(data_dir: Path, tmp_path: Path):
    wrong = dataset_dir(data_dir, "solana", SOLANA_BLOCKS)
    shutil.copy(parquet_files(data_dir, "solana", SOLANA_BLOCKS)[0], wrong / "copy.parquet")
    with duckdb.connect() as connection:
        connection.execute(
            f"COPY (SELECT * REPLACE ('bitcoin' AS chain) FROM '{wrong / 'copy.parquet'}') "
            f"TO '{wrong / 'mislabelled.parquet'}' (FORMAT parquet)"
        )
    (wrong / "copy.parquet").unlink()
    with pytest.raises(LoadError, match="expected solana"):
        load_raw(DuckDBTarget(tmp_path / "w.duckdb"), data_dir, get_chain("solana"))


def _count(target: DuckDBTarget, table: str, where: str = "true") -> int:
    with duckdb.connect(str(target.path), read_only=True) as connection:
        return connection.execute(
            f"SELECT count(*) FROM chainstar_raw.{table} WHERE {where}"
        ).fetchone()[0]


def test_load_range_replaces_only_its_blocks(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    with attach(target) as warehouse:
        prepare_raw(warehouse)
        with transaction(warehouse):
            first = load_range(warehouse, data_dir, ETHEREUM, BlockRange(26239738, 26239739))
        assert first["erc20_transfers"] == 541
        assert first["erc20_metadata"] == 11
        with transaction(warehouse):
            second = load_range(warehouse, data_dir, ETHEREUM, BlockRange(26239739, 26239740))
            # Loading the same range again replaces rather than adds.
            load_range(warehouse, data_dir, ETHEREUM, BlockRange(26239739, 26239740))
        assert second["erc20_transfers"] == 21
    assert _count(target, "erc20_transfers") == 562
    assert _count(target, "erc20_metadata") == 11


def test_overlapping_files_by_name(tmp_path: Path):
    folder = tmp_path / "ethereum" / "blocks"
    folder.mkdir(parents=True)
    for name in (
        "ethereum__blocks__00000100_to_00000109.parquet",
        "ethereum__blocks__00000110_to_00000119.parquet",
        "ethereum__blocks__abc123__00000120_to_00000125.parquet",
        "no-span.parquet",
    ):
        (folder / name).touch()

    def found(first: int, last: int | None = None) -> int:
        return len(overlapping_files(tmp_path, ETHEREUM, BLOCKS, first, last))

    assert found(105, 112) == 3  # two chunks and the file without a span
    assert found(119) == 3
    assert found(126) == 1


def test_delete_from_block_and_stored_blocks(data_dir: Path, tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    load_raw(target, data_dir, ETHEREUM)
    with attach(target) as warehouse:
        newest = stored_blocks(warehouse, ETHEREUM, 26239739, 5)
        assert [b.number for b in newest] == [26239739, 26239738]
        assert newest[0].hash.startswith("0x") and newest[0].time > 1_700_000_000
        deleted = delete_from_block(warehouse, ETHEREUM, 26239739)
    assert deleted == 21 + 25 + 1  # transfers, transactions and the block
    assert _count(target, "erc20_metadata") == 11  # metadata belongs to no block
    assert _count(target, "blocks") == 1
