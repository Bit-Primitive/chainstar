import shutil
from pathlib import Path

import duckdb
import pytest

from chainstar.chains import get_chain
from chainstar.datasets import BLOCKS, TRANSACTIONS, dataset_dir, parquet_files
from chainstar.load import LoadError, attach, load_raw
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
