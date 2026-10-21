"""End-to-end against ClickHouse: the same star schema as DuckDB, row for row.

Skipped unless CHAINSTAR_TEST_CLICKHOUSE_URL points at a throwaway server's HTTP
interface, for example

    docker run -d -p 127.0.0.1:8123:8123 -e CLICKHOUSE_USER=chainstar \\
        -e CLICKHOUSE_PASSWORD=chainstar clickhouse/clickhouse-server:25.8
    CHAINSTAR_TEST_CLICKHOUSE_URL=http://chainstar:chainstar@127.0.0.1:8123 uv run pytest

The tests drop and recreate the chainstar, chainstar_raw, chainstar_staging and
chainstar_state databases there.
"""

import os
from pathlib import Path

import pytest

from chainstar import sync
from chainstar.export import ExportRequest, export
from chainstar.load import attach
from chainstar.state import read_states
from chainstar.targets import DuckDBTarget, parse_target
from e2e_checks import ALL_CHAINS, load_and_build
from fixture_chain import FIXTURE_BLOCKS
from sync_checks import (
    assert_matches_full_rebuild,
    make_syncer,
    reorg_matches_a_full_rebuild,
    snapshot,
    two_syncs_equal_a_full_rebuild,
)

URL = os.environ.get("CHAINSTAR_TEST_CLICKHOUSE_URL")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(not URL, reason="set CHAINSTAR_TEST_CLICKHOUSE_URL to run"),
]

DATABASES = ("chainstar", "chainstar_staging", "chainstar_raw", "chainstar_state")


def _empty_target():
    target = parse_target(f"clickhouse:{URL}")
    with attach(target) as warehouse:
        for database in DATABASES:
            warehouse.query(f"DROP DATABASE IF EXISTS {database}")
        # dbt-clickhouse checks once per process that its database exists.
        warehouse.query("CREATE DATABASE chainstar")
    return target


def test_star_schema_matches_duckdb(data_dir: Path, tmp_path: Path):
    target = _empty_target()
    load_and_build(target, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    reference = DuckDBTarget(tmp_path / "reference.duckdb")
    load_and_build(reference, data_dir, tmp_path / "dbt-reference", chains=ALL_CHAINS)
    built, expected = snapshot(target), snapshot(reference)
    for table, rows in expected.items():
        assert built[table] == rows, f"{table} differs from DuckDB's"
    # Same column types as DuckDB, once fetched: booleans stay booleans, counts bigints.
    with attach(target) as warehouse, attach(reference) as duck:
        for table in ("dim_wallets", "dim_tokens", "dim_contracts", "dim_dates", "fct_fees"):
            warehouse.fetch_into("t", f"SELECT * FROM {warehouse.table('chainstar', table)}", [])
            fetched = warehouse.local.execute("DESCRIBE t").fetchall()
            native = duck.connection.execute(f"DESCRIBE chainstar.{table}").fetchall()
            assert [c[:2] for c in fetched] == [c[:2] for c in native], table


def test_two_syncs_equal_a_full_rebuild_on_clickhouse(tmp_path: Path):
    two_syncs_equal_a_full_rebuild(_empty_target(), tmp_path)


def test_reorg_rollback_matches_a_full_rebuild_on_clickhouse(tmp_path: Path):
    reorg_matches_a_full_rebuild(_empty_target(), tmp_path)


def test_interrupted_batch_is_redone_by_the_next_sync(tmp_path: Path, monkeypatch):
    """ClickHouse has no multi-statement transaction: progress is written last, so a
    crash after the rows went in leaves the old progress, and the next sync replaces
    the same blocks instead of adding them twice."""
    target = _empty_target()
    lines: list[str] = []
    syncer, _, _ = make_syncer(target, tmp_path, ("ethereum",), lines)
    real_write_state = sync.write_state

    def fail(warehouse, state):
        raise OSError("connection lost (simulated)")

    monkeypatch.setattr(sync, "write_state", fail)
    assert syncer.run_pass() is False
    with attach(target) as warehouse:
        assert read_states(warehouse) == {}
        loaded = warehouse.query("SELECT count() FROM chainstar_raw.erc20_transfers")[0][0]
    assert loaded == 541  # the rows went in before the progress failed

    monkeypatch.setattr(sync, "write_state", real_write_state)
    assert syncer.run_pass(), "\n".join(lines)
    with attach(target) as warehouse:
        assert read_states(warehouse)["ethereum"].last_block == FIXTURE_BLOCKS["ethereum"][0]
        # Replaced, not added again.
        assert warehouse.query("SELECT count() FROM chainstar_raw.erc20_transfers") == [(541,)]
    assert_matches_full_rebuild(target, syncer.plan.work_dir)


def test_export_matches_duckdb(data_dir: Path, tmp_path: Path):
    target = _empty_target()
    load_and_build(target, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    reference = DuckDBTarget(tmp_path / "reference.duckdb")
    load_and_build(reference, data_dir, tmp_path / "dbt-reference", chains=ALL_CHAINS)
    for request in (
        ExportRequest("transfers", tmp_path / "{}-transfers.csv", "csv"),
        ExportRequest("fct_fees", tmp_path / "{}-fees.csv", "csv", chain="bitcoin"),
        ExportRequest(
            "transfers",
            tmp_path / "{}-usdt.csv",
            "csv",
            token="0xdac17f958d2ee523a2206206994597c13d831ec7",
            limit=5,
        ),
    ):
        files = {}
        for name, where in (("clickhouse", target), ("duckdb", reference)):
            path = Path(str(request.output).format(name))
            rows = export(where, ExportRequest(**{**request.__dict__, "output": path}))
            files[name] = (rows, path.read_text())
        assert files["clickhouse"] == files["duckdb"], request.table
        assert files["duckdb"][0] > 0
