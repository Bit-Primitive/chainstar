"""Same end-to-end checks against Postgres.

Skipped unless CHAINSTAR_TEST_POSTGRES_DSN is set to a throwaway database, e.g.
postgresql://postgres:postgres@127.0.0.1:5432/chainstar_test. The tests drop and
recreate the chainstar, chainstar_raw, chainstar_staging and chainstar_state
schemas there.
"""

import os
from pathlib import Path

import pytest

from chainstar import sync
from chainstar.load import attach
from chainstar.state import read_states
from chainstar.targets import parse_target
from e2e_checks import ALL_CHAINS, check_all_chains, check_star_schema, load_and_build
from sync_checks import make_syncer, reorg_matches_a_full_rebuild, two_syncs_equal_a_full_rebuild

DSN = os.environ.get("CHAINSTAR_TEST_POSTGRES_DSN")

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="set CHAINSTAR_TEST_POSTGRES_DSN to run"),
]

SCHEMAS = ("chainstar", "chainstar_staging", "chainstar_raw", "chainstar_state")


def _empty_target():
    target = parse_target(f"postgres:{DSN}")
    with attach(target) as warehouse:
        for schema in SCHEMAS:
            warehouse.connection.execute(
                f"CALL postgres_execute('{warehouse.catalog}', "
                f"'DROP SCHEMA IF EXISTS {schema} CASCADE')"
            )
    return target


def test_build_star_schema_on_postgres(data_dir: Path, tmp_path: Path):
    target = _empty_target()
    load_and_build(target, data_dir, tmp_path / "dbt")
    load_and_build(target, data_dir, tmp_path / "dbt")
    check_star_schema(target)
    # Then the other three chains into the same warehouse.
    load_and_build(target, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    check_all_chains(target)


def test_two_syncs_equal_a_full_rebuild_on_postgres(tmp_path: Path):
    target = _empty_target()
    two_syncs_equal_a_full_rebuild(target, tmp_path)
    # On Postgres a sync indexes the raw tables by chain and block, and dbt the
    # incremental tables by key and watermark, so neither is scanned in full.
    sql = (
        "SELECT schemaname, tablename, indexdef FROM pg_indexes "
        "WHERE schemaname IN ('chainstar_raw', 'chainstar')"
    )
    with attach(target) as warehouse:
        indexes = warehouse.connection.execute(
            "SELECT * FROM postgres_query(?, ?)", [warehouse.catalog, sql]
        ).fetchall()
    defined = {
        (schema, table, definition.split(" USING ")[-1]) for schema, table, definition in indexes
    }
    assert ("chainstar_raw", "erc20_transfers", "btree (chain_id, block_number)") in defined
    assert ("chainstar_raw", "solana_blocks", "btree (chain, slot)") in defined
    assert ("chainstar", "fct_transfers", "btree (transfer_id)") in defined
    assert ("chainstar", "dim_wallets", "btree (chain, last_seen_block)") in defined


def test_reorg_rollback_matches_a_full_rebuild_on_postgres(tmp_path: Path):
    reorg_matches_a_full_rebuild(_empty_target(), tmp_path)


def test_failed_load_commits_nothing_on_postgres(tmp_path: Path, monkeypatch):
    target = _empty_target()
    lines: list[str] = []
    syncer, _, _ = make_syncer(target, tmp_path, ("ethereum",), lines)

    def fail(warehouse, state):
        raise OSError("connection lost (simulated)")

    monkeypatch.setattr(sync, "write_state", fail)
    assert syncer.run_pass() is False
    with attach(target) as warehouse:
        assert read_states(warehouse) == {}
        rows = warehouse.connection.execute(
            f"SELECT count(*) FROM {warehouse.table('chainstar_raw', 'erc20_transfers')}"
        ).fetchone()
    assert rows == (0,)
