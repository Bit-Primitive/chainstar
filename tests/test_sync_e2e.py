"""Sync end to end on DuckDB with the real dbt project (Postgres: test_e2e_postgres.py)."""

from pathlib import Path

from chainstar.targets import DuckDBTarget
from sync_checks import reorg_matches_a_full_rebuild, two_syncs_equal_a_full_rebuild


def test_two_syncs_equal_a_full_rebuild_on_duckdb(tmp_path: Path):
    two_syncs_equal_a_full_rebuild(DuckDBTarget(tmp_path / "warehouse.duckdb"), tmp_path)


def test_reorg_rollback_matches_a_full_rebuild_on_duckdb(tmp_path: Path):
    reorg_matches_a_full_rebuild(DuckDBTarget(tmp_path / "warehouse.duckdb"), tmp_path)
