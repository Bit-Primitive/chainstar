"""Same end-to-end check against Postgres.

Skipped unless CHAINSTAR_TEST_POSTGRES_DSN is set to a throwaway database, e.g.
postgresql://postgres:postgres@127.0.0.1:5432/chainstar_test. The test drops and
recreates the chainstar, chainstar_raw and chainstar_staging schemas there.
"""

import os
from pathlib import Path

import pytest

from chainstar.load import attach
from chainstar.targets import parse_target
from e2e_checks import ALL_CHAINS, check_all_chains, check_star_schema, load_and_build

DSN = os.environ.get("CHAINSTAR_TEST_POSTGRES_DSN")

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="set CHAINSTAR_TEST_POSTGRES_DSN to run"),
]


def test_build_star_schema_on_postgres(data_dir: Path, tmp_path: Path):
    target = parse_target(f"postgres:{DSN}")
    with attach(target) as warehouse:
        for schema in ("chainstar", "chainstar_staging", "chainstar_raw"):
            warehouse.connection.execute(
                f"CALL postgres_execute('{warehouse.catalog}', "
                f"'DROP SCHEMA IF EXISTS {schema} CASCADE')"
            )
    load_and_build(target, data_dir, tmp_path / "dbt")
    load_and_build(target, data_dir, tmp_path / "dbt")
    check_star_schema(target)
    # Then the other three chains into the same warehouse.
    load_and_build(target, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    check_all_chains(target)
