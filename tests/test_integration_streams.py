"""The plug-ins' stream definitions match the star schema dbt builds, column for column."""

import subprocess
import sys
from pathlib import Path

from chainstar.integrations.streams import STREAMS
from chainstar.model import MART_SCHEMA, MART_TABLES
from chainstar.targets import DuckDBTarget
from conftest import FIXTURES
from e2e_checks import ALL_CHAINS, fetch, load_and_build


def test_streams_match_the_built_star_schema(tmp_path: Path):
    target = DuckDBTarget(tmp_path / "warehouse.duckdb")
    load_and_build(target, FIXTURES, tmp_path / "work", ALL_CHAINS)
    assert list(STREAMS) == MART_TABLES
    for name, stream in STREAMS.items():
        columns = fetch(
            target,
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{MART_SCHEMA}' AND table_name = '{name}' "
            "ORDER BY ordinal_position",
        )
        assert tuple(columns) == stream.columns, name
        duplicates = fetch(
            target,
            f"SELECT count(*) - count(DISTINCT {stream.primary_key}) FROM {{schema}}.{name}",
        )
        assert duplicates == [(0,)], f"{stream.primary_key} is not unique in {name}"


def test_base_install_does_not_import_the_plug_in_libraries():
    """The feed works without dlt or airbyte-cdk: they stay optional extras."""
    code = (
        "import sys, chainstar.integrations.feed, chainstar.cli; "
        "print(sorted(m for m in ('dlt', 'airbyte_cdk', 'pyarrow') if m in sys.modules))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"
