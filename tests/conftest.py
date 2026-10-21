"""Shared fixtures.

tests/fixtures/<chain>/<dataset>/ holds small slices of real cryo 0.3.2 output
(produced with `--hex --u256-types string` exactly as chainstar runs it), copied
with DuckDB so cryo's Parquet column types are preserved. No test needs network.
"""

import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """A writable copy of the fixture data directory."""
    target = tmp_path / "data"
    shutil.copytree(FIXTURES, target)
    return target
