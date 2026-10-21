"""The dbt project compiles for BigQuery and Snowflake, and the SQL parses in each dialect.

Neither has a free server to test against, so this checks the SQL chainstar's
models produce there, for a first (full) build and an incremental one: every
compiled model and test must parse as that warehouse's SQL (with sqlglot, which
dbt brings), and the dialect-specific pieces must be the ones each warehouse
needs. It does not run the SQL; see the README's Targets table.
"""

import subprocess
import sys
from pathlib import Path

import pytest
import sqlglot

HELPER = Path(__file__).parent / "compile_offline.py"
TARGETS = {
    "bigquery": "bigquery:offline-project",
    "snowflake": "snowflake:ana@xy12345/ANALYTICS?warehouse=WH",
}


def _compile(adapter: str, mode: str, tmp_path: Path) -> Path:
    pytest.importorskip(f"dbt.adapters.{adapter}")
    out = tmp_path / f"{adapter}-{mode}"
    completed = subprocess.run(
        [sys.executable, str(HELPER), TARGETS[adapter], str(out), mode],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return out / "target" / "compiled" / "chainstar"


def _parse_all(root: Path, dialect: str) -> dict[str, str]:
    files = {str(path.relative_to(root)): path.read_text() for path in root.rglob("*.sql")}
    assert len(files) > 70  # 23 models, a seed's tests and 54 data tests
    for name, sql in files.items():
        try:
            sqlglot.parse(sql, read=dialect)
        except sqlglot.errors.ParseError as error:
            pytest.fail(f"{name} does not parse as {dialect}: {error}")
    return files


@pytest.mark.parametrize("mode", ["full", "incremental"])
def test_bigquery_sql(tmp_path: Path, mode: str):
    files = _parse_all(_compile("bigquery", mode, tmp_path), "bigquery")
    blocks = files["models/staging/stg_blocks.sql"]
    # Backticks, not double quotes, which BigQuery reads as a string.
    assert "datetime(timestamp_seconds(blocks.`timestamp`))" in blocks
    fees = files["models/marts/fct_fees.sql"]
    assert "as bignumeric(38, 0)" in fees and "as float64)" in fees
    assert "varchar" not in "".join(files.values()).lower()
    dates = files["models/marts/dim_dates.sql"]
    assert "generate_date_array(" in dates
    if mode == "incremental":
        assert (
            "left join `offline-project`.`chainstar`.`dim_wallets` as existing"
            in (files["models/marts/dim_wallets.sql"])
        )
        assert "select max(block_number) from `offline-project`.`chainstar`.`fct_transfers`" in (
            (tmp_path / "bigquery-incremental" / "introspection.sql").read_text()
        )


@pytest.mark.parametrize("mode", ["full", "incremental"])
def test_snowflake_sql(tmp_path: Path, mode: str):
    files = _parse_all(_compile("snowflake", mode, tmp_path), "snowflake")
    # chainstar creates raw columns in upper case, as Snowflake folds unquoted names.
    assert 'to_timestamp_ntz(blocks."TIMESTAMP")' in files["models/staging/stg_blocks.sql"]
    dates = files["models/marts/dim_dates.sql"]
    assert "generator(rowcount => 100000)" in dates and "dayofweekiso(date_day)" in dates
    if mode == "incremental":
        assert (
            "left join ANALYTICS.chainstar.dim_wallets as existing"
            in (files["models/marts/dim_wallets.sql"])
        )
