"""chainstar's BigQuery SQL, run on the BigQuery emulator over the fixtures.

dbt-bigquery itself cannot run against the emulator (its `create schema` job
fails there), and the emulator crashes on Parquet load jobs, so this checks the
SQL alone: the models are compiled offline for BigQuery (compile_offline.py),
the raw fixtures are loaded as JSON, and each model is built with CREATE TABLE
or CREATE VIEW ... AS its compiled SELECT, in dependency order. The emulator
analyses the SQL with GoogleSQL, BigQuery's own front end, and the star schema
must equal DuckDB's row for row. The incremental SELECTs are run as well, over
the built tables. dbt-bigquery's merge statements are not.

Skipped unless CHAINSTAR_TEST_BIGQUERY_EMULATOR is the emulator's URL. Publish
its port as the same number on the host: the emulator hands out upload URLs
with its own port.

    docker run -d -p 127.0.0.1:9050:9050 ghcr.io/goccy/bigquery-emulator:0.8.1 \\
        --project=chainstar-test
    CHAINSTAR_TEST_BIGQUERY_EMULATOR=http://127.0.0.1:9050 uv run pytest tests/test_bigquery*
"""

import csv
import io
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from chainstar.datasets import ALL_DATASETS, parquet_files
from chainstar.load import _select_distinct, attach
from chainstar.model import DBT_PROJECT_DIR, MART_TABLES
from chainstar.targets import DuckDBTarget
from e2e_checks import ALL_CHAINS, MODELS, load_and_build
from sync_checks import snapshot

ENDPOINT = os.environ.get("CHAINSTAR_TEST_BIGQUERY_EMULATOR")
PROJECT = "chainstar-test"
HELPER = Path(__file__).parent / "compile_offline.py"

pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set CHAINSTAR_TEST_BIGQUERY_EMULATOR")


@pytest.fixture
def client():
    bigquery = pytest.importorskip("google.cloud.bigquery")
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials

    client = bigquery.Client(
        project=PROJECT,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=ENDPOINT),
    )
    from google.api_core.exceptions import NotFound

    # Every table is created with CREATE OR REPLACE: the emulator cannot delete a
    # dataset whose tables were made with SQL, and answers a second create with an
    # error that is retried rather than "already exists".
    for dataset in ("chainstar_raw", "chainstar_staging", "chainstar"):
        try:
            client.get_dataset(f"{PROJECT}.{dataset}")
        except NotFound:
            client.create_dataset(f"{PROJECT}.{dataset}")
    yield client
    client.close()


def _run(client, sql: str):
    from google.cloud import bigquery

    job = client.query(sql, job_retry=None, retry=bigquery.DEFAULT_RETRY.with_deadline(30))
    return job.result(timeout=120)


def _load_raw(client, data_dir: Path) -> None:
    from google.cloud import bigquery

    from chainstar.remote import _bigquery_type

    with duckdb.connect() as local:
        for dataset in ALL_DATASETS:
            ddl = ", ".join(f"`{name}` {_bigquery_type(kind)}" for name, kind in dataset.columns)
            _run(
                client, f"CREATE OR REPLACE TABLE `{PROJECT}.chainstar_raw.{dataset.name}` ({ddl})"
            )
            files = [str(p) for c in ALL_CHAINS for p in parquet_files(data_dir, c, dataset)]
            rows = local.execute(_select_distinct(dataset), [files]).fetchall()
            names = [name for name, _ in dataset.columns]
            lines = [
                json.dumps(
                    {
                        name: str(value) if isinstance(value, Decimal) else value
                        for name, value in zip(names, row, strict=True)
                    }
                )
                for row in rows
            ]
            config = bigquery.LoadJobConfig(source_format="NEWLINE_DELIMITED_JSON")
            client.load_table_from_file(
                io.BytesIO("\n".join(lines).encode()),
                f"{PROJECT}.chainstar_raw.{dataset.name}",
                job_config=config,
            ).result(timeout=60)
    # The seeds, with the types _seeds.yml gives them: whole numbers as INT64.
    integers = {"chain_id", "native_decimals", "decimals"}
    for seed in sorted((DBT_PROJECT_DIR / "seeds").glob("*.csv")):
        rows = list(csv.DictReader(seed.open()))
        names = list(rows[0])
        ddl = ", ".join(f"{n} {'INT64' if n in integers else 'STRING'}" for n in names)
        _run(client, f"CREATE OR REPLACE TABLE `{PROJECT}.chainstar_staging.{seed.stem}` ({ddl})")

        def literal(name: str, value: str) -> str:
            if name in integers:
                return value or "NULL"
            return "'" + value + "'"

        values = ", ".join("(" + ", ".join(literal(n, row[n]) for n in names) + ")" for row in rows)
        _run(client, f"INSERT INTO `{PROJECT}.chainstar_staging.{seed.stem}` VALUES {values}")


def _compiled(tmp_path: Path, mode: str) -> tuple[Path, list[dict]]:
    out = tmp_path / f"compiled-{mode}"
    completed = subprocess.run(
        [sys.executable, str(HELPER), "bigquery:offline-project", str(out), mode],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    manifest = json.loads((out / "target" / "manifest.json").read_text())
    models = {
        key: node for key, node in manifest["nodes"].items() if node["resource_type"] == "model"
    }
    ordered: list[dict] = []

    def visit(key: str) -> None:
        if key in models and models[key] not in ordered:
            for dependency in models[key]["depends_on"]["nodes"]:
                visit(dependency)
            ordered.append(models[key])

    for key in models:
        visit(key)
    return out / "target" / "compiled" / "chainstar", ordered


def _sql(root: Path, node: dict) -> str:
    return (root / node["original_file_path"]).read_text().replace("offline-project", PROJECT)


def test_star_schema_sql_on_the_emulator(client, data_dir: Path, tmp_path: Path):
    from chainstar.remote import narrow_decimals

    _load_raw(client, data_dir)
    root, models = _compiled(tmp_path, "full")
    for node in models:
        kind = "VIEW" if node["config"]["materialized"] == "view" else "TABLE"
        relation = f"`{PROJECT}.{node['schema']}.{node['alias']}`"
        _run(client, f"CREATE OR REPLACE {kind} {relation} AS {_sql(root, node)}")
    assert len(models) == MODELS

    reference = DuckDBTarget(tmp_path / "reference.duckdb")
    load_and_build(reference, data_dir, tmp_path / "dbt", chains=ALL_CHAINS)
    expected = snapshot(reference)
    with attach(reference) as duck, duckdb.connect() as local:
        for table in MART_TABLES:
            job = client.query(f"SELECT * FROM `{PROJECT}.chainstar.{table}`")
            local.register("fetched", narrow_decimals(job.to_arrow()))
            names = [row[0] for row in local.execute("DESCRIBE fetched").fetchall()]
            columns = duck.connection.execute(f"DESCRIBE chainstar.{table}").fetchall()
            assert names == [row[0] for row in columns], table
            if table == "fct_fees":
                # An emulator bug (0.8.1): in `SELECT x, x AS y FROM t UNION ALL SELECT
                # 2, 3` it returns 2 for y in the second branch. int_fees selects
                # execution_fee_wei twice in its EVM branch (the second time as
                # fee_raw), so the emulator gives Bitcoin and Solana fees as NULL. EVM
                # fees, where both are the same, are compared; BigQuery itself returns
                # (2, 3).
                fetched = local.execute(
                    "SELECT * REPLACE (CASE WHEN chain IN ('ethereum', 'base') THEN fee_raw END "
                    "AS fee_raw, CASE WHEN chain IN ('ethereum', 'base') THEN fee_native END "
                    "AS fee_native) FROM fetched ORDER BY ALL"
                ).fetchall()
                expected_rows = duck.connection.execute(
                    "SELECT * REPLACE (CASE WHEN chain IN ('ethereum', 'base') THEN fee_raw END "
                    "AS fee_raw, CASE WHEN chain IN ('ethereum', 'base') THEN fee_native END "
                    "AS fee_native) FROM chainstar.fct_fees ORDER BY ALL"
                ).fetchall()
                assert fetched == expected_rows, "fct_fees differs from DuckDB's"
                continue
            built = local.execute("SELECT * FROM fetched ORDER BY ALL").fetchall()
            assert built == expected[table], f"{table} differs from DuckDB's"

    # The incremental SELECTs, over the tables just built: GoogleSQL accepts them.
    root, models = _compiled(tmp_path, "incremental")
    for node in models:
        if node["config"]["materialized"] == "incremental":
            _run(client, f"SELECT count(*) FROM ({_sql(root, node)})")
