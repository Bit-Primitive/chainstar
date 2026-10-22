"""The dlt source, end to end: real dlt pipelines into DuckDB and Postgres.

The fixture chain stands in for RPC nodes (plugin_checks.py); everything else is
real: chainstar's sync and dbt build into the working warehouse, the change feed,
and dlt's extract, normalize and merge load. After every run the destination's
tables must equal the working warehouse's star schema, row for row.

Postgres runs when CHAINSTAR_TEST_POSTGRES_DSN names a throwaway server:

    docker run -d --rm -p 127.0.0.1:5432:5432 -e POSTGRES_USER=chainstar \\
      -e POSTGRES_PASSWORD=chainstar -e POSTGRES_DB=chainstar_test postgres:16-alpine
"""

import os
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

dlt = pytest.importorskip("dlt")
pytest.importorskip("pyarrow")

import duckdb  # noqa: E402

from chainstar.integrations.dlt import chainstar_source, column_hints  # noqa: E402
from chainstar.integrations.streams import (  # noqa: E402
    DELETED_COLUMN,
    GENERATION_COLUMN,
    STREAM_LIST,
    STREAMS,
)
from fixture_chain import FIXTURE_BLOCKS  # noqa: E402
from plugin_checks import (  # noqa: E402
    ALL_CHAINS,
    CONFIRMATIONS,
    SECRET_RPC,
    assert_same_tables,
    by_key,
    fixture_chains,  # noqa: F401 (a pytest fixture)
    working_tables,
)

POSTGRES_DSN = os.environ.get("CHAINSTAR_TEST_POSTGRES_DSN")

Reader = Callable[[str, str], list[tuple]]


def make_source(working_dir: Path, **options):
    return chainstar_source(
        chains=list(ALL_CHAINS),
        working_dir=str(working_dir),
        from_recent=1,
        confirmations=CONFIRMATIONS,
        rpc_urls={"ethereum": SECRET_RPC},
        chunk_size=1,
        **options,
    )


def destination_tables(read: Reader, dataset: str) -> dict[str, list[tuple]]:
    tables = {}
    for stream in STREAM_LIST:
        columns = ", ".join(stream.column_names)
        try:
            tables[stream.name] = by_key(read(dataset, f"SELECT {columns} FROM {{}}.{stream.name}"))
        except Exception as error:  # a table never loaded
            if "not exist" not in str(error).lower():
                raise
    return tables


def loaded_rows(pipeline) -> dict[str, int]:
    counts = pipeline.last_trace.last_normalize_info.row_counts
    return {name: rows for name, rows in counts.items() if name in STREAMS}


def run_scenario(pipeline, read: Reader, tmp_path: Path, chains, caplog) -> None:
    working = tmp_path / "work"
    dataset = pipeline.dataset_name

    # Run 1: one block per chain.
    pipeline.run(make_source(working))
    first = loaded_rows(pipeline)
    tables = working_tables(working)
    assert first == {name: len(rows) for name, rows in tables.items() if rows}
    assert_same_tables(destination_tables(read, dataset), tables)
    assert SECRET_RPC not in caplog.text and "very-secret-key" not in caplog.text

    # Run 2: the next block per chain. Only new and changed rows are loaded.
    chains.advance()
    pipeline.run(make_source(working))
    second = loaded_rows(pipeline)
    tables = working_tables(working)
    assert_same_tables(destination_tables(read, dataset), tables)
    assert 0 < second["fct_transfers"] < len(tables["fct_transfers"])
    assert second["fct_transfers"] >= len(tables["fct_transfers"]) - first["fct_transfers"]
    generations = read(dataset, f"SELECT DISTINCT {GENERATION_COLUMN} FROM {{}}.fct_transfers")
    assert sorted(row[0] for row in generations) == [1, 2]
    state = pipeline.state["sources"]["chainstar"]["resources"]["fct_transfers"]
    assert state["incremental"][GENERATION_COLUMN]["last_value"] == 2

    # Run 3: nothing new on the chain, so nothing to load.
    pipeline.run(make_source(working))
    assert loaded_rows(pipeline) == {}

    # Run 4: Ethereum's second block is reorganised. The rollback reaches the
    # destination: rows of the old block that are gone are deleted there.
    before = destination_tables(read, dataset)
    chains.reorganise_ethereum()
    pipeline.run(make_source(working))
    tables = working_tables(working)
    after = destination_tables(read, dataset)
    assert_same_tables(after, tables)
    reorged = FIXTURE_BLOCKS["ethereum"][1]

    def ethereum_rows(rows):
        return [row for row in rows if row[1] == "ethereum" and row[2] == reorged]

    assert len(ethereum_rows(after["fct_transfers"])) < len(ethereum_rows(before["fct_transfers"]))
    assert "rolled back from block 26,239,739" in caplog.text

    # A full reload with dlt's refresh gives the same tables.
    pipeline.run(make_source(working), refresh="drop_resources")
    assert loaded_rows(pipeline)["fct_transfers"] == len(tables["fct_transfers"])
    assert_same_tables(destination_tables(read, dataset), tables)


def test_dlt_source_into_duckdb(tmp_path: Path, fixture_chains, caplog):  # noqa: F811
    caplog.set_level("INFO")
    path = tmp_path / "destination.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="chainstar_test",
        destination=dlt.destinations.duckdb(str(path)),
        dataset_name="chainstar_dlt",
        pipelines_dir=str(tmp_path / "dlt"),
    )

    def read(dataset: str, sql: str) -> list[tuple]:
        with duckdb.connect(str(path)) as connection:
            return connection.execute(sql.format(dataset)).fetchall()

    run_scenario(pipeline, read, tmp_path, fixture_chains, caplog)

    # Types as in the working warehouse: exact amounts stay text or exact decimals.
    with duckdb.connect(str(path)) as connection:
        types = dict(
            connection.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'chainstar_dlt' AND table_name = 'fct_fees'"
            ).fetchall()
        )
        transfers = dict(
            connection.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'chainstar_dlt' AND table_name = 'fct_transfers'"
            ).fetchall()
        )
    assert transfers["amount_raw"] == "VARCHAR"
    assert types["fee_raw"] == "DECIMAL(38,0)"
    assert types[GENERATION_COLUMN] == "BIGINT"
    assert types[DELETED_COLUMN] == "BOOLEAN"


@pytest.mark.postgres
@pytest.mark.skipif(not POSTGRES_DSN, reason="set CHAINSTAR_TEST_POSTGRES_DSN")
def test_dlt_source_into_postgres(tmp_path: Path, fixture_chains, caplog):  # noqa: F811
    import psycopg2

    caplog.set_level("INFO")
    dataset = f"chainstar_dlt_{uuid.uuid4().hex[:8]}"
    pipeline = dlt.pipeline(
        pipeline_name="chainstar_test_pg",
        destination=dlt.destinations.postgres(credentials=POSTGRES_DSN),
        dataset_name=dataset,
        pipelines_dir=str(tmp_path / "dlt"),
    )

    def read(dataset: str, sql: str) -> list[tuple]:
        with psycopg2.connect(POSTGRES_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(sql.format(dataset))
            return cursor.fetchall()

    try:
        run_scenario(pipeline, read, tmp_path, fixture_chains, caplog)
        types = dict(
            read(
                dataset,
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = '{}' AND table_name = 'fct_transfers'",
            )
        )
        assert types["amount_raw"] == "character varying"
    finally:
        with psycopg2.connect(POSTGRES_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{dataset}" CASCADE')
            cursor.execute(f'DROP SCHEMA IF EXISTS "{dataset}_staging" CASCADE')


def test_column_hints_keep_exact_types():
    hints = column_hints(STREAMS["fct_fees"])
    assert hints["fee_raw"] == {"data_type": "decimal", "precision": 38, "scale": 0}
    assert hints["fee_id"]["nullable"] is False
    assert hints[DELETED_COLUMN]["hard_delete"] is True
    assert column_hints(STREAMS["fct_transfers"])["amount_raw"] == {"data_type": "text"}


def test_dlt_telemetry_is_off_by_default():
    assert os.environ.get("RUNTIME__DLTHUB_TELEMETRY") == "false"


def test_bad_options_fail_before_extracting(tmp_path: Path):
    with pytest.raises(Exception, match="from_block is a block of one chain"):
        chainstar_source(chains=["base", "ethereum"], working_dir=str(tmp_path), from_block=1)
    with pytest.raises(Exception, match="unknown table"):
        chainstar_source(chains=["base"], working_dir=str(tmp_path), tables=["fct_nope"])


def test_two_pipelines_share_one_working_folder(tmp_path: Path, fixture_chains):  # noqa: F811
    """Each pipeline has its own ledger, so neither misses rows the other was given."""
    working = tmp_path / "work"

    def pipeline(name: str):
        path = tmp_path / f"{name}.duckdb"
        return path, dlt.pipeline(
            pipeline_name=name,
            destination=dlt.destinations.duckdb(str(path)),
            dataset_name="chainstar_dlt",
            pipelines_dir=str(tmp_path / "dlt"),
        )

    def tables(path: Path) -> dict[str, list[tuple]]:
        def read(dataset: str, sql: str) -> list[tuple]:
            with duckdb.connect(str(path)) as connection:
                return connection.execute(sql.format(dataset)).fetchall()

        return destination_tables(read, "chainstar_dlt")

    first_path, first = pipeline("first")
    second_path, second = pipeline("second")
    first.run(make_source(working))
    fixture_chains.advance()
    second.run(make_source(working))
    first.run(make_source(working))
    assert 0 < loaded_rows(first)["fct_transfers"] < len(working_tables(working)["fct_transfers"])
    assert_same_tables(tables(first_path), working_tables(working))
    assert_same_tables(tables(second_path), working_tables(working))
