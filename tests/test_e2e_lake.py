"""Parquet lakes end to end: sync into the engine, publish, and read the lake back.

The local lake runs always. The S3 tests run only when CHAINSTAR_TEST_S3_ENDPOINT
points at S3-compatible storage with a bucket CHAINSTAR_TEST_S3_BUCKET (default
chainstar-test), for example adobe/s3mock:

    docker run -d -p 127.0.0.1:9090:9090 \
        -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=chainstar-test adobe/s3mock:5.2.3
    CHAINSTAR_TEST_S3_ENDPOINT=http://127.0.0.1:9090 uv run pytest tests/test_e2e_lake.py

Each test writes under its own prefix, so nothing needs cleaning up.
"""

import os
import uuid
from pathlib import Path

import duckdb
import pytest

from chainstar.export import ExportRequest, export
from chainstar.lake import configure, publish
from chainstar.load import attach, attach_reader
from chainstar.model import MART_SCHEMA, MART_TABLES
from chainstar.state import read_states
from chainstar.status import collect
from chainstar.targets import DuckDBTarget, LakeTarget, parse_target
from fixture_chain import FIXTURE_BLOCKS
from sync_checks import reorg_matches_a_full_rebuild, two_syncs_equal_a_full_rebuild

S3_ENDPOINT = os.environ.get("CHAINSTAR_TEST_S3_ENDPOINT")
S3_BUCKET = os.environ.get("CHAINSTAR_TEST_S3_BUCKET", "chainstar-test")


def _local_lake(tmp_path: Path) -> LakeTarget:
    return parse_target(f"parquet:{tmp_path}/lake?engine={tmp_path}/engine.duckdb")


@pytest.fixture
def s3_lake(tmp_path: Path, monkeypatch) -> LakeTarget:
    if not S3_ENDPOINT:
        pytest.skip("set CHAINSTAR_TEST_S3_ENDPOINT to run")
    # s3mock and MinIO's defaults; a real bucket's keys come from the environment.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
    prefix = f"lake-{uuid.uuid4().hex[:12]}"
    return parse_target(
        f"parquet:s3://{S3_BUCKET}/{prefix}?endpoint={S3_ENDPOINT}&region=us-east-1"
        f"&engine={tmp_path}/engine.duckdb"
    )


def _tables(warehouse) -> dict[str, tuple[list, list]]:
    """Each mart table's columns (name, type) and rows, sorted."""
    connection = warehouse.connection
    found = {}
    for table in MART_TABLES:
        reference = warehouse.table(MART_SCHEMA, table)
        columns = [row[:2] for row in connection.execute(f"DESCRIBE {reference}").fetchall()]
        rows = connection.execute(f"SELECT * FROM {reference} ORDER BY ALL").fetchall()
        found[table] = (columns, rows)
    return found


def assert_lake_matches_engine(target: LakeTarget) -> dict[str, tuple[list, list]]:
    with attach(target) as engine:
        built = _tables(engine)
        progress = read_states(engine)
    with attach_reader(target) as lake:
        published = _tables(lake)
        assert read_states(lake) == progress
    for table in MART_TABLES:
        assert published[table][0] == built[table][0], f"{table}: columns differ"
        assert published[table][1] == built[table][1], f"{table}: rows differ"
    return published


def _sync_and_check(target: LakeTarget, tmp_path: Path) -> None:
    two_syncs_equal_a_full_rebuild(target, tmp_path)
    tables = assert_lake_matches_engine(target)
    assert {row[1] for row in tables["fct_transfers"][1]} == set(FIXTURE_BLOCKS)
    # Nothing changed since the sync published, so nothing is written again.
    again = publish(target)
    assert (again.written, again.emptied) == (0, 0)
    assert again.unchanged == 21  # 2 days of Bitcoin facts, 1 of the others, x2, + dims


def test_local_lake_sync_publishes_the_star_schema(tmp_path: Path):
    target = _local_lake(tmp_path)
    _sync_and_check(target, tmp_path)
    lake = Path(target.location)
    day = lake / "chainstar/fct_transfers/chain=bitcoin"
    assert sorted(p.name for p in day.iterdir()) == ["date_day=2026-10-21", "date_day=2026-10-22"]
    assert (lake / "chainstar/dim_wallets/chain=solana/data_0.parquet").exists()
    assert (lake / "chainstar/dim_dates/data_0.parquet").exists()
    assert (lake / "chainstar_state/sync_progress/data_0.parquet").exists()
    # Partition columns live in the path only, as Spark, Athena and Hive expect.
    with duckdb.connect() as connection:
        columns = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
            [f"{day}/date_day=2026-10-22/data_0.parquet"],
        ).fetchall()
    names = {column[0] for column in columns}
    assert "transfer_id" in names and not names & {"chain", "date_day"}


def test_local_lake_empties_partitions_that_are_gone(tmp_path: Path):
    target = _local_lake(tmp_path)
    two_syncs_equal_a_full_rebuild(target, tmp_path)
    with attach(target) as engine:
        for table in ("fct_transfers", "fct_fees"):
            engine.connection.execute(
                f"DELETE FROM chainstar.{table} WHERE chain = 'bitcoin' AND block_number = ?",
                [FIXTURE_BLOCKS["bitcoin"][1]],
            )
    published = publish(target)
    assert (published.written, published.emptied) == (0, 2)
    gone = Path(target.location) / "chainstar/fct_fees/chain=bitcoin/date_day=2026-10-22"
    assert not gone.exists()
    assert_lake_matches_engine(target)
    assert publish(target).emptied == 0


def test_local_lake_rollback_reaches_the_lake(tmp_path: Path):
    target = _local_lake(tmp_path)
    reorg_matches_a_full_rebuild(target, tmp_path)
    assert_lake_matches_engine(target)


def test_status_and_export_read_the_lake_without_the_engine(tmp_path: Path):
    target = _local_lake(tmp_path)
    two_syncs_equal_a_full_rebuild(target, tmp_path)
    from_engine = tmp_path / "engine.csv"
    export(DuckDBTarget(target.engine), ExportRequest("transfers", from_engine, "csv"))
    target.engine.unlink()
    from_lake = tmp_path / "lake.csv"
    rows = export(target, ExportRequest("transfers", from_lake, "csv", chain="bitcoin"))
    assert rows == 26
    export(target, ExportRequest("transfers", from_lake, "csv"))
    assert from_lake.read_text() == from_engine.read_text()

    class Node:
        def identity(self):
            raise OSError("no node in this test")

    report = collect(target, [], lambda chain: Node(), None)
    assert [status.state.chain for status in report.chains] == sorted(FIXTURE_BLOCKS)
    assert report.chains[1].rows["fct_transfers"] == 26  # bitcoin


@pytest.mark.s3
def test_s3_lake_sync_and_rollback(s3_lake: LakeTarget, tmp_path: Path):
    _sync_and_check(s3_lake, tmp_path)
    with attach(s3_lake) as engine:
        engine.connection.execute(
            "DELETE FROM chainstar.fct_transfers WHERE chain = 'bitcoin' AND block_number = ?",
            [FIXTURE_BLOCKS["bitcoin"][1]],
        )
    # Object stores are written through DuckDB, which cannot delete: the partition's
    # file is replaced by an empty one, which every reader sees as no rows.
    assert publish(s3_lake).emptied == 1
    assert_lake_matches_engine(s3_lake)


@pytest.mark.s3
def test_s3_lake_rollback(s3_lake: LakeTarget, tmp_path: Path):
    reorg_matches_a_full_rebuild(s3_lake, tmp_path)
    assert_lake_matches_engine(s3_lake)


@pytest.mark.s3
def test_s3_multipart_upload_and_listing(s3_lake: LakeTarget):
    """A file bigger than one upload part is written in parts, then listed and read back."""
    with duckdb.connect() as connection:
        configure(connection, s3_lake)
        # 60MB / 10,000 parts: DuckDB then uses S3's smallest part size, 5 MiB.
        connection.execute("SET s3_uploader_max_filesize = '60MB'")
        location = s3_lake.path("big/data_0.parquet")
        connection.execute(
            "COPY (SELECT range AS i, md5(range::VARCHAR) || md5((range * 7)::VARCHAR) AS s "
            f"FROM range(600000)) TO '{location}' (FORMAT parquet, COMPRESSION uncompressed)"
        )
        listed = connection.execute("SELECT file FROM glob(?)", [s3_lake.path("**")]).fetchall()
        assert listed == [(location,)]
        rows = connection.execute(f"SELECT count(*), sum(i) FROM '{location}'").fetchone()
    assert rows == (600000, 599999 * 600000 // 2)
