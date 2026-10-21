"""The remote warehouses' SQL and bulk loads, checked against stand-in drivers.

ClickHouse is also tested against a real server (test_e2e_clickhouse.py). There
is no free Snowflake to test against, and the BigQuery emulator cannot run
Parquet load jobs or dbt-bigquery, so for those two these tests check what
chainstar sends: statements, parameters and their types, and the Parquet files
handed to the bulk loader.
"""

import sys
import types
from datetime import datetime
from pathlib import Path
from typing import ClassVar

import duckdb
import pytest

from chainstar import load as load_module
from chainstar.chains import get_chain
from chainstar.config import BlockRange
from chainstar.load import attach, load_range, prepare_raw, transaction
from chainstar.remote import (
    BigQueryWarehouse,
    ClickHouseWarehouse,
    MissingDriver,
    _bigquery_type,
    _clickhouse_type,
    _snowflake_type,
)
from chainstar.state import SyncState, ensure_state_table, read_states, write_state
from chainstar.targets import parse_target

ETHEREUM = get_chain("ethereum")
FIRST_BLOCK = BlockRange(26239738, 26239739)
NOW = datetime(2026, 10, 21, 12, 30, 15, 123456)


def _state(**changes) -> SyncState:
    values = dict(
        chain="ethereum",
        first_block=26239738,
        last_block=26239738,
        checkpoint_block=26239738,
        checkpoint_hash="0xabc",
        checkpoint_time=NOW,
        confirmations="12",
        modelled_block=None,
        rebuild_pending=True,
        updated_at=NOW,
        last_run_at=None,
        last_run_status=None,
        last_error=None,
        chainstar_version="0.1.0",
    )
    values.update(changes)
    return SyncState(**values)


def _parquet_rows(path: str) -> int:
    with duckdb.connect() as connection:
        return connection.execute("SELECT count(*) FROM read_parquet(?)", [path]).fetchone()[0]


def test_type_names():
    assert [_clickhouse_type(t) for t in ("BIGINT", "VARCHAR", "BOOLEAN", "DECIMAL(20,0)")] == [
        "Int64",
        "String",
        "Bool",
        "Decimal(20, 0)",
    ]
    assert _clickhouse_type("TIMESTAMP") == "DateTime64(6, 'UTC')"
    assert [_bigquery_type(t) for t in ("BIGINT", "VARCHAR", "BOOLEAN", "TIMESTAMP")] == [
        "INT64",
        "STRING",
        "BOOL",
        "DATETIME",
    ]
    assert _bigquery_type("DECIMAL(20,0)") == "NUMERIC(20, 0)"
    assert _bigquery_type("DECIMAL(38,0)") == "BIGNUMERIC(38, 0)"
    assert _snowflake_type("DECIMAL(20,0)") == "NUMBER(20, 0)"
    assert _snowflake_type("TIMESTAMP") == "TIMESTAMP_NTZ"


def test_clickhouse_binds_question_marks_client_side():
    warehouse = ClickHouseWarehouse.__new__(ClickHouseWarehouse)
    sql, values = warehouse._bind("SELECT ? WHERE x LIKE 'a%' AND t = ?", ["base", NOW])
    assert sql == "SELECT %s WHERE x LIKE 'a%%' AND t = %s"
    # With microseconds, which the driver's own datetime formatting drops.
    assert values == ["base", "2026-10-21 12:30:15.123456"]
    assert warehouse._bind("SELECT 1", []) == ("SELECT 1", None)


def test_missing_driver_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "clickhouse_connect", None)
    with (
        pytest.raises(MissingDriver, match=r"pip install 'chainstar\[clickhouse\]'"),
        attach(parse_target("clickhouse:http://localhost:8123")),
    ):
        pass


# Snowflake ------------------------------------------------------------------


class FakeSnowflakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = 0
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.connection.statements.append((sql, params))
        if sql.startswith("PUT"):
            path = sql.split("'file://", 1)[1].split("'", 1)[0]
            self.connection.uploaded.append((sql, _parquet_rows(path)))
        if sql.startswith("SELECT count(*) FROM information_schema.tables"):
            self.description = [("COUNT(*)",)]
            self._rows = [(0,)]
        elif sql.startswith("DELETE"):
            self.rowcount = 7

    def fetchall(self):
        return self._rows


class FakeSnowflakeConnection:
    def __init__(self, **options):
        self.options = options
        self.statements = []
        self.uploaded = []

    def cursor(self):
        return FakeSnowflakeCursor(self)

    def close(self):
        pass


@pytest.fixture
def snowflake(monkeypatch):
    connections = []

    def connect(**options):
        connections.append(FakeSnowflakeConnection(**options))
        return connections[-1]

    connector = types.ModuleType("snowflake.connector")
    connector.connect = connect
    package = types.ModuleType("snowflake")
    package.connector = connector
    monkeypatch.setitem(sys.modules, "snowflake", package)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "sf-secret")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
    return connections


def test_snowflake_loads_through_stage_and_copy(snowflake, data_dir: Path):
    target = parse_target("snowflake:ana@xy12345/ANALYTICS?warehouse=WH&role=LOADER")
    with attach(target) as warehouse:
        prepare_raw(warehouse)
        with transaction(warehouse):
            counts = load_range(warehouse, data_dir, ETHEREUM, FIRST_BLOCK)
    connection = snowflake[0]
    assert connection.options == {
        "account": "xy12345",
        "user": "ana",
        "database": "ANALYTICS",
        "warehouse": "WH",
        "role": "LOADER",
        "paramstyle": "qmark",
        "application": "chainstar",
        "password": "sf-secret",
    }
    sql = [statement for statement, _ in connection.statements]
    assert 'CREATE SCHEMA IF NOT EXISTS "ANALYTICS"."CHAINSTAR_RAW"' in sql
    blocks_ddl = next(
        s
        for s in sql
        if s.startswith('CREATE TABLE IF NOT EXISTS "ANALYTICS"."CHAINSTAR_RAW"."BLOCKS"')
    )
    assert '"BASE_FEE_PER_GAS" NUMBER(20, 0)' in blocks_ddl
    begin, commit = sql.index("BEGIN"), sql.index("COMMIT")
    loads = [i for i, s in enumerate(sql) if s.startswith(("DELETE", "PUT", "COPY"))]
    assert loads and all(begin < i < commit for i in loads)
    delete = next(s for s in connection.statements if s[0].startswith("DELETE"))
    assert delete == (
        'DELETE FROM "ANALYTICS"."CHAINSTAR_RAW"."ERC20_TRANSFERS" WHERE "CHAIN_ID" = ? '
        'AND "BLOCK_NUMBER" >= ? AND "BLOCK_NUMBER" < ?',
        [1, 26239738, 26239739],
    )
    put, rows = next(u for u in connection.uploaded if "ERC20_TRANSFERS" in u[0])
    assert put.endswith(
        '@"ANALYTICS"."CHAINSTAR_RAW".%"ERC20_TRANSFERS" AUTO_COMPRESS = FALSE OVERWRITE = TRUE'
    )
    assert rows == counts["erc20_transfers"] == 541
    copy = next(
        s for s in sql if s.startswith('COPY INTO "ANALYTICS"."CHAINSTAR_RAW"."ERC20_TRANSFERS"')
    )
    assert "FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy
    assert "FORCE = TRUE PURGE = TRUE" in copy


def test_snowflake_progress_in_one_transaction(snowflake):
    target = parse_target("snowflake:ana@xy12345/ANALYTICS")
    with attach(target) as warehouse, transaction(warehouse):
        ensure_state_table(warehouse)
        write_state(warehouse, _state())
    statements = snowflake[0].statements
    sql = [statement for statement, _ in statements]
    ddl = next(s for s in sql if "SYNC_PROGRESS" in s and s.startswith("CREATE TABLE"))
    assert '"CHECKPOINT_TIME" TIMESTAMP_NTZ' in ddl and '"_VERSION"' not in ddl
    insert, params = statements[-2]
    assert insert.startswith('INSERT INTO "ANALYTICS"."CHAINSTAR_STATE"."SYNC_PROGRESS" VALUES (?')
    # qmark binding is told a datetime's Snowflake type.
    assert ("TIMESTAMP_NTZ", NOW) in params
    assert sql[-1] == "COMMIT"


# BigQuery -------------------------------------------------------------------


class FakeJob:
    def __init__(self, sql, config, rows=(), statement_type="SELECT", affected=None):
        self.sql = sql
        self.config = config
        self.rows = list(rows)
        self.statement_type = statement_type
        self.num_dml_affected_rows = affected

    def result(self):
        return [types.SimpleNamespace(values=lambda row=row: row) for row in self.rows]


class FakeBigQueryClient:
    instances: ClassVar[list["FakeBigQueryClient"]] = []

    def __init__(self, project=None, location=None):
        self.project = project
        self.location = location
        self.jobs: list[FakeJob] = []
        self.loads: list[tuple[str, object, int]] = []
        self.datasets: list[str] = []
        self.tables: set[str] = set()
        FakeBigQueryClient.instances.append(self)

    def query(self, sql, job_config=None):
        kind = sql.split(None, 1)[0].upper()
        statement_type = {"SELECT": "SELECT", "DELETE": "DELETE", "INSERT": "INSERT"}.get(
            kind, "CREATE_TABLE"
        )
        if kind == "CREATE":
            self.tables.add(sql.split("`")[5])
        job = FakeJob(sql, job_config, statement_type=statement_type, affected=3)
        self.jobs.append(job)
        return job

    def load_table_from_file(self, handle, table_id, job_config=None):
        path = handle.name
        self.loads.append((table_id, job_config, _parquet_rows(path)))
        return FakeJob("load", job_config)

    def get_table(self, table_id):
        from google.api_core.exceptions import NotFound

        if table_id.split(".")[-1] not in self.tables:
            raise NotFound(table_id)

    def create_dataset(self, dataset, exists_ok=False):
        self.datasets.append(dataset.dataset_id)

    def close(self):
        pass


@pytest.fixture
def bigquery(monkeypatch):
    from google.cloud import bigquery as real

    FakeBigQueryClient.instances = []
    monkeypatch.setattr(real, "Client", FakeBigQueryClient)
    return FakeBigQueryClient.instances


def test_bigquery_loads_with_parquet_load_jobs(bigquery, data_dir: Path):
    target = parse_target("bigquery:my-project?location=EU")
    with attach(target) as warehouse:
        assert isinstance(warehouse, BigQueryWarehouse)
        assert not warehouse.transactional
        prepare_raw(warehouse)
        with transaction(warehouse):
            counts = load_range(warehouse, data_dir, ETHEREUM, FIRST_BLOCK)
    client = bigquery[0]
    assert (client.project, client.location) == ("my-project", "EU")
    assert "chainstar_raw" in client.datasets
    ddl = next(
        job.sql for job in client.jobs if "`blocks`" in job.sql and job.sql.startswith("CREATE")
    )
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `my-project`.`chainstar_raw`.`blocks` (")
    assert "`base_fee_per_gas` NUMERIC(20, 0)" in ddl
    assert ddl.endswith("CLUSTER BY `chain_id`, `block_number`")
    delete = next(job for job in client.jobs if job.sql.startswith("DELETE"))
    assert delete.sql == (
        "DELETE FROM `my-project`.`chainstar_raw`.`erc20_transfers` WHERE `chain_id` = ? "
        "AND `block_number` >= ? AND `block_number` < ?"
    )
    assert [(p.type_, p.value) for p in delete.config.query_parameters] == [
        ("INT64", 1),
        ("INT64", 26239738),
        ("INT64", 26239739),
    ]
    table_id, config, rows = next(
        load for load in client.loads if load[0].endswith("erc20_transfers")
    )
    assert table_id == "my-project.chainstar_raw.erc20_transfers"
    assert (config.source_format, config.write_disposition) == ("PARQUET", "WRITE_APPEND")
    assert rows == counts["erc20_transfers"] == 541


def test_bigquery_progress_is_versioned(bigquery):
    with attach(parse_target("bigquery:my-project")) as warehouse:
        ensure_state_table(warehouse)
        write_state(warehouse, _state())
    client = bigquery[0]
    ddl = next(job.sql for job in client.jobs if "sync_progress" in job.sql)
    assert "`_version` INT64" in ddl and "`checkpoint_time` DATETIME" in ddl
    insert = next(job for job in client.jobs if job.sql.startswith("INSERT"))
    assert insert.sql.startswith(
        "INSERT INTO `my-project`.`chainstar_state`.`sync_progress` (chain, first_block"
    )
    parameters = {p.value: p.type_ for p in insert.config.query_parameters if p.value is not None}
    assert parameters[NOW] == "DATETIME" and parameters["0xabc"] == "STRING"
    # A NULL still has its column's type.
    nulls = [p.type_ for p in insert.config.query_parameters if p.value is None]
    assert nulls == ["INT64", "DATETIME", "STRING", "STRING"]
    # The new version first, then older ones go: a crash never leaves no row.
    delete = client.jobs[client.jobs.index(insert) + 1]
    assert delete.sql.endswith("WHERE chain = ? AND _version < ?")


def test_bigquery_reads_the_newest_version(bigquery):
    with attach(parse_target("bigquery:my-project")) as warehouse:
        warehouse.client.tables.add("sync_progress")
        assert read_states(warehouse) == {}
        select = warehouse.client.jobs[-1].sql
    assert "row_number() OVER (PARTITION BY chain ORDER BY _version DESC)" in select
    assert select.endswith("WHERE recency = 1 ORDER BY chain")


# MotherDuck -----------------------------------------------------------------


def test_motherduck_connects_with_its_md_path(monkeypatch, data_dir: Path):
    opened = []

    real_connect = duckdb.connect

    def connect(path=":memory:", **options):
        opened.append(path)
        return real_connect()

    monkeypatch.setattr(load_module.duckdb, "connect", connect)
    with attach(parse_target("motherduck:analytics")) as warehouse:
        assert warehouse.via_duckdb and warehouse.transactional
        prepare_raw(warehouse)
        with transaction(warehouse):
            counts = load_range(warehouse, data_dir, ETHEREUM, FIRST_BLOCK)
    assert opened[0] == "md:analytics"
    assert counts["erc20_transfers"] == 541
