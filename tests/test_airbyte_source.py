"""The Airbyte source through its command-line entrypoint: spec, check, discover, read.

Each command runs as Airbyte runs it (the CDK's `launch` with arguments and JSON
files), in this process so the fixture chain can stand in for RPC nodes; every
line on stdout must be an Airbyte protocol message. The CDK leaves NULL values out of
records, which destinations read as NULL. Reads are applied the way an
`Incremental | Append + Deduped` destination applies them (the newest record per
key by cursor; a record with `_ab_cdc_deleted_at` deletes its key), and the
result must equal the working warehouse's star schema.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("airbyte_cdk")

import duckdb
from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from chainstar.integrations import airbyte as connector
from chainstar.integrations.airbyte import CDC_DELETED_AT, run
from chainstar.integrations.feed import WAREHOUSE_FILE
from chainstar.integrations.streams import (
    DELETED_COLUMN,
    GENERATION_COLUMN,
    STREAM_LIST,
    STREAMS,
)
from chainstar.model import MART_SCHEMA
from chainstar.rpc import NodeIdentity
from fixture_chain import FIXTURE_BLOCKS
from plugin_checks import (
    ALL_CHAINS,
    SECRET_RPC,
    fixture_chains,  # noqa: F401  (a pytest fixture, used by name)
)


def cli(capsys, tmp_path: Path, command: str, **files) -> list[dict]:
    """Run one connector command; return its messages, checking stdout holds nothing else."""
    args = [command]
    for name, content in files.items():
        path = tmp_path / f"{command}-{name}.json"
        path.write_text(json.dumps(content))
        args += [f"--{name}", str(path)]
    capsys.readouterr()
    try:
        run(args)
    except SystemExit as exit_:
        assert exit_.code in (0, None), capsys.readouterr().out[-3000:]
    out = capsys.readouterr().out
    assert SECRET_RPC not in out and "very-secret-key" not in out
    messages = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert all("type" in message for message in messages)
    return messages


def of_type(messages: list[dict], kind: str) -> list[dict]:
    return [message for message in messages if message["type"] == kind]


def make_config(working: Path) -> dict:
    return {
        "chains": list(ALL_CHAINS),
        "working_directory": str(working),
        "from_recent": 1,
        "confirmations": {"ethereum": "1", "base": "1", "bitcoin": "1"},
        "rpc_urls": {"ethereum": SECRET_RPC},
        "chunk_size": 1,
    }


def configured_catalog(catalog: dict, sync_mode: str = "incremental") -> dict:
    return {
        "streams": [
            {
                "stream": stream,
                "sync_mode": sync_mode,
                "destination_sync_mode": "append_dedup"
                if sync_mode == "incremental"
                else "overwrite",
                "cursor_field": stream["default_cursor_field"],
                "primary_key": stream["source_defined_primary_key"],
            }
            for stream in catalog["streams"]
        ]
    }


def latest_states(messages: list[dict]) -> list[dict]:
    """The state to pass to the next read: the last state message per stream."""
    by_stream = {}
    for message in of_type(messages, "STATE"):
        by_stream[message["state"]["stream"]["stream_descriptor"]["name"]] = message["state"]
    return list(by_stream.values())


def generations(messages: list[dict]) -> dict[str, int]:
    return {
        state["stream"]["stream_descriptor"]["name"]: state["stream"]["stream_state"][
            GENERATION_COLUMN
        ]
        for state in latest_states(messages)
    }


class DedupedDestination:
    """What `Incremental | Append + Deduped` keeps: per key, the newest record, unless deleted."""

    def __init__(self) -> None:
        self.tables: dict[str, dict] = {stream.name: {} for stream in STREAM_LIST}

    def apply(self, messages: list[dict]) -> None:
        for message in of_type(messages, "RECORD"):
            stream = STREAMS[message["record"]["stream"]]
            data = message["record"]["data"]
            rows = self.tables[stream.name]
            key = data[stream.primary_key]
            if key in rows and rows[key][GENERATION_COLUMN] > data[GENERATION_COLUMN]:
                continue
            if data.get(CDC_DELETED_AT):
                rows.pop(key, None)
            else:
                rows[key] = data

    def rows(self, name: str) -> list[tuple]:
        stream = STREAMS[name]
        return sorted(
            (
                tuple(row.get(column) for column in stream.column_names)
                for row in self.tables[name].values()
            ),
            key=lambda row: row[0],
        )


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def working_rows(working: Path) -> dict[str, list[tuple]]:
    """The working warehouse's tables, as JSON values (what the records carry)."""
    with duckdb.connect(str(working / WAREHOUSE_FILE)) as connection:
        return {
            stream.name: sorted(
                (
                    tuple(json_value(value) for value in row)
                    for row in connection.execute(
                        f"SELECT * FROM {MART_SCHEMA}.{stream.name}"
                    ).fetchall()
                ),
                key=lambda row: row[0],
            )
            for stream in STREAM_LIST
        }


def assert_destination_matches(destination: DedupedDestination, working: Path) -> None:
    for name, rows in working_rows(working).items():
        assert destination.rows(name) == rows, f"{name} differs from the working warehouse"


def test_spec(capsys, tmp_path: Path):
    [message] = of_type(cli(capsys, tmp_path, "spec"), "SPEC")
    spec = message["spec"]["connectionSpecification"]
    assert spec["required"] == ["chains", "working_directory"]
    assert all(
        url["airbyte_secret"] for url in spec["properties"]["rpc_urls"]["properties"].values()
    )
    assert set(spec["properties"]["streams"]["items"]["enum"]) == set(STREAMS)


def test_check(capsys, tmp_path: Path, monkeypatch):
    config = make_config(tmp_path / "work")

    def identify(chain, url):
        return NodeIdentity(True, f"fixture {chain.name}", chain.name, 1)

    monkeypatch.setattr(connector, "identify_node", identify)
    monkeypatch.setattr(connector, "find_cryo", lambda: "/usr/local/bin/cryo")
    [status] = of_type(cli(capsys, tmp_path, "check", config=config), "CONNECTION_STATUS")
    assert status["connectionStatus"]["status"] == "SUCCEEDED"

    def wrong_chain(chain, url):
        return NodeIdentity(False, "chain id 10", "chain id 1", 1)

    monkeypatch.setattr(connector, "identify_node", wrong_chain)
    [status] = of_type(cli(capsys, tmp_path, "check", config=config), "CONNECTION_STATUS")
    assert status["connectionStatus"]["status"] == "FAILED"
    assert "reports chain id 10" in status["connectionStatus"]["message"]

    two_chains_from_block = {**config, "from_block": 5}
    [status] = of_type(
        cli(capsys, tmp_path, "check", config=two_chains_from_block), "CONNECTION_STATUS"
    )
    assert status["connectionStatus"]["status"] == "FAILED"
    assert "from_block is a block of one chain" in status["connectionStatus"]["message"]


def test_discover(capsys, tmp_path: Path):
    [message] = of_type(
        cli(capsys, tmp_path, "discover", config=make_config(tmp_path / "work")), "CATALOG"
    )
    streams = {stream["name"]: stream for stream in message["catalog"]["streams"]}
    assert list(streams) == list(STREAMS)
    fees = streams["fct_fees"]
    assert fees["supported_sync_modes"] == ["full_refresh", "incremental"]
    assert fees["source_defined_cursor"] is True
    assert fees["default_cursor_field"] == [GENERATION_COLUMN]
    assert fees["source_defined_primary_key"] == [["fee_id"]]
    properties = fees["json_schema"]["properties"]
    assert properties["fee_raw"] == {"type": ["null", "string"]}  # exact, as text
    assert properties["fee_id"] == {"type": "string"}
    assert properties["block_timestamp"]["airbyte_type"] == "timestamp_without_timezone"
    assert CDC_DELETED_AT in properties
    only_two = {**make_config(tmp_path / "work"), "streams": ["fct_swaps", "dim_pools"]}
    [message] = of_type(cli(capsys, tmp_path, "discover", config=only_two), "CATALOG")
    assert [stream["name"] for stream in message["catalog"]["streams"]] == [
        "fct_swaps",
        "dim_pools",
    ]


def test_read_incremental_full_refresh_and_reorg(capsys, caplog, tmp_path: Path, fixture_chains):  # noqa: F811
    caplog.set_level("INFO")
    working = tmp_path / "work"
    config = make_config(working)
    [catalog] = of_type(cli(capsys, tmp_path, "discover", config=config), "CATALOG")
    incremental = configured_catalog(catalog["catalog"])
    destination = DedupedDestination()

    # Read 1, no state: everything, as generation 1.
    first = cli(capsys, tmp_path, "read", config=config, catalog=incremental)
    destination.apply(first)
    assert_destination_matches(destination, working)
    assert set(generations(first).values()) == {1}
    assert {record["record"]["data"][GENERATION_COLUMN] for record in of_type(first, "RECORD")} == {
        1
    }
    first_count = len(of_type(first, "RECORD"))

    # Read 2 with read 1's state, after the next block: only what changed.
    fixture_chains.advance()
    second = cli(
        capsys, tmp_path, "read", config=config, catalog=incremental, state=latest_states(first)
    )
    destination.apply(second)
    assert_destination_matches(destination, working)
    assert generations(second)["fct_transfers"] == 2
    total = sum(len(rows) for rows in working_rows(working).values())
    assert 0 < len(of_type(second, "RECORD")) < total

    # The platform never committed read 2: a read from read 1's state again gives
    # the same changes, so nothing is lost.
    again = cli(
        capsys, tmp_path, "read", config=config, catalog=incremental, state=latest_states(first)
    )
    assert len(of_type(again, "RECORD")) == len(of_type(second, "RECORD"))
    assert generations(again) == generations(second)

    # Read 3, nothing new on the chain: no records, the same state.
    third = cli(
        capsys, tmp_path, "read", config=config, catalog=incremental, state=latest_states(second)
    )
    assert of_type(third, "RECORD") == []
    assert generations(third) == generations(second)

    # Read 4: Ethereum's second block reorganised; the rollback arrives as deletes.
    fixture_chains.reorganise_ethereum()
    fourth = cli(
        capsys, tmp_path, "read", config=config, catalog=incremental, state=latest_states(third)
    )
    deletes = [m for m in of_type(fourth, "RECORD") if m["record"]["data"].get(CDC_DELETED_AT)]
    assert deletes and all(m["record"]["data"][DELETED_COLUMN] for m in deletes)
    reorged = FIXTURE_BLOCKS["ethereum"][1]
    assert any(
        m["record"]["stream"] == "fct_transfers" and "chain" not in m["record"]["data"]
        for m in deletes
    )
    before = [
        row
        for row in destination.rows("fct_transfers")
        if row[2] == reorged and row[1] == "ethereum"
    ]
    destination.apply(fourth)
    assert_destination_matches(destination, working)
    after = [
        row
        for row in destination.rows("fct_transfers")
        if row[2] == reorged and row[1] == "ethereum"
    ]
    assert len(after) < len(before)
    assert "rolled back from block 26,239,739" in caplog.text

    # Full refresh: every row, generation 0, the incremental ledger untouched.
    full = cli(
        capsys,
        tmp_path,
        "read",
        config=config,
        catalog=configured_catalog(catalog["catalog"], "full_refresh"),
    )
    records = of_type(full, "RECORD")
    assert len(records) == sum(len(rows) for rows in working_rows(working).values())
    assert {m["record"]["data"][GENERATION_COLUMN] for m in records} == {0}
    refreshed = DedupedDestination()
    refreshed.apply(full)
    assert_destination_matches(refreshed, working)
    assert first_count > 0
    fifth = cli(
        capsys, tmp_path, "read", config=config, catalog=incremental, state=latest_states(fourth)
    )
    assert of_type(fifth, "RECORD") == []


def test_failed_sync_emits_nothing(capsys, tmp_path: Path, fixture_chains):  # noqa: F811
    """A sync that fails (here: the node serves another chain) stops the read before any
    record or state, so the platform keeps the old state and retries."""
    config = {
        **make_config(tmp_path / "work"),
        "chains": ["bitcoin"],
        "rpc_urls": {"bitcoin": SECRET_RPC},
        "confirmations": {"bitcoin": "1"},
    }
    fixture_chains.sources["bitcoin"].identity = lambda: NodeIdentity(
        False, "chain test", "chain main", 1
    )
    [catalog] = of_type(cli(capsys, tmp_path, "discover", config=config), "CATALOG")
    capsys.readouterr()
    args = ["read"]
    for name, content in {
        "config": config,
        "catalog": configured_catalog(catalog["catalog"]),
    }.items():
        path = tmp_path / f"failed-{name}.json"
        path.write_text(json.dumps(content))
        args += [f"--{name}", str(path)]
    # In a process of its own the CDK's exception hook turns this into an ERROR trace
    # message and exit status 1; in-process it arrives as the exception itself.
    with pytest.raises(AirbyteTracedException) as raised:
        run(args)
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert of_type(messages, "RECORD") == [] and of_type(messages, "STATE") == []
    assert raised.value.failure_type == FailureType.transient_error
    message = raised.value.message
    assert "RPC reports chain test" in message and SECRET_RPC not in message
