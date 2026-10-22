"""Shared by the dlt and Airbyte plug-in tests: a fixture chain in place of RPC nodes, and
a check that a destination holds exactly the working warehouse's star schema."""

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from chainstar.chains import get_chain
from chainstar.integrations import feed
from chainstar.integrations.feed import WAREHOUSE_FILE
from chainstar.integrations.streams import STREAM_LIST
from chainstar.model import MART_SCHEMA
from fixture_chain import FIXTURE_BLOCKS, FixtureSource, make_source_dir, rewrite_block

ALL_CHAINS = ("ethereum", "base", "bitcoin", "solana")
# Every fixture block is final at once, so each run can take one more block.
CONFIRMATIONS = {"ethereum": 1, "base": 1, "bitcoin": 1}
# Never contacted (the fixture chain answers instead); checks that URLs stay out of logs.
SECRET_RPC = "https://rpc.example/v2/very-secret-key"
REORG_HASH = "0x" + "ab" * 32


class FixtureChains:
    """Fixture nodes the plug-ins' syncs talk to, with heads the test moves."""

    def __init__(self, tmp_path: Path) -> None:
        self.source_dir = make_source_dir(tmp_path)
        self.sources = {
            name: FixtureSource(get_chain(name), self.source_dir, FIXTURE_BLOCKS[name][0])
            for name in ALL_CHAINS
        }

    def advance(self) -> None:
        for name, source in self.sources.items():
            source.head = FIXTURE_BLOCKS[name][1]

    def reorganise_ethereum(self) -> None:
        """Ethereum's second block gets a new hash and loses half its rows."""
        rewrite_block(
            self.source_dir, get_chain("ethereum"), FIXTURE_BLOCKS["ethereum"][1], REORG_HASH
        )


@pytest.fixture
def fixture_chains(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FixtureChains:
    chains = FixtureChains(tmp_path)

    def sources(plan):
        return {
            plan_chain.chain.name: chains.sources[plan_chain.chain.name]
            for plan_chain in plan.chains
        }

    monkeypatch.setattr(feed, "rpc_sources", sources)
    return chains


def working_tables(working_dir: Path) -> dict[str, list[tuple]]:
    """The working warehouse's star schema, each table's rows sorted."""
    # A plain connect: dbt-duckdb may hold the file open in this process.
    with duckdb.connect(str(working_dir / WAREHOUSE_FILE)) as connection:
        return {
            stream.name: by_key(
                connection.execute(f"SELECT * FROM {MART_SCHEMA}.{stream.name}").fetchall()
            )
            for stream in STREAM_LIST
        }


def by_key(rows: Iterable[Iterable]) -> list[tuple]:
    """Rows normalised and sorted by their first column, every table's key."""
    return sorted((normalise(row) for row in rows), key=lambda row: row[0])


def normalise(row: Iterable) -> tuple:
    """Timestamps as naive UTC, as the working warehouse keeps them (destinations add a zone)."""
    return tuple(
        value.astimezone(UTC).replace(tzinfo=None)
        if isinstance(value, datetime) and value.tzinfo is not None
        else value
        for value in row
    )


def assert_same_tables(
    destination: Mapping[str, list[tuple]], working: Mapping[str, list[tuple]]
) -> None:
    for name, rows in working.items():
        assert destination.get(name, []) == rows, f"{name} differs from the working warehouse"
