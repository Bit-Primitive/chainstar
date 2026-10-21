"""`chainstar status`: what it reads from the warehouse and the node, and its exit code."""

import json
from datetime import timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chainstar import cli
from chainstar.cli import app
from chainstar.load import attach, transaction
from chainstar.rpc import NodeIdentity, RpcError
from chainstar.state import SyncState, ensure_state_table, utc_now, write_state
from chainstar.status import StatusError, collect, format_duration, render_text, to_json
from chainstar.targets import DuckDBTarget

runner = CliRunner()


def _state(chain: str, last_block: int, behind: timedelta, **changes) -> SyncState:
    now = utc_now()
    values = dict(
        chain=chain,
        first_block=last_block - 99,
        last_block=last_block,
        checkpoint_block=last_block,
        checkpoint_hash="0xabc",
        checkpoint_time=now - behind,
        confirmations="60",
        modelled_block=last_block,
        rebuild_pending=False,
        updated_at=now,
        last_run_at=now,
        last_run_status="ok",
        last_error=None,
        chainstar_version="0.1.0",
    )
    values.update(changes)
    return SyncState(**values)


@pytest.fixture
def warehouse(tmp_path: Path) -> DuckDBTarget:
    target = DuckDBTarget(tmp_path / "w.duckdb")
    with attach(target) as opened, transaction(opened):
        ensure_state_table(opened)
        write_state(opened, _state("base", 52_905_900, timedelta(minutes=2)))
        write_state(
            opened,
            _state("bitcoin", 972_500, timedelta(hours=3), confirmations="6", last_error="x"),
        )
        opened.connection.execute("CREATE SCHEMA chainstar")
        opened.connection.execute(
            "CREATE TABLE chainstar.fct_transfers AS "
            "SELECT * FROM (VALUES ('base'), ('base'), ('bitcoin')) AS t(chain)"
        )
    return target


class FakeNode:
    def __init__(self, head: int):
        self.head = head

    def identity(self):
        return NodeIdentity(True, f"latest block {self.head:,}", "x", self.head)

    def finalized_block(self):
        return self.head - 100


HEADS = {"base": 52_905_962, "bitcoin": 972_506}


def test_collect(warehouse):
    report = collect(warehouse, [], lambda chain: FakeNode(HEADS[chain.name]), None)
    base, bitcoin = report.chains
    assert base.state.chain == "base"
    assert (base.head, base.safe_head, base.lag_blocks) == (52_905_962, 52_905_903, 62)
    assert 115 <= base.lag_seconds <= 125
    assert base.rows["fct_transfers"] == 2
    assert base.rows["dim_wallets"] == 0  # tables not built yet count as empty
    assert bitcoin.safe_head == 972_501
    assert report.healthy is None  # no --max-lag


def test_max_lag(warehouse):
    report = collect(warehouse, [], lambda chain: FakeNode(HEADS[chain.name]), 15 * 60)
    assert [s.healthy for s in report.chains] == [True, False]
    assert report.healthy is False
    text = "\n".join(render_text(report))
    assert "UNHEALTHY: bitcoin is 3h behind, more than --max-lag 15m" in text


def test_unreachable_node_still_reports_time_lag(warehouse):
    def unreachable(chain):
        raise RpcError("eth_chainId: could not reach https://base-rpc.publicnode.com (URLError)")

    report = collect(warehouse, ["base"], unreachable, 600)
    (base,) = report.chains
    assert base.head is None and "could not reach" in base.head_error
    assert base.lag_seconds is not None
    assert base.healthy is True


def test_text_and_json(warehouse):
    report = collect(warehouse, ["base"], lambda chain: FakeNode(HEADS[chain.name]), None)
    text = "\n".join(render_text(report))
    assert "synced to    block 52,905,900" in text
    assert "head         52,905,962; safe head 52,905,903 (60 confirmations)" in text
    assert "lag          62 blocks, 2m" in text
    assert "star schema  built to block 52,905,900" in text
    data = to_json(report)
    assert json.dumps(data)  # serialisable
    (chain,) = data["chains"]
    assert chain["synced_to"] == 52_905_900
    assert chain["lag_blocks"] == 62
    assert chain["rows"]["fct_transfers"] == 2


def test_unknown_chain(warehouse):
    with pytest.raises(StatusError, match="solana has never been synced"):
        collect(warehouse, ["solana"], lambda chain: FakeNode(1), None)


def test_no_warehouse(tmp_path):
    with pytest.raises(StatusError, match="no warehouse"):
        collect(DuckDBTarget(tmp_path / "none.duckdb"), [], lambda chain: FakeNode(1), None)


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (0, "0s"),
        (59, "59s"),
        (125, "2m 5s"),
        (3 * 3600 + 7, "3h 7s"),
        (90061, "1d 1h"),
        (None, "unknown"),
    ],
)
def test_format_duration(seconds, text):
    assert format_duration(seconds) == text


def test_cli_exit_code_follows_max_lag(warehouse, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "ChainNode", lambda chain, url, rps: FakeNode(HEADS[chain.name]))
    base = ["status", "--target", f"duckdb:{warehouse.path}", "--config", str(tmp_path / "x.yml")]
    healthy = runner.invoke(app, [*base, "--chain", "base", "--max-lag", "15m"])
    assert healthy.exit_code == 0, healthy.output
    assert "healthy: every chain is within 15m" in healthy.output
    late = runner.invoke(app, [*base, "--max-lag", "15m", "--json"])
    assert late.exit_code == 1
    assert json.loads(late.output)["healthy"] is False
    assert runner.invoke(app, base).exit_code == 0  # without --max-lag, only a report
