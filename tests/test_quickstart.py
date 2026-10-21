from pathlib import Path

import pytest
from typer.testing import CliRunner

from chainstar import cli
from chainstar.chains import get_chain
from chainstar.cli import app
from chainstar.config import BlockRange, ConfigError
from chainstar.quickstart import next_steps, recent_range
from chainstar.rpc import NodeIdentity
from chainstar.targets import DuckDBTarget

runner = CliRunner()


def test_recent_range_stays_behind_the_evm_head():
    # END is exclusive: 20 blocks ending 5 behind head 1,000.
    assert recent_range(get_chain("base"), 1000, 20) == BlockRange(976, 996)


def test_recent_range_takes_the_latest_bitcoin_block_and_solana_slots():
    assert recent_range(get_chain("bitcoin"), 972476, 1) == BlockRange(972476, 972477)
    assert recent_range(get_chain("solana"), 500, 20) == BlockRange(481, 501)
    with pytest.raises(ConfigError):
        recent_range(get_chain("solana"), 500, 0)


def test_next_steps_show_export_query_and_files():
    text = "\n".join(
        next_steps(get_chain("base"), DuckDBTarget(Path("warehouse.duckdb")), Path("data"))
    )
    assert "chainstar export transfers --chain base --output base-transfers.xlsx" in text
    assert 'duckdb warehouse.duckdb "select' in text
    assert "chainstar.fct_transfers" in text
    assert "data/base/" in text


def test_quickstart_without_cryo_explains_instead_of_failing(monkeypatch):
    monkeypatch.setattr(cli, "find_cryo", lambda: None)
    result = runner.invoke(app, ["quickstart", "--chain", "ethereum", "--yes"])
    assert result.exit_code == 1
    assert "cargo install cryo_cli" in result.output
    assert "docker run" in result.output
    assert "Traceback" not in result.output


def test_quickstart_resolves_the_range_and_runs(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("CHAINSTAR_RPC_URL", raising=False)
    monkeypatch.setattr(
        cli, "identify_node", lambda chain, url: NodeIdentity(True, "x", "solana", 1000)
    )
    seen = {}

    def fake_pipeline(settings, echo, *args):
        seen["settings"] = settings

    monkeypatch.setattr(cli, "run_pipeline", fake_pipeline)
    result = runner.invoke(
        app,
        [
            "quickstart",
            "--chain",
            "solana",
            "--recent",
            "3",
            "--yes",
            "--target",
            f"duckdb:{tmp_path / 'w.duckdb'}",
            "--data-dir",
            str(tmp_path / "data"),
        ],
    )
    assert result.exit_code == 0, result.output
    settings = seen["settings"]
    assert settings.blocks == BlockRange(998, 1001)
    assert settings.rpc_is_public
    assert "Next steps" in result.output
