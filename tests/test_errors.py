from pathlib import Path

import pytest
from typer.testing import CliRunner

from chainstar import cli, pipeline
from chainstar.cli import app
from chainstar.errors import hint_for
from chainstar.load import LoadError
from chainstar.rpc import NodeIdentity, RpcError

SECRET_URL = "https://eth.example.com/v2/supersecretkey"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RpcError("eth_chainId: https://x answered HTTP 429 (too many requests)"), "limiting"),
        (RpcError("getBlock: could not reach https://x (URLError)"), "internet connection"),
        (LoadError('FATAL:  password authentication failed for user "me"'), "PGPASSWORD"),
        (LoadError("connection to server failed: Connection refused"), "Postgres is running"),
        (PermissionError(13, "Permission denied", "/root/w.duckdb"), "folder you can write"),
        (OSError(28, "No space left on device", "/data/x"), "disk space"),
    ],
)
def test_hints_for_common_failures(error, expected):
    assert expected in hint_for(error)


def test_unexpected_errors_point_to_debug_and_hide_the_url(monkeypatch, tmp_path: Path):
    def broken(settings, echo, *args):
        raise KeyError(f"boom at {SECRET_URL}")

    monkeypatch.setattr(cli, "run_pipeline", broken)
    args = ["run", "--config", str(tmp_path / "none.yml"), "--chain", "base", "--blocks", "1:2"]
    args += ["--rpc-url", SECRET_URL]

    result = CliRunner().invoke(app, args)
    assert result.exit_code == 1
    assert "--debug" in result.output
    assert "Traceback" not in result.output
    assert "supersecretkey" not in result.output

    debug = CliRunner().invoke(app, ["--debug", *args])
    assert "Traceback" in debug.output
    assert "supersecretkey" not in debug.output


def test_unwritable_target_is_one_line(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        pipeline, "identify_node", lambda chain, url: NodeIdentity(True, "x", "base", 10)
    )
    monkeypatch.setattr(pipeline, "_extract_evm", lambda settings, echo: None)
    blocker = tmp_path / "file"
    blocker.write_text("")
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--config",
            str(tmp_path / "none.yml"),
            "--chain",
            "base",
            "--blocks",
            "1:2",
            "--rpc-url",
            "https://x.example",
            "--target",
            f"duckdb:{blocker / 'sub' / 'w.duckdb'}",
            "--data-dir",
            str(tmp_path / "data"),
        ],
    )
    assert result.exit_code == 1
    assert f"error: could not use {blocker}" in result.output
    assert "Traceback" not in result.output
