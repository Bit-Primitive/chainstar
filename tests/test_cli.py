from pathlib import Path

from typer.testing import CliRunner

from chainstar import __version__, cli
from chainstar.cli import app
from chainstar.rpc import NodeIdentity

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_init_writes_config_and_refuses_to_overwrite(tmp_path: Path):
    path = tmp_path / "chainstar.yml"
    assert runner.invoke(app, ["init", str(path)]).exit_code == 0
    assert "rpc_url_env: CHAINSTAR_RPC_URL" in path.read_text()

    again = runner.invoke(app, ["init", str(path)])
    assert again.exit_code == 1
    assert runner.invoke(app, ["init", str(path), "--force"]).exit_code == 0


def test_run_without_rpc_fails_cleanly(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("CHAINSTAR_RPC_URL", raising=False)
    result = runner.invoke(
        app,
        ["run", "--config", str(tmp_path / "none.yml"), "--chain", "base", "--blocks", "1:2"],
    )
    assert result.exit_code == 1
    assert "CHAINSTAR_RPC_URL" in result.output
    assert "Traceback" not in result.output


def test_run_rejects_unknown_chain(tmp_path: Path):
    result = runner.invoke(
        app,
        ["run", "--config", str(tmp_path / "none.yml"), "--chain", "dogecoin", "--blocks", "1:2"],
    )
    assert result.exit_code == 1
    assert "supported: ethereum, base, bitcoin, solana" in result.output


def test_doctor_does_not_need_cryo_for_bitcoin(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cli, "find_cryo", lambda: None)
    monkeypatch.setattr(
        cli,
        "identify_node",
        lambda chain, url: NodeIdentity(True, "chain main, height 972,467", "bitcoin (main)", 1),
    )
    result = runner.invoke(
        app,
        [
            "doctor",
            "--config",
            str(tmp_path / "none.yml"),
            "--chain",
            "bitcoin",
            "--rpc-url",
            "https://btc.example/v1/secretkey",
            "--target",
            f"duckdb:{tmp_path / 'w.duckdb'}",
        ],
    )
    assert "ok    cryo    not needed for bitcoin" in result.output
    assert "chain main, height 972,467" in result.output
    assert "secretkey" not in result.output


def test_doctor_reports_the_wrong_network(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        cli,
        "identify_node",
        lambda chain, url: NodeIdentity(False, "chain test, height 10", "bitcoin (main)", 10),
    )
    result = runner.invoke(
        app,
        [
            "doctor",
            "--config",
            str(tmp_path / "none.yml"),
            "--chain",
            "bitcoin",
            "--rpc-url",
            "https://x.example",
        ],
    )
    assert result.exit_code == 1
    assert (
        "FAIL  rpc     https://x.example chain test, height 10; expected bitcoin (main)"
        in result.output
    )
