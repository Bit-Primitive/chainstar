from pathlib import Path

from typer.testing import CliRunner

from chainstar import __version__, cli, pipeline
from chainstar.cli import app
from chainstar.rpc import NodeIdentity, RpcError

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


def test_run_without_rpc_uses_the_public_endpoint_and_fails_cleanly(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("CHAINSTAR_RPC_URL", raising=False)
    urls = []

    def unreachable(chain, url):
        urls.append(url)
        raise RpcError("eth_chainId: could not reach https://base-rpc.publicnode.com (URLError)")

    monkeypatch.setattr(pipeline, "identify_node", unreachable)
    result = runner.invoke(
        app,
        ["run", "--config", str(tmp_path / "none.yml"), "--chain", "base", "--blocks", "1:2"],
    )
    assert result.exit_code == 1
    assert urls == ["https://base-rpc.publicnode.com"]
    assert "free shared public endpoint" in result.output
    assert "CHAINSTAR_RPC_URL" in result.output
    assert "hint:  check your internet connection" in result.output
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


def _fixture_sources(tmp_path: Path, heads: dict[str, int]):
    from chainstar.chains import get_chain
    from fixture_chain import FixtureSource, make_source_dir

    source = make_source_dir(tmp_path)
    return {name: FixtureSource(get_chain(name), source, head) for name, head in heads.items()}


def test_sync_command_resumes_and_hides_the_key(tmp_path: Path, monkeypatch):
    from chainstar import sync
    from chainstar.model import ModelResult

    sources = _fixture_sources(tmp_path, {"ethereum": 26239738})
    monkeypatch.setattr(cli, "rpc_sources", lambda plan: sources)
    monkeypatch.setattr(
        sync, "build_models", lambda target, work_dir, full_refresh: ModelResult(models=23)
    )
    args = [
        "sync",
        "--config",
        str(tmp_path / "none.yml"),
        "--chain",
        "ethereum",
        "--rpc-url",
        "https://eth.example/v2/supersecretkey",
        "--target",
        f"duckdb:{tmp_path / 'w.duckdb'}",
        "--data-dir",
        str(tmp_path / "data"),
        "--confirmations",
        "1",
    ]
    first = runner.invoke(app, args)
    assert first.exit_code == 1
    assert "first sync" in first.output

    started = runner.invoke(app, [*args, "--from", "26239738"])
    assert started.exit_code == 0, started.output
    assert "synced to 26,239,738" in started.output
    assert "eth.example/<redacted>" in started.output
    assert "supersecretkey" not in started.output

    sources["ethereum"].head = 26239739
    resumed = runner.invoke(app, [*args, "--json-logs"])
    assert resumed.exit_code == 0, resumed.output
    import json

    stages = [json.loads(line)["stage"] for line in resumed.output.splitlines()]
    assert stages[:4] == ["start", "rpc", "head", "check"]
    assert "load" in stages and "model" in stages


def test_sync_argument_checks(tmp_path: Path):
    base = ["sync", "--config", str(tmp_path / "none.yml"), "--target", "duckdb:x.duckdb"]
    cases = [
        ([], "no chain to sync"),
        (["--chain", "base", "--chain", "solana", "--rpc-url", "https://x"], "one endpoint"),
        (["--chain", "base", "--chain", "solana", "--from", "5"], "--from is a block of one"),
        (["--chain", "base", "--from", "5", "--from-recent", "5"], "not both"),
        (["--chain", "base", "--every", "5"], "duration must be"),
        (["--chain", "solana", "--confirmations", "3"], "Solana syncs only finalized"),
    ]
    for extra, message in cases:
        result = runner.invoke(app, [*base, *extra])
        assert result.exit_code == 1, extra
        assert message in result.output, (extra, result.output)


def test_wallet_sync_flags_need_sync(tmp_path: Path):
    result = runner.invoke(app, ["wallet", "0xabc", "--recent", "5", "--every", "5m"])
    assert result.exit_code == 1
    assert "--every needs --sync" in result.output
    result = runner.invoke(app, ["wallet", "0xabc", "--sync", "--blocks", "1:5"])
    assert result.exit_code == 1
    assert "--blocks is a fixed range" in result.output


def test_wallet_sync_keeps_a_spreadsheet_current(tmp_path: Path, monkeypatch):
    # The real dbt build; the fixture stands in for the node's filtered transfers.
    sources = _fixture_sources(tmp_path, {"ethereum": 26239738})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "find_cryo", lambda: "/usr/bin/cryo")
    monkeypatch.setattr(cli, "rpc_sources", lambda plan: sources)
    wallet = "0x73D53553EE552c1f2a9722e6407D43E41e19593f"  # in both fixture blocks
    args = ["wallet", wallet, "--chain", "ethereum", "--sync", "--recent", "1"]
    args += ["--confirmations", "1", "--output", "mine.csv"]
    first = runner.invoke(app, args)
    assert first.exit_code == 0, first.output
    rows_first = len((tmp_path / "mine.csv").read_text().splitlines())
    assert (tmp_path / "data" / f"wallet-ethereum-{wallet.lower()}" / "warehouse.duckdb").exists()

    sources["ethereum"].head = 26239739
    second = runner.invoke(app, args)
    assert second.exit_code == 0, second.output
    assert "dbt build (incremental)" in second.output
    assert "--from and --from-recent apply only to a chain's first sync" in second.output
    rows_second = len((tmp_path / "mine.csv").read_text().splitlines())
    assert rows_second > rows_first > 1


def _doctor(tmp_path: Path, monkeypatch, target: str):
    monkeypatch.setattr(cli, "find_cryo", lambda: None)
    monkeypatch.setattr(
        cli,
        "identify_node",
        lambda chain, url: NodeIdentity(True, "chain main, height 972,467", "bitcoin (main)", 1),
    )
    args = ["doctor", "--config", str(tmp_path / "none.yml"), "--chain", "bitcoin"]
    return runner.invoke(app, [*args, "--rpc-url", "https://x.example", "--target", target])


def test_doctor_checks_a_local_lake_without_creating_it(tmp_path: Path, monkeypatch):
    result = _doctor(tmp_path, monkeypatch, f"parquet:{tmp_path}/lake?engine={tmp_path}/e.duckdb")
    assert f"ok    target  parquet:{tmp_path}/lake usable; engine {tmp_path}/e.duckdb" in (
        result.output
    )
    assert "dbt-duckdb" in result.output
    assert not (tmp_path / "lake").exists() and not (tmp_path / "e.duckdb").exists()


def test_doctor_asks_for_the_motherduck_token(tmp_path: Path, monkeypatch):
    for name in ("motherduck_token", "MOTHERDUCK_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    result = _doctor(tmp_path, monkeypatch, "motherduck:analytics")
    assert result.exit_code == 1
    assert "FAIL  target  ConfigError: set the motherduck_token environment variable" in (
        result.output
    )


def test_doctor_hides_a_clickhouse_password(tmp_path: Path, monkeypatch):
    # Port 1 on localhost refuses connections.
    result = _doctor(tmp_path, monkeypatch, "clickhouse:http://ana:hunter22secret@127.0.0.1:1")
    assert result.exit_code == 1
    assert "FAIL  target" in result.output
    assert "hunter22secret" not in result.output
    assert "dbt-clickhouse" in result.output


def test_doctor_names_the_extra_for_a_missing_adapter(tmp_path: Path, monkeypatch):
    real_version = cli.version

    def version(package):
        if package == "dbt-snowflake":
            raise cli.PackageNotFoundError(package)
        return real_version(package)

    monkeypatch.setattr(cli, "version", version)
    monkeypatch.setattr(cli, "_check_target", lambda target: "stubbed")
    result = _doctor(tmp_path, monkeypatch, "snowflake:ana@acct/DB")
    assert "FAIL  dbt     dbt-core" in result.output
    assert "dbt-snowflake missing; pip install 'chainstar[snowflake]'" in result.output


def test_errors_never_show_warehouse_credentials(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "sf-secret-value")
    monkeypatch.setattr(
        cli,
        "export",
        lambda target, request: (_ for _ in ()).throw(
            RuntimeError(f"login failed for {target.user} with password {target.password}")
        ),
    )
    result = runner.invoke(
        app,
        ["export", "--config", str(tmp_path / "none.yml"), "--target", "snowflake:ana@acct/DB"],
    )
    assert result.exit_code == 1
    assert "sf-secret-value" not in result.output
    assert "login failed for ana with password <redacted>" in result.output
