"""Command-line interface: `chainstar doctor`, `chainstar init`, `chainstar run`."""

import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

import duckdb
import typer

from chainstar import __version__
from chainstar.chains import SUPPORTED_CHAINS, UnknownChainError, get_chain
from chainstar.config import (
    CONFIG_FILENAME,
    CONFIG_TEMPLATE,
    DEFAULT_TARGET,
    TARGET_ENV,
    ConfigError,
    load_config_file,
    resolve_rpc_url,
    resolve_settings,
)
from chainstar.cryo import INSTALL_HINT, CryoError, cryo_version, find_cryo
from chainstar.load import LoadError, attach
from chainstar.model import ModelError
from chainstar.pipeline import PipelineError, run_pipeline
from chainstar.redact import redact_url
from chainstar.rpc import RpcError, get_chain_id, get_latest_block
from chainstar.targets import TARGET_HELP, DuckDBTarget, TargetError, parse_target

app = typer.Typer(
    help="Raw blockchain data to clean dimensional tables in your own warehouse, from one command.",
    add_completion=False,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

ConfigOption = Annotated[Path, typer.Option("--config", help="Path to chainstar.yml (optional).")]
ChainOption = Annotated[
    str | None, typer.Option(help=f"Chain to read: {', '.join(SUPPORTED_CHAINS)}.")
]
RpcOption = Annotated[
    str | None,
    typer.Option(
        "--rpc-url", help="JSON-RPC URL. Defaults to the CHAINSTAR_RPC_URL environment variable."
    ),
]
TargetOption = Annotated[
    str | None,
    typer.Option(help=f"{TARGET_HELP}. Defaults to {TARGET_ENV}, then chainstar.yml."),
]

# Errors we expect users to hit (bad input, unreachable services). They get a one-line
# message instead of a traceback.
USER_ERRORS = (
    duckdb.Error,
    ConfigError,
    UnknownChainError,
    TargetError,
    RpcError,
    CryoError,
    LoadError,
    ModelError,
    PipelineError,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"chainstar {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    _version: Annotated[
        bool,
        typer.Option("--version", callback=_version_callback, is_eager=True, help="Show version."),
    ] = False,
) -> None:
    pass


@app.command()
def init(
    path: Annotated[Path, typer.Argument(help="Where to write the file.")] = Path(CONFIG_FILENAME),
    force: Annotated[bool, typer.Option(help="Overwrite an existing file.")] = False,
) -> None:
    """Write a commented chainstar.yml to start from."""
    if path.exists() and not force:
        typer.echo(f"{path} already exists; use --force to overwrite it.", err=True)
        raise typer.Exit(1)
    path.write_text(CONFIG_TEMPLATE)
    typer.echo(f"wrote {path}")
    typer.echo("next: set CHAINSTAR_RPC_URL, choose a block range, then run `chainstar doctor`.")


@app.command()
def doctor(
    config: ConfigOption = Path(CONFIG_FILENAME),
    chain: ChainOption = None,
    rpc_url: RpcOption = None,
    target: TargetOption = None,
) -> None:
    """Check that cryo, the RPC endpoint and the target warehouse are ready."""
    problems = 0

    def report(ok: bool, name: str, detail: str) -> None:
        nonlocal problems
        problems += 0 if ok else 1
        typer.echo(f"{'ok  ' if ok else 'FAIL'}  {name:<7} {detail}")

    try:
        file_config = load_config_file(config)
    except ConfigError as error:
        report(False, "config", str(error))
        raise typer.Exit(1) from None

    executable = find_cryo()
    if executable:
        report(True, "cryo", f"{cryo_version()} at {executable}")
    else:
        report(False, "cryo", f"not found on PATH; {INSTALL_HINT}")

    expected_chain = None
    chain_name = chain or file_config.get("chain")
    if chain_name:
        try:
            expected_chain = get_chain(chain_name)
        except UnknownChainError as error:
            report(False, "chain", str(error))

    try:
        url = resolve_rpc_url(rpc_url, file_config)
        chain_id = get_chain_id(url)
        latest = get_latest_block(url)
        detail = f"{redact_url(url)} chain id {chain_id}, latest block {latest:,}"
        if expected_chain and chain_id != expected_chain.chain_id:
            report(
                False,
                "rpc",
                f"{detail}; expected {expected_chain.name} ({expected_chain.chain_id})",
            )
        else:
            report(True, "rpc", detail)
    except (ConfigError, RpcError) as error:
        report(False, "rpc", str(error))

    spec = target or os.environ.get(TARGET_ENV) or file_config.get("target", DEFAULT_TARGET)
    try:
        parsed = parse_target(spec)
        if isinstance(parsed, DuckDBTarget) and not parsed.path.exists():
            # Connecting would create the file; doctor should not change anything.
            report(True, "target", f"{parsed.describe()} does not exist yet; run will create it")
        else:
            with attach(parsed) as warehouse:
                warehouse.connection.execute("SELECT 1").fetchone()
            report(True, "target", f"{parsed.describe()} reachable")
    except TargetError as error:
        report(False, "target", str(error))
    except Exception as error:  # any driver error means "not reachable"; show why
        report(False, "target", f"{type(error).__name__}: {error}")

    dbt_detail = _dbt_versions()
    report("missing" not in dbt_detail, "dbt", dbt_detail)
    if problems:
        typer.echo(f"\n{problems} problem(s) found.")
        raise typer.Exit(1)
    typer.echo("\nall checks passed.")


@app.command()
def run(
    config: ConfigOption = Path(CONFIG_FILENAME),
    chain: ChainOption = None,
    blocks: Annotated[
        str | None,
        typer.Option(help="Block range START:END, END exclusive (as in cryo)."),
    ] = None,
    rpc_url: RpcOption = None,
    target: TargetOption = None,
    data_dir: Annotated[
        Path | None, typer.Option(help="Where Parquet files are kept. Default ./data.")
    ] = None,
    requests_per_second: Annotated[
        int | None, typer.Option(help="Cap on RPC requests per second sent by cryo.")
    ] = None,
    chunk_size: Annotated[
        int | None,
        typer.Option(help="Blocks per Parquet file. Default 100; lower it for flaky RPCs."),
    ] = None,
) -> None:
    """Extract, decode, load and model a block range into star-schema tables."""
    try:
        settings = resolve_settings(
            file_config=load_config_file(config),
            chain=chain,
            blocks=blocks,
            rpc_url=rpc_url,
            target=target,
            data_dir=data_dir,
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
        )
        run_pipeline(settings, typer.echo)
    except USER_ERRORS as error:
        typer.echo(f"\nerror: {error}", err=True)
        raise typer.Exit(1) from None


def _dbt_versions() -> str:
    parts = []
    for package in ("dbt-core", "dbt-duckdb", "dbt-postgres"):
        try:
            parts.append(f"{package} {version(package)}")
        except PackageNotFoundError:
            parts.append(f"{package} missing")
    return ", ".join(parts)
