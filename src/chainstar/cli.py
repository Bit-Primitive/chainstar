"""Command-line interface: quickstart, run, sync, status, export, wallet, doctor and init."""

import json
import os
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

import click
import duckdb
import typer

from chainstar import __version__
from chainstar.chains import EVM, SUPPORTED_CHAINS, Chain, UnknownChainError, get_chain
from chainstar.chunks import ExtractError
from chainstar.config import (
    CONFIG_FILENAME,
    CONFIG_TEMPLATE,
    DEFAULT_DATA_DIR,
    DEFAULT_RPC_ENV,
    DEFAULT_TARGET,
    TARGET_ENV,
    BlockRange,
    ConfigError,
    load_config_file,
    parse_duration,
    resolve_connection,
    resolve_data_dir,
    resolve_rpc,
    resolve_settings,
    rpc_env_name,
    rpc_secrets,
    target_spec,
)
from chainstar.cryo import INSTALL_HINT, CryoError, cryo_version, find_cryo
from chainstar.errors import bug_report_hint, describe, hint_for
from chainstar.export import (
    FORMATS,
    TABLES,
    TRANSFERS_VIEW,
    ExportError,
    ExportRequest,
    export,
    normalize_address,
)
from chainstar.export import resolve_format as resolve_export_format
from chainstar.heads import ChainNode, confirmations_for
from chainstar.lake import configure as configure_lake
from chainstar.load import LoadError, attach
from chainstar.model import ModelError
from chainstar.pipeline import PipelineError, run_pipeline
from chainstar.public_rpc import public_notice
from chainstar.quickstart import DEFAULT_RECENT, cryo_missing_message, next_steps, recent_range
from chainstar.redact import redact_text, redact_url, redact_values
from chainstar.reorg import ReorgTooDeep
from chainstar.rpc import RpcError, identify_node
from chainstar.state import read_states
from chainstar.status import StatusError, collect, render_text, to_json
from chainstar.sync import (
    ChainPlan,
    StopFlag,
    Syncer,
    SyncError,
    SyncPlan,
    describe_rpc,
    rpc_sources,
)
from chainstar.synclog import ALL_CHAINS, SyncLog
from chainstar.targets import (
    MOTHERDUCK_TOKEN_ENVS,
    TARGET_HELP,
    BigQueryTarget,
    ClickHouseTarget,
    DuckDBTarget,
    LakeTarget,
    MotherDuckTarget,
    PostgresTarget,
    SnowflakeTarget,
    Target,
    TargetError,
    credential_secrets,
    parse_target,
)

app = typer.Typer(
    help="Raw blockchain data to clean dimensional tables in your own warehouse, from one command."
    "\n\nNew here? Run `chainstar quickstart`.",
    add_completion=False,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    # Markdown keeps the example blocks in each command's help as written.
    rich_markup_mode="markdown",
)

ConfigOption = Annotated[Path, typer.Option("--config", help="Path to chainstar.yml (optional).")]
ChainOption = Annotated[
    str | None, typer.Option(help=f"Chain to read: {', '.join(SUPPORTED_CHAINS)}.")
]
RpcOption = Annotated[
    str | None,
    typer.Option(
        "--rpc-url",
        help="JSON-RPC URL. Defaults to the CHAINSTAR_RPC_URL environment variable, then to a "
        "free shared public endpoint for the chain.",
    ),
]
TargetOption = Annotated[
    str | None,
    typer.Option(help=f"{TARGET_HELP}. Defaults to {TARGET_ENV}, then chainstar.yml."),
]
RateOption = Annotated[
    int | None,
    typer.Option(
        help="Cap on RPC requests per second. Default: none for your own RPC, a gentle "
        "per-chain rate for the public endpoints."
    ),
]
ChunkOption = Annotated[
    int | None,
    typer.Option(
        help="Blocks (Solana: slots) per Parquet file. Default 100 (10 on public endpoints); "
        "lower it for flaky RPCs."
    ),
]
DataDirOption = Annotated[
    Path | None, typer.Option(help="Where Parquet files are kept. Default ./data.")
]
EveryOption = Annotated[
    str | None,
    typer.Option(
        help="Keep running: sync again every DURATION (30s, 5m, 2h) until stopped with "
        "Ctrl-C or SIGTERM, which stop at the next safe point."
    ),
]
ConfirmationsOption = Annotated[
    str | None,
    typer.Option(
        help="Confirmations a block needs before it is synced, or `finalized` (Ethereum, "
        "Base). Default: ethereum 12, base 60, bitcoin 6; Solana always syncs finalized slots."
    ),
]
JsonLogsOption = Annotated[
    bool, typer.Option("--json-logs", help="Write each log line as a JSON object.")
]

# Errors we expect users to hit (bad input, unreachable services, files). They get
# one line and a hint instead of a traceback; --debug shows the traceback too.
USER_ERRORS = (
    duckdb.Error,
    OSError,
    ConfigError,
    UnknownChainError,
    TargetError,
    RpcError,
    CryoError,
    ExtractError,
    LoadError,
    ModelError,
    PipelineError,
    ExportError,
    SyncError,
    StatusError,
    ReorgTooDeep,
)

_state = {"debug": False}


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
    debug: Annotated[
        bool, typer.Option("--debug", help="Show the full traceback when something fails.")
    ] = False,
) -> None:
    _state["debug"] = debug


@contextmanager
def _friendly_errors(secrets: list[str | None]) -> Iterator[None]:
    """Turn failures into `error:` and `hint:` lines, with RPC URLs and credentials redacted."""
    known = [secret for secret in secrets if secret]
    try:
        yield
    except (typer.Exit, click.exceptions.Abort, click.exceptions.ClickException):
        raise
    except KeyboardInterrupt:
        typer.echo(
            "\nstopped. Finished chunks are kept; run the same command again to continue.",
            err=True,
        )
        raise typer.Exit(130) from None
    except Exception as error:

        def redact(text: str) -> str:
            return redact_values(redact_text(text, known), credential_secrets())

        if _state["debug"]:
            typer.echo(redact(traceback.format_exc()), err=True)
        typer.echo(f"\nerror: {redact(describe(error))}", err=True)
        hint = hint_for(error) if isinstance(error, USER_ERRORS) else bug_report_hint()
        if hint:
            typer.echo(f"hint:  {hint}", err=True)
        raise typer.Exit(1) from None


def _secrets(rpc_url: str | None) -> list[str | None]:
    return [*rpc_secrets(rpc_url, {}), os.environ.get(DEFAULT_RPC_ENV)]


@app.command(
    epilog="**Examples**\n\n```\nchainstar init\nchainstar init my-project.yml --force\n```"
)
def init(
    path: Annotated[Path, typer.Argument(help="Where to write the file.")] = Path(CONFIG_FILENAME),
    force: Annotated[bool, typer.Option(help="Overwrite an existing file.")] = False,
) -> None:
    """Write a commented chainstar.yml to start from."""
    if path.exists() and not force:
        typer.echo(f"{path} already exists; use --force to overwrite it.", err=True)
        raise typer.Exit(1)
    with _friendly_errors([]):
        path.write_text(CONFIG_TEMPLATE)
    typer.echo(f"wrote {path}")
    typer.echo(
        "next: choose a block range, optionally set CHAINSTAR_RPC_URL to your own RPC, "
        "then run `chainstar doctor`."
    )


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar doctor --chain base\n"
    'chainstar doctor --chain ethereum --rpc-url "$MY_RPC"\n'
    "chainstar doctor --target postgres:postgresql://me@localhost:5432/chain\n```"
)
def doctor(
    config: ConfigOption = Path(CONFIG_FILENAME),
    chain: ChainOption = None,
    rpc_url: RpcOption = None,
    target: TargetOption = None,
) -> None:
    """Check that cryo (EVM chains), the RPC endpoint and the target warehouse are ready."""
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

    expected_chain = None
    chain_name = chain or file_config.get("chain")
    if chain_name:
        try:
            expected_chain = get_chain(chain_name)
        except UnknownChainError as error:
            report(False, "chain", str(error))

    executable = find_cryo()
    if expected_chain and expected_chain.family != EVM:
        report(True, "cryo", f"not needed for {expected_chain.name}")
    elif executable:
        report(True, "cryo", f"{cryo_version()} at {executable}")
    else:
        report(False, "cryo", f"not found on PATH; {INSTALL_HINT}")

    # Without --chain, assume an EVM endpoint, as chainstar always has.
    rpc_chain = expected_chain or SUPPORTED_CHAINS["ethereum"]
    rpc = resolve_rpc(rpc_url, file_config, rpc_chain)
    try:
        node = identify_node(rpc_chain, rpc.url)
        detail = f"{redact_url(rpc.url)} {node.detail}"
        if rpc.public:
            detail += " (free shared public endpoint)"
        if expected_chain and not node.matches:
            report(False, "rpc", f"{detail}; expected {node.expected}")
        else:
            report(True, "rpc", detail)
    except RpcError as error:
        report(False, "rpc", str(error))

    spec = target or os.environ.get(TARGET_ENV) or file_config.get("target", DEFAULT_TARGET)
    parsed = None
    try:
        parsed = parse_target(spec)
        report(True, "target", _check_target(parsed))
    except TargetError as error:
        report(False, "target", str(error))
    except Exception as error:  # any driver error means "not reachable"; show why
        hint = hint_for(error)
        message = redact_values(f"{type(error).__name__}: {error}", credential_secrets(parsed))
        report(False, "target", message + (f" ({hint})" if hint else ""))

    dbt_detail = _dbt_versions(parsed)
    report("missing" not in dbt_detail, "dbt", dbt_detail)
    if problems:
        typer.echo(f"\n{problems} problem(s) found.")
        raise typer.Exit(1)
    typer.echo("\nall checks passed.")


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar run --chain base --blocks 52900000:52900020\n"
    'chainstar run --chain ethereum --blocks 20000000:20000100 --rpc-url "$MY_RPC"\n'
    "chainstar run --chain solana --blocks 459564290:459564310   # slots\n"
    "chainstar run --chain bitcoin --blocks 972460:972461 "
    "--target postgres:postgresql://me@localhost:5432/chain\n```\n\n"
    "Each run loads every chunk already saved for the chain, so ranges add up."
)
def run(
    config: ConfigOption = Path(CONFIG_FILENAME),
    chain: ChainOption = None,
    blocks: Annotated[
        str | None,
        typer.Option(help="Block range START:END, END exclusive (as in cryo). Slots on Solana."),
    ] = None,
    rpc_url: RpcOption = None,
    target: TargetOption = None,
    data_dir: DataDirOption = None,
    requests_per_second: RateOption = None,
    chunk_size: ChunkOption = None,
) -> None:
    """Extract, decode, load and model a block range into star-schema tables."""
    with _friendly_errors(_secrets(rpc_url)):
        file_config = load_config_file(config)
        settings = resolve_settings(
            file_config=file_config,
            chain=chain,
            blocks=blocks,
            rpc_url=rpc_url,
            target=target,
            data_dir=data_dir,
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
        )
        run_pipeline(settings, typer.echo, rpc_env_name(file_config))


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar sync --chain base --from-recent 100        # first sync: the latest 100 blocks\n"
    "chainstar sync --chain base                          # later: continue where it stopped\n"
    "chainstar sync --chain ethereum --chain solana --every 5m\n"
    "chainstar sync --chain bitcoin --from 972000 "
    "--target postgres:postgresql://me@localhost:5432/chain\n```\n\n"
    "A chain's first sync needs --from or --from-recent; later syncs resume from the "
    "warehouse's own record (chainstar_state.sync_progress). Blocks are synced only once "
    "they have enough confirmations, and a deeper reorganisation is detected and rolled "
    "back. With several chains, give each its RPC URL in `CHAINSTAR_RPC_URL_<CHAIN>`, e.g. "
    "CHAINSTAR_RPC_URL_BASE."
)
def sync(
    chain: Annotated[
        list[str] | None,
        typer.Option(
            "--chain",
            help="Chain to sync; repeat for several. Default: `chains` in chainstar.yml, "
            "else every chain already synced into the target.",
        ),
    ] = None,
    from_block: Annotated[
        int | None,
        typer.Option("--from", help="First sync only: start at this block (Solana: slot)."),
    ] = None,
    from_recent: Annotated[
        int | None,
        typer.Option(
            "--from-recent",
            help="First sync only: start this many blocks (Solana: slots) before the safe head.",
        ),
    ] = None,
    every: EveryOption = None,
    confirmations: ConfirmationsOption = None,
    full_refresh: Annotated[
        bool,
        typer.Option(
            "--full-refresh", help="Rebuild the star-schema tables in full, not incrementally."
        ),
    ] = False,
    json_logs: JsonLogsOption = False,
    rpc_url: Annotated[
        str | None,
        typer.Option(
            "--rpc-url",
            help="JSON-RPC URL, for a single chain. Defaults to `CHAINSTAR_RPC_URL_<CHAIN>`, then "
            "CHAINSTAR_RPC_URL, then the chain's free shared public endpoint.",
        ),
    ] = None,
    target: TargetOption = None,
    data_dir: DataDirOption = None,
    requests_per_second: RateOption = None,
    chunk_size: ChunkOption = None,
    config: ConfigOption = Path(CONFIG_FILENAME),
) -> None:
    """Bring the warehouse up to date from where it left off, once or on an interval."""
    with _friendly_errors(_secrets(rpc_url)):
        file_config = load_config_file(config)
        parsed_target = parse_target(target_spec(target, file_config))
        chains = _sync_chains(chain, file_config, parsed_target)
        if rpc_url and len(chains) > 1:
            raise ConfigError(
                "--rpc-url names one endpoint, but several chains are being synced; set "
                "CHAINSTAR_RPC_URL_<CHAIN> for each instead, e.g. CHAINSTAR_RPC_URL_BASE"
            )
        if from_block is not None and len(chains) > 1:
            raise ConfigError("--from is a block of one chain; with several, use --from-recent N")
        plan = SyncPlan(
            target=parsed_target,
            data_dir=resolve_data_dir(data_dir, file_config),
            chains=tuple(
                _chain_plan(c, file_config, rpc_url, requests_per_second, chunk_size, confirmations)
                for c in chains
            ),
            from_block=from_block,
            from_recent=_check_from(from_block, from_recent),
            full_refresh=full_refresh,
        )
        seconds = parse_duration(every) if every else None
        log = SyncLog(typer.echo, json_lines=json_logs, secrets=rpc_secrets(rpc_url, file_config))
    _run_sync(plan, log, seconds, rpc_env_name(file_config), rpc_url)


def _sync_chains(names: list[str] | None, file_config: dict, target: Target) -> list[Chain]:
    """--chain, then chainstar.yml's chains (or chain), then the chains already synced."""
    configured = file_config.get("chains") or (
        [file_config["chain"]] if file_config.get("chain") else []
    )
    chosen = names or configured
    if not chosen and not (isinstance(target, DuckDBTarget) and not target.path.exists()):
        with attach(target) as warehouse:
            chosen = list(read_states(warehouse))
    if not chosen:
        raise ConfigError(
            "no chain to sync: pass --chain (repeat it for several) or set `chains` in "
            "chainstar.yml"
        )
    selected = [get_chain(name) for name in chosen]
    return list(dict.fromkeys(selected))


def _check_from(from_block: int | None, from_recent: int | None) -> int | None:
    if from_block is not None and from_recent is not None:
        raise ConfigError("give --from or --from-recent, not both")
    if from_block is not None and from_block < 0:
        raise ConfigError(f"--from must be a block number, got {from_block}")
    if from_recent is not None and from_recent < 1:
        raise ConfigError(f"--from-recent must be at least 1, got {from_recent}")
    return from_recent


def _chain_plan(
    chain: Chain,
    file_config: dict,
    rpc_url: str | None,
    requests_per_second: int | None,
    chunk_size: int | None,
    confirmations: str | None,
) -> ChainPlan:
    return ChainPlan(
        chain=chain,
        connection=resolve_connection(
            file_config=file_config,
            chain=chain,
            rpc_url=rpc_url,
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
        ),
        confirmations=confirmations_for(chain, confirmations, file_config.get("confirmations")),
    )


def _run_sync(
    plan: SyncPlan,
    log: SyncLog,
    every: int | None,
    rpc_env: str,
    rpc_url: str | None,
    after_pass=None,
) -> None:
    """Run a sync plan with its header lines, signal handling and exit code."""
    log.info(
        ALL_CHAINS,
        "start",
        f"chainstar {__version__}; target {plan.target.describe()}; data {plan.data_dir}"
        + (f"; every {every}s" if every else ""),
    )
    for chain_plan in plan.chains:
        log.info(chain_plan.chain.name, "rpc", describe_rpc(chain_plan))
        if chain_plan.connection.rpc.public:
            log.info(chain_plan.chain.name, "note", public_notice(chain_plan.chain, rpc_env))
    stop = StopFlag()
    syncer = Syncer(plan, rpc_sources(plan), log, stop, debug=_state["debug"])
    with _friendly_errors(_secrets(rpc_url)), stop.handling_signals():
        ok = syncer.run(every, after_pass)
    raise typer.Exit(0 if ok else 1)


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar status\n"
    "chainstar status --chain base --json\n"
    "chainstar status --max-lag 15m          # exits 1 if any chain is further behind\n```\n\n"
    "Lag in time is measured from the newest synced block's own time, so it includes the "
    "confirmation depth (about an hour on Bitcoin); set --max-lag per chain accordingly."
)
def status(
    chain: Annotated[
        list[str] | None,
        typer.Option("--chain", help="Only this chain; repeat for several. Default: all synced."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print JSON instead of text.")
    ] = False,
    max_lag: Annotated[
        str | None,
        typer.Option(
            help="Exit with status 1 when any chain's newest synced block is older than "
            "DURATION (30s, 15m, 2h), for health checks and monitoring."
        ),
    ] = None,
    rpc_url: Annotated[
        str | None,
        typer.Option("--rpc-url", help="JSON-RPC URL for the head, for a single chain."),
    ] = None,
    target: TargetOption = None,
    config: ConfigOption = Path(CONFIG_FILENAME),
) -> None:
    """How far each chain is synced, the chain's head, the lag, and rows per table."""
    with _friendly_errors(_secrets(rpc_url)):
        file_config = load_config_file(config)
        names = [get_chain(name).name for name in chain or []]
        if rpc_url and len(names) != 1:
            raise ConfigError("--rpc-url applies to one chain; pass exactly one --chain with it")

        def node_for(selected: Chain) -> ChainNode:
            rpc = resolve_rpc(rpc_url, file_config, selected)
            return ChainNode(selected, rpc.url, None)

        report = collect(
            parse_target(target_spec(target, file_config)),
            names,
            node_for,
            parse_duration(max_lag) if max_lag else None,
        )
        if not report.chains:
            raise StatusError(
                f"nothing has been synced into {report.target} yet; "
                "run `chainstar sync --chain CHAIN --from-recent N` first"
            )
        secrets = rpc_secrets(rpc_url, file_config)
        if json_output:
            typer.echo(redact_text(json.dumps(to_json(report), indent=2), secrets))
        else:
            for line in render_text(report):
                typer.echo(redact_text(line, secrets))
    if report.healthy is False:
        raise typer.Exit(1)


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar quickstart                                  # asks a few questions\n"
    "chainstar quickstart --chain base --yes               # no questions, for scripts\n"
    "chainstar quickstart --chain solana --recent 50 --yes\n"
    'docker run --rm -v "$PWD:/work" ghcr.io/bit-primitive/chainstar quickstart --yes\n```'
)
def quickstart(
    chain: Annotated[
        str | None,
        typer.Option(help=f"Chain to read: {', '.join(SUPPORTED_CHAINS)}. Default base."),
    ] = None,
    recent: Annotated[
        int | None,
        typer.Option(
            help="How many of the latest blocks (Solana: slots) to fetch. "
            "Default 20, or 1 on Bitcoin."
        ),
    ] = None,
    rpc_url: RpcOption = None,
    target: Annotated[
        str, typer.Option(help=f"{TARGET_HELP}. Default: a DuckDB file in this folder.")
    ] = DEFAULT_TARGET,
    data_dir: Annotated[Path, typer.Option(help="Where Parquet files are kept.")] = Path(
        DEFAULT_DATA_DIR
    ),
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Ask nothing; use the options and defaults.")
    ] = False,
) -> None:
    """A guided first run: fetch a few of the latest blocks, build the tables, show what next.

    Without --rpc-url or CHAINSTAR_RPC_URL it uses a free shared public endpoint, so
    nothing needs setting up first.
    """
    # Ask only when someone is there to answer; scripts and `docker run` without
    # -it have no terminal, and get the defaults as with --yes.
    interactive = not yes and sys.stdin.isatty()
    with _friendly_errors(_secrets(rpc_url)):
        if interactive:
            typer.echo("Welcome to chainstar. Two questions, then it fetches and builds.\n")
            chain = typer.prompt(
                f"Which chain? ({', '.join(SUPPORTED_CHAINS)})",
                default=chain or "base",
                type=click.Choice(list(SUPPORTED_CHAINS)),
            )
        selected = get_chain(chain or "base")
        if selected.family == EVM and find_cryo() is None:
            typer.echo(cryo_missing_message(selected), err=True)
            raise typer.Exit(1)
        count = recent or DEFAULT_RECENT[selected.name]
        if interactive:
            count = typer.prompt(f"How many of the latest {selected.block_noun}?", default=count)

        blocks = _recent_blocks(selected, count, rpc_url)
        settings = resolve_settings(
            file_config={},
            chain=selected.name,
            blocks=str(blocks),
            rpc_url=rpc_url,
            target=target,
            data_dir=data_dir,
            requests_per_second=None,
            chunk_size=None,
        )
        if interactive and not typer.confirm(
            f"Fetch {selected.name} {selected.block_noun} {blocks.start:,} to {blocks.end - 1:,} "
            f"into {settings.target.describe()}?",
            default=True,
        ):
            raise typer.Exit(0)
        typer.echo("")
        run_pipeline(settings, typer.echo)
        typer.echo("")
        for line in next_steps(selected, settings.target, settings.data_dir):
            typer.echo(line)


def _recent_blocks(chain: Chain, count: int, rpc_url: str | None) -> BlockRange:
    """The latest `count` blocks (Solana: slots), read from the chain head."""
    rpc = resolve_rpc(rpc_url, {}, chain)
    return recent_range(chain, identify_node(chain, rpc.url).head, count)


@app.command(
    name="export",
    epilog="**Examples**\n\n```\n"
    "chainstar export                                   # transfers.csv\n"
    "chainstar export transfers --output transfers.xlsx\n"
    "chainstar export transfers --chain base --wallet 0xYourAddress --output mine.xlsx\n"
    "chainstar export transfers --token 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 --limit 1000\n"
    "chainstar export fct_fees --format parquet\n```\n\n"
    f"Tables: {', '.join(TABLES)}. `transfers` is fct_transfers with each token's "
    "symbol and name. amount_raw stays exact text; amount is a number. Excel sheets "
    "hold at most 1,048,575 rows; larger exports are refused with a suggestion.",
)
def export_command(
    table: Annotated[
        str, typer.Argument(help=f"What to export: {', '.join(TABLES)}.")
    ] = TRANSFERS_VIEW,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="File to write. Default <table>.<format> here."),
    ] = None,
    format_name: Annotated[
        str | None,
        typer.Option(
            "--format", help=f"{', '.join(FORMATS)}. Default: from --output's extension, or csv."
        ),
    ] = None,
    chain: Annotated[str | None, typer.Option(help="Only this chain's rows.")] = None,
    wallet: Annotated[
        str | None, typer.Option(help="Only rows where this address sends or receives.")
    ] = None,
    token: Annotated[
        str | None,
        typer.Option(help="Only this token: its contract address, SPL mint, or `native`."),
    ] = None,
    limit: Annotated[int | None, typer.Option(help="At most this many rows.")] = None,
    target: TargetOption = None,
    config: ConfigOption = Path(CONFIG_FILENAME),
) -> None:
    """Save a table as a CSV, Excel (.xlsx) or Parquet file, optionally filtered."""
    with _friendly_errors([]):
        file_config = load_config_file(config)
        spec = target or os.environ.get(TARGET_ENV) or file_config.get("target", DEFAULT_TARGET)
        path, chosen = resolve_export_format(output, format_name, table)
        request = ExportRequest(table, path, chosen, chain, wallet, token, limit)
        rows = export(parse_target(spec), request)
        typer.echo(f"wrote {rows:,} rows to {path}")
        if rows == 0:
            typer.echo("(no rows matched; check the filters, or load more blocks first)")


@app.command(
    epilog="**Examples**\n\n```\n"
    "chainstar wallet 0xYourAddress --chain base --recent 1000\n"
    "chainstar wallet 0xYourAddress --chain ethereum --blocks 20000000:20001000 "
    '--rpc-url "$MY_RPC" --blocks-per-request 500\n'
    "chainstar wallet YourSolanaAddress --chain solana --recent 50 --output mine.csv\n"
    "chainstar wallet 0xYourAddress --chain base --recent 1000 --sync --every 10m\n```\n\n"
    "On Ethereum and Base the RPC node filters the transfers, so only this address's "
    "are downloaded: two requests per --blocks-per-request blocks, plus one per block "
    "that has a transfer. Bitcoin and Solana nodes cannot filter by address, so every "
    "block in the range is read. Each wallet gets its own folder under ./data, so it "
    "never mixes with your main warehouse. With --sync the history is kept current: "
    "the first run starts at --recent N (or --from BLOCK), each later run continues "
    "where the last stopped, and the spreadsheet is rewritten every time."
)
def wallet(
    address: Annotated[str, typer.Argument(help="The address whose transfers you want.")],
    chain: Annotated[
        str, typer.Option(help=f"Chain to read: {', '.join(SUPPORTED_CHAINS)}.")
    ] = "ethereum",
    blocks: Annotated[
        str | None,
        typer.Option(help="Block range START:END, END exclusive. Slots on Solana."),
    ] = None,
    recent: Annotated[
        int | None,
        typer.Option(help="Instead of --blocks: this many of the latest blocks (Solana: slots)."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output", "-o", help="Spreadsheet to write. Default <chain>-<address>.xlsx here."
        ),
    ] = None,
    rpc_url: RpcOption = None,
    blocks_per_request: Annotated[
        int,
        typer.Option(
            help="EVM only: blocks per filtered log request. Keep 1 on public endpoints; "
            "many paid providers accept hundreds or more."
        ),
    ] = 1,
    requests_per_second: RateOption = None,
    chunk_size: ChunkOption = None,
    keep_synced: Annotated[
        bool,
        typer.Option(
            "--sync",
            help="Keep the history current: continue from where the last run stopped, "
            "and rewrite the spreadsheet each time.",
        ),
    ] = False,
    from_block: Annotated[
        int | None,
        typer.Option("--from", help="With --sync, first run only: start at this block."),
    ] = None,
    every: EveryOption = None,
    confirmations: ConfirmationsOption = None,
    json_logs: JsonLogsOption = False,
) -> None:
    """All token transfers sent or received by one address in a range, as a spreadsheet."""
    if keep_synced:
        _sync_wallet(
            address=address,
            chain=chain,
            blocks=blocks,
            recent=recent,
            from_block=from_block,
            output=output,
            rpc_url=rpc_url,
            blocks_per_request=blocks_per_request,
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
            every=every,
            confirmations=confirmations,
            json_logs=json_logs,
        )
    with _friendly_errors(_secrets(rpc_url)):
        sync_only = {"--from": from_block, "--every": every, "--confirmations": confirmations}
        used = [flag for flag, value in sync_only.items() if value is not None]
        if used or json_logs:
            raise ConfigError(f"{', '.join(used or ['--json-logs'])} needs --sync")
        selected = get_chain(chain)
        if (blocks is None) == (recent is None):
            raise ConfigError("give either --blocks START:END or --recent N")
        if selected.family == EVM and find_cryo() is None:
            typer.echo(cryo_missing_message(selected), err=True)
            raise typer.Exit(1)
        if recent is not None:
            blocks = str(_recent_blocks(selected, recent, rpc_url))
        normalized = normalize_address(address)
        folder = _wallet_folder(selected, normalized)
        settings = resolve_settings(
            file_config={},
            chain=selected.name,
            blocks=blocks,
            rpc_url=rpc_url,
            target=f"duckdb:{folder / 'warehouse.duckdb'}",
            data_dir=folder,
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
            wallet=normalized,
            blocks_per_request=blocks_per_request,
        )
        run_pipeline(settings, typer.echo)
        path, chosen = resolve_export_format(
            output or Path(f"{selected.name}-{normalized}.xlsx"), None, TRANSFERS_VIEW
        )
        rows = export(
            settings.target,
            ExportRequest(TRANSFERS_VIEW, path, chosen, chain=selected.name, wallet=normalized),
        )
        typer.echo("")
        typer.echo(f"wrote {rows:,} transfers of {normalized} to {path}")


def _wallet_folder(chain: Chain, address: str) -> Path:
    # Its own folder and warehouse, so a wallet never mixes with the main warehouse.
    return Path(DEFAULT_DATA_DIR) / f"wallet-{chain.name}-{address}"


def _sync_wallet(
    *,
    address: str,
    chain: str,
    blocks: str | None,
    recent: int | None,
    from_block: int | None,
    output: Path | None,
    rpc_url: str | None,
    blocks_per_request: int,
    requests_per_second: int | None,
    chunk_size: int | None,
    every: str | None,
    confirmations: str | None,
    json_logs: bool,
) -> None:
    """`chainstar wallet --sync`: a sync of one address, re-exporting after each pass."""
    with _friendly_errors(_secrets(rpc_url)):
        selected = get_chain(chain)
        if blocks is not None:
            raise ConfigError(
                "--blocks is a fixed range; with --sync give --recent N or --from BLOCK for "
                "the first run, and later runs continue on their own"
            )
        if selected.family == EVM and find_cryo() is None:
            typer.echo(cryo_missing_message(selected), err=True)
            raise typer.Exit(1)
        normalized = normalize_address(address)
        folder = _wallet_folder(selected, normalized)
        plan = SyncPlan(
            target=DuckDBTarget(folder / "warehouse.duckdb"),
            data_dir=folder,
            chains=(
                _chain_plan(selected, {}, rpc_url, requests_per_second, chunk_size, confirmations),
            ),
            from_block=from_block,
            from_recent=_check_from(from_block, recent),
            wallet=normalized,
            blocks_per_request=blocks_per_request,
        )
        seconds = parse_duration(every) if every else None
        log = SyncLog(typer.echo, json_lines=json_logs, secrets=rpc_secrets(rpc_url, {}))
        path, chosen = resolve_export_format(
            output or Path(f"{selected.name}-{normalized}.xlsx"), None, TRANSFERS_VIEW
        )
        request = ExportRequest(TRANSFERS_VIEW, path, chosen, selected.name, normalized)

    def export_wallet() -> None:
        rows = export(plan.target, request)
        log.info(selected.name, "export", f"wrote {rows:,} transfers of {normalized} to {path}")

    _run_sync(plan, log, seconds, DEFAULT_RPC_ENV, rpc_url, export_wallet)


def _check_target(target: Target) -> str:
    """Reach the target without changing it; say what was found, or raise why not."""
    if isinstance(target, DuckDBTarget) and not target.path.exists():
        # Connecting would create the file; doctor should not change anything.
        return f"{target.describe()} does not exist yet; run will create it"
    if isinstance(target, MotherDuckTarget) and not any(
        os.environ.get(name) for name in MOTHERDUCK_TOKEN_ENVS
    ):
        raise ConfigError("set the motherduck_token environment variable to your MotherDuck token")
    if isinstance(target, LakeTarget):
        return _check_lake(target)
    with attach(target) as warehouse:
        if isinstance(target, ClickHouseTarget):
            version_row = warehouse.query("SELECT version()")
            return f"{target.describe()} reachable, ClickHouse {version_row[0][0]}"
        if isinstance(target, SnowflakeTarget):
            version_row = warehouse.query("SELECT CURRENT_VERSION()")
            return f"{target.describe()} reachable, Snowflake {version_row[0][0]}"
        warehouse.query("SELECT 1")
    return f"{target.describe()} reachable"


def _check_lake(target: LakeTarget) -> str:
    engine = target.engine
    if not engine.exists() and not os.access(_existing_parent(engine), os.W_OK):
        raise OSError(f"cannot create the lake's engine file {engine}: folder not writable")
    engine_note = f"engine {engine}" + ("" if engine.exists() else " (created on first run)")
    if not target.remote:
        location = Path(target.location)
        if not location.exists() and not os.access(_existing_parent(location), os.W_OK):
            raise OSError(f"cannot create the lake folder {location}: folder not writable")
        return f"{target.describe()} usable; {engine_note}"
    connection = duckdb.connect()
    try:
        configure_lake(connection, target)
        # Listing is what readers do; it needs the same credentials as writing.
        row = connection.execute("SELECT count(*) FROM glob(?)", [target.path("**")]).fetchone()
    finally:
        connection.close()
    files = int(row[0]) if row else 0
    return f"{target.describe()} reachable, {files:,} files listed; {engine_note}"


def _existing_parent(path: Path) -> Path:
    path = path.resolve()
    while not path.exists():
        path = path.parent
    return path


# The dbt adapter each kind of target needs, and the extra that installs it.
DBT_ADAPTERS = {
    ClickHouseTarget: ("dbt-clickhouse", "clickhouse"),
    BigQueryTarget: ("dbt-bigquery", "bigquery"),
    SnowflakeTarget: ("dbt-snowflake", "snowflake"),
    PostgresTarget: ("dbt-postgres", None),
}


def _dbt_versions(target: Target | None = None) -> str:
    adapter, extra = DBT_ADAPTERS.get(type(target), ("dbt-duckdb", None))
    packages = ["dbt-core", adapter] if target else ["dbt-core", "dbt-duckdb", "dbt-postgres"]
    parts = []
    for package in packages:
        try:
            parts.append(f"{package} {version(package)}")
        except PackageNotFoundError:
            install = f"; pip install 'chainstar[{extra}]'" if package == adapter and extra else ""
            parts.append(f"{package} missing{install}")
    return ", ".join(parts)
