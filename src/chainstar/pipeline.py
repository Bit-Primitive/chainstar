"""`chainstar run`: the four stages in order, with progress and a final summary."""

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from chainstar import __version__, bitcoin, solana
from chainstar.chains import BITCOIN, EVM, SOLANA
from chainstar.config import DEFAULT_RPC_ENV, RunSettings
from chainstar.datasets import RANGED_DATASETS, TRANSACTIONS
from chainstar.extract import (
    decode_token_metadata,
    extract_blocks_with_transfers,
    extract_dataset,
    extract_wallet_transfers,
)
from chainstar.lake import publish as publish_lake
from chainstar.load import RAW_SCHEMA, held_ranges, load_raw, table_row_counts
from chainstar.model import MART_SCHEMA, MART_TABLES, ModelError, build_models
from chainstar.public_rpc import public_notice
from chainstar.redact import redact_url
from chainstar.rpc import identify_node
from chainstar.targets import LakeTarget

# Extractors for the chains cryo does not support. Each returns rows per dataset.
JSON_RPC_EXTRACTORS = {BITCOIN: bitcoin.extract, SOLANA: solana.extract}

Echo = Callable[[str], None]


class PipelineError(RuntimeError):
    pass


@contextmanager
def _timed() -> Iterator[Callable[[], str]]:
    started = time.monotonic()
    yield lambda: f"{time.monotonic() - started:.1f}s"


def _line(echo: Echo, stage: str, what: str, detail: str, elapsed: str) -> None:
    echo(f"{stage:<8} {what:<16} {detail:<34} {elapsed:>7}".rstrip())


def describe_spans(spans: list[tuple[int, int]], noun: str) -> str:
    """e.g. "blocks 100 to 139 (40 blocks)"; several spans when earlier runs left gaps."""
    total = sum(last - first + 1 for first, last in spans)
    shown = ", ".join(f"{first:,} to {last:,}" for first, last in spans[:3])
    if len(spans) > 3:
        shown += f" and {len(spans) - 3} more spans"
    return f"{noun} {shown} ({total:,} {noun if total != 1 else noun[:-1]})"


def run_pipeline(settings: RunSettings, echo: Echo, rpc_env: str = DEFAULT_RPC_ENV) -> None:
    chain = settings.chain
    identity = f"chain id {chain.chain_id}" if chain.family == EVM else f"network {chain.network}"
    echo(f"chainstar {__version__}")
    echo(f"chain    {chain.name} ({identity})")
    echo(
        f"{chain.block_noun:<8} {settings.blocks} "
        f"({len(settings.blocks):,} {chain.block_noun}, end exclusive)"
    )
    echo(f"rpc      {redact_url(settings.rpc_url)}")
    if settings.rpc_is_public:
        echo(f"note     {public_notice(chain, rpc_env)}")
    echo(f"target   {settings.target.describe()}")
    if settings.wallet:
        echo(f"wallet   {settings.wallet}")
        if chain.family != EVM:
            # Neither Bitcoin Core nor Solana RPC can filter blocks by address.
            echo(
                f"note     a {chain.name} node cannot filter by address, so every "
                f"{chain.block_noun[:-1]} in the range is read; the export keeps this wallet's rows"
            )
    echo("")

    # Checked before extracting: nothing downstream could tell one chain's data
    # from another's.
    node = identify_node(chain, settings.rpc_url)
    if not node.matches:
        raise PipelineError(
            f"RPC reports {node.detail}, but --chain is {node.expected}; "
            f"point --rpc-url at an endpoint for {chain.name}, or change --chain"
        )
    if settings.blocks.end - 1 > node.head:
        # Solana's getBlocks lists only finalized slots, so a range past the head
        # would be saved as empty chunks that later runs skip; cryo would fail on
        # blocks that do not exist yet. Refuse it up front instead.
        raise PipelineError(
            f"END {settings.blocks.end} is past the chain head ({node.detail}); "
            f"END must be at most {node.head + 1}, since END is exclusive"
        )

    with _timed() as total:
        if chain.family == EVM:
            _extract_evm(settings, echo)
        else:
            with _timed() as elapsed:
                counts = JSON_RPC_EXTRACTORS[chain.family](settings)
            # One pass over the range fills every dataset, so the time is shown once.
            # The chain is in the header, so "bitcoin_inputs" is shown as "inputs".
            for index, (name, rows) in enumerate(counts.items()):
                label = name.removeprefix(f"{chain.family}_")
                _line(echo, "extract", label, f"{rows:,} rows", elapsed() if index == 0 else "")

        with _timed() as elapsed:
            raw_counts = load_raw(settings.target, settings.data_dir, chain)
        _line(
            echo,
            "load",
            RAW_SCHEMA,
            f"{len(raw_counts)} tables, {sum(raw_counts.values()):,} rows",
            elapsed(),
        )
        _describe_load(settings, echo)

        with _timed() as elapsed:
            # A run loads every chunk on disk, which may include blocks below what the
            # incremental tables already hold, so it always rebuilds them in full.
            result = build_models(settings.target, settings.data_dir / ".dbt", full_refresh=True)
        if result.failures:
            echo("")
            for failure in result.failures:
                echo(f"  {failure}")
            raise ModelError(
                f"dbt build failed ({len(result.failures)} problem(s)); full log: {result.log_path}"
            )
        detail = f"{result.models} models, {result.tests_passed} tests passed"
        if result.warnings:
            detail += f", {result.warnings} warnings"
        _line(echo, "model", "dbt build", detail, elapsed())

        if isinstance(settings.target, LakeTarget):
            with _timed() as elapsed:
                published = publish_lake(settings.target)
            _line(echo, "publish", "parquet lake", published.describe(), elapsed())

    counts = table_row_counts(settings.target, MART_SCHEMA, MART_TABLES)
    echo("")
    echo(f"done in {total()}")
    echo("")
    # Counts cover every chain loaded into this warehouse, not just this run's.
    echo(f"tables   {settings.target.describe()} (schema {MART_SCHEMA}, all chains)")
    width = max(len(table) for table in MART_TABLES)
    for table in MART_TABLES:
        echo(f"  {table:<{width}} {counts[table]:>12,}")
    echo(f"parquet  {settings.data_dir / chain.name}")


def _describe_load(settings: RunSettings, echo: Echo) -> None:
    """Say which blocks the warehouse now holds for the chain.

    A load takes every chunk saved in the chain's data directory, so ranges from
    earlier runs are loaded too and the raw row count covers all of them.
    """
    chain = settings.chain
    spans = held_ranges(settings.data_dir, chain)
    if not spans:
        return
    echo(f"{'':<8} {chain.name} now in the warehouse: {describe_spans(spans, chain.block_noun)}")
    requested = [(settings.blocks.start, settings.blocks.end - 1)]
    if spans != requested:
        echo(
            f"{'':<8} (each run loads every chunk saved in {settings.data_dir / chain.name}, "
            "so earlier runs' blocks are included)"
        )


def _extract_evm(settings: RunSettings, echo: Echo) -> None:
    if settings.wallet:
        _extract_evm_wallet(settings, settings.wallet, echo)
        return
    for dataset in RANGED_DATASETS:
        with _timed() as elapsed:
            rows = extract_dataset(settings, dataset)
        _line(echo, "extract", dataset.name, f"{rows:,} rows", elapsed())

    _decode(settings, echo)


def _extract_evm_wallet(settings: RunSettings, wallet: str, echo: Echo) -> None:
    with _timed() as elapsed:
        transfers = extract_wallet_transfers(settings, wallet)
    _line(echo, "extract", "erc20_transfers", f"{transfers:,} sent or received", elapsed())
    with _timed() as elapsed:
        blocks = extract_blocks_with_transfers(settings)
    _line(echo, "extract", "blocks", f"{blocks:,} new (blocks with transfers)", elapsed())
    # Fees need every transaction of the range; a wallet history does not.
    _line(echo, "extract", TRANSACTIONS.name, "skipped for a wallet", "")
    _decode(settings, echo)


def _decode(settings: RunSettings, echo: Echo) -> None:
    with _timed() as elapsed:
        metadata = decode_token_metadata(settings)
    cached = metadata.tokens_seen - metadata.tokens_fetched
    _line(
        echo,
        "decode",
        "erc20_metadata",
        f"{metadata.tokens_seen:,} tokens ({metadata.tokens_fetched:,} new, {cached:,} cached)",
        elapsed(),
    )
