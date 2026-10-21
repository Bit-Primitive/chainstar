"""`chainstar run`: the four stages in order, with progress and a final summary."""

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from chainstar import __version__, bitcoin, solana
from chainstar.chains import BITCOIN, EVM, SOLANA
from chainstar.config import RunSettings
from chainstar.datasets import RANGED_DATASETS
from chainstar.extract import decode_token_metadata, extract_dataset
from chainstar.load import RAW_SCHEMA, load_raw, table_row_counts
from chainstar.model import MART_SCHEMA, MART_TABLES, ModelError, build_models
from chainstar.redact import redact_url
from chainstar.rpc import identify_node

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


def run_pipeline(settings: RunSettings, echo: Echo) -> None:
    chain = settings.chain
    identity = f"chain id {chain.chain_id}" if chain.family == EVM else f"network {chain.network}"
    echo(f"chainstar {__version__}")
    echo(f"chain    {chain.name} ({identity})")
    echo(
        f"{chain.block_noun:<8} {settings.blocks} "
        f"({len(settings.blocks):,} {chain.block_noun}, end exclusive)"
    )
    echo(f"rpc      {redact_url(settings.rpc_url)}")
    echo(f"target   {settings.target.describe()}")
    echo("")

    # Checked before extracting: nothing downstream could tell one chain's data
    # from another's.
    node = identify_node(chain, settings.rpc_url)
    if not node.matches:
        raise PipelineError(f"RPC reports {node.detail}, but --chain is {node.expected}")
    if chain.family != EVM and settings.blocks.end - 1 > node.head:
        # Solana's getBlocks lists only finalized slots, so a range past the head
        # would be saved as empty chunks that later runs skip; refuse it instead.
        raise PipelineError(
            f"END {settings.blocks.end} is past the node's head ({node.detail}); "
            f"END must be at most {node.head + 1}"
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

        with _timed() as elapsed:
            result = build_models(settings.target, settings.data_dir / ".dbt")
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


def _extract_evm(settings: RunSettings, echo: Echo) -> None:
    for dataset in RANGED_DATASETS:
        with _timed() as elapsed:
            rows = extract_dataset(settings, dataset)
        _line(echo, "extract", dataset.name, f"{rows:,} rows", elapsed())

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
