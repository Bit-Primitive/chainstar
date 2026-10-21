"""`chainstar sync`: bring a warehouse up to date, once or on an interval.

For each chain a sync pass:

1. asks the node for its head and works out the safe head (heads.py), the
   newest block with enough confirmations;
2. checks that the last synced block still has the hash it had when synced. If
   not, the chain reorganised deeper than the confirmation depth: the newest
   stored block that still matches is found (reorg.py), everything after it
   (chunk files, raw rows, fact rows) is deleted, and that range is synced again;
3. extracts the blocks from where it left off to the safe head in batches, and
   loads each batch in one transaction together with the progress it makes, so a
   crash never records blocks that were not loaded.

Then dbt builds the star schema: incrementally, or in full after a chain's first
sync or a rollback. A pass is safe to interrupt anywhere: chunk files appear
only when complete, and uncommitted loads are rolled back.
"""

import fcntl
import signal
import time
import traceback
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from chainstar import __version__
from chainstar.chains import EVM, Chain
from chainstar.config import BlockRange, Connection, RunSettings
from chainstar.datasets import RANGED_DATASETS, ranged_datasets_for
from chainstar.errors import describe, hint_for
from chainstar.extract import (
    decode_token_metadata,
    extract_blocks_with_transfers,
    extract_dataset,
    extract_wallet_transfers,
)
from chainstar.heads import (
    FINALIZED,
    ChainBlock,
    ChainNode,
    Confirmations,
    describe_confirmations,
    safe_head,
)
from chainstar.lake import publish as publish_lake
from chainstar.load import (
    StoredBlock,
    attach,
    chunk_span,
    delete_from_block,
    index_raw_tables,
    load_range,
    overlapping_files,
    prepare_raw,
    stored_blocks,
    transaction,
)
from chainstar.model import MART_SCHEMA, build_models
from chainstar.pipeline import JSON_RPC_EXTRACTORS
from chainstar.redact import redact_url
from chainstar.reorg import MAX_REORG_SEARCH, first_changed_block
from chainstar.rpc import NodeIdentity
from chainstar.state import (
    SyncState,
    ensure_state_table,
    mark_modelled,
    read_states,
    record_run,
    utc_now,
    write_state,
)
from chainstar.swaps import decode_pool_metadata, extract_wallet_pricing_swaps
from chainstar.synclog import ALL_CHAINS, SyncLog, block_span, describe_counts
from chainstar.targets import LakeTarget, Target

# Blocks per batch, in chunks. Each batch is loaded and its progress committed on
# its own, so a long catch-up keeps what it has done if it stops half-way.
CHUNKS_PER_BATCH = 10
# Fact tables hold rows bound to blocks; a rollback deletes theirs too, so the
# marts never show a reorganised-away block, even before the rebuild. Each with
# the column that holds the block. A price's last_block_number is the chain's
# newest block in its time bucket; prices of other chains derived from this
# one's swaps (Bitcoin's, from wrapped BTC) are corrected by the rebuild.
FACT_TABLES = {
    "fct_transfers": "block_number",
    "fct_fees": "block_number",
    "fct_swaps": "block_number",
    "fct_token_prices": "last_block_number",
}
LOCK_FILE = ".chainstar-sync.lock"


class SyncError(RuntimeError):
    pass


class _BatchAbandoned(Exception):
    """A stop was requested between two stages of a batch, before its load."""


@dataclass(frozen=True)
class ChainPlan:
    chain: Chain
    connection: Connection
    confirmations: Confirmations


@dataclass(frozen=True)
class SyncPlan:
    target: Target
    data_dir: Path
    chains: tuple[ChainPlan, ...]
    # Where a chain's first sync starts: a block, or the latest N blocks.
    from_block: int | None = None
    from_recent: int | None = None
    full_refresh: bool = False
    # `chainstar wallet --sync`: extract only this address's transfers (EVM).
    wallet: str | None = None
    blocks_per_request: int = 1

    @property
    def work_dir(self) -> Path:
        return self.data_dir / ".dbt"


@dataclass(frozen=True)
class Stage:
    """One extraction step of a batch, for its log line."""

    name: str
    counts: dict[str, int]
    seconds: float


class ChainSource(Protocol):
    """What a sync needs from a chain. RpcSource is the real one; tests use fakes."""

    def identity(self) -> NodeIdentity: ...

    def finalized_block(self) -> int: ...

    def block(self, number: int) -> ChainBlock | None: ...

    def extract(self, settings: RunSettings, report: Callable[[Stage], None]) -> None:
        """Extract the batch, calling report() as each stage finishes."""


class RpcSource(ChainNode):
    """A chain read over JSON-RPC: cryo for Ethereum and Base, chainstar's own for the rest."""

    def extract(self, settings: RunSettings, report: Callable[[Stage], None]) -> None:
        started = time.monotonic()
        if settings.chain.family != EVM:
            counts = JSON_RPC_EXTRACTORS[settings.chain.family](settings)
            report(Stage("extract", counts, time.monotonic() - started))
            return
        if settings.wallet:
            counts = {"erc20_transfers": extract_wallet_transfers(settings, settings.wallet)}
            pools, counts["dex_swaps"] = extract_wallet_pricing_swaps(settings)
            counts["pricing pools"] = pools
            # After the swaps: their blocks are needed too, for their times.
            counts["blocks fetched"] = extract_blocks_with_transfers(settings)
        else:
            counts = {
                dataset.name: extract_dataset(settings, dataset) for dataset in RANGED_DATASETS
            }
        report(Stage("extract", counts, time.monotonic() - started))
        started = time.monotonic()
        pools_seen, pools_fetched = decode_pool_metadata(settings)
        metadata = decode_token_metadata(settings)
        report(
            Stage(
                "decode",
                {
                    "pools": pools_seen,
                    "pools fetched": pools_fetched,
                    "tokens": metadata.tokens_seen,
                    "fetched": metadata.tokens_fetched,
                },
                time.monotonic() - started,
            )
        )


class StopFlag:
    """Set by the first Ctrl-C or SIGTERM: stop at the next safe point, then exit.

    Safe points are between batches and, on EVM chains, between a batch's
    extraction and its token-metadata step (a batch stopped before its load is
    loaded by the next sync, reusing the chunk files already saved), so stopping
    waits for at most one stage.

    A second one stops at once (KeyboardInterrupt), which is also safe. The
    handler only sets the flag: printing from a signal handler can collide with
    a print already in progress.
    """

    def __init__(self) -> None:
        self.requested = False

    def _handle(self, _signum: int, _frame: object) -> None:
        if self.requested:
            raise KeyboardInterrupt
        self.requested = True

    @contextmanager
    def handling_signals(self) -> Iterator[None]:
        previous = {
            sig: signal.signal(sig, self._handle) for sig in (signal.SIGINT, signal.SIGTERM)
        }
        try:
            yield
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    def sleep(self, seconds: float, *, step: Callable[[float], None] = time.sleep) -> bool:
        """Sleep, waking early when a stop is requested; True if one was."""
        deadline = time.monotonic() + seconds
        while not self.requested and (left := deadline - time.monotonic()) > 0:
            step(min(left, 1.0))
        return self.requested


@contextmanager
def data_dir_lock(data_dir: Path) -> Iterator[None]:
    """One sync per data folder at a time: two would extract and load the same blocks."""
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / LOCK_FILE).open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SyncError(
                f"another chainstar sync is already using {data_dir}; "
                "stop it first, or give this one its own --data-dir"
            ) from None
        yield


def _utc(seconds: int | None) -> datetime | None:
    if seconds is None:
        return None
    return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None)


def _short(block_hash: str | None) -> str:
    if not block_hash:
        return "none"
    return block_hash if len(block_hash) <= 18 else f"{block_hash[:10]}...{block_hash[-6:]}"


class Syncer:
    """One sync plan against its sources, logging as it goes."""

    def __init__(
        self,
        plan: SyncPlan,
        sources: Mapping[str, ChainSource],
        log: SyncLog,
        stop: StopFlag,
        *,
        debug: bool = False,
    ) -> None:
        self.plan = plan
        self.sources = sources
        self.log = log
        self.stop = stop
        self.debug = debug

    def run(self, every: int | None = None, after_pass: Callable[[], None] | None = None) -> bool:
        """One pass, or with `every` (seconds) a pass per interval until stopped.

        Returns whether the last pass succeeded; a requested stop counts as success.
        """
        with data_dir_lock(self.plan.data_dir):
            while True:
                started = time.monotonic()
                ok = self.run_pass(after_pass)
                if every is None:
                    return ok
                if self.stop.requested:
                    self.log.info(ALL_CHAINS, "stop", "stopped")
                    return True
                wait = max(every - (time.monotonic() - started), 0)
                self.log.info(ALL_CHAINS, "wait", f"next sync in {wait:.0f}s")
                if self.stop.sleep(wait):
                    self.log.info(ALL_CHAINS, "stop", "stopped")
                    return True

    def run_pass(self, after_pass: Callable[[], None] | None = None) -> bool:
        ok = True
        for chain_plan in self.plan.chains:
            if self.stop.requested:
                break
            name = chain_plan.chain.name
            error = None
            try:
                self.sync_chain(chain_plan)
            except Exception as failure:
                error = self._report(name, "sync", failure)
                ok = False
            self._record_run(name, error)
        try:
            built = self.build()
            ok = built and ok
            if built and not self.stop.requested:
                self.publish()
            if built and after_pass is not None and not self.stop.requested:
                after_pass()
        except Exception as failure:
            self._report(ALL_CHAINS, "model", failure)
            ok = False
        return ok

    def _report(self, chain: str, stage: str, failure: Exception) -> str:
        """Log a failure with its hint; return the message to store, redacted."""
        if self.debug:
            self.log.error(chain, stage, traceback.format_exc().rstrip())
        message = describe(failure)
        hint = hint_for(failure)
        self.log.error(chain, stage, message + (f" (hint: {hint})" if hint else ""))
        return self.log.redact(message)

    def _record_run(self, chain: str, error: str | None) -> None:
        try:
            with attach(self.plan.target) as warehouse:
                if read_states(warehouse).get(chain):
                    record_run(warehouse, chain, error)
        except Exception as failure:
            # The failure that matters was already logged; this is bookkeeping.
            self.log.warning(chain, "state", f"could not record this run: {describe(failure)}")

    # One chain ----------------------------------------------------------------

    def sync_chain(self, chain_plan: ChainPlan) -> int:
        """Sync one chain up to its safe head; return the blocks loaded."""
        chain = chain_plan.chain
        source = self.sources[chain.name]
        node = source.identity()
        if not node.matches:
            raise SyncError(
                f"RPC reports {node.detail}, but the chain is {node.expected}; point its RPC URL "
                f"at an endpoint for {chain.name}"
            )
        finalized = None
        if chain.family == EVM and chain_plan.confirmations == FINALIZED:
            finalized = source.finalized_block()
        safe = safe_head(chain, chain_plan.confirmations, node.head, finalized)
        self.log.info(
            chain.name,
            "head",
            f"{node.detail}; safe head {safe:,} "
            f"({describe_confirmations(chain_plan.confirmations)})",
            head=node.head,
            safe_head=safe,
        )

        with attach(self.plan.target) as warehouse:
            prepare_raw(warehouse)
            index_raw_tables(warehouse)
            ensure_state_table(warehouse)
            state = read_states(warehouse).get(chain.name)

        if state is None:
            start = self._first_block(chain, safe)
        else:
            if self.plan.from_block is not None or self.plan.from_recent is not None:
                self.log.info(
                    chain.name,
                    "resume",
                    f"already synced to {state.last_block:,}; --from and --from-recent "
                    "apply only to a chain's first sync",
                )
            state = self._check_for_reorg(chain_plan, source, state)
            start = state.last_block + 1

        if start > safe:
            synced = f"synced to {start - 1:,}" if state else "nothing synced yet"
            self.log.info(chain.name, "sync", f"up to date: {synced}, safe head {safe:,}")
            return 0

        batch_size = chain_plan.connection.chunk_size * CHUNKS_PER_BATCH
        loaded = 0
        for first in range(start, safe + 1, batch_size):
            if self.stop.requested:
                self.log.info(
                    chain.name,
                    "stop",
                    f"stop requested; blocks from {first:,} wait for the next sync",
                )
                break
            blocks = BlockRange(first, min(first + batch_size, safe + 1))
            try:
                state = self._sync_batch(chain_plan, source, blocks, state)
            except _BatchAbandoned:
                self.log.info(
                    chain.name,
                    "stop",
                    f"stop requested; {block_span(blocks.start, blocks.end - 1)} not loaded, "
                    "the next sync loads it (saved chunks are reused)",
                )
                break
            loaded += len(blocks)
        return loaded

    def _first_block(self, chain: Chain, safe: int) -> int:
        if self.plan.from_block is not None:
            if self.plan.from_block > safe:
                raise SyncError(
                    f"--from {self.plan.from_block:,} is past {chain.name}'s safe head "
                    f"({safe:,}); choose an earlier block"
                )
            return self.plan.from_block
        if self.plan.from_recent is not None:
            return max(safe - self.plan.from_recent + 1, 0)
        raise SyncError(
            f"this is {chain.name}'s first sync into {self.plan.target.describe()}, so say "
            "where to start: --from BLOCK, or --from-recent N for the latest N "
            f"{chain.block_noun}. Later syncs continue from where the last one stopped."
        )

    def _settings(self, chain_plan: ChainPlan, blocks: BlockRange) -> RunSettings:
        connection = chain_plan.connection
        return RunSettings(
            chain=chain_plan.chain,
            blocks=blocks,
            rpc_url=connection.rpc.url,
            target=self.plan.target,
            data_dir=self.plan.data_dir,
            requests_per_second=connection.requests_per_second,
            chunk_size=connection.chunk_size,
            rpc_is_public=connection.rpc.public,
            wallet=self.plan.wallet,
            blocks_per_request=self.plan.blocks_per_request,
        )

    def _sync_batch(
        self,
        chain_plan: ChainPlan,
        source: ChainSource,
        blocks: BlockRange,
        state: SyncState | None,
    ) -> SyncState:
        chain = chain_plan.chain
        span = block_span(blocks.start, blocks.end - 1)

        def report(stage: Stage) -> None:
            self.log.info(
                chain.name,
                stage.name,
                f"{span}: {describe_counts(stage.counts, f'{chain.family}_')}",
                seconds=stage.seconds,
                first_block=blocks.start,
                last_block=blocks.end - 1,
                rows=stage.counts,
            )
            # Only decode can follow extract (EVM); once extraction is done the load
            # takes a second, so the batch is finished rather than thrown away.
            if self.stop.requested and stage.name == "extract" and chain.family == EVM:
                raise _BatchAbandoned

        source.extract(self._settings(chain_plan, blocks), report)

        # A wallet's EVM data has blocks only where its transfers are, so the
        # batch's last block is asked of the chain instead.
        wallet_checkpoint = None
        if self.plan.wallet and chain.family == EVM:
            wallet_checkpoint = source.block(blocks.end - 1)

        started = time.monotonic()
        with attach(self.plan.target) as warehouse, transaction(warehouse):
            counts = load_range(warehouse, self.plan.data_dir, chain, blocks)
            checkpoint = _checkpoint(
                wallet_checkpoint, stored_blocks(warehouse, chain, blocks.end - 1, 1)
            )
            new_state = _advance(state, chain, blocks, checkpoint, chain_plan.confirmations)
            write_state(warehouse, new_state)
        self.log.info(
            chain.name,
            "load",
            f"{span}: {sum(counts.values()):,} raw rows; synced to {blocks.end - 1:,}",
            seconds=time.monotonic() - started,
            first_block=blocks.start,
            last_block=blocks.end - 1,
            rows=counts,
        )
        return new_state

    # Reorganisations ------------------------------------------------------------

    def _check_for_reorg(
        self, chain_plan: ChainPlan, source: ChainSource, state: SyncState
    ) -> SyncState:
        chain = chain_plan.chain
        if state.checkpoint_block is None:
            return state
        current = source.block(state.checkpoint_block)
        current_hash = current.hash if current else None
        if current_hash == state.checkpoint_hash:
            self.log.info(
                chain.name,
                "check",
                f"block {state.checkpoint_block:,} still has hash {_short(current_hash)}; "
                f"continuing from {state.last_block + 1:,}",
            )
            return state
        self.log.warning(
            chain.name,
            "reorg",
            f"block {state.checkpoint_block:,} now has hash {_short(current_hash)}, not "
            f"{_short(state.checkpoint_hash)} as synced: the chain reorganised deeper than "
            f"{describe_confirmations(chain_plan.confirmations)}",
            block=state.checkpoint_block,
            stored_hash=state.checkpoint_hash,
            chain_hash=current_hash,
        )
        with attach(self.plan.target) as warehouse:
            stored = stored_blocks(warehouse, chain, state.last_block, MAX_REORG_SEARCH)

        def hash_now(number: int) -> str | None:
            block = source.block(number)
            return block.hash if block else None

        changed = first_changed_block(stored, hash_now)
        return self._roll_back(chain, state, state.first_block if changed is None else changed)

    def _roll_back(self, chain: Chain, state: SyncState, from_block: int) -> SyncState:
        """Delete everything from `from_block` on: chunk files, then raw and fact rows.

        Whole chunk files go, so the rollback starts at the first block of the
        earliest file involved. Files are deleted first: if the process stops
        before the database commit, the state still points past them, the hash
        check fails again next time, and the rollback simply repeats.
        """
        files = [
            path
            for dataset in ranged_datasets_for(chain)
            for path in overlapping_files(self.plan.data_dir, chain, dataset, from_block)
        ]
        start = min([from_block, *(span[0] for path in files if (span := chunk_span(path)))])
        for path in files:
            path.unlink(missing_ok=True)

        with attach(self.plan.target) as warehouse, transaction(warehouse):
            raw_rows = delete_from_block(warehouse, chain, start)
            fact_rows = 0
            for table, column in FACT_TABLES.items():
                if warehouse.has_table(MART_SCHEMA, table):
                    fact_rows += warehouse.delete(
                        warehouse.table(MART_SCHEMA, table),
                        f"chain = ? AND {warehouse.quote(column)} >= ?",
                        [chain.name, start],
                    )
            remaining = stored_blocks(warehouse, chain, start - 1, 1)
            checkpoint = remaining[0] if remaining else None
            new_state = replace(
                state,
                first_block=min(state.first_block, start),
                last_block=start - 1,
                checkpoint_block=checkpoint.number if checkpoint else None,
                checkpoint_hash=checkpoint.hash if checkpoint else None,
                checkpoint_time=_utc(checkpoint.time) if checkpoint else None,
                modelled_block=None,
                rebuild_pending=True,
                updated_at=utc_now(),
            )
            write_state(warehouse, new_state)
        self.log.warning(
            chain.name,
            "rollback",
            f"rolled back from {chain.block_noun[:-1]} {start:,}: deleted {len(files)} chunk "
            f"files, {raw_rows:,} raw rows and {fact_rows:,} fact rows; syncing them again, "
            "then rebuilding the star schema in full",
            from_block=start,
            files=len(files),
            raw_rows=raw_rows,
            fact_rows=fact_rows,
        )
        return new_state

    # The star schema ------------------------------------------------------------

    def build(self) -> bool:
        """Build the star schema if any chain has rows it does not include yet."""
        with attach(self.plan.target) as warehouse:
            states = read_states(warehouse)
        pending = sorted(name for name, state in states.items() if state.rebuild_pending)
        behind = [state for state in states.values() if state.modelled_block != state.last_block]
        if not (self.plan.full_refresh or pending or behind):
            self.log.info(ALL_CHAINS, "model", "star schema already up to date; dbt not run")
            return True
        if self.stop.requested:
            self.log.info(
                ALL_CHAINS, "model", "stop requested; the next sync builds the star schema"
            )
            return True
        full = self.plan.full_refresh or bool(pending)
        if self.plan.full_refresh:
            how = "full refresh, as asked"
        elif pending:
            how = f"full refresh: first sync or rollback of {', '.join(pending)}"
        else:
            how = "incremental"
        started = time.monotonic()
        result = build_models(self.plan.target, self.plan.work_dir, full_refresh=full)
        if result.failures:
            for failure in result.failures:
                self.log.error(ALL_CHAINS, "model", failure)
            self.log.error(
                ALL_CHAINS,
                "model",
                f"dbt build failed ({len(result.failures)} problem(s)); the next sync tries "
                f"again; full log: {result.log_path}",
            )
            return False
        with attach(self.plan.target) as warehouse, transaction(warehouse):
            mark_modelled(warehouse, states)
        detail = f"{result.models} models, {result.tests_passed} tests passed"
        if result.warnings:
            detail += f", {result.warnings} warnings"
        self.log.info(
            ALL_CHAINS,
            "model",
            f"dbt build ({how}): {detail}",
            seconds=time.monotonic() - started,
            full_refresh=full,
            models=result.models,
            tests_passed=result.tests_passed,
        )
        return True

    def publish(self) -> None:
        """A Parquet lake: copy what changed in the star schema, and the progress, to it."""
        target = self.plan.target
        if not isinstance(target, LakeTarget):
            return
        started = time.monotonic()
        published = publish_lake(target)
        self.log.info(
            ALL_CHAINS,
            "publish",
            f"{target.describe()}: {published.describe()}",
            seconds=time.monotonic() - started,
            partitions_written=published.written,
            partitions_unchanged=published.unchanged,
            partitions_emptied=published.emptied,
            rows=published.rows,
        )


def _checkpoint(from_chain: ChainBlock | None, stored: list[StoredBlock]) -> ChainBlock | None:
    if from_chain is not None:
        return from_chain
    if stored:
        return ChainBlock(stored[0].number, stored[0].hash, stored[0].time)
    return None


def _advance(
    state: SyncState | None,
    chain: Chain,
    blocks: BlockRange,
    checkpoint: ChainBlock | None,
    confirmations: Confirmations,
) -> SyncState:
    """The state after loading `blocks`."""
    now = utc_now()
    if state is None:
        # A chain's first sync rebuilds the tables in full: blocks loaded earlier
        # by `chainstar run` may lie below what they would treat as new.
        state = SyncState(
            chain=chain.name,
            first_block=blocks.start,
            last_block=blocks.start - 1,
            checkpoint_block=None,
            checkpoint_hash=None,
            checkpoint_time=None,
            confirmations=str(confirmations),
            modelled_block=None,
            rebuild_pending=True,
            updated_at=now,
            last_run_at=None,
            last_run_status=None,
            last_error=None,
            chainstar_version=__version__,
        )
    if checkpoint is None:
        # No block with a hash in this batch (only skipped Solana slots): keep the old one.
        checkpoint = (
            ChainBlock(state.checkpoint_block, state.checkpoint_hash, None)
            if state.checkpoint_block is not None and state.checkpoint_hash is not None
            else None
        )
        checkpoint_time = state.checkpoint_time
    else:
        checkpoint_time = _utc(checkpoint.time)
    return replace(
        state,
        last_block=blocks.end - 1,
        checkpoint_block=checkpoint.number if checkpoint else None,
        checkpoint_hash=checkpoint.hash if checkpoint else None,
        checkpoint_time=checkpoint_time,
        confirmations=str(confirmations),
        updated_at=now,
        chainstar_version=__version__,
    )


def rpc_sources(plan: SyncPlan) -> dict[str, ChainSource]:
    return {
        chain_plan.chain.name: RpcSource(
            chain_plan.chain,
            chain_plan.connection.rpc.url,
            chain_plan.connection.requests_per_second,
        )
        for chain_plan in plan.chains
    }


def describe_rpc(chain_plan: ChainPlan) -> str:
    rpc = chain_plan.connection.rpc
    return redact_url(rpc.url) + (" (free shared public endpoint)" if rpc.public else "")
