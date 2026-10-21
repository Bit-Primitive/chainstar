"""`chainstar status`: how far each chain is synced, how far behind, and how big its tables are.

Lag in time is measured from the newest synced block's own time, so it needs no
RPC call and still works when the node is down; lag in blocks needs the node's
head. With --max-lag the command exits non-zero when any chain is further
behind than that, so a health check or monitoring job can run it.
"""

import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb

from chainstar import __version__
from chainstar.chains import EVM, Chain, get_chain
from chainstar.errors import describe
from chainstar.heads import (
    FINALIZED,
    ChainNode,
    describe_confirmations,
    parse_confirmations,
    safe_head,
)
from chainstar.load import Warehouse, attach
from chainstar.model import MART_SCHEMA, MART_TABLES
from chainstar.state import SyncState, read_states, utc_now
from chainstar.targets import DuckDBTarget, Target

# Tables counted per chain (dim_dates has no chain).
COUNTED_TABLES = [table for table in MART_TABLES if table != "dim_dates"]
# A DuckDB file has one writer at a time; while a sync step holds it, wait a little.
LOCK_WAIT_SECONDS = 15


class StatusError(RuntimeError):
    pass


@dataclass
class ChainStatus:
    state: SyncState
    head: int | None = None
    safe_head: int | None = None
    head_error: str | None = None
    lag_blocks: int | None = None
    lag_seconds: int | None = None
    rows: dict[str, int] = field(default_factory=dict)
    # None when no --max-lag was given.
    healthy: bool | None = None


@dataclass
class Report:
    target: str
    time: datetime
    max_lag_seconds: int | None
    chains: list[ChainStatus]

    @property
    def healthy(self) -> bool | None:
        if self.max_lag_seconds is None:
            return None
        return bool(self.chains) and all(status.healthy for status in self.chains)


def collect(
    target: Target,
    chains: Iterable[str],
    node_for: Callable[[Chain], ChainNode],
    max_lag_seconds: int | None,
) -> Report:
    """Read the sync state and table sizes, then ask each chain's node for its head."""
    now = utc_now()
    with _open(target) as warehouse:
        states = read_states(warehouse)
        rows = _row_counts(warehouse)
    wanted = set(chains) or set(states)
    report = Report(target.describe(), now, max_lag_seconds, [])
    for name in sorted(wanted):
        if name not in states:
            raise StatusError(
                f"{name} has never been synced into {target.describe()}; "
                f"synced chains: {', '.join(sorted(states)) or 'none'}"
            )
        state = states[name]
        status = ChainStatus(state, rows={t: rows[t].get(name, 0) for t in COUNTED_TABLES})
        if state.checkpoint_time is not None:
            status.lag_seconds = max(int((now - state.checkpoint_time).total_seconds()), 0)
        _read_head(status, get_chain(name), node_for)
        if max_lag_seconds is not None:
            status.healthy = (
                status.lag_seconds is not None and status.lag_seconds <= max_lag_seconds
            )
        report.chains.append(status)
    return report


@contextmanager
def _open(target: Target) -> Iterator[Warehouse]:
    if isinstance(target, DuckDBTarget) and not target.path.exists():
        raise StatusError(
            f"there is no warehouse at {target.path}; run `chainstar sync` first, "
            "or point --target at your warehouse"
        )
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        stack = ExitStack()
        try:
            warehouse = stack.enter_context(attach(target, read_only=True))
        except duckdb.IOException as error:
            if "lock" not in str(error).lower() or time.monotonic() > deadline:
                raise
            time.sleep(1)
            continue
        with stack:
            yield warehouse
        return


def _row_counts(warehouse: Warehouse) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for table in COUNTED_TABLES:
        counts[table] = {}
        if warehouse.has_table(MART_SCHEMA, table):
            rows = warehouse.connection.execute(
                f"SELECT chain, count(*) FROM {warehouse.table(MART_SCHEMA, table)} GROUP BY chain"
            ).fetchall()
            counts[table] = {chain: int(count) for chain, count in rows}
    return counts


def _read_head(status: ChainStatus, chain: Chain, node_for: Callable[[Chain], ChainNode]) -> None:
    try:
        node = node_for(chain)
        identity = node.identity()
        if not identity.matches:
            status.head_error = f"RPC reports {identity.detail}, expected {identity.expected}"
            return
        confirmations = parse_confirmations(status.state.confirmations, chain)
        finalized = None
        if chain.family == EVM and confirmations == FINALIZED:
            finalized = node.finalized_block()
        status.head = identity.head
        status.safe_head = safe_head(chain, confirmations, identity.head, finalized)
        status.lag_blocks = max(identity.head - status.state.last_block, 0)
    except Exception as error:
        # Status still reports what the warehouse knows when the node is unreachable.
        status.head_error = describe(error)


def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "unknown"
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    parts = [(days, "d"), (hours, "h"), (minutes, "m"), (secs, "s")]
    shown = [f"{value}{unit}" for value, unit in parts if value][:2]
    return " ".join(shown) or "0s"


def _when(moment: datetime | None) -> str:
    return "never" if moment is None else f"{moment:%Y-%m-%d %H:%M:%S} UTC"


def render_text(report: Report) -> list[str]:
    lines = [f"chainstar status: {report.target} at {_when(report.time)}", ""]
    for status in report.chains:
        state = status.state
        noun = get_chain(state.chain).block_noun[:-1]
        synced = f"{noun} {state.last_block:,}"
        if state.checkpoint_time is not None:
            synced += f", {_when(state.checkpoint_time)}"
        lines.append(state.chain)
        lines.append(f"  synced to    {synced} (first synced {noun} {state.first_block:,})")
        if status.head is not None:
            lines.append(
                f"  head         {status.head:,}; safe head {status.safe_head:,} "
                f"({describe_confirmations(state.confirmations)})"
            )
        else:
            lines.append(f"  head         unknown: {status.head_error}")
        blocks = "unknown" if status.lag_blocks is None else f"{status.lag_blocks:,} {noun}s"
        lines.append(f"  lag          {blocks}, {format_duration(status.lag_seconds)}")
        run = _when(state.last_run_at)
        if state.last_run_status:
            run += f", {state.last_run_status}"
        if state.last_error:
            run += f": {state.last_error}"
        lines.append(f"  last run     {run}")
        if state.rebuild_pending:
            model = "rebuild pending (the next sync rebuilds it in full)"
        elif state.modelled_block is None:
            model = "not built yet"
        else:
            model = f"built to {noun} {state.modelled_block:,}"
        lines.append(f"  star schema  {model}")
        lines.append("  rows         " + ", ".join(f"{t} {n:,}" for t, n in status.rows.items()))
        lines.append("")
    if report.max_lag_seconds is not None:
        limit = format_duration(report.max_lag_seconds)
        late = [s for s in report.chains if not s.healthy]
        if late:
            for status in late:
                lines.append(
                    f"UNHEALTHY: {status.state.chain} is {format_duration(status.lag_seconds)} "
                    f"behind, more than --max-lag {limit}"
                )
        else:
            lines.append(f"healthy: every chain is within {limit}")
    return lines


def to_json(report: Report) -> dict[str, Any]:
    def stamp(moment: datetime | None) -> str | None:
        return None if moment is None else f"{moment:%Y-%m-%dT%H:%M:%SZ}"

    return {
        "chainstar_version": __version__,
        "target": report.target,
        "time": stamp(report.time),
        "max_lag_seconds": report.max_lag_seconds,
        "healthy": report.healthy,
        "chains": [
            {
                "chain": s.state.chain,
                "first_block": s.state.first_block,
                "synced_to": s.state.last_block,
                "synced_to_time": stamp(s.state.checkpoint_time),
                "checkpoint_block": s.state.checkpoint_block,
                "checkpoint_hash": s.state.checkpoint_hash,
                "head": s.head,
                "safe_head": s.safe_head,
                "confirmations": s.state.confirmations,
                "lag_blocks": s.lag_blocks,
                "lag_seconds": s.lag_seconds,
                "healthy": s.healthy,
                "head_error": s.head_error,
                "modelled_block": s.state.modelled_block,
                "rebuild_pending": s.state.rebuild_pending,
                "last_run_at": stamp(s.state.last_run_at),
                "last_run_status": s.state.last_run_status,
                "last_error": s.state.last_error,
                "updated_at": stamp(s.state.updated_at),
                "synced_by_version": s.state.chainstar_version,
                "rows": s.rows,
            }
            for s in report.chains
        ],
    }
