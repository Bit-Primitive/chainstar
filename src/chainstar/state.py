"""Sync progress, kept in the warehouse itself (chainstar_state.sync_progress).

Progress is written in the same transaction as the raw rows it describes, so
after a crash the table never claims blocks that were not loaded. One row per
chain; numbers are blocks (Solana: slots) and times are UTC.

ClickHouse and BigQuery have no transaction spanning several statements. There
the progress row is written last, after the data it describes, and as a new
version (column _version) rather than in place: a row is only ever added, and
older versions are removed after it, so a crash at any point leaves either the
old progress or the new one, never none. Loads replace whole block ranges, so a
sync that finds the old progress simply loads the same blocks again.
"""

import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from chainstar.load import Param, Warehouse

STATE_SCHEMA = "chainstar_state"
STATE_TABLE = "sync_progress"

COLUMNS = (
    ("chain", "VARCHAR"),
    # Where the first sync started.
    ("first_block", "BIGINT"),
    # Every block from first_block to here is loaded.
    ("last_block", "BIGINT"),
    # The newest loaded block with a known hash, checked against the chain before
    # each sync to detect a reorganisation. It equals last_block, except on Solana
    # when the last slots were skipped (no block), and is NULL when nothing is left.
    ("checkpoint_block", "BIGINT"),
    ("checkpoint_hash", "VARCHAR"),
    ("checkpoint_time", "TIMESTAMP"),
    # As configured for the last sync: a number, or `finalized`.
    ("confirmations", "VARCHAR"),
    # The star-schema tables were last built with raw data up to this block.
    ("modelled_block", "BIGINT"),
    # True when the tables must be rebuilt in full: after the chain's first sync
    # (older blocks may sit below the tables' watermarks) and after a rollback.
    ("rebuild_pending", "BOOLEAN"),
    # When progress was last recorded.
    ("updated_at", "TIMESTAMP"),
    # When a sync last ran for the chain, whether it succeeded, and why not.
    ("last_run_at", "TIMESTAMP"),
    ("last_run_status", "VARCHAR"),
    ("last_error", "VARCHAR"),
    ("chainstar_version", "VARCHAR"),
)


@dataclass(frozen=True)
class SyncState:
    chain: str
    first_block: int
    last_block: int
    checkpoint_block: int | None
    checkpoint_hash: str | None
    checkpoint_time: datetime | None
    confirmations: str
    modelled_block: int | None
    rebuild_pending: bool
    updated_at: datetime
    last_run_at: datetime | None
    last_run_status: str | None
    last_error: str | None
    chainstar_version: str


def utc_now() -> datetime:
    """Naive UTC, as the warehouse stores every timestamp."""
    return datetime.now(UTC).replace(tzinfo=None)


# Only on warehouses without multi-statement transactions; see the module docstring.
VERSION_COLUMN = ("_version", "BIGINT")


def _table(warehouse: Warehouse) -> str:
    return warehouse.table(STATE_SCHEMA, STATE_TABLE)


def _columns(warehouse: Warehouse) -> tuple[tuple[str, str], ...]:
    return COLUMNS if warehouse.transactional else (*COLUMNS, VERSION_COLUMN)


def ensure_state_table(warehouse: Warehouse) -> None:
    warehouse.create_schema(STATE_SCHEMA)
    warehouse.create_table(STATE_SCHEMA, STATE_TABLE, _columns(warehouse))


def read_states(warehouse: Warehouse) -> dict[str, SyncState]:
    """Every chain's progress; empty if nothing was ever synced into this warehouse."""
    if not warehouse.has_table(STATE_SCHEMA, STATE_TABLE):
        return {}
    names = ", ".join(name for name, _ in COLUMNS)
    source = _table(warehouse)
    if not warehouse.transactional:
        # The newest version of each chain's row.
        source = (
            f"(SELECT {names}, row_number() OVER (PARTITION BY chain ORDER BY _version DESC) "
            f"AS recency FROM {source}) AS versions WHERE recency = 1"
        )
    rows = warehouse.query(f"SELECT {names} FROM {source} ORDER BY chain")
    return {row[0]: _state_from_row(row) for row in rows}


def _state_from_row(row: tuple) -> SyncState:
    # Drivers differ in how they return times (naive or UTC-aware) and integers.
    values = [
        value.astimezone(UTC).replace(tzinfo=None)
        if isinstance(value, datetime) and value.tzinfo is not None
        else value
        for value in row
    ]
    state = SyncState(*values)
    if state.rebuild_pending is not None and not isinstance(state.rebuild_pending, bool):
        state = replace(state, rebuild_pending=bool(state.rebuild_pending))
    return state


def write_state(warehouse: Warehouse, state: SyncState) -> None:
    """Replace the chain's row. Call inside the transaction that loads its data."""
    table = _table(warehouse)
    values = [Param(getattr(state, name), sql_type) for name, sql_type in COLUMNS]
    if warehouse.transactional:
        warehouse.delete(table, "chain = ?", [state.chain])
        placeholders = ", ".join("?" for _ in COLUMNS)
        warehouse.query(f"INSERT INTO {table} VALUES ({placeholders})", values)
        return
    version = time.time_ns()
    names = ", ".join(name for name, _ in _columns(warehouse))
    placeholders = ", ".join("?" for _ in _columns(warehouse))
    warehouse.query(
        f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
        [*values, Param(version, VERSION_COLUMN[1])],
    )
    warehouse.delete(table, "chain = ? AND _version < ?", [state.chain, version])


def record_run(warehouse: Warehouse, chain: str, error: str | None) -> None:
    """Note that a sync ran for the chain, and its error if it failed.

    Only the run columns change, so this never records progress.
    """
    status = "error" if error else "ok"
    if not warehouse.transactional:
        state = read_states(warehouse).get(chain)
        if state is not None:
            changed = replace(
                state, last_run_at=utc_now(), last_run_status=status, last_error=error
            )
            write_state(warehouse, changed)
        return
    warehouse.query(
        f"UPDATE {_table(warehouse)} SET last_run_at = ?, last_run_status = ?, last_error = ? "
        "WHERE chain = ?",
        [Param(utc_now(), "TIMESTAMP"), status, Param(error, "VARCHAR"), chain],
    )


def mark_modelled(warehouse: Warehouse, states: dict[str, SyncState]) -> None:
    """After a successful build: the tables cover each chain up to its last_block."""
    for state in states.values():
        write_state(
            warehouse, replace(state, modelled_block=state.last_block, rebuild_pending=False)
        )
