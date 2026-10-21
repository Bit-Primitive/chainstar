"""Sync progress, kept in the warehouse itself (chainstar_state.sync_progress).

Progress is written in the same transaction as the raw rows it describes, so
after a crash the table never claims blocks that were not loaded. One row per
chain; numbers are blocks (Solana: slots) and times are UTC.
"""

from dataclasses import dataclass, replace
from datetime import UTC, datetime

from chainstar.load import Warehouse

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


def _table(warehouse: Warehouse) -> str:
    return warehouse.table(STATE_SCHEMA, STATE_TABLE)


def ensure_state_table(warehouse: Warehouse) -> None:
    connection = warehouse.connection
    connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{warehouse.catalog}".{STATE_SCHEMA}')
    ddl = ", ".join(f"{name} {sql_type}" for name, sql_type in COLUMNS)
    connection.execute(f"CREATE TABLE IF NOT EXISTS {_table(warehouse)} ({ddl})")


def read_states(warehouse: Warehouse) -> dict[str, SyncState]:
    """Every chain's progress; empty if nothing was ever synced into this warehouse."""
    if not warehouse.has_table(STATE_SCHEMA, STATE_TABLE):
        return {}
    names = ", ".join(name for name, _ in COLUMNS)
    rows = warehouse.connection.execute(
        f"SELECT {names} FROM {_table(warehouse)} ORDER BY chain"
    ).fetchall()
    return {row[0]: SyncState(*row) for row in rows}


def write_state(warehouse: Warehouse, state: SyncState) -> None:
    """Replace the chain's row. Call inside the transaction that loads its data."""
    connection = warehouse.connection
    connection.execute(f"DELETE FROM {_table(warehouse)} WHERE chain = ?", [state.chain])
    placeholders = ", ".join("?" for _ in COLUMNS)
    values = [getattr(state, name) for name, _ in COLUMNS]
    connection.execute(f"INSERT INTO {_table(warehouse)} VALUES ({placeholders})", values)


def record_run(warehouse: Warehouse, chain: str, error: str | None) -> None:
    """Note that a sync ran for the chain, and its error if it failed.

    Only the run columns change, so this never records progress.
    """
    warehouse.connection.execute(
        f"UPDATE {_table(warehouse)} SET last_run_at = ?, last_run_status = ?, last_error = ? "
        "WHERE chain = ?",
        [utc_now(), "error" if error else "ok", error, chain],
    )


def mark_modelled(warehouse: Warehouse, states: dict[str, SyncState]) -> None:
    """After a successful build: the tables cover each chain up to its last_block."""
    for state in states.values():
        write_state(
            warehouse, replace(state, modelled_block=state.last_block, rebuild_pending=False)
        )
