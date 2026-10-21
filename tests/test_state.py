from dataclasses import fields, replace
from pathlib import Path

from chainstar.load import attach, transaction
from chainstar.state import (
    COLUMNS,
    SyncState,
    ensure_state_table,
    mark_modelled,
    read_states,
    record_run,
    utc_now,
    write_state,
)
from chainstar.targets import DuckDBTarget


def _state(**changes) -> SyncState:
    values = dict(
        chain="base",
        first_block=100,
        last_block=199,
        checkpoint_block=199,
        checkpoint_hash="0xabc",
        checkpoint_time=utc_now(),
        confirmations="60",
        modelled_block=None,
        rebuild_pending=True,
        updated_at=utc_now(),
        last_run_at=None,
        last_run_status=None,
        last_error=None,
        chainstar_version="0.1.0",
    )
    values.update(changes)
    return SyncState(**values)


def test_columns_match_the_dataclass():
    assert [f.name for f in fields(SyncState)] == [name for name, _ in COLUMNS]


def test_round_trip_and_replace(tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    with attach(target) as warehouse:
        assert read_states(warehouse) == {}  # no table yet
        ensure_state_table(warehouse)
        state = _state()
        write_state(warehouse, state)
        write_state(warehouse, replace(state, last_block=299))
        write_state(warehouse, _state(chain="solana", confirmations="finalized"))
        states = read_states(warehouse)
    assert list(states) == ["base", "solana"]
    assert states["base"] == replace(state, last_block=299)


def test_record_run_and_mark_modelled(tmp_path: Path):
    target = DuckDBTarget(tmp_path / "w.duckdb")
    with attach(target) as warehouse, transaction(warehouse):
        ensure_state_table(warehouse)
        write_state(warehouse, _state())
    with attach(target) as warehouse:
        record_run(warehouse, "base", "could not reach https://base-rpc.publicnode.com")
        failed = read_states(warehouse)["base"]
        assert (failed.last_run_status, failed.last_error[:15]) == ("error", "could not reach")
        assert failed.last_block == 199  # a run record never moves progress
        record_run(warehouse, "base", None)
        mark_modelled(warehouse, read_states(warehouse))
        done = read_states(warehouse)["base"]
    assert (done.last_run_status, done.last_error) == ("ok", None)
    assert (done.modelled_block, done.rebuild_pending) == (199, False)
