"""The sync engine against fixture chains: state, resuming, batches, reorgs, stopping.

Loads go into a real DuckDB file; the dbt build is stubbed here (it is covered
end to end in test_sync_e2e.py), recording whether each build was incremental.
"""

import json
from pathlib import Path

import duckdb
import pytest

from chainstar import sync
from chainstar.chains import get_chain
from chainstar.config import Connection, RpcChoice
from chainstar.datasets import parquet_files
from chainstar.heads import FINALIZED, Confirmations
from chainstar.load import attach
from chainstar.model import ModelResult
from chainstar.reorg import ReorgTooDeep
from chainstar.state import read_states
from chainstar.sync import ChainPlan, StopFlag, Syncer, SyncError, SyncPlan, data_dir_lock
from chainstar.synclog import SyncLog
from chainstar.targets import DuckDBTarget
from fixture_chain import FIXTURE_BLOCKS, FixtureSource, make_source_dir, rewrite_block

ETHEREUM = get_chain("ethereum")
FIRST, LAST = FIXTURE_BLOCKS["ethereum"]
SECRET_URL = "https://rpc.example/v2/supersecretkey"


def _plan(tmp_path: Path, chains: dict[str, Confirmations], chunk_size: int = 1, **options):
    return SyncPlan(
        target=DuckDBTarget(tmp_path / "w.duckdb"),
        data_dir=tmp_path / "data",
        chains=tuple(
            ChainPlan(
                get_chain(name),
                Connection(RpcChoice(SECRET_URL, public=False), None, chunk_size),
                confirmations,
            )
            for name, confirmations in chains.items()
        ),
        **options,
    )


class Run:
    """A Syncer over fixture sources, with its log lines and dbt builds recorded."""

    def __init__(self, tmp_path: Path, monkeypatch, plan: SyncPlan, heads: dict[str, int]):
        self.source_dir = make_source_dir(tmp_path)
        self.sources = {
            name: FixtureSource(get_chain(name), self.source_dir, head)
            for name, head in heads.items()
        }
        self.lines: list[str] = []
        self.builds: list[bool] = []
        self.stop = StopFlag()
        self.plan = plan

        def fake_build(target, work_dir, *, full_refresh):
            self.builds.append(full_refresh)
            return ModelResult(models=23)

        monkeypatch.setattr(sync, "build_models", fake_build)

    def syncer(self, plan: SyncPlan | None = None) -> Syncer:
        log = SyncLog(self.lines.append, secrets=[SECRET_URL])
        return Syncer(plan or self.plan, self.sources, log, self.stop)

    def run_pass(self, plan: SyncPlan | None = None) -> bool:
        return self.syncer(plan).run_pass()

    def state(self, chain: str = "ethereum"):
        with attach(self.plan.target) as warehouse:
            return read_states(warehouse).get(chain)

    def raw_count(self, table: str, where: str = "true") -> int:
        with duckdb.connect(str(self.plan.target.path), read_only=True) as connection:
            return connection.execute(
                f"SELECT count(*) FROM chainstar_raw.{table} WHERE {where}"
            ).fetchone()[0]

    def text(self) -> str:
        return "\n".join(self.lines)


@pytest.fixture
def eth(tmp_path, monkeypatch) -> Run:
    plan = _plan(tmp_path, {"ethereum": 1}, from_block=FIRST)
    return Run(tmp_path, monkeypatch, plan, {"ethereum": FIRST})


def test_first_sync_needs_a_start(tmp_path, monkeypatch):
    run = Run(tmp_path, monkeypatch, _plan(tmp_path, {"ethereum": 1}), {"ethereum": LAST})
    assert run.run_pass() is False
    assert "first sync" in run.text()
    assert "--from-recent N" in run.text()
    assert run.state() is None


def test_first_sync_then_resume(eth: Run):
    assert eth.run_pass()
    state = eth.state()
    assert (state.first_block, state.last_block, state.checkpoint_block) == (FIRST, FIRST, FIRST)
    assert state.checkpoint_hash == eth.sources["ethereum"].block(FIRST).hash
    assert state.checkpoint_time is not None
    assert state.last_run_status == "ok"
    # The first sync of a chain rebuilds in full; afterwards the build is recorded.
    assert eth.builds == [True]
    assert (state.rebuild_pending, state.modelled_block) == (False, FIRST)
    assert eth.raw_count("erc20_transfers") == 541

    eth.sources["ethereum"].head = LAST
    assert eth.run_pass()
    assert eth.sources["ethereum"].extracted == [(FIRST, FIRST), (LAST, LAST)]
    assert eth.builds == [True, False]  # the second build is incremental
    assert eth.state().last_block == LAST
    assert eth.raw_count("erc20_transfers") == 562
    assert "still has hash" in eth.text()

    # Nothing new: no extraction, no dbt.
    assert eth.run_pass()
    assert len(eth.sources["ethereum"].extracted) == 2
    assert eth.builds == [True, False]
    assert "up to date: synced to 26,239,739" in eth.text()
    assert "dbt not run" in eth.text()


def test_from_is_ignored_once_synced(eth: Run):
    eth.run_pass()
    eth.sources["ethereum"].head = LAST
    eth.run_pass()
    assert "--from and --from-recent apply only to a chain's first sync" in eth.text()


def test_from_recent_counts_back_from_the_safe_head(tmp_path, monkeypatch):
    plan = _plan(tmp_path, {"ethereum": 2}, from_recent=1)
    run = Run(tmp_path, monkeypatch, plan, {"ethereum": LAST})
    run.run_pass()
    # 2 confirmations: the head (LAST) has 1, so the safe head is FIRST.
    assert run.sources["ethereum"].extracted == [(FIRST, FIRST)]
    assert "safe head 26,239,738 (2 confirmations)" in run.text()


def test_from_past_the_safe_head_is_refused(tmp_path, monkeypatch):
    plan = _plan(tmp_path, {"ethereum": 1}, from_block=LAST + 5)
    run = Run(tmp_path, monkeypatch, plan, {"ethereum": LAST})
    assert run.run_pass() is False
    assert "past ethereum's safe head" in run.text()


def test_each_batch_commits_its_own_progress(eth: Run, monkeypatch):
    monkeypatch.setattr(sync, "CHUNKS_PER_BATCH", 1)
    eth.sources["ethereum"].head = LAST
    real_load = sync.load_range
    calls = []

    def load_then_fail_second(warehouse, data_dir, chain, blocks):
        calls.append(blocks)
        counts = real_load(warehouse, data_dir, chain, blocks)
        if len(calls) == 2:
            raise OSError("disk full (simulated)")
        return counts

    monkeypatch.setattr(sync, "load_range", load_then_fail_second)
    assert eth.run_pass() is False
    # The first batch is kept; the second, which failed inside its transaction,
    # left neither rows nor progress behind.
    state = eth.state()
    assert state.last_block == FIRST
    assert state.last_run_status == "error"
    assert "disk full (simulated)" in state.last_error
    assert eth.raw_count("erc20_transfers", f"block_number = {LAST}") == 0
    assert eth.raw_count("erc20_transfers", f"block_number = {FIRST}") == 541


def test_crash_before_commit_records_nothing(eth: Run, monkeypatch):
    def fail(warehouse, state):
        raise KeyboardInterrupt  # what a second Ctrl-C raises

    monkeypatch.setattr(sync, "write_state", fail)
    with pytest.raises(KeyboardInterrupt):
        eth.run_pass()
    with attach(eth.plan.target) as warehouse:
        assert read_states(warehouse) == {}
    assert eth.raw_count("erc20_transfers") == 0


def _reorg(run: Run, number: int, new_hash: str) -> None:
    rewrite_block(run.source_dir, ETHEREUM, number, new_hash)


NEW_HASH = "0x" + "ab" * 32


def test_reorg_rolls_back_and_resyncs(eth: Run):
    eth.sources["ethereum"].head = LAST
    eth.run_pass()
    old_hash = eth.state().checkpoint_hash
    old_transfers = eth.raw_count("erc20_transfers", f"block_number = {LAST}")

    _reorg(eth, LAST, NEW_HASH)
    assert eth.run_pass()

    text = eth.text()
    assert f"block 26,239,739 now has hash {NEW_HASH[:10]}" in text
    assert f"not {old_hash[:10]}" in text
    # Transfers, swaps, blocks and transactions: one chunk file each.
    assert "rolled back from block 26,239,739: deleted 4 chunk files" in text
    state = eth.state()
    assert (state.last_block, state.checkpoint_hash) == (LAST, NEW_HASH)
    # The reorganised block is synced again from the new fork; the block before
    # it, which still matched, is not fetched again.
    assert eth.sources["ethereum"].extracted == [(FIRST, LAST), (LAST, LAST)]
    new_transfers = eth.raw_count("erc20_transfers", f"block_number = {LAST}")
    assert 0 < new_transfers < old_transfers
    assert eth.raw_count("blocks", f"block_hash = '{old_hash}'") == 0
    # The rollback forces a full rebuild, then records it.
    assert eth.builds == [True, True]
    assert eth.state().rebuild_pending is False


def test_rollback_takes_whole_chunks(tmp_path, monkeypatch):
    plan = _plan(tmp_path, {"ethereum": 1}, chunk_size=2, from_block=FIRST)
    run = Run(tmp_path, monkeypatch, plan, {"ethereum": LAST})
    run.run_pass()
    _reorg(run, LAST, NEW_HASH)
    run.run_pass()
    # Only LAST changed, but it shares a chunk file with FIRST.
    assert "rolled back from block 26,239,738" in run.text()
    assert run.sources["ethereum"].extracted == [(FIRST, LAST), (FIRST, LAST)]
    assert run.state().checkpoint_hash == NEW_HASH


def test_rollback_deletes_fact_rows(eth: Run):
    eth.sources["ethereum"].head = LAST
    eth.run_pass()
    with attach(eth.plan.target) as warehouse:
        connection = warehouse.connection
        connection.execute("CREATE SCHEMA chainstar")
        connection.execute(
            "CREATE TABLE chainstar.fct_transfers AS SELECT 'ethereum' AS chain, "
            "unnest([26239738, 26239739, 26239739]) AS block_number"
        )
    _reorg(eth, LAST, NEW_HASH)
    eth.run_pass()
    assert "and 2 fact rows" in eth.text()


def test_reorg_below_every_stored_block_resyncs_everything(eth: Run):
    eth.sources["ethereum"].head = LAST
    eth.run_pass()
    _reorg(eth, FIRST, "0x" + "01" * 32)
    _reorg(eth, LAST, NEW_HASH)
    eth.run_pass()
    assert "rolled back from block 26,239,738" in eth.text()
    assert eth.sources["ethereum"].extracted[-1] == (FIRST, LAST)


def test_reorg_deeper_than_the_search_stops(eth: Run, monkeypatch):
    monkeypatch.setattr(
        sync,
        "first_changed_block",
        lambda stored, current_hash: (_ for _ in ()).throw(ReorgTooDeep("too deep (simulated)")),
    )
    eth.sources["ethereum"].head = LAST
    eth.run_pass()
    _reorg(eth, LAST, NEW_HASH)
    assert eth.run_pass() is False
    assert "too deep (simulated)" in eth.text()
    assert eth.state().last_block == LAST  # nothing rolled back


def test_stop_between_batches(eth: Run, monkeypatch):
    monkeypatch.setattr(sync, "CHUNKS_PER_BATCH", 1)
    eth.sources["ethereum"].head = LAST
    real_load = sync.load_range

    def load_then_stop(*args):
        counts = real_load(*args)
        eth.stop.requested = True  # as the first Ctrl-C or SIGTERM would
        return counts

    monkeypatch.setattr(sync, "load_range", load_then_stop)
    assert eth.run_pass()
    assert eth.sources["ethereum"].extracted == [(FIRST, FIRST)]
    assert eth.state().last_block == FIRST  # the batch being loaded was finished
    assert "blocks from 26,239,739 wait for the next sync" in eth.text()
    assert eth.builds == []
    assert "the next sync builds the star schema" in eth.text()


def test_stop_during_a_batch_abandons_it_before_loading(eth: Run):
    source = eth.sources["ethereum"]
    real_extract = source.extract

    def extract_then_stop(settings, report):
        eth.stop.requested = True
        real_extract(settings, report)

    source.extract = extract_then_stop
    assert eth.run_pass()
    assert "26,239,738 not loaded, the next sync loads it" in eth.text()
    assert eth.state() is None
    assert eth.raw_count("erc20_transfers") == 0
    # The next sync reuses the chunk files already saved.
    eth.stop.requested = False
    source.extract = real_extract
    files = sorted(eth.plan.data_dir.rglob("*.parquet"))
    mtimes = [f.stat().st_mtime_ns for f in files]
    assert eth.run_pass()
    assert eth.state().last_block == FIRST
    assert [f.stat().st_mtime_ns for f in files] == mtimes


def test_every_keeps_syncing_until_stopped(eth: Run, monkeypatch):
    sleeps = []

    def sleep(self, seconds, **_options):
        sleeps.append(seconds)
        eth.sources["ethereum"].head = LAST
        if len(sleeps) == 2:
            self.requested = True
        return self.requested

    monkeypatch.setattr(StopFlag, "sleep", sleep)
    assert eth.syncer().run(every=300)
    assert eth.sources["ethereum"].extracted == [(FIRST, FIRST), (LAST, LAST)]
    assert len(sleeps) == 2 and all(0 < s <= 300 for s in sleeps)
    assert eth.lines[-1].endswith("stopped")


def test_stop_flag_signals(monkeypatch):
    stop = StopFlag()
    with stop.handling_signals():
        stop._handle(15, None)
        assert stop.requested
        with pytest.raises(KeyboardInterrupt):
            stop._handle(2, None)  # a second signal stops at once
    assert stop.sleep(10) is True  # returns immediately once requested


def test_one_sync_per_data_folder(tmp_path):
    with (
        data_dir_lock(tmp_path),
        pytest.raises(SyncError, match="already using"),
        data_dir_lock(tmp_path),
    ):
        pass
    with data_dir_lock(tmp_path):  # released again
        pass


def test_wallet_checkpoint_comes_from_the_chain(tmp_path, monkeypatch):
    # A wallet's EVM data has blocks only where its transfers are, so the last
    # block's hash is asked of the node.
    plan = _plan(tmp_path, {"ethereum": 1}, from_block=FIRST, wallet="0xabc")
    run = Run(tmp_path, monkeypatch, plan, {"ethereum": LAST})
    asked = []
    source = run.sources["ethereum"]
    real_block = source.block
    source.block = lambda number: asked.append(number) or real_block(number)
    run.run_pass()
    assert asked == [LAST]
    assert run.state().checkpoint_hash == real_block(LAST).hash


def test_several_chains_one_pass(tmp_path, monkeypatch):
    chains = {"ethereum": 1, "solana": FINALIZED, "bitcoin": 1}
    heads = {name: FIXTURE_BLOCKS[name][1] for name in chains}
    run = Run(tmp_path, monkeypatch, _plan(tmp_path, chains, from_recent=2), heads)
    # Solana slots 459564295 and 459564296; the extractor is asked for both.
    assert run.run_pass()
    with attach(run.plan.target) as warehouse:
        states = read_states(warehouse)
    assert {name: s.last_block for name, s in states.items()} == heads
    assert run.builds == [True]  # one dbt build for all chains
    assert run.raw_count("bitcoin_outputs") == 26
    assert run.raw_count("solana_transfers") == 20


def test_a_failing_chain_does_not_stop_the_others(tmp_path, monkeypatch):
    chains = {"ethereum": 1, "bitcoin": 1}
    heads = {name: FIXTURE_BLOCKS[name][1] for name in chains}
    run = Run(tmp_path, monkeypatch, _plan(tmp_path, chains, from_recent=1), heads)

    def unreachable():
        raise OSError(f"could not reach {SECRET_URL}")

    run.sources["ethereum"].identity = unreachable
    assert run.run_pass() is False
    assert run.state("bitcoin").last_block == heads["bitcoin"]
    assert "supersecretkey" not in run.text()
    assert "rpc.example/<redacted>" in run.text()


def test_json_logs(eth: Run):
    eth.lines.clear()
    syncer = eth.syncer()
    syncer.log = SyncLog(eth.lines.append, json_lines=True, secrets=[SECRET_URL])
    syncer.run_pass()
    records = [json.loads(line) for line in eth.lines]
    load = next(r for r in records if r["stage"] == "load")
    assert load["chain"] == "ethereum"
    assert load["first_block"] == load["last_block"] == FIRST
    assert load["rows"]["erc20_transfers"] == 541
    assert isinstance(load["seconds"], float)
    assert {r["level"] for r in records} == {"info"}


def test_extract_writes_chunk_files_once(eth: Run):
    eth.run_pass()
    files = parquet_files(eth.plan.data_dir, "ethereum", sync.RANGED_DATASETS[0])
    assert [f.name for f in files] == ["ethereum__erc20_transfers__26239738_to_26239738.parquet"]
