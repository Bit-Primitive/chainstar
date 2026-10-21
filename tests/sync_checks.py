"""End-to-end sync scenarios shared by the DuckDB and Postgres tests, with the real dbt build.

The key check: after two syncs, the incrementally built tables hold exactly what a
full rebuild of the same raw data gives, row for row.
"""

from pathlib import Path

from chainstar.chains import get_chain
from chainstar.config import Connection, RpcChoice
from chainstar.heads import FINALIZED
from chainstar.load import attach
from chainstar.model import MART_SCHEMA, MART_TABLES, build_models
from chainstar.sync import ChainPlan, StopFlag, Syncer, SyncPlan
from chainstar.synclog import SyncLog
from chainstar.targets import Target
from fixture_chain import FIXTURE_BLOCKS, FixtureSource, make_source_dir, rewrite_block

ALL_CHAINS = ("ethereum", "base", "bitcoin", "solana")
# Every fixture block is final at once, so two syncs take one block each.
CONFIRMATIONS = {"ethereum": 1, "base": 1, "bitcoin": 1, "solana": FINALIZED}
SNAPSHOT_TABLES = [
    *((MART_SCHEMA, table) for table in MART_TABLES),
    ("chainstar_staging", "int_chain_coverage"),
]


def snapshot(target: Target) -> dict[str, list[tuple]]:
    with attach(target) as warehouse:
        if not warehouse.via_duckdb:
            # A remote warehouse's rows, sorted by DuckDB.
            tables = {}
            for schema, table in SNAPSHOT_TABLES:
                warehouse.fetch_into("rows", f"SELECT * FROM {warehouse.table(schema, table)}", [])
                tables[table] = warehouse.local.execute(
                    "SELECT * FROM rows ORDER BY ALL"
                ).fetchall()
            return tables
        return {
            table: warehouse.connection.execute(
                f"SELECT * FROM {warehouse.table(schema, table)} ORDER BY ALL"
            ).fetchall()
            for schema, table in SNAPSHOT_TABLES
        }


def make_syncer(target: Target, tmp_path: Path, chains: tuple[str, ...], lines: list[str]):
    source = make_source_dir(tmp_path)
    sources = {
        name: FixtureSource(get_chain(name), source, FIXTURE_BLOCKS[name][0]) for name in chains
    }
    plan = SyncPlan(
        target=target,
        data_dir=tmp_path / "data",
        chains=tuple(
            ChainPlan(
                get_chain(name),
                Connection(RpcChoice("https://rpc.example", public=False), None, 1),
                CONFIRMATIONS[name],
            )
            for name in chains
        ),
        from_recent=1,
    )
    return Syncer(plan, sources, SyncLog(lines.append), StopFlag()), sources, source


def assert_matches_full_rebuild(target: Target, work_dir: Path) -> dict[str, list[tuple]]:
    built = snapshot(target)
    result = build_models(target, work_dir, full_refresh=True)
    assert result.failures == [], result.failures
    rebuilt = snapshot(target)
    for table in built:
        assert built[table] == rebuilt[table], f"{table} differs from a full rebuild"
    return built


def two_syncs_equal_a_full_rebuild(target: Target, tmp_path: Path) -> None:
    lines: list[str] = []
    syncer, sources, _ = make_syncer(target, tmp_path, ALL_CHAINS, lines)
    assert syncer.run_pass(), "\n".join(lines)
    assert any("dbt build (full refresh: first sync" in line for line in lines)

    for name, source in sources.items():
        source.head = FIXTURE_BLOCKS[name][1]
    del lines[:]
    assert syncer.run_pass(), "\n".join(lines)
    assert any("dbt build (incremental)" in line for line in lines), "\n".join(lines)

    tables = assert_matches_full_rebuild(target, syncer.plan.work_dir)

    # The second sync really added to the first: every chain has rows from both
    # blocks, and wallets seen in both were merged rather than duplicated.
    for name in ALL_CHAINS:
        blocks = {row[2] for row in tables["fct_transfers"] if row[1] == name}
        assert blocks == set(FIXTURE_BLOCKS[name]), name
    wallets = [row for row in tables["dim_wallets"] if row[1] == "bitcoin"]
    both = [row for row in wallets if (row[4], row[6]) == FIXTURE_BLOCKS["bitcoin"]]
    assert both, "no Bitcoin wallet spans both syncs"
    assert all(row[5] != row[7] for row in both)  # first and last seen a day apart
    assert len(tables["dim_dates"]) == 2  # the synthetic Bitcoin block is a day later
    tokens = {row[1]: row for row in tables["dim_tokens"] if row[2] == "native"}
    assert tokens["bitcoin"][-2:] == FIXTURE_BLOCKS["bitcoin"]  # first and last seen


def reorg_matches_a_full_rebuild(target: Target, tmp_path: Path) -> None:
    lines: list[str] = []
    syncer, sources, source_dir = make_syncer(target, tmp_path, ("ethereum",), lines)
    first, last = FIXTURE_BLOCKS["ethereum"]
    sources["ethereum"].head = last
    assert syncer.run_pass(), "\n".join(lines)
    before = snapshot(target)

    rewrite_block(source_dir, get_chain("ethereum"), last, "0x" + "ab" * 32)
    assert syncer.run_pass(), "\n".join(lines)
    assert any("rolled back from block 26,239,739" in line for line in lines)

    after = assert_matches_full_rebuild(target, syncer.plan.work_dir)
    with attach(target) as warehouse:
        raw = warehouse.query(
            f"SELECT count(*) FROM {warehouse.table('chainstar_raw', 'erc20_transfers')} "
            "WHERE block_number = ?",
            [last],
        )[0][0]
    in_mart = [row for row in after["fct_transfers"] if row[2] == last]
    assert len(in_mart) == raw
    assert raw < len([row for row in before["fct_transfers"] if row[2] == last])
    # The block before the reorganisation is untouched.
    assert [r for r in after["fct_transfers"] if r[2] == first] == [
        r for r in before["fct_transfers"] if r[2] == first
    ]
