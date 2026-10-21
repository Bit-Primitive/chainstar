"""Decoding real swap logs, reading pools, and a wallet's pricing pools.

tests/rpc_fixtures/cryo_swap_logs_26239738_52892500.parquet is cryo 0.3.2's raw
`logs` output (the three swap topics) for Ethereum blocks 26239738-26239739 and
Base blocks 52892500-52892501, unedited: 54 and 30 logs. Every swap decoded from
it was checked against the Transfer events in its transaction's receipt when the
fixture was made (the amount the pool received and paid, to the unit). The
committed tests/fixtures/*/dex_swaps files are this decoder's output, and
tests/fixtures/*/dex_pools what read_pools read for their pools at the chain head.
"""

import shutil
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from chainstar import swaps
from chainstar.chains import get_chain
from chainstar.config import BlockRange, RunSettings
from chainstar.datasets import DEX_SWAPS, dataset_dir, parquet_files
from chainstar.targets import DuckDBTarget
from chainstar.venues import SELECTORS
from conftest import FIXTURES

RAW_LOGS = Path(__file__).parent / "rpc_fixtures" / "cryo_swap_logs_26239738_52892500.parquet"


def _raw_logs(chain_id: int) -> list[dict]:
    with duckdb.connect() as connection:
        cursor = connection.execute(
            f"SELECT {', '.join(swaps.LOG_COLUMNS)} FROM read_parquet(?) WHERE chain_id = ? "
            "ORDER BY block_number, log_index",
            [str(RAW_LOGS), chain_id],
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _committed(chain: str) -> list[tuple]:
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY block_number, log_index",
            [[str(f) for f in parquet_files(FIXTURES, chain, DEX_SWAPS)]],
        ).fetchall()


@pytest.mark.parametrize(("chain", "chain_id", "count"), [("ethereum", 1, 54), ("base", 8453, 30)])
def test_decoding_real_logs_reproduces_the_committed_fixture(tmp_path, chain, chain_id, count):
    logs = _raw_logs(chain_id)
    assert len(logs) == count
    rows = [swaps.decode_log(log) for log in logs]
    assert all(row is not None for row in rows)  # every real log is a well-formed swap
    # Decoding in place, as after a cryo run, gives the committed file.
    folder = dataset_dir(tmp_path, chain, DEX_SWAPS)
    folder.mkdir(parents=True)
    name = parquet_files(FIXTURES, chain, DEX_SWAPS)[0].name
    with duckdb.connect() as connection:
        connection.execute(
            f"COPY (SELECT * FROM read_parquet(?) WHERE chain_id = ?) TO '{folder / name}' "
            "(FORMAT parquet)",
            [str(RAW_LOGS), chain_id],
        )
    assert swaps.decode_swap_files(tmp_path, chain) == 1
    assert swaps.decode_swap_files(tmp_path, chain) == 0  # already decoded: left alone
    with duckdb.connect() as connection:
        decoded = connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY block_number, log_index",
            [str(folder / name)],
        ).fetchall()
    assert decoded == _committed(chain)


def _log(chain_id: int, block: int, log_index: int) -> dict:
    return next(
        log
        for log in _raw_logs(chain_id)
        if (log["block_number"], log["log_index"]) == (block, log_index)
    )


def _word(data: str, index: int) -> int:
    return int(data[2 + 64 * index : 2 + 64 * (index + 1)], 16)


def test_uniswap_v3_amounts_are_signed():
    # Ethereum block 26239739, log 22: the pool paid out token0 (a negative
    # amount0, in two's complement) and received token1.
    log = _log(1, 26239739, 22)
    row = swaps.decode_log(log)
    amount0 = _word(log["data"], 0) - 2**256
    amount1 = _word(log["data"], 1)
    assert amount0 < 0 < amount1
    assert row["event_family"] == "uniswap_v3"
    assert row["zero_for_one"] is False
    assert row["amount_in"] == str(amount1) == row["amount1_in"]
    assert row["amount_out"] == str(-amount0) == row["amount0_out"]
    assert (row["amount0_in"], row["amount1_out"]) == ("0", "0")
    assert row["tick"] == _word(log["data"], 4) - 2**256 < 0
    assert row["sqrt_price_x96"] == str(_word(log["data"], 2))
    assert row["liquidity"] == str(_word(log["data"], 3))
    assert row["sender"] == "0x" + log["topic1"][-40:]
    assert row["recipient"] == "0x" + log["topic2"][-40:]


def test_aerodrome_swaps_have_their_own_layout():
    # Base block 52892500, log 1467, an Aerodrome USDC/AERO pool: AERO in
    # (amount1In) and 742,267 units of USDC (0.742267) out (amount0Out).
    log = _log(8453, 52892500, 1467)
    row = swaps.decode_log(log)
    assert row["event_family"] == "solidly_v2"
    assert row["zero_for_one"] is False
    assert row["amount_in"] == str(int("0c41ef9b7ca0c468", 16))
    assert row["amount_out"] == "742267"
    assert row["sqrt_price_x96"] is None and row["tick"] is None


def test_uniswap_v2_direction_nets_in_and_out():
    log = _log(1, 26239739, 70)  # Uniswap v2: amount0In and amount1Out only
    row = swaps.decode_log(log)
    assert row["event_family"] == "uniswap_v2"
    assert row["zero_for_one"] is True
    assert row["amount_in"] == str(_word(log["data"], 0))
    assert row["amount_out"] == str(_word(log["data"], 3))
    # A flash-swap shape with no single direction keeps its amounts but no direction.
    both_in = log | {"data": "0x" + "".join(format(v, "064x") for v in (5, 5, 1, 1))}
    row = swaps.decode_log(both_in)
    assert row["zero_for_one"] is None and row["amount_in"] is None
    assert row["amount0_in"] == "5"


def test_malformed_logs_are_not_swaps():
    log = _log(1, 26239739, 22)
    assert swaps.decode_log(log | {"data": log["data"][:-64]}) is None  # a word short
    assert swaps.decode_log(log | {"topic3": log["topic1"]}) is None  # another indexing
    assert swaps.decode_log(log | {"topic1": "0x" + "f" * 64}) is None  # not an address
    assert swaps.decode_log(log | {"topic0": "0x" + "0" * 64}) is None


def _word_hex(value: int) -> str:
    return "0x" + format(value % 2**256, "064x")


def _address(value: str) -> str:
    return "0x" + value[2:].rjust(64, "0")


class FakeChain:
    """Answers eth_call batches from a table of (to, calldata) -> result."""

    def __init__(self, answers: dict[tuple[str, str], str]) -> None:
        self.answers = answers
        self.batches: list[list] = []

    def __call__(self, *_args, **_kwargs) -> "FakeChain":
        return self

    def call(self, method: str, params=None, **_):
        assert method == "eth_blockNumber"
        return hex(1000)

    def batch(self, calls: list) -> list:
        self.batches.append(calls)
        results = []
        for method, (call, block) in calls:
            assert method == "eth_call" and block in ("latest", hex(1000))
            results.append(self.answers.get((call["to"], call["data"])))
        return results


POOL = "0x" + "a1" * 20
TOKEN0, TOKEN1 = "0x" + "0a" * 20, "0x" + "0b" * 20
FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"


def _pool_answers(pool: str, factory: str, extra: dict) -> dict:
    answers = {
        (pool, SELECTORS["token0()"]): _address(TOKEN0),
        (pool, SELECTORS["token1()"]): _address(TOKEN1),
        (pool, SELECTORS["factory()"]): _address(factory),
    }
    answers.update(extra)
    return answers


def _lookup(factory: str, signature: str, last: int) -> tuple[str, str]:
    data = SELECTORS[signature] + TOKEN0[2:].rjust(64, "0") + TOKEN1[2:].rjust(64, "0")
    return (factory, data + format(last, "064x"))


def test_pools_count_only_when_their_factory_confirms_them():
    honest, impostor, slipstream = POOL, "0x" + "a2" * 20, "0x" + "a3" * 20
    cl_factory = "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a"
    answers = {
        **_pool_answers(
            honest,
            FACTORY,
            {
                (honest, SELECTORS["fee()"]): _word_hex(500),
                (honest, SELECTORS["tickSpacing()"]): _word_hex(10),
                _lookup(FACTORY, "getPool(address,address,uint24)", 500): _address(honest),
            },
        ),
        # Claims Uniswap's factory, which knows a different pool for its tokens and fee.
        **_pool_answers(
            impostor,
            FACTORY,
            {(impostor, SELECTORS["fee()"]): _word_hex(3000)},
        ),
        **_pool_answers(
            slipstream,
            cl_factory,
            {
                (slipstream, SELECTORS["fee()"]): _word_hex(400),
                (slipstream, SELECTORS["tickSpacing()"]): _word_hex(100),
                _lookup(cl_factory, "getPool(address,address,int24)", 100): _address(slipstream),
            },
        ),
    }
    answers[_lookup(FACTORY, "getPool(address,address,uint24)", 3000)] = _address(honest)
    chain = FakeChain(answers)
    families = {honest: "uniswap_v3", impostor: "uniswap_v3", slipstream: "uniswap_v3"}
    rows = {row["pool"]: row for row in swaps.read_pools(chain, families, 1000, 1)}
    assert rows[honest]["factory_confirms"] is True
    assert (rows[honest]["fee"], rows[honest]["tick_spacing"]) == (500, 10)
    assert rows[impostor]["factory_confirms"] is False
    assert rows[impostor]["token0"] == TOKEN0  # what it claims is kept, unverified
    assert rows[slipstream]["factory_confirms"] is True  # found by tick spacing
    # Every call is made against the block the pools are recorded at.
    assert {call[1][1] for batch in chain.batches for call in batch} == {hex(1000)}


def test_a_contract_without_pool_functions_reads_as_unknown():
    chain = FakeChain({})
    (row,) = swaps.read_pools(chain, {POOL: "uniswap_v2"}, 1000, 1)
    assert (row["token0"], row["token1"], row["factory"], row["factory_confirms"]) == (
        None,
        None,
        None,
        False,
    )


def _settings(data_dir: Path, chain: str = "base") -> RunSettings:
    return RunSettings(
        chain=get_chain(chain),
        blocks=BlockRange(52892500, 52892502),
        rpc_url="https://rpc.example",
        target=DuckDBTarget(data_dir / "w.duckdb"),
        data_dir=data_dir,
        requests_per_second=None,
        chunk_size=10,
        wallet="0x" + "cc" * 20,
    )


def test_wallet_pricing_pools_are_looked_up_once_per_token(tmp_path: Path, monkeypatch):
    found = "0x" + "d1" * 20
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    weth = "0x4200000000000000000000000000000000000006"
    uniswap_v3 = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
    data = (
        SELECTORS["getPool(address,address,uint24)"]
        + weth[2:].rjust(64, "0")
        + usdc[2:].rjust(64, "0")
        + format(500, "064x")
    )
    chain = FakeChain({(uniswap_v3, data): _address(found)})
    monkeypatch.setattr(swaps, "RpcClient", chain)
    settings = _settings(tmp_path)
    assert swaps.pricing_pools(settings, []) == [found]
    # Uniswap v2 (1), v3 (4 fee tiers) and Aerodrome (2): 7 lookups for WETH/USDC
    # and 7 for USDC/WETH.
    assert sum(len(batch) for batch in chain.batches) == 14
    assert swaps.pricing_pools(settings, []) == [found]
    assert sum(len(batch) for batch in chain.batches) == 14  # cached


def test_wallet_pricing_swaps_are_filtered_by_pool(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(swaps, "pricing_pools", lambda settings, tokens: [POOL])
    calls = []
    monkeypatch.setattr(swaps, "run_cryo", lambda dataset, **kwargs: calls.append(kwargs))
    shutil.copytree(FIXTURES / "base", tmp_path / "base")
    pools, rows = swaps.extract_wallet_pricing_swaps(replace(_settings(tmp_path), chunk_size=2))
    args = calls[0]["args"]
    assert args[args.index("--address") + 1] == POOL
    assert args[args.index("--inner-request-size") + 1] == "2"
    assert args[args.index("--label") + 1].startswith("pricing-")
    assert (pools, rows) == (1, 30)  # the fixture's swaps stand in for what cryo wrote
