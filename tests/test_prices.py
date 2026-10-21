"""USD prices from swaps: the method of fct_token_prices, checked by hand on a small made-up market.

The swaps here are invented, so every expected number can be worked out on
paper; the decoding of real swaps is tested in test_swaps.py. One UTC hour on
Ethereum (2026-01-01 00:00) holds:

- WETH (priced as ETH) against USDC and USDT, in two pools: 7 swaps of at least
  10 USD at 2000, 2010, 1990, 2005, 1995, 3000 and 1000 USD per ETH, and a 5 USD
  swap at 5000. The 5 USD swap is dust and dropped. The lower median of the 7 is
  2000; 3000 and 1000 are more than 10% from it and rejected; the lower median
  of the 5 kept (1990 1995 2000 2005 2010) is 2000. 5 observations, 15,015 USD
  (four of 3,000 and one of 3,015), 2 pools: high confidence.
- TOK against WETH only: 3 swaps of 1 WETH for 1000, 950 and 1050 TOK, that is
  2000/1000 = 2.0, 2000/950 = 2.105 and 2000/1050 = 1.905 USD per TOK, priced
  through the hour's ETH price: 3 observations, 6,000 USD, one pool: medium.
- USDC for USDT, 1000 for 999: USDT observed at 1000/999, USDC at 0.999; both
  stay 1 USD (the peg).
- TOK2 against USDC, but in a pool whose factory is not a known venue: unpriced.
- WBTC against USDC: 3 swaps at 60,000, 60,100 and 59,900: medium; Bitcoin's
  block in the same hour takes 60,000 from it, cross-chain.

The next hour has one WETH swap at 2100: one observation, low confidence, so no
usd_price. The day's ETH price uses all 8: lower median 2000 (the 4th of 8), kept
1990 to 2100 (6), whose lower median is the 3rd, 2000.
"""

from pathlib import Path

import pytest

from chainstar.chains import get_chain
from chainstar.chunks import write_parquet
from chainstar.datasets import (
    BITCOIN_BLOCKS,
    BITCOIN_OUTPUTS,
    BITCOIN_TRANSACTIONS,
    BLOCKS,
    DEX_POOLS,
    DEX_SWAPS,
    ERC20_METADATA,
    ERC20_TRANSFERS,
    TRANSACTIONS,
    Dataset,
    dataset_dir,
)
from chainstar.load import attach, load_raw
from chainstar.model import build_models
from chainstar.targets import DuckDBTarget
from sync_checks import snapshot

HOUR = 1_767_225_600  # 2026-01-01 00:00:00 UTC
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
TOK = "0x" + "11" * 20
TOK2 = "0x" + "22" * 20
UNISWAP_V3 = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
UNKNOWN_FACTORY = "0x" + "99" * 20
DECIMALS = {USDC: 6, USDT: 6, WETH: 18, WBTC: 8, TOK: 18, TOK2: 18}
# pool: (token0, token1, factory). Uniswap orders a pool's tokens by address.
POOLS = {
    "0x" + "a1" * 20: (USDC, WETH, UNISWAP_V3),
    "0x" + "a2" * 20: (WETH, USDT, UNISWAP_V3),
    "0x" + "a3" * 20: (TOK, WETH, UNISWAP_V3),
    "0x" + "a4" * 20: (USDC, USDT, UNISWAP_V3),
    "0x" + "a5" * 20: (TOK2, USDC, UNKNOWN_FACTORY),
    "0x" + "a6" * 20: (WBTC, USDC, UNISWAP_V3),
}
POOL = {tokens[:2]: pool for pool, tokens in POOLS.items()}


def _raw(token: str, amount: float) -> str:
    return str(round(amount * 10 ** DECIMALS[token]))


def _swap(block: int, log: int, token_in: str, amount_in: float, token_out: str, amount_out: float):
    pool = POOL.get((token_in, token_out)) or POOL[(token_out, token_in)]
    zero_for_one = POOLS[pool][0] == token_in
    return {
        "block_number": block,
        "transaction_index": log,
        "log_index": log,
        "transaction_hash": f"0x{block:032x}{log:032x}",
        "pool": pool,
        "event_family": "uniswap_v3",
        "sender": "0x" + "ee" * 20,
        "recipient": "0x" + "ee" * 20,
        "zero_for_one": zero_for_one,
        "amount_in": _raw(token_in, amount_in),
        "amount_out": _raw(token_out, amount_out),
        "chain_id": 1,
    }


# (block, seconds after HOUR) of the Ethereum blocks.
BLOCK_TIMES = {100: 60, 101: 120, 102: 3600 + 60}


def _eth_swaps(block: int) -> list[dict]:
    if block == 100:
        # ETH at 2000 and 2010 (USDC), 1990 (USDT); the 5 USD dust swap.
        return [
            _swap(100, 1, USDC, 3000, WETH, 1.5),
            _swap(100, 2, WETH, 1.5, USDC, 3015),
            _swap(100, 3, USDT, 3000, WETH, 3000 / 1990),
            _swap(100, 4, USDC, 5, WETH, 0.001),
        ]
    if block == 101:
        return [
            # ETH at 2005 and 1995, and the two outliers.
            _swap(101, 1, WETH, 3000 / 2005, USDT, 3000),
            _swap(101, 2, USDC, 3000, WETH, 3000 / 1995),
            _swap(101, 3, USDC, 3000, WETH, 1.0),
            _swap(101, 4, USDC, 1000, WETH, 1.0),
            # TOK through WETH.
            _swap(101, 5, WETH, 1, TOK, 1000),
            _swap(101, 6, TOK, 950, WETH, 1),
            _swap(101, 7, WETH, 1, TOK, 1050),
            # Stablecoins against each other; a pool of an unknown factory.
            _swap(101, 8, USDC, 1000, USDT, 999),
            _swap(101, 9, USDC, 500, TOK2, 100),
            # WBTC.
            _swap(101, 10, USDC, 6000, WBTC, 0.1),
            _swap(101, 11, WBTC, 0.1, USDC, 6010),
            _swap(101, 12, USDC, 5990, WBTC, 0.1),
        ]
    return [_swap(102, 1, USDC, 2100, WETH, 1.0)]


def _transfer(block: int, log: int, token: str, amount: float) -> dict:
    return {
        "block_number": block,
        "transaction_index": 0,
        "log_index": 100 + log,
        "transaction_hash": f"0x{block:032x}{0:032x}",
        "erc20": token,
        "from_address": "0x" + "01" * 20,
        "to_address": "0x" + "02" * 20,
        "value_string": _raw(token, amount),
        "chain_id": 1,
    }


def _write(data_dir: Path, chain: str, dataset: Dataset, name: str, rows: list[dict]) -> None:
    write_parquet(dataset_dir(data_dir, chain, dataset) / name, dataset, rows)


def _write_ethereum(data_dir: Path, blocks: list[int]) -> None:
    for block in blocks:
        span = f"ethereum__{{}}__{block:08}_to_{block:08}.parquet"
        _write(
            data_dir,
            "ethereum",
            BLOCKS,
            span.format("blocks"),
            [
                {
                    "block_number": block,
                    "block_hash": f"0x{block:064x}",
                    "timestamp": HOUR + BLOCK_TIMES[block],
                    "base_fee_per_gas": 1,
                    "gas_used": 21000,
                    "chain_id": 1,
                }
            ],
        )
        _write(
            data_dir,
            "ethereum",
            TRANSACTIONS,
            span.format("transactions"),
            [
                {
                    "block_number": block,
                    "transaction_index": 0,
                    "transaction_hash": f"0x{block:032x}{0:032x}",
                    "from_address": "0x" + "01" * 20,
                    "to_address": "0x" + "02" * 20,
                    "value_string": "0",
                    "gas_limit": 21000,
                    "gas_used": 21000,
                    "gas_price": 10**9,  # 21,000 gwei: 0.000021 ETH
                    "transaction_type": 2,
                    "success": True,
                    "chain_id": 1,
                }
            ],
        )
        transfers = [
            _transfer(block, 1, WETH, 2),
            _transfer(block, 2, USDC, 5),
            _transfer(block, 3, TOK, 10),
            _transfer(block, 4, TOK2, 10),
        ]
        _write(data_dir, "ethereum", ERC20_TRANSFERS, span.format("erc20_transfers"), transfers)
        _write(data_dir, "ethereum", DEX_SWAPS, span.format("logs"), _eth_swaps(block))
    _write(
        data_dir,
        "ethereum",
        ERC20_METADATA,
        "ethereum__erc20_metadata__test.parquet",
        [
            {"block_number": 200, "erc20": token, "decimals": decimals, "chain_id": 1}
            for token, decimals in DECIMALS.items()
        ],
    )
    _write(
        data_dir,
        "ethereum",
        DEX_POOLS,
        "ethereum__dex_pools__test.parquet",
        [
            {
                "block_number": 200,
                "pool": pool,
                "event_family": "uniswap_v3",
                "factory": factory,
                "token0": token0,
                "token1": token1,
                "fee": 500,
                "tick_spacing": 10,
                "factory_confirms": True,
                "chain_id": 1,
            }
            for pool, (token0, token1, factory) in POOLS.items()
        ],
    )


def _write_bitcoin(data_dir: Path) -> None:
    span = "bitcoin__{}__00000900_to_00000900.parquet"
    common = {"chain": "bitcoin", "block_number": 900}
    _write(
        data_dir,
        "bitcoin",
        BITCOIN_BLOCKS,
        span.format("bitcoin_blocks"),
        [{**common, "block_hash": "00" * 32, "timestamp": HOUR + 300, "median_time": HOUR}],
    )
    tx = {**common, "transaction_index": 0, "transaction_hash": "ab" * 32}
    _write(
        data_dir,
        "bitcoin",
        BITCOIN_TRANSACTIONS,
        span.format("bitcoin_transactions"),
        [{**tx, "is_coinbase": True, "input_count": 1, "output_count": 1}],
    )
    _write(
        data_dir,
        "bitcoin",
        BITCOIN_OUTPUTS,
        span.format("bitcoin_outputs"),
        [{**tx, "output_index": 0, "address": "bc1qminer", "value_sat": 312_500_000}],
    )


def _build(target: DuckDBTarget, data_dir: Path, work: Path, *, full: bool) -> None:
    for chain in ("ethereum", "bitcoin"):
        load_raw(target, data_dir, get_chain(chain))
    result = build_models(target, work, full_refresh=full)
    assert result.failures == [], result.failures
    assert result.warnings == 0


def _prices(target: DuckDBTarget) -> dict[tuple, tuple]:
    with attach(target) as warehouse:
        rows = warehouse.query(
            "SELECT chain, token_address, granularity, bucket_start_unix, usd_price, "
            "observed_price, price_low, price_high, price_method, confidence, observations, "
            "rejected_observations, volume_usd, sources FROM chainstar.fct_token_prices"
        )
    return {row[:4]: row[4:] for row in rows}


@pytest.fixture
def market(tmp_path: Path) -> DuckDBTarget:
    data_dir = tmp_path / "data"
    _write_ethereum(data_dir, [100, 101, 102])
    _write_bitcoin(data_dir)
    target = DuckDBTarget(tmp_path / "w.duckdb")
    _build(target, data_dir, tmp_path / "dbt", full=True)
    return target


def test_eth_price_drops_dust_and_rejects_outliers(market: DuckDBTarget):
    price = _prices(market)[("ethereum", "native", "hour", HOUR)]
    usd, observed, low, high, method, confidence, kept, rejected, volume, sources = price
    assert usd == pytest.approx(2000) and observed == usd
    assert (low, high) == (pytest.approx(1990), pytest.approx(2010))
    assert (method, confidence) == ("swaps_vs_stablecoins", "high")
    assert (kept, rejected, sources) == (5, 2, 2)  # the dust swap is not even counted
    assert volume == pytest.approx(15015, abs=0.01)


def test_token_priced_through_eth(market: DuckDBTarget):
    usd, _, low, high, method, confidence, kept, rejected, volume, sources = _prices(market)[
        ("ethereum", TOK, "hour", HOUR)
    ]
    assert usd == pytest.approx(2.0)
    assert (low, high) == (pytest.approx(2000 / 1050), pytest.approx(2000 / 950))
    assert (method, confidence, kept, rejected, sources) == ("swaps_vs_native", "medium", 3, 0, 1)
    assert volume == pytest.approx(6000, abs=0.01)


def test_stablecoins_stay_at_the_peg_and_show_what_swaps_said(market: DuckDBTarget):
    prices = _prices(market)
    usdc = prices[("ethereum", USDC, "hour", HOUR)]
    usdt = prices[("ethereum", USDT, "hour", HOUR)]
    assert usdc[0] == usdt[0] == 1.0
    assert usdc[4:6] == usdt[4:6] == ("usd_stablecoin_peg", "peg")
    assert usdc[1] == pytest.approx(0.999)
    assert usdt[1] == pytest.approx(1000 / 999)
    # DAI had no swaps: still 1 USD, with nothing observed.
    dai = prices[("ethereum", "0x6b175474e89094c44da98b954eedeac495271d0f", "hour", HOUR)]
    assert (dai[0], dai[1], dai[6]) == (1.0, None, 0)


def test_unverified_pools_price_nothing(market: DuckDBTarget):
    assert not [key for key in _prices(market) if key[1] == TOK2]


def test_bitcoin_is_priced_from_wrapped_btc_cross_chain(market: DuckDBTarget):
    prices = _prices(market)
    wbtc = prices[("ethereum", WBTC, "hour", HOUR)]
    assert (wbtc[0], wbtc[5]) == (pytest.approx(60000), "medium")
    bitcoin = prices[("bitcoin", "native", "hour", HOUR)]
    assert bitcoin[0] == pytest.approx(60000)
    assert bitcoin[4:6] == ("cross_chain_wrapped_btc", "medium")


def test_one_observation_is_low_confidence_and_unpriced(market: DuckDBTarget):
    usd, observed, *_, method, confidence, kept, _, _, _ = _prices(market)[
        ("ethereum", "native", "hour", HOUR + 3600)
    ]
    assert (usd, observed, confidence, kept) == (None, pytest.approx(2100), "low", 1)
    assert method == "swaps_vs_stablecoins"


def test_day_price_uses_every_swap_of_the_day(market: DuckDBTarget):
    usd, _, low, high, *_, kept, rejected, _, _ = _prices(market)[
        ("ethereum", "native", "day", HOUR)
    ]
    assert usd == pytest.approx(2000)
    assert (low, high, kept, rejected) == (pytest.approx(1990), pytest.approx(2100), 6, 2)


def test_transfers_fees_and_swaps_are_valued_at_the_hourly_price(market: DuckDBTarget):
    with attach(market) as warehouse:
        transfers = dict(
            warehouse.query(
                "SELECT token_address || ':' || block_number, amount_usd "
                "FROM chainstar.fct_transfers WHERE chain = 'ethereum'"
            )
        )
        fee = warehouse.query(
            "SELECT fee_usd, native_usd_price, price_method, price_confidence, "
            "price_bucket_start FROM chainstar.fct_fees WHERE block_number = 100"
        )[0]
        coinbase = warehouse.query(
            "SELECT amount_usd, price_method FROM chainstar.fct_transfers WHERE chain = 'bitcoin'"
        )[0]
        dust = warehouse.query(
            "SELECT usd_value, usd_value_side FROM chainstar.fct_swaps WHERE log_index = 4 "
            "AND block_number = 100"
        )[0]
        unknown = warehouse.query(
            "SELECT venue, pool_verified, usd_value FROM chainstar.fct_swaps "
            f"WHERE token_in_address = '{USDC}' AND token_out_address = '{TOK2}'"
        )[0]
    assert transfers[f"{WETH}:100"] == pytest.approx(4000)  # 2 WETH at 2000
    assert transfers[f"{USDC}:100"] == pytest.approx(5)
    assert transfers[f"{TOK}:100"] == pytest.approx(20)  # 10 TOK at 2.0
    assert transfers[f"{TOK2}:100"] is None
    assert transfers[f"{WETH}:102"] is None  # the next hour has only a low-confidence price
    assert fee[0] == pytest.approx(0.000021 * 2000)
    assert fee[1:4] == (pytest.approx(2000), "swaps_vs_stablecoins", "high")
    assert str(fee[4]) == "2026-01-01 00:00:00"
    assert coinbase[0] == pytest.approx(3.125 * 60000)
    assert coinbase[1] == "cross_chain_wrapped_btc"
    # A swap is valued by its stablecoin side; an unverified pool's is not valued.
    assert dust == (pytest.approx(5), "in")
    assert unknown == ("unknown", False, None)


def test_incremental_builds_revalue_the_open_hour_and_bitcoin(tmp_path: Path):
    """Block 100 first, alone with Bitcoin's block; then 101 and 102. The second
    build is incremental: it must reprice hour 0 (now with block 101's swaps),
    revalue block 100's transfers and Bitcoin's coinbase, and so equal a full build."""
    data_dir = tmp_path / "data"
    _write_ethereum(data_dir, [100])
    _write_bitcoin(data_dir)
    target = DuckDBTarget(tmp_path / "w.duckdb")
    _build(target, data_dir, tmp_path / "dbt", full=True)
    first = _prices(target)
    assert first[("bitcoin", "native", "hour", HOUR)][0] is None  # no WBTC swaps yet
    assert first[("ethereum", "native", "hour", HOUR)][5] == "medium"  # 3 kept, 9,015 USD

    _write_ethereum(data_dir, [100, 101, 102])
    _build(target, data_dir, tmp_path / "dbt", full=False)
    incremental = snapshot(target)
    _build(target, data_dir, tmp_path / "dbt", full=True)
    assert snapshot(target) == incremental
    assert _prices(target)[("bitcoin", "native", "hour", HOUR)][0] == pytest.approx(60000)
