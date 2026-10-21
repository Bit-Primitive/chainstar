"""DEX swaps on Ethereum and Base: extract the Swap logs, decode them, read each pool.

extract: cryo's `logs` dataset, filtered to the three swap events' topic0 (see
         venues.py), one eth_getLogs per block as for ERC-20 transfers. cryo
         writes raw logs; each chunk file is then decoded in place into the
         dex_swaps layout (datasets.py). The file keeps its name, so cryo skips
         the chunk on later runs exactly as before. Decoding is chainstar's own,
         in Python, because the amounts are 256-bit integers (signed for
         Uniswap v3), which no SQL type shared by every warehouse can hold.
decode:  for every pool seen, token0(), token1() and factory(), plus fee() and
         tickSpacing() (Uniswap v3-style) or stable() (Aerodrome), then the
         factory's own lookup of the pool for those tokens: getPair (v2),
         getPool(tokens, fee) or getPool(tokens, tickSpacing) (v3-style),
         getPool(tokens, stable) (Aerodrome). Only a pool its factory confirms
         counts as that factory's venue. Read once per pool at the chain head
         and cached, like token metadata; factory event logs (PairCreated,
         PoolCreated) would need every block since each factory was deployed.

A wallet history (`chainstar wallet`) does not read every swap. It looks up, for
each token the wallet moved, its pools against the chain's wrapped native token
and USDC on Uniswap v2 and v3 (and Aerodrome on Base), and fetches only those
pools' swaps: eth_getLogs filtered by pool address, which public endpoints
answer for many blocks at once.
"""

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb

from chainstar.chunks import write_parquet
from chainstar.config import RunSettings
from chainstar.cryo import run_cryo
from chainstar.datasets import (
    DEX_POOLS,
    DEX_SWAPS,
    ERC20_TRANSFERS,
    dataset_dir,
    parquet_files,
)
from chainstar.rpc import RpcClient
from chainstar.venues import (
    SELECTORS,
    SOLIDLY_V2,
    SWAP_TOPICS,
    UNISWAP_V2,
    UNISWAP_V3,
    UNISWAP_V3_FEE_TIERS,
    USD_STABLECOIN,
    VENUES,
    WRAPPED_NATIVE,
    assets_with_role,
)

# cryo's raw log columns, in the order cryo 0.3.2 writes them.
LOG_COLUMNS = (
    "block_number",
    "transaction_index",
    "log_index",
    "transaction_hash",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
    "chain_id",
)
# Calls per JSON-RPC batch: small, so a batch is no burst for a public endpoint.
BATCH_CALLS = 10
# Pools read and saved together: a failure later keeps the pools already read.
POOLS_PER_FILE = 50
WORD = 64  # hex digits in a 32-byte ABI word
ZERO_ADDRESS = "0x" + "0" * 40


# Decoding ---------------------------------------------------------------------


def _words(data: str | None, count: int) -> list[int] | None:
    """The ABI words of a log's data as unsigned integers; None if not exactly `count`."""
    if not data or not data.startswith("0x") or len(data) != 2 + WORD * count:
        return None
    try:
        return [int(data[2 + WORD * i : 2 + WORD * (i + 1)], 16) for i in range(count)]
    except ValueError:
        return None


def _signed(word: int) -> int:
    return word - (1 << 256) if word >> 255 else word


def _topic_address(topic: str | None) -> str | None:
    """An indexed address: a 32-byte topic whose first 12 bytes are zero."""
    if not topic or len(topic) != 2 + WORD or topic[2:26] != "0" * 24:
        return None
    return "0x" + topic[26:].lower()


def _direction(net0: int, net1: int) -> tuple[bool, int, int] | None:
    """(zero_for_one, amount_in, amount_out) from what the pool gained of each token."""
    if net0 > 0 and net1 < 0:
        return True, net0, -net1
    if net1 > 0 and net0 < 0:
        return False, net1, -net0
    return None


def decode_log(log: dict[str, Any]) -> dict[str, Any] | None:
    """One cryo log row as a dex_swaps row, or None if it is not a well-formed swap.

    Any contract can emit a log with a swap event's topic0 but another layout
    (different indexing, extra data), so the shape is checked, not assumed.
    """
    family = SWAP_TOPICS.get((log.get("topic0") or "").lower())
    if family is None or log.get("topic3") is not None:
        return None
    sender, recipient = _topic_address(log.get("topic1")), _topic_address(log.get("topic2"))
    if sender is None or recipient is None:
        return None
    row: dict[str, Any] = {
        "block_number": log["block_number"],
        "transaction_index": log["transaction_index"],
        "log_index": log["log_index"],
        "transaction_hash": log["transaction_hash"],
        "pool": str(log["address"]).lower(),
        "event_family": family,
        "sender": sender,
        "recipient": recipient,
        "sqrt_price_x96": None,
        "liquidity": None,
        "tick": None,
        "chain_id": log["chain_id"],
    }
    if family == UNISWAP_V3:
        words = _words(log.get("data"), 5)
        if words is None:
            return None
        amount0, amount1 = _signed(words[0]), _signed(words[1])
        tick = _signed(words[4])
        if words[2] >> 160 or words[3] >> 128 or not -(1 << 23) <= tick < (1 << 23):
            return None
        row.update(
            amount0_in=str(max(amount0, 0)),
            amount1_in=str(max(amount1, 0)),
            amount0_out=str(max(-amount0, 0)),
            amount1_out=str(max(-amount1, 0)),
            sqrt_price_x96=str(words[2]),
            liquidity=str(words[3]),
            tick=tick,
        )
        net0, net1 = amount0, amount1
    else:
        # Uniswap v2 and Aerodrome: the same four amounts, in the same data order;
        # only the indexed `to` moved (topic2 in both).
        words = _words(log.get("data"), 4)
        if words is None:
            return None
        in0, in1, out0, out1 = words
        row.update(
            amount0_in=str(in0), amount1_in=str(in1), amount0_out=str(out0), amount1_out=str(out1)
        )
        net0, net1 = in0 - out0, in1 - out1
    direction = _direction(net0, net1)
    row["zero_for_one"] = direction[0] if direction else None
    row["amount_in"] = str(direction[1]) if direction else None
    row["amount_out"] = str(direction[2]) if direction else None
    return row


def _is_decoded(connection: duckdb.DuckDBPyConnection, path: Path) -> bool:
    cursor = connection.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(path)])
    return "event_family" in {column[0] for column in cursor.description}


def decode_swap_files(data_dir: Path, chain: str) -> int:
    """Decode every chunk file cryo wrote that is still raw logs; return how many.

    Each file is replaced in one rename, so a file is either raw or decoded.
    """
    decoded = 0
    with duckdb.connect() as connection:
        for path in parquet_files(data_dir, chain, DEX_SWAPS):
            if _is_decoded(connection, path):
                continue
            cursor = connection.execute(
                f"SELECT {', '.join(LOG_COLUMNS)} FROM read_parquet(?) "
                "ORDER BY block_number, log_index",
                [str(path)],
            )
            names = [column[0] for column in cursor.description]
            rows = [
                decode_log(dict(zip(names, values, strict=True))) for values in cursor.fetchall()
            ]
            write_parquet(path, DEX_SWAPS, [row for row in rows if row is not None])
            decoded += 1
    return decoded


# Extraction -------------------------------------------------------------------


def _log_args(settings: RunSettings) -> list[str]:
    return [
        "--blocks",
        str(settings.blocks),
        "--chunk-size",
        str(settings.chunk_size),
        "--topic0",
        *SWAP_TOPICS,
        "--columns",
        *LOG_COLUMNS,
    ]


def extract_swaps(settings: RunSettings) -> int:
    """Extract and decode every swap event in the range; return the swaps in it."""
    run_cryo(
        "logs",
        rpc_url=settings.rpc_url,
        output_dir=dataset_dir(settings.data_dir, settings.chain.name, DEX_SWAPS),
        args=_log_args(settings),
        requests_per_second=settings.requests_per_second,
    )
    decode_swap_files(settings.data_dir, settings.chain.name)
    return _swaps_in_range(settings)


def _swaps_in_range(settings: RunSettings) -> int:
    files = parquet_files(settings.data_dir, settings.chain.name, DEX_SWAPS)
    if not files:
        return 0
    with duckdb.connect() as connection:
        row = connection.execute(
            "SELECT count(*) FROM (SELECT DISTINCT transaction_hash, log_index "
            "FROM read_parquet(?, union_by_name = true) "
            "WHERE block_number >= ? AND block_number < ?)",
            [[str(f) for f in files], settings.blocks.start, settings.blocks.end],
        ).fetchone()
    return int(row[0]) if row else 0


# Reading pools ----------------------------------------------------------------


def _address_word(address: str) -> str:
    return address.lower().removeprefix("0x").rjust(WORD, "0")


def _int_word(value: int) -> str:
    return format(value % (1 << 256), "x").rjust(WORD, "0")


def _call(to: str, signature: str, *words: str) -> tuple[str, list]:
    return ("eth_call", [{"to": to, "data": SELECTORS[signature] + "".join(words)}, "latest"])


def _at(calls: list[tuple[str, list]], block: int) -> list[tuple[str, list]]:
    """The same calls, made against one block rather than whatever is latest."""
    return [(method, [params[0], hex(block)]) for method, params in calls]


def _as_address(result: Any) -> str | None:
    if not isinstance(result, str) or len(result) != 2 + WORD or result[2:26] != "0" * 24:
        return None
    return "0x" + result[26:].lower()


def _as_int(result: Any, *, signed: bool = False) -> int | None:
    if not isinstance(result, str) or len(result) != 2 + WORD:
        return None
    value = int(result, 16)
    return _signed(value) if signed else value


def _batched(client: RpcClient, calls: list[tuple[str, list]]) -> list[Any]:
    results: list[Any] = []
    for start in range(0, len(calls), BATCH_CALLS):
        results.extend(client.batch(calls[start : start + BATCH_CALLS]))
    return results


def _pools_seen(data_dir: Path, chain: str) -> dict[str, str]:
    """Pool address -> event family, for every decoded swap on disk."""
    files = parquet_files(data_dir, chain, DEX_SWAPS)
    if not files:
        return {}
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT pool, min(event_family) FROM read_parquet(?, union_by_name = true) "
            "WHERE event_family IS NOT NULL GROUP BY pool",
            [[str(f) for f in files]],
        ).fetchall()
    return dict(rows)


def _pools_known(data_dir: Path, chain: str) -> set[str]:
    files = parquet_files(data_dir, chain, DEX_POOLS)
    if not files:
        return set()
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT DISTINCT pool FROM read_parquet(?)", [[str(f) for f in files]]
        ).fetchall()
    return {row[0] for row in rows}


def read_pools(client: RpcClient, pools: dict[str, str], block: int, chain_id: int) -> list[dict]:
    """dex_pools rows for `pools` (address -> event family), read at the chain head."""
    order = sorted(pools)
    first: list[tuple[str, list]] = []
    for pool in order:
        first += [_call(pool, "token0()"), _call(pool, "token1()"), _call(pool, "factory()")]
        if pools[pool] == UNISWAP_V3:
            first += [_call(pool, "fee()"), _call(pool, "tickSpacing()")]
        elif pools[pool] == SOLIDLY_V2:
            first.append(_call(pool, "stable()"))
    answers = iter(_batched(client, _at(first, block)))
    rows = []
    for pool in order:
        family = pools[pool]
        token0, token1, factory = (_as_address(next(answers)) for _ in range(3))
        fee = tick_spacing = stable = None
        if family == UNISWAP_V3:
            fee = _as_int(next(answers))
            tick_spacing = _as_int(next(answers), signed=True)
        elif family == SOLIDLY_V2:
            flag = _as_int(next(answers))
            stable = None if flag not in (0, 1) else bool(flag)
        rows.append(
            {
                "block_number": block,
                "pool": pool,
                "event_family": family,
                "factory": factory,
                "token0": token0,
                "token1": token1,
                "fee": fee,
                "tick_spacing": tick_spacing,
                "stable": stable,
                "factory_confirms": False,
                "chain_id": chain_id,
            }
        )
    _confirm(client, rows, block)
    return rows


def _confirmations(row: dict) -> list[tuple[str, list]]:
    """The factory lookups that should return this pool, if it is the factory's."""
    factory, token0, token1 = row["factory"], row["token0"], row["token1"]
    if not (factory and token0 and token1) or factory == ZERO_ADDRESS:
        return []
    tokens = (_address_word(token0), _address_word(token1))
    if row["event_family"] == UNISWAP_V2:
        return [_call(factory, "getPair(address,address)", *tokens)]
    if row["event_family"] == SOLIDLY_V2:
        if row["stable"] is None:
            return []
        return [_call(factory, "getPool(address,address,bool)", *tokens, _int_word(row["stable"]))]
    calls = []
    if row["fee"] is not None:
        calls.append(
            _call(factory, "getPool(address,address,uint24)", *tokens, _int_word(row["fee"]))
        )
    if row["tick_spacing"] is not None:
        # Aerodrome Slipstream keys its pools by tick spacing instead of fee.
        calls.append(
            _call(
                factory, "getPool(address,address,int24)", *tokens, _int_word(row["tick_spacing"])
            )
        )
    return calls


def _confirm(client: RpcClient, rows: list[dict], block: int) -> None:
    calls, owners = [], []
    for index, row in enumerate(rows):
        for call in _confirmations(row):
            calls.append(call)
            owners.append(index)
    for index, result in zip(owners, _batched(client, _at(calls, block)), strict=True):
        if _as_address(result) == rows[index]["pool"]:
            rows[index]["factory_confirms"] = True


def decode_pool_metadata(settings: RunSettings) -> tuple[int, int]:
    """Read pools that have no metadata on disk yet; return (pools seen, pools read)."""
    chain = settings.chain.name
    seen = _pools_seen(settings.data_dir, chain)
    missing = {
        pool: family
        for pool, family in seen.items()
        if pool not in _pools_known(settings.data_dir, chain)
    }
    if missing:
        client = RpcClient(settings.rpc_url, requests_per_second=settings.requests_per_second)
        block = int(str(client.call("eth_blockNumber")), 16)
        order = sorted(missing)
        for start in range(0, len(order), POOLS_PER_FILE):
            group = {pool: missing[pool] for pool in order[start : start + POOLS_PER_FILE]}
            rows = read_pools(client, group, block, int(settings.chain.chain_id or 0))
            label = hashlib.sha256("".join(group).encode()).hexdigest()[:12]
            path = (
                dataset_dir(settings.data_dir, chain, DEX_POOLS)
                / f"{chain}__dex_pools__{block}_{label}.parquet"
            )
            write_parquet(path, DEX_POOLS, rows)
    return len(seen), len(missing)


def pool_tokens(data_dir: Path, chain: str) -> set[str]:
    """token0 and token1 of every pool read, so their decimals are read too."""
    files = parquet_files(data_dir, chain, DEX_POOLS)
    if not files:
        return set()
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT token0 FROM read_parquet(?) WHERE token0 IS NOT NULL "
            "UNION SELECT token1 FROM read_parquet(?) WHERE token1 IS NOT NULL",
            [[str(f) for f in files]] * 2,
        ).fetchall()
    return {row[0] for row in rows}


# A wallet's pricing pools ------------------------------------------------------

LOOKUPS_FILE = ".pricing_pools.json"


def _lookup_calls(chain: str, token: str, quote: str) -> list[tuple[tuple[str, list], str]]:
    """(call, event family) for each factory lookup of a pool of `token` and `quote`."""
    tokens = (_address_word(token), _address_word(quote))
    calls = []
    for venue in VENUES:
        if venue.chain != chain:
            continue
        if venue.venue == "uniswap_v2":
            calls.append((_call(venue.factory, "getPair(address,address)", *tokens), UNISWAP_V2))
        elif venue.venue == "uniswap_v3":
            for fee in UNISWAP_V3_FEE_TIERS:
                call = _call(
                    venue.factory, "getPool(address,address,uint24)", *tokens, _int_word(fee)
                )
                calls.append((call, UNISWAP_V3))
        elif venue.venue == "aerodrome_v2":
            for stable in (0, 1):
                call = _call(
                    venue.factory, "getPool(address,address,bool)", *tokens, _int_word(stable)
                )
                calls.append((call, SOLIDLY_V2))
    return calls


def pricing_pools(settings: RunSettings, tokens: Iterable[str]) -> list[str]:
    """Pools pairing each token with the wrapped native token or USDC; cached per token."""
    chain = settings.chain.name
    cache_path = settings.data_dir / chain / LOOKUPS_FILE
    cache: dict[str, list[str]] = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    quotes = assets_with_role(chain, WRAPPED_NATIVE) + assets_with_role(chain, USD_STABLECOIN)[:1]
    wanted = sorted({*tokens, *quotes} - set(cache))
    if wanted:
        client = RpcClient(settings.rpc_url, requests_per_second=settings.requests_per_second)
        calls, owners = [], []
        for token in wanted:
            for quote in quotes:
                if quote == token:
                    continue
                for call, _family in _lookup_calls(chain, token, quote):
                    calls.append(call)
                    owners.append(token)
        found: dict[str, set[str]] = {token: set() for token in wanted}
        for token, result in zip(owners, _batched(client, calls), strict=True):
            pool = _as_address(result)
            if pool and pool != ZERO_ADDRESS:
                found[token].add(pool)
        cache.update({token: sorted(pools) for token, pools in found.items()})
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(cache, indent=1, sort_keys=True))
    return sorted({pool for token in {*tokens, *quotes} for pool in cache.get(token, [])})


def wallet_tokens(data_dir: Path, chain: str) -> set[str]:
    files = parquet_files(data_dir, chain, ERC20_TRANSFERS)
    if not files:
        return set()
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT DISTINCT erc20 FROM read_parquet(?)", [[str(f) for f in files]]
        ).fetchall()
    return {row[0] for row in rows}


def extract_wallet_pricing_swaps(settings: RunSettings) -> tuple[int, int]:
    """Swaps of the pools that price the wallet's tokens; return (pools, swaps in range)."""
    chain = settings.chain.name
    pools = pricing_pools(settings, wallet_tokens(settings.data_dir, chain))
    if not pools:
        return 0, 0
    # The label names the pool set, so a later run with more pools fetches its own
    # files instead of skipping chunks that lack them; duplicates are removed on load.
    label = "pricing-" + hashlib.sha256(",".join(pools).encode()).hexdigest()[:12]
    run_cryo(
        "logs",
        rpc_url=settings.rpc_url,
        output_dir=dataset_dir(settings.data_dir, chain, DEX_SWAPS),
        args=[
            *_log_args(settings),
            "--address",
            *pools,
            # Filtered by address, so one request can cover a whole chunk.
            "--inner-request-size",
            str(settings.chunk_size),
            "--label",
            label,
        ],
        requests_per_second=settings.requests_per_second,
    )
    decode_swap_files(settings.data_dir, chain)
    return len(pools), _swaps_in_range(settings)
