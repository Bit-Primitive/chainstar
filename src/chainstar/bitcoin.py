"""Bitcoin extraction over Bitcoin Core-compatible JSON-RPC.

Each block takes two calls: getblockhash, then getblock at verbosity 3. Verbosity 3
(Bitcoin Core 23 and later) adds `prevout` to every input, with the value and
script of the output it spends. Without it, input values and addresses, and
therefore fees, would need one extra lookup per input. Core includes prevout only
when it has the block's undo data, which pruned nodes drop for older blocks.
"""

from decimal import Decimal
from typing import Any

from chainstar.chunks import ExtractError, Rows, extract_chunks
from chainstar.config import RunSettings
from chainstar.datasets import (
    BITCOIN_BLOCKS,
    BITCOIN_DATASETS,
    BITCOIN_INPUTS,
    BITCOIN_OUTPUTS,
    BITCOIN_TRANSACTIONS,
)
from chainstar.rpc import RpcClient

SATOSHIS_PER_BTC = 100_000_000
GETBLOCK_VERBOSITY_WITH_PREVOUT = 3


def extract(settings: RunSettings) -> dict[str, int]:
    client = RpcClient(settings.rpc_url, requests_per_second=settings.requests_per_second)
    chain = settings.chain.name

    def fetch_chunk(first: int, last: int) -> dict[str, Rows]:
        rows: dict[str, Rows] = {dataset.name: [] for dataset in BITCOIN_DATASETS}
        for height in range(first, last + 1):
            block_hash = client.call("getblockhash", [height])
            block = client.call(
                "getblock", [block_hash, GETBLOCK_VERBOSITY_WITH_PREVOUT], exact_floats=True
            )
            for name, block_rows in decode_block(block, chain).items():
                rows[name].extend(block_rows)
        return rows

    return extract_chunks(
        data_dir=settings.data_dir,
        chain=chain,
        datasets=BITCOIN_DATASETS,
        blocks=settings.blocks,
        chunk_size=settings.chunk_size,
        fetch_chunk=fetch_chunk,
    )


def decode_block(block: dict[str, Any], chain: str) -> dict[str, Rows]:
    """Turn one verbosity-3 getblock result into rows for the four Bitcoin datasets."""
    height = int(block["height"])
    transactions, inputs, outputs = [], [], []
    for tx_index, tx in enumerate(block["tx"]):
        txid = tx["txid"]
        common = {
            "chain": chain,
            "block_number": height,
            "transaction_index": tx_index,
            "transaction_hash": txid,
        }
        # Only a coinbase input has a "coinbase" field; it spends no earlier output.
        is_coinbase = "coinbase" in tx["vin"][0]
        if not is_coinbase:
            for input_index, vin in enumerate(tx["vin"]):
                if "prevout" not in vin:
                    raise ExtractError(
                        f"block {height}: the node returned no prevout for input "
                        f"{input_index} of {txid}. chainstar needs getblock verbosity 3 "
                        "with undo data: Bitcoin Core 23 or later, not pruned below this block."
                    )
                script = vin["prevout"]["scriptPubKey"]
                inputs.append(
                    {
                        **common,
                        "input_index": input_index,
                        "spent_transaction_hash": vin["txid"],
                        "spent_output_index": vin["vout"],
                        "address": script.get("address"),
                        "script_type": script.get("type"),
                        "value_sat": to_satoshis(vin["prevout"]["value"]),
                    }
                )
        for vout in tx["vout"]:
            script = vout["scriptPubKey"]
            outputs.append(
                {
                    **common,
                    "output_index": vout["n"],
                    "address": script.get("address"),
                    "script_type": script.get("type"),
                    "value_sat": to_satoshis(vout["value"]),
                }
            )
        transactions.append(
            {
                **common,
                "is_coinbase": is_coinbase,
                "input_count": len(tx["vin"]),
                "output_count": len(tx["vout"]),
                "fee_sat": None if is_coinbase or "fee" not in tx else to_satoshis(tx["fee"]),
                "size": tx["size"],
                "vsize": tx["vsize"],
                "weight": tx["weight"],
            }
        )
    block_row = {
        "chain": chain,
        "block_number": height,
        "block_hash": block["hash"],
        "timestamp": block["time"],
        "median_time": block["mediantime"],
        "transaction_count": len(block["tx"]),
        "size": block["size"],
        "weight": block["weight"],
    }
    return {
        BITCOIN_BLOCKS.name: [block_row],
        BITCOIN_TRANSACTIONS.name: transactions,
        BITCOIN_INPUTS.name: inputs,
        BITCOIN_OUTPUTS.name: outputs,
    }


def to_satoshis(btc: Decimal | int) -> int:
    """Exact BTC-to-satoshi conversion; refuses anything finer than a satoshi."""
    satoshis = Decimal(btc) * SATOSHIS_PER_BTC
    if satoshis != satoshis.to_integral_value():
        raise ExtractError(f"amount {btc} BTC is not a whole number of satoshis")
    return int(satoshis)
