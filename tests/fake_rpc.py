"""A stand-in for RpcClient that answers from real responses saved in tests/rpc_fixtures/.

The saved responses are real replies from Bitcoin Core 29.3 and solana-core 4.3.0,
sliced to a handful of transactions so the fixtures stay small: Bitcoin block
972460, and Solana slots 459564295 and 459564296 (459564297 was skipped).
Slicing renumbers transactions, so transaction_index in the fixtures is the
position within the slice, not within the real block.

tests/fixtures/bitcoin/ and tests/fixtures/solana/ hold what chainstar's own
extractors write from these responses; test_json_rpc_extractors.py checks that
they still do.
"""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

RPC_FIXTURES = Path(__file__).parent / "rpc_fixtures"

BITCOIN_HEIGHT = 972460
BITCOIN_HASH = "00000000000000000001484404695074c1d276a03022eff3e6f45a2dc74ad562"
# getBlocks(459564295, 459564297) lists two blocks: slot 459564297 was skipped.
SOLANA_SLOTS = (459564295, 459564298)


def _load(name: str, exact_floats: bool = False) -> Any:
    text = (RPC_FIXTURES / name).read_text()
    return json.loads(text, parse_float=Decimal if exact_floats else float)["result"]


class FakeRpc:
    """Serves saved responses and records each call as (method, params)."""

    def __init__(self, url: str = "", **_options: Any) -> None:
        self.calls: list[tuple[str, list]] = []

    def call(self, method: str, params: list | None = None, *, exact_floats: bool = False) -> Any:
        params = params or []
        self.calls.append((method, params))
        if method == "getblockhash" and params == [BITCOIN_HEIGHT]:
            return BITCOIN_HASH
        if method == "getblock" and params == [BITCOIN_HASH, 3]:
            return _load(f"bitcoin_getblock_{BITCOIN_HEIGHT}.json", exact_floats)
        if method == "getBlocks" and params[:2] == [459564295, 459564297]:
            return _load("solana_getBlocks_459564295_459564297.json")
        if method == "getBlock" and params[0] in (459564295, 459564296):
            return _load(f"solana_getBlock_{params[0]}.json")
        raise AssertionError(f"no saved response for {method} {params}")
