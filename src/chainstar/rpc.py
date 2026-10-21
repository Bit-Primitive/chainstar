"""JSON-RPC over HTTP: the calls chainstar makes itself, outside cryo.

For EVM chains that is only the chain-identity check. Bitcoin and Solana are
extracted entirely through this client, so it also carries the retry and
rate-limit behaviour cryo provides for EVM chains.
"""

import gzip
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from chainstar.chains import BITCOIN, SOLANA, Chain
from chainstar.redact import redact_url

ATTEMPTS = 3
RETRY_DELAY_SECONDS = 3.0
BATCH_RETRY_DELAY_SECONDS = 15.0
# A Bitcoin block with input values is ~15 MB of JSON, a Solana block ~8 MB.
TIMEOUT_SECONDS = 60.0
# Read-only by design: only finalized Solana data, which cannot be rolled back.
SOLANA_COMMITMENT = "finalized"
RATE_LIMITED = 429


class RpcError(RuntimeError):
    pass


class RpcClient:
    """Sequential JSON-RPC calls with an optional request-rate cap and retries.

    Each failed call (network error, HTTP error such as 429, or a JSON-RPC error)
    is retried with a growing delay, because RPC providers fail requests
    transiently in many ways: rate limits, load-balanced nodes that lack a block.
    """

    def __init__(
        self,
        url: str,
        *,
        requests_per_second: int | None = None,
        attempts: int = ATTEMPTS,
        timeout: float = TIMEOUT_SECONDS,
    ) -> None:
        self.url = url
        self.attempts = attempts
        self.timeout = timeout
        self._min_interval = 1.0 / requests_per_second if requests_per_second else 0.0
        self._last_request = 0.0

    def call(self, method: str, params: list | None = None, *, exact_floats: bool = False) -> Any:
        """Call `method` and return its result.

        exact_floats parses JSON numbers with a fraction as Decimal: Bitcoin Core
        sends amounts as BTC with 8 decimals, and a float would lose satoshis.
        """
        attempt = 1
        while True:
            try:
                return self._call_once(method, params or [], exact_floats)
            except RpcError:
                if attempt >= self.attempts:
                    raise
                time.sleep(RETRY_DELAY_SECONDS * attempt)
                attempt += 1

    def _call_once(self, method: str, params: list, exact_floats: bool) -> Any:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        payload = self._post(method, body, exact_floats=exact_floats)
        # Bitcoin Core sends "error": null on success, so test the value, not the key.
        if not isinstance(payload, dict) or payload.get("error") is not None:
            error = payload.get("error") if isinstance(payload, dict) else payload
            raise RpcError(f"{method} failed at {redact_url(self.url)}: {error}")
        return payload.get("result")

    def batch(self, calls: list[tuple[str, list]]) -> list[Any]:
        """Send several calls in one JSON-RPC batch; return each result, or None if it failed.

        A call that fails on its own (an eth_call that reverts, say) gives None; a
        failure of the whole request is retried like call(). Each call in the
        batch counts against the request-rate cap, since providers count them so.
        """
        if not calls:
            return []
        attempt = 1
        while True:
            try:
                return self._batch_once(calls)
            except RpcError:
                if attempt >= self.attempts:
                    raise
                # Longer than for single calls: a batch is many requests at once, and
                # public endpoints that refuse one tend to refuse for a while.
                time.sleep(BATCH_RETRY_DELAY_SECONDS * attempt)
                attempt += 1

    def _batch_once(self, calls: list[tuple[str, list]]) -> list[Any]:
        for _ in calls[1:]:
            self._wait_for_rate_limit()
        body = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
            for index, (method, params) in enumerate(calls)
        ]
        payload = self._post("batch", body, exact_floats=False)
        if not isinstance(payload, list):
            # A single error object: the endpoint refused the batch as a whole.
            error = payload.get("error") if isinstance(payload, dict) else payload
            raise RpcError(f"batch of {len(calls)} calls failed at {redact_url(self.url)}: {error}")
        results: list[Any] = [None] * len(calls)
        for item in payload:
            index = item.get("id") if isinstance(item, dict) else None
            if isinstance(index, int) and 0 <= index < len(calls) and item.get("error") is None:
                results[index] = item.get("result")
        return results

    def _post(self, method: str, body: Any, *, exact_floats: bool) -> Any:
        self._wait_for_rate_limit()
        request = urllib.request.Request(
            self.url,
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "chainstar",
                # Measured: a Bitcoin block shrinks ~2.5x, a Solana block ~7x.
                "Accept-Encoding": "gzip",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
            return json.loads(raw, parse_float=Decimal if exact_floats else float)
        except urllib.error.HTTPError as error:
            # Before URLError, its parent: the status says whether we were rate limited
            # (429) or refused, which the user can act on.
            raise RpcError(
                f"{method}: {redact_url(self.url)} answered HTTP {error.code}"
                + (" (too many requests: rate limited)" if error.code == RATE_LIMITED else "")
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
            # The exception text can echo the full URL, so report only the redacted form.
            raise RpcError(
                f"{method}: could not reach {redact_url(self.url)} ({type(error).__name__})"
            ) from None

    def _wait_for_rate_limit(self) -> None:
        wait = self._last_request + self._min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()


@dataclass(frozen=True)
class NodeIdentity:
    """What a node says about itself, and whether that is the chain we expect."""

    matches: bool
    # e.g. "chain id 1, latest block 20,000,000"
    detail: str
    # e.g. "ethereum (1)"; shown when the node serves something else.
    expected: str
    # Highest block (Bitcoin, EVM) or finalized slot (Solana) the node can serve.
    head: int


def identify_node(chain: Chain, rpc_url: str, timeout: float = 10.0) -> NodeIdentity:
    client = RpcClient(rpc_url, attempts=1, timeout=timeout)
    if chain.family == BITCOIN:
        info = client.call("getblockchaininfo")
        network, height = info["chain"], int(info["blocks"])
        detail = f"chain {network}, height {height:,}"
        if info.get("pruned"):
            detail += " (pruned: old blocks lack the input values chainstar needs)"
        return NodeIdentity(
            network == chain.network, detail, f"{chain.name} ({chain.network})", height
        )
    if chain.family == SOLANA:
        genesis = client.call("getGenesisHash")
        slot = int(client.call("getSlot", [{"commitment": SOLANA_COMMITMENT}]))
        return NodeIdentity(
            genesis == chain.network,
            f"genesis {genesis}, finalized slot {slot:,}",
            f"{chain.name} (genesis {chain.network})",
            slot,
        )
    chain_id = int(str(client.call("eth_chainId")), 16)
    latest = int(str(client.call("eth_blockNumber")), 16)
    return NodeIdentity(
        chain_id == chain.chain_id,
        f"chain id {chain_id}, latest block {latest:,}",
        f"{chain.name} ({chain.chain_id})",
        latest,
    )
