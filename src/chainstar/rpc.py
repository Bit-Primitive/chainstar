"""The two JSON-RPC calls chainstar makes itself; everything else goes through cryo."""

import json
import urllib.error
import urllib.request

from chainstar.redact import redact_url


class RpcError(RuntimeError):
    pass


def _call(rpc_url: str, method: str, timeout: float) -> object:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": []}).encode()
    request = urllib.request.Request(
        rpc_url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "chainstar"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        # The exception text can echo the full URL, so report only the redacted form.
        raise RpcError(f"could not reach {redact_url(rpc_url)} ({type(error).__name__})") from None
    if "error" in payload:
        raise RpcError(f"{method} failed at {redact_url(rpc_url)}: {payload['error']}")
    return payload.get("result")


def get_chain_id(rpc_url: str, timeout: float = 10.0) -> int:
    return int(str(_call(rpc_url, "eth_chainId", timeout)), 16)


def get_latest_block(rpc_url: str, timeout: float = 10.0) -> int:
    return int(str(_call(rpc_url, "eth_blockNumber", timeout)), 16)
