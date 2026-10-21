import gzip
import io
import json
import urllib.error
from decimal import Decimal

import pytest

from chainstar import rpc
from chainstar.chains import get_chain
from chainstar.rpc import RpcClient, RpcError, identify_node

SECRET_URL = "https://btc.example.com/v1/supersecretkey"


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, encoding: str | None = None) -> None:
        super().__init__(body)
        self.headers = {"Content-Encoding": encoding} if encoding else {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def server(monkeypatch):
    """Replaces urlopen. Queue replies as bytes or exceptions; requests are recorded."""
    state = {"replies": [], "requests": []}

    def fake_urlopen(request, timeout):
        state["requests"].append(json.loads(request.data))
        reply = state["replies"].pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(rpc.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(rpc.time, "sleep", lambda seconds: None)
    return state


def _reply(payload: dict, encoding: str | None = None) -> FakeResponse:
    body = json.dumps(payload).encode()
    return FakeResponse(gzip.compress(body) if encoding == "gzip" else body, encoding)


def test_error_null_is_success(server):
    # Bitcoin Core's reply shape: an "error" key that is null on success.
    server["replies"].append(_reply({"result": 972469, "error": None, "id": 1}))
    assert RpcClient(SECRET_URL).call("getblockcount") == 972469


def test_gzip_responses_are_decoded(server):
    server["replies"].append(_reply({"jsonrpc": "2.0", "result": [1, 2], "id": 1}, "gzip"))
    assert RpcClient(SECRET_URL).call("getBlocks", [1, 2]) == [1, 2]
    assert server["requests"][0]["params"] == [1, 2]


def test_exact_floats_parse_as_decimal(server):
    server["replies"].append(FakeResponse(b'{"result": {"value": 0.05563443}, "error": null}'))
    result = RpcClient(SECRET_URL).call("getblock", exact_floats=True)
    assert result["value"] == Decimal("0.05563443")


def test_transient_failures_are_retried(server):
    server["replies"] += [
        urllib.error.URLError("connection reset"),
        _reply({"error": {"code": -32004, "message": "Block not available"}}),
        _reply({"result": "ok"}),
    ]
    assert RpcClient(SECRET_URL, attempts=3).call("getBlock") == "ok"
    assert len(server["requests"]) == 3


def test_gives_up_with_redacted_url(server):
    server["replies"] += [urllib.error.URLError(f"failed: {SECRET_URL}")] * 2
    with pytest.raises(RpcError) as raised:
        RpcClient(SECRET_URL, attempts=2).call("getblockhash", [1])
    assert "supersecretkey" not in str(raised.value)
    assert "btc.example.com/<redacted>" in str(raised.value)


def test_rpc_error_is_reported(server):
    server["replies"].append(
        _reply({"error": {"code": -8, "message": "Block height out of range"}})
    )
    with pytest.raises(RpcError, match="Block height out of range"):
        RpcClient(SECRET_URL, attempts=1).call("getblockhash", [99999999])


def test_requests_per_second_spaces_calls(server, monkeypatch):
    clock = {"now": 100.0}
    waits = []
    monkeypatch.setattr(rpc.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(rpc.time, "sleep", waits.append)
    server["replies"] += [_reply({"result": 1}), _reply({"result": 2})]
    client = RpcClient(SECRET_URL, requests_per_second=4)
    client.call("a")
    client.call("b")
    assert waits == [pytest.approx(0.25)]


def test_identify_bitcoin(server):
    server["replies"].append(
        _reply({"result": {"chain": "main", "blocks": 972467, "pruned": False}, "error": None})
    )
    node = identify_node(get_chain("bitcoin"), SECRET_URL)
    assert node.matches
    assert node.head == 972467
    assert node.detail == "chain main, height 972,467"


def test_identify_bitcoin_testnet_does_not_match(server):
    server["replies"].append(
        _reply({"result": {"chain": "test", "blocks": 10, "pruned": True}, "error": None})
    )
    node = identify_node(get_chain("bitcoin"), SECRET_URL)
    assert not node.matches
    assert "pruned" in node.detail
    assert node.expected == "bitcoin (main)"


def test_identify_solana(server):
    server["replies"] += [
        _reply({"jsonrpc": "2.0", "result": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"}),
        _reply({"jsonrpc": "2.0", "result": 459613673}),
    ]
    node = identify_node(get_chain("solana"), SECRET_URL)
    assert node.matches
    assert node.head == 459613673
    assert server["requests"][1]["params"] == [{"commitment": "finalized"}]


def test_identify_solana_devnet_does_not_match(server):
    server["replies"] += [
        _reply({"jsonrpc": "2.0", "result": "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"}),
        _reply({"jsonrpc": "2.0", "result": 1}),
    ]
    assert not identify_node(get_chain("solana"), SECRET_URL).matches


def test_identify_evm(server):
    server["replies"] += [_reply({"result": "0x2105"}), _reply({"result": "0x10"})]
    node = identify_node(get_chain("base"), SECRET_URL)
    assert node.matches
    assert node.detail == "chain id 8453, latest block 16"
