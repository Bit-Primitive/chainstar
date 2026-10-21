"""Safe-head maths per chain, the confirmation settings, and the hash lookups."""

import pytest

from chainstar.chains import get_chain
from chainstar.config import ConfigError
from chainstar.heads import (
    DEFAULT_CONFIRMATIONS,
    FINALIZED,
    ChainNode,
    confirmations_for,
    describe_confirmations,
    parse_confirmations,
    safe_head,
)

ETHEREUM, BASE, BITCOIN, SOLANA = (get_chain(n) for n in ("ethereum", "base", "bitcoin", "solana"))


def test_defaults():
    assert DEFAULT_CONFIRMATIONS == {
        "ethereum": 12,
        "base": 60,
        "bitcoin": 6,
        "solana": FINALIZED,
    }


@pytest.mark.parametrize(
    ("chain", "confirmations", "head", "expected"),
    [
        # The head has one confirmation, so N confirmations stop N - 1 behind it.
        (ETHEREUM, 12, 20_000_000, 19_999_989),
        (BASE, 60, 37_000_000, 36_999_941),
        (BITCOIN, 6, 972_506, 972_501),
        (BITCOIN, 1, 972_506, 972_506),
    ],
)
def test_safe_head_counts_confirmations(chain, confirmations, head, expected):
    assert safe_head(chain, confirmations, head) == expected
    # A block exactly at the safe head has exactly the required confirmations.
    assert head - expected + 1 == confirmations


def test_safe_head_finalized():
    assert safe_head(ETHEREUM, FINALIZED, 26_242_268, finalized=26_242_176) == 26_242_176
    with pytest.raises(ValueError):
        safe_head(BASE, FINALIZED, 100)


def test_solana_head_is_already_finalized():
    # identify_node reports the latest finalized slot as Solana's head.
    assert safe_head(SOLANA, FINALIZED, 459_702_168) == 459_702_168


@pytest.mark.parametrize(
    ("value", "chain", "expected"),
    [("12", ETHEREUM, 12), (3, BITCOIN, 3), ("Finalized", BASE, FINALIZED)],
)
def test_parse_confirmations(value, chain, expected):
    assert parse_confirmations(value, chain) == expected


@pytest.mark.parametrize(
    ("value", "chain", "message"),
    [
        ("finalized", BITCOIN, "Bitcoin has no finality"),
        ("10", SOLANA, "Solana syncs only finalized slots"),
        ("0", ETHEREUM, "at least 1"),
        ("soon", BASE, "at least 1"),
    ],
)
def test_parse_confirmations_refuses(value, chain, message):
    with pytest.raises(ConfigError, match=message):
        parse_confirmations(value, chain)


def test_confirmations_for_precedence():
    configured = {"base": 120}
    assert confirmations_for(BASE, None, configured) == 120
    assert confirmations_for(BASE, "30", configured) == 30
    assert confirmations_for(ETHEREUM, None, configured) == 12
    assert confirmations_for(SOLANA, None, None) == FINALIZED


def test_describe_confirmations():
    assert describe_confirmations(1) == "1 confirmation"
    assert describe_confirmations("12") == "12 confirmations"
    assert describe_confirmations(FINALIZED) == "finalized"


class FakeClient:
    def __init__(self, replies: dict):
        self.replies = replies
        self.calls = []

    def call(self, method, params=None, **_options):
        self.calls.append((method, params))
        return self.replies[method]


def _node(chain, replies) -> ChainNode:
    node = ChainNode(chain, "https://rpc.example", None)
    node.client = FakeClient(replies)
    return node


def test_evm_block_and_finalized():
    node = _node(
        BASE,
        {"eth_getBlockByNumber": {"number": "0x10", "hash": "0xabc", "timestamp": "0x64"}},
    )
    block = node.block(16)
    assert (block.number, block.hash, block.time) == (16, "0xabc", 100)
    assert node.client.calls[0] == ("eth_getBlockByNumber", ["0x10", False])
    assert node.finalized_block() == 16
    assert node.client.calls[1] == ("eth_getBlockByNumber", ["finalized", False])


def test_bitcoin_block_hash():
    node = _node(BITCOIN, {"getblockhash": "0000abc"})
    assert node.block(972460).hash == "0000abc"
    assert node.client.calls == [("getblockhash", [972460])]


def test_solana_block_hash_without_transactions():
    node = _node(SOLANA, {"getBlock": {"blockhash": "GLrL", "blockTime": 1792601274}})
    block = node.block(459702168)
    assert (block.hash, block.time) == ("GLrL", 1792601274)
    options = node.client.calls[0][1][1]
    assert options["transactionDetails"] == "none"
    assert options["commitment"] == "finalized"


def test_missing_block_is_none():
    assert _node(ETHEREUM, {"eth_getBlockByNumber": None}).block(1) is None
