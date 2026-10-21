"""How far a sync may go (the safe head), and what the chain says a block's hash is.

A block's confirmations are the blocks built on top of it, plus one: the head
has one. A sync never goes past the newest block with the configured number of
confirmations, so a reorganisation shallower than that never reaches the
warehouse. Deeper ones are caught by checking the last synced block's hash
before each sync (see sync.py).

Defaults, per chain (`--confirmations` or chainstar.yml override them):

- Ethereum, 12: 11 blocks, about 2.2 minutes at 12-second slots, past the short
  reorganisations that late blocks cause; a margin, not a guarantee.
  `finalized` waits for the node's finalized block instead, which cannot be
  reverted without a third of all stake being slashed, at the cost of lagging:
  92 blocks (about 18 minutes) on 21 October 2026.
- Base, 60: about 2 minutes at 2-second blocks, past the node's `safe` head (the
  newest block derived from data already posted to Ethereum), which was 38
  blocks behind the latest on 21 October 2026. `finalized` (581 blocks, about
  19 minutes behind that day) also waits for Ethereum to finalize that data.
- Bitcoin, 6: the long-standing convention; five blocks (about 50 minutes) on top.
  Bitcoin has no finality, so there is no `finalized` option.
- Solana: always the latest finalized slot, which the cluster has rooted and
  cannot roll back. Its finalized slot is already the head chainstar reads, so a
  number of confirmations does not apply.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from chainstar.chains import BITCOIN, EVM, SOLANA, Chain
from chainstar.config import ConfigError
from chainstar.rpc import SOLANA_COMMITMENT, NodeIdentity, RpcClient, identify_node

FINALIZED = "finalized"
# A number of confirmations, or FINALIZED.
Confirmations = int | str

DEFAULT_CONFIRMATIONS: dict[str, Confirmations] = {
    "ethereum": 12,
    "base": 60,
    "bitcoin": 6,
    "solana": FINALIZED,
}


def parse_confirmations(value: Any, chain: Chain) -> Confirmations:
    """Check a --confirmations value (a whole number, or `finalized`) for `chain`."""
    text = str(value).strip().lower()
    if chain.family == SOLANA:
        if text != FINALIZED:
            raise ConfigError(
                "Solana syncs only finalized slots, which cannot be rolled back, so "
                "--confirmations does not apply to it; sync Solana in its own command"
            )
        return FINALIZED
    if text == FINALIZED:
        if chain.family == BITCOIN:
            raise ConfigError("Bitcoin has no finality; give a number of confirmations, such as 6")
        return FINALIZED
    if not text.isdigit() or int(text) < 1:
        raise ConfigError(
            f"confirmations must be a whole number of at least 1 or `finalized`, got {value!r}"
        )
    return int(text)


def confirmations_for(
    chain: Chain, flag: str | None, configured: Mapping[str, Any] | None
) -> Confirmations:
    """--confirmations, then chainstar.yml's `confirmations` for the chain, then the default."""
    value = flag if flag is not None else (configured or {}).get(chain.name)
    if value is None:
        return DEFAULT_CONFIRMATIONS[chain.name]
    return parse_confirmations(value, chain)


def describe_confirmations(confirmations: Confirmations) -> str:
    """'12 confirmations', '1 confirmation' or 'finalized' (also from the stored text)."""
    if str(confirmations) == FINALIZED:
        return FINALIZED
    return f"{confirmations} confirmation{'' if str(confirmations) == '1' else 's'}"


def safe_head(
    chain: Chain, confirmations: Confirmations, head: int, finalized: int | None = None
) -> int:
    """The newest block a sync may take.

    `head` is what identify_node reports: the latest block, or on Solana the
    latest finalized slot. `finalized` is the node's finalized block, needed only
    when confirmations is FINALIZED on an EVM chain.
    """
    if chain.family == SOLANA:
        return head
    if confirmations == FINALIZED:
        if finalized is None:
            raise ValueError("the finalized block is needed for confirmations=finalized")
        return finalized
    return head - (int(confirmations) - 1)


@dataclass(frozen=True)
class ChainBlock:
    """A block as the chain has it now."""

    number: int
    hash: str
    # Unix seconds, when the call that gave the hash also gave the time.
    time: int | None = None


class ChainNode:
    """The RPC calls a sync makes besides extraction."""

    def __init__(self, chain: Chain, rpc_url: str, requests_per_second: int | None) -> None:
        self.chain = chain
        self.rpc_url = rpc_url
        self.client = RpcClient(rpc_url, requests_per_second=requests_per_second)

    def identity(self) -> NodeIdentity:
        return identify_node(self.chain, self.rpc_url)

    def finalized_block(self) -> int:
        """EVM only: the node's finalized block (the `finalized` block tag)."""
        block = self.client.call("eth_getBlockByNumber", [FINALIZED, False])
        return int(block["number"], 16)

    def block(self, number: int) -> ChainBlock | None:
        """The block at `number` as the chain has it now; None if there is none."""
        if self.chain.family == EVM:
            block = self.client.call("eth_getBlockByNumber", [hex(number), False])
            if block is None:
                return None
            return ChainBlock(number, block["hash"], int(block["timestamp"], 16))
        if self.chain.family == BITCOIN:
            return ChainBlock(number, self.client.call("getblockhash", [number]))
        # transactionDetails "none" returns the hash and time without the block's
        # transactions: a few hundred bytes instead of several megabytes.
        block = self.client.call(
            "getBlock",
            [
                number,
                {
                    "transactionDetails": "none",
                    "rewards": False,
                    "commitment": SOLANA_COMMITMENT,
                    "maxSupportedTransactionVersion": 1,
                },
            ],
        )
        if block is None:
            return None
        return ChainBlock(number, block["blockhash"], block.get("blockTime"))
