"""The chains this release supports.

The dbt seed `seeds/chains.csv` carries the same rows so the warehouse can label
data by chain name; a unit test keeps the two in step.
"""

from dataclasses import dataclass

EVM = "evm"
BITCOIN = "bitcoin"
SOLANA = "solana"


@dataclass(frozen=True)
class Chain:
    name: str
    # Which extractor and raw tables the chain uses: EVM chains go through cryo,
    # Bitcoin and Solana through chainstar's own JSON-RPC extractors.
    family: str
    native_symbol: str
    native_decimals: int
    # EVM chain id, as eth_chainId reports it. Bitcoin and Solana have none.
    chain_id: int | None = None
    # How a non-EVM node proves which network it serves: the `chain` field of
    # Bitcoin's getblockchaininfo, or Solana's genesis hash.
    network: str | None = None

    @property
    def block_noun(self) -> str:
        # Solana ranges count slots, some of which never produce a block.
        return "slots" if self.family == SOLANA else "blocks"


SUPPORTED_CHAINS: dict[str, Chain] = {
    chain.name: chain
    for chain in (
        Chain(name="ethereum", family=EVM, native_symbol="ETH", native_decimals=18, chain_id=1),
        Chain(name="base", family=EVM, native_symbol="ETH", native_decimals=18, chain_id=8453),
        Chain(
            name="bitcoin", family=BITCOIN, native_symbol="BTC", native_decimals=8, network="main"
        ),
        Chain(
            name="solana",
            family=SOLANA,
            native_symbol="SOL",
            native_decimals=9,
            # Mainnet-beta's genesis hash, as both api.mainnet-beta.solana.com and
            # solana-rpc.publicnode.com report it (devnet's differs).
            network="5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
        ),
    )
}


class UnknownChainError(ValueError):
    pass


def get_chain(name: str) -> Chain:
    try:
        return SUPPORTED_CHAINS[name.strip().lower()]
    except KeyError:
        supported = ", ".join(SUPPORTED_CHAINS)
        raise UnknownChainError(f"unknown chain {name!r}; supported: {supported}") from None
