"""The chains this release supports.

The dbt seed `seeds/chains.csv` carries the same rows so the warehouse can label
data by chain name; a unit test keeps the two in step.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Chain:
    name: str
    chain_id: int
    native_symbol: str


SUPPORTED_CHAINS: dict[str, Chain] = {
    chain.name: chain
    for chain in (
        Chain(name="ethereum", chain_id=1, native_symbol="ETH"),
        Chain(name="base", chain_id=8453, native_symbol="ETH"),
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
