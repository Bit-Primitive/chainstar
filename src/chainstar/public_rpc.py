"""Free shared public RPC endpoints, used when no RPC URL is configured.

They let anyone try chainstar without signing up for an RPC provider. They are
shared by everyone and rate limited, so chainstar asks them gently: a request-rate
cap and small chunks, so a refused request costs little to retry. Your own
--rpc-url or CHAINSTAR_RPC_URL always takes precedence, and these limits then do
not apply unless you set them.
"""

from dataclasses import dataclass

from chainstar.chains import Chain


@dataclass(frozen=True)
class PublicEndpoint:
    url: str
    requests_per_second: int
    # Blocks (Solana: slots) per Parquet file.
    chunk_size: int


# All four answered chainstar's identity calls and real runs in October 2026 (see
# README). The Ethereum endpoint refused a few percent of requests at 5 per second
# with 100-block chunks, and none with 10-block chunks. A Bitcoin block is ~6 MB
# compressed, so it is fetched a little more slowly.
PUBLIC_ENDPOINTS: dict[str, PublicEndpoint] = {
    "ethereum": PublicEndpoint("https://ethereum-rpc.publicnode.com", 5, 10),
    "base": PublicEndpoint("https://base-rpc.publicnode.com", 5, 10),
    "bitcoin": PublicEndpoint("https://bitcoin-rpc.publicnode.com", 2, 10),
    "solana": PublicEndpoint("https://solana-rpc.publicnode.com", 5, 10),
}


def public_endpoint(chain: Chain) -> PublicEndpoint:
    return PUBLIC_ENDPOINTS[chain.name]


def public_notice(chain: Chain, env_name: str) -> str:
    endpoint = public_endpoint(chain)
    return (
        f"no RPC URL set, so chainstar uses a free shared public endpoint "
        f"({endpoint.requests_per_second} requests/s, fine for trying small ranges); "
        f"for more, use your own with --rpc-url or {env_name}"
    )
