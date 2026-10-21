"""The DEX venues chainstar reads swaps from, and the assets its USD prices are anchored to.

Every identifier here was checked against the protocol's own published source
or documentation and against real chain data (October 2026); none is inferred
from a name or symbol. tests/test_venues.py recomputes each event topic and
function selector from its signature with Keccak-256, and keeps the dbt seeds
`dex_venues.csv` and `price_assets.csv` in step with these tables.

EVM swap events (topic0 = keccak256 of the signature):

- `uniswap_v2`: Swap(address indexed sender, uint256 amount0In, uint256 amount1In,
  uint256 amount0Out, uint256 amount1Out, address indexed to), from
  Uniswap/v2-core IUniswapV2Pair.sol. SushiSwap v2 pairs are the same contract.
- `uniswap_v3`: Swap(address indexed sender, address indexed recipient, int256
  amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick),
  from Uniswap/v3-core IUniswapV3PoolEvents.sol. SushiSwap v3 and Aerodrome
  Slipstream (aerodrome-finance/slipstream ICLPoolEvents.sol) emit the same event.
- `solidly_v2`: Swap(address indexed sender, address indexed to, uint256
  amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out), from
  aerodrome-finance/contracts IPool.sol: the same fields as Uniswap v2 in a
  different order, so a different topic0.

Any contract can emit these events, so a swap counts as a venue's only when its
pool names one of the factories below and that factory, asked for the pool of
the same tokens and fee, answers with the pool's own address (see swaps.py).
"""

from dataclasses import dataclass

UNISWAP_V2 = "uniswap_v2"
UNISWAP_V3 = "uniswap_v3"
SOLIDLY_V2 = "solidly_v2"

# topic0 of each swap event, lowercase 0x hex.
SWAP_TOPICS: dict[str, str] = {
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": UNISWAP_V2,
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": UNISWAP_V3,
    "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b": SOLIDLY_V2,
}
SWAP_SIGNATURES: dict[str, str] = {
    UNISWAP_V2: "Swap(address,uint256,uint256,uint256,uint256,address)",
    UNISWAP_V3: "Swap(address,address,int256,int256,uint160,uint128,int24)",
    SOLIDLY_V2: "Swap(address,address,uint256,uint256,uint256,uint256)",
}

# Four-byte selectors of the view functions chainstar calls (eth_call).
SELECTORS: dict[str, str] = {
    "token0()": "0x0dfe1681",
    "token1()": "0xd21220a7",
    "factory()": "0xc45a0155",
    "fee()": "0xddca3f43",
    "tickSpacing()": "0xd0c93a7c",
    "stable()": "0x22be3de1",
    "getPair(address,address)": "0xe6a43905",
    "getPool(address,address,uint24)": "0x1698ee82",
    "getPool(address,address,int24)": "0x28af8d0b",
    "getPool(address,address,bool)": "0x79bc57d5",
}


@dataclass(frozen=True)
class Venue:
    chain: str
    # Lowercase 0x hex.
    factory: str
    venue: str
    event_family: str
    # Where the address was published.
    source: str


VENUES: tuple[Venue, ...] = (
    # Uniswap: developers.uniswap.org v2 and v3 deployment pages.
    Venue(
        "ethereum",
        "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
        "uniswap_v2",
        UNISWAP_V2,
        "developers.uniswap.org/docs/protocols/v2/deployments",
    ),
    Venue(
        "ethereum",
        "0x1f98431c8ad98523631ae4a59f267346ea31f984",
        "uniswap_v3",
        UNISWAP_V3,
        "developers.uniswap.org/docs/protocols/v3/deployments/v3-ethereum-deployments",
    ),
    Venue(
        "base",
        "0x8909dc15e40173ff4699343b6eb8132c65e18ec6",
        "uniswap_v2",
        UNISWAP_V2,
        "developers.uniswap.org/docs/protocols/v2/deployments",
    ),
    Venue(
        "base",
        "0x33128a8fc17869897dce68ed026d694621f6fdfd",
        "uniswap_v3",
        UNISWAP_V3,
        "developers.uniswap.org/docs/protocols/v3/deployments/v3-base-deployments",
    ),
    # SushiSwap: sushi-labs/sushi src/evm/config/features/sushiswap-v2.ts and -v3.ts.
    Venue(
        "ethereum",
        "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac",
        "sushiswap_v2",
        UNISWAP_V2,
        "github.com/sushi-labs/sushi src/evm/config/features/sushiswap-v2.ts",
    ),
    Venue(
        "ethereum",
        "0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f",
        "sushiswap_v3",
        UNISWAP_V3,
        "github.com/sushi-labs/sushi src/evm/config/features/sushiswap-v3.ts",
    ),
    Venue(
        "base",
        "0x71524b4f93c58fcbf659783284e38825f0622859",
        "sushiswap_v2",
        UNISWAP_V2,
        "github.com/sushi-labs/sushi src/evm/config/features/sushiswap-v2.ts",
    ),
    Venue(
        "base",
        "0xc35dadb65012ec5796536bd9864ed8773abc74c4",
        "sushiswap_v3",
        UNISWAP_V3,
        "github.com/sushi-labs/sushi src/evm/config/features/sushiswap-v3.ts",
    ),
    # Aerodrome: aerodrome-finance/contracts and aerodrome-finance/slipstream READMEs.
    Venue(
        "base",
        "0x420dd381b31aef6683db6b902084cb0ffece40da",
        "aerodrome_v2",
        SOLIDLY_V2,
        "github.com/aerodrome-finance/contracts README (PoolFactory)",
    ),
    Venue(
        "base",
        "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a",
        "aerodrome_slipstream",
        UNISWAP_V3,
        "github.com/aerodrome-finance/slipstream README (initial PoolFactory)",
    ),
    Venue(
        "base",
        "0xade65c38cd4849adba595a4323a8c7ddfe89716a",
        "aerodrome_slipstream",
        UNISWAP_V3,
        "github.com/aerodrome-finance/slipstream README (gauge caps PoolFactory)",
    ),
    Venue(
        "base",
        "0xf8f2eb4940cfe7d13603dddd87f123820fc061ef",
        "aerodrome_slipstream",
        UNISWAP_V3,
        "github.com/aerodrome-finance/slipstream README (gauges v3 PoolFactory)",
    ),
)


def venue_for(chain: str, factory: str | None) -> Venue | None:
    for venue in VENUES:
        if venue.chain == chain and venue.factory == factory:
            return venue
    return None


# Price anchors, by contract address or mint (never by symbol), from the issuers'
# or the protocols' own lists: Circle's USDC contract addresses page, Tether's
# supported-protocols page, the Uniswap Labs default token list
# (tokens.uniswap.org, for DAI, USDbC, Base USDT, WBTC, cbBTC and WETH) and
# Uniswap's v3 deployment pages (WETH). cbBTC's address is from the Uniswap list,
# not from a Coinbase page (coinbase.com refuses scripted requests). Decimals were
# read on chain (decimals(), or the mint account on Solana), October 2026.
USD_STABLECOIN = "usd_stablecoin"
WRAPPED_NATIVE = "wrapped_native"
WRAPPED_BTC = "wrapped_btc"


@dataclass(frozen=True)
class PriceAsset:
    chain: str
    token_address: str
    role: str
    symbol: str
    decimals: int


PRICE_ASSETS: tuple[PriceAsset, ...] = (
    PriceAsset("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", USD_STABLECOIN, "USDC", 6),
    PriceAsset("ethereum", "0xdac17f958d2ee523a2206206994597c13d831ec7", USD_STABLECOIN, "USDT", 6),
    PriceAsset("ethereum", "0x6b175474e89094c44da98b954eedeac495271d0f", USD_STABLECOIN, "DAI", 18),
    PriceAsset(
        "ethereum", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", WRAPPED_NATIVE, "WETH", 18
    ),
    PriceAsset("ethereum", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", WRAPPED_BTC, "WBTC", 8),
    PriceAsset("ethereum", "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf", WRAPPED_BTC, "cbBTC", 8),
    PriceAsset("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", USD_STABLECOIN, "USDC", 6),
    PriceAsset("base", "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", USD_STABLECOIN, "USDbC", 6),
    PriceAsset("base", "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2", USD_STABLECOIN, "USDT", 6),
    PriceAsset("base", "0x50c5725949a6f0c72e6c4a641f24049a917db0cb", USD_STABLECOIN, "DAI", 18),
    PriceAsset("base", "0x4200000000000000000000000000000000000006", WRAPPED_NATIVE, "WETH", 18),
    PriceAsset("base", "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf", WRAPPED_BTC, "cbBTC", 8),
    PriceAsset("solana", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", USD_STABLECOIN, "USDC", 6),
    PriceAsset("solana", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", USD_STABLECOIN, "USDT", 6),
    PriceAsset("solana", "So11111111111111111111111111111111111111112", WRAPPED_NATIVE, "wSOL", 9),
)


def assets_with_role(chain: str, role: str) -> list[str]:
    return [a.token_address for a in PRICE_ASSETS if a.chain == chain and a.role == role]


# Solana programs whose transactions chainstar derives swaps from, with the source
# each id was read from (the program's own declare_id! or published IDL).
SOLANA_DEX_PROGRAMS: dict[str, str] = {
    # raydium-io/raydium-amm program/src/lib.rs (not devnet)
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium_amm_v4",
    # raydium-io/raydium-clmm programs/amm/src/lib.rs (not devnet)
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "raydium_clmm",
    # raydium-io/raydium-cp-swap programs/cp-swap/src/lib.rs (not devnet)
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "raydium_cpmm",
    # orca-so/whirlpools programs/whirlpool/src/lib.rs
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpool",
    # MeteoraAg/dlmm-sdk idls/dlmm.json
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "meteora_dlmm",
    # jup-ag/jupiter-cpi src/lib.rs (Jupiter aggregator v6)
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter_v6",
    # pump-fun/pump-public-docs idl/pump.json (bonding curve) and idl/pump_amm.json
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump_fun",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "pump_swap",
}
WRAPPED_SOL = "So11111111111111111111111111111111111111112"

# Uniswap v3 fee tiers (hundredths of a basis point) that a wallet's pricing-pool
# lookup asks the factory for: the tiers every Uniswap and SushiSwap v3 factory
# above has enabled (feeAmountTickSpacing non-zero for each, checked October 2026).
UNISWAP_V3_FEE_TIERS = (100, 500, 3000, 10000)
