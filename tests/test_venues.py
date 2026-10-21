"""The identifiers chainstar hard-codes for swaps and prices: recomputed, and kept in step."""

import csv

from chainstar.model import DBT_PROJECT_DIR
from chainstar.venues import (
    PRICE_ASSETS,
    SELECTORS,
    SOLANA_DEX_PROGRAMS,
    SWAP_SIGNATURES,
    SWAP_TOPICS,
    VENUES,
)
from keccak import keccak256


def test_keccak_matches_known_ethereum_hashes():
    # The empty string, and the ERC-20 Transfer event every block is full of.
    assert keccak256(b"").hex().startswith("c5d2460186f7233c")
    assert (
        keccak256(b"Transfer(address,address,uint256)").hex()
        == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )


def test_swap_topics_are_the_keccak_of_their_signatures():
    for topic, family in SWAP_TOPICS.items():
        assert topic == "0x" + keccak256(SWAP_SIGNATURES[family].encode()).hex(), family
    assert set(SWAP_TOPICS.values()) == set(SWAP_SIGNATURES)


def test_selectors_are_the_first_four_bytes_of_the_keccak():
    for signature, selector in SELECTORS.items():
        assert selector == "0x" + keccak256(signature.encode()).hex()[:8], signature


def test_addresses_are_lowercase_and_distinct():
    for venue in VENUES:
        assert venue.factory == venue.factory.lower() and len(venue.factory) == 42
        assert venue.event_family in SWAP_SIGNATURES
    assert len({(v.chain, v.factory) for v in VENUES}) == len(VENUES)
    for asset in PRICE_ASSETS:
        if asset.chain != "solana":
            assert asset.token_address == asset.token_address.lower()
    assert len({(a.chain, a.token_address) for a in PRICE_ASSETS}) == len(PRICE_ASSETS)
    assert len(set(SOLANA_DEX_PROGRAMS.values())) == len(SOLANA_DEX_PROGRAMS)


def _seed(name: str) -> list[dict]:
    with (DBT_PROJECT_DIR / "seeds" / f"{name}.csv").open() as handle:
        return list(csv.DictReader(handle))


def test_seeds_match_venues_py():
    assert _seed("dex_venues") == [
        {
            "chain": v.chain,
            "factory_address": v.factory,
            "venue": v.venue,
            "event_family": v.event_family,
        }
        for v in VENUES
    ]
    assert _seed("price_assets") == [
        {
            "chain": a.chain,
            "token_address": a.token_address,
            "role": a.role,
            "symbol": a.symbol,
            "decimals": str(a.decimals),
        }
        for a in PRICE_ASSETS
    ]
