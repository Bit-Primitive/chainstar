import csv

import pytest

from chainstar.chains import SUPPORTED_CHAINS, UnknownChainError, get_chain
from chainstar.model import DBT_PROJECT_DIR


def test_seed_matches_python_registry():
    with (DBT_PROJECT_DIR / "seeds" / "chains.csv").open() as handle:
        seed = {row["chain"]: row for row in csv.DictReader(handle)}
    assert set(seed) == set(SUPPORTED_CHAINS)
    for name, chain in SUPPORTED_CHAINS.items():
        seed_chain_id = int(seed[name]["chain_id"]) if seed[name]["chain_id"] else None
        assert seed_chain_id == chain.chain_id
        assert seed[name]["native_symbol"] == chain.native_symbol
        assert int(seed[name]["native_decimals"]) == chain.native_decimals


def test_every_chain_has_exactly_one_identity():
    # EVM chains are identified by chain id, the others by a network name or hash.
    for chain in SUPPORTED_CHAINS.values():
        assert (chain.chain_id is None) != (chain.network is None), chain.name


def test_get_chain_is_case_insensitive():
    assert get_chain(" Base ").chain_id == 8453


def test_unknown_chain_lists_supported():
    with pytest.raises(UnknownChainError, match="ethereum, base, bitcoin, solana"):
        get_chain("dogecoin")


def test_every_chain_has_a_public_endpoint():
    from chainstar.public_rpc import PUBLIC_ENDPOINTS

    assert set(PUBLIC_ENDPOINTS) == set(SUPPORTED_CHAINS)
    assert all(e.url.startswith("https://") for e in PUBLIC_ENDPOINTS.values())
