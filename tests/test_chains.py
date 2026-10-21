import csv

import pytest

from chainstar.chains import SUPPORTED_CHAINS, UnknownChainError, get_chain
from chainstar.model import DBT_PROJECT_DIR


def test_seed_matches_python_registry():
    with (DBT_PROJECT_DIR / "seeds" / "chains.csv").open() as handle:
        seed = {row["chain"]: row for row in csv.DictReader(handle)}
    assert set(seed) == set(SUPPORTED_CHAINS)
    for name, chain in SUPPORTED_CHAINS.items():
        assert int(seed[name]["chain_id"]) == chain.chain_id
        assert seed[name]["native_symbol"] == chain.native_symbol


def test_get_chain_is_case_insensitive():
    assert get_chain(" Base ").chain_id == 8453


def test_unknown_chain_lists_supported():
    with pytest.raises(UnknownChainError, match="ethereum, base"):
        get_chain("solana")
