"""The Bitcoin and Solana extractors, run against real RPC responses (see fake_rpc.py)."""

import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from chainstar import bitcoin, solana
from chainstar.chains import get_chain
from chainstar.chunks import ExtractError
from chainstar.config import BlockRange, RunSettings
from chainstar.datasets import BITCOIN_DATASETS, SOLANA_DATASETS, parquet_files
from chainstar.targets import DuckDBTarget
from conftest import FIXTURES
from fake_rpc import BITCOIN_HEIGHT, RPC_FIXTURES, SOLANA_SLOTS, FakeRpc


@pytest.fixture
def fake_rpc(monkeypatch) -> FakeRpc:
    rpc = FakeRpc()
    monkeypatch.setattr(bitcoin, "RpcClient", lambda *args, **kwargs: rpc)
    monkeypatch.setattr(solana, "RpcClient", lambda *args, **kwargs: rpc)
    return rpc


def _settings(chain: str, blocks: BlockRange, data_dir: Path) -> RunSettings:
    return RunSettings(
        chain=get_chain(chain),
        blocks=blocks,
        rpc_url="https://rpc.example",
        target=DuckDBTarget(data_dir / "w.duckdb"),
        data_dir=data_dir,
        requests_per_second=None,
        chunk_size=100,
    )


def _rows(files: list[Path]) -> list[tuple]:
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY ALL", [[str(f) for f in files]]
        ).fetchall()


BITCOIN_RANGE = BlockRange(BITCOIN_HEIGHT, BITCOIN_HEIGHT + 1)
SOLANA_RANGE = BlockRange(*SOLANA_SLOTS)


@pytest.mark.parametrize(
    ("module", "chain", "blocks", "datasets"),
    [
        (bitcoin, "bitcoin", BITCOIN_RANGE, BITCOIN_DATASETS),
        (solana, "solana", SOLANA_RANGE, SOLANA_DATASETS),
    ],
)
def test_extract_reproduces_committed_fixtures(
    fake_rpc, tmp_path: Path, module, chain, blocks, datasets
):
    counts = module.extract(_settings(chain, blocks, tmp_path))
    for dataset in datasets:
        written = parquet_files(tmp_path, chain, dataset)
        committed = parquet_files(FIXTURES, chain, dataset)
        assert [f.name for f in written] == [f.name for f in committed]
        assert _rows(written) == _rows(committed), dataset.name
        assert counts[dataset.name] == len(_rows(committed))


def test_rerun_reuses_chunks_on_disk(fake_rpc, tmp_path: Path):
    settings = _settings("solana", SOLANA_RANGE, tmp_path)
    first = solana.extract(settings)
    calls = len(fake_rpc.calls)
    assert solana.extract(settings) == first
    assert len(fake_rpc.calls) == calls


def test_bitcoin_asks_for_input_values(fake_rpc, tmp_path: Path):
    bitcoin.extract(_settings("bitcoin", BITCOIN_RANGE, tmp_path))
    assert [method for method, _ in fake_rpc.calls] == ["getblockhash", "getblock"]
    assert fake_rpc.calls[1][1][1] == 3  # verbosity 3 adds prevout to every input


def _bitcoin_block() -> dict:
    text = (RPC_FIXTURES / f"bitcoin_getblock_{BITCOIN_HEIGHT}.json").read_text()
    return json.loads(text, parse_float=Decimal)["result"]


def test_bitcoin_fee_is_inputs_minus_outputs():
    rows = bitcoin.decode_block(_bitcoin_block(), "bitcoin")
    for tx in rows["bitcoin_transactions"]:
        inputs = [
            i["value_sat"]
            for i in rows["bitcoin_inputs"]
            if i["transaction_hash"] == tx["transaction_hash"]
        ]
        outputs = [
            o["value_sat"]
            for o in rows["bitcoin_outputs"]
            if o["transaction_hash"] == tx["transaction_hash"]
        ]
        if tx["is_coinbase"]:
            assert inputs == []
            assert tx["fee_sat"] is None
        else:
            assert tx["fee_sat"] == sum(inputs) - sum(outputs) > 0


def test_bitcoin_outputs_without_address_are_kept():
    outputs = bitcoin.decode_block(_bitcoin_block(), "bitcoin")["bitcoin_outputs"]
    op_return = [o for o in outputs if o["script_type"] == "nulldata"]
    assert op_return
    assert all(o["address"] is None and o["value_sat"] == 0 for o in op_return)


def test_bitcoin_without_prevout_fails_clearly():
    block = _bitcoin_block()
    del block["tx"][1]["vin"][0]["prevout"]  # what a pruned node or old Core returns
    with pytest.raises(ExtractError, match="verbosity 3"):
        bitcoin.decode_block(block, "bitcoin")


@pytest.mark.parametrize(
    ("btc", "satoshis"),
    [(Decimal("0.00000001"), 1), (Decimal("3.12948908"), 312948908), (Decimal("0E-8"), 0), (0, 0)],
)
def test_to_satoshis_is_exact(btc, satoshis):
    assert bitcoin.to_satoshis(btc) == satoshis


def test_to_satoshis_refuses_fractions_of_a_satoshi():
    with pytest.raises(ExtractError):
        bitcoin.to_satoshis(Decimal("0.000000001"))


def test_solana_never_requests_skipped_slots(fake_rpc, tmp_path: Path):
    solana.extract(_settings("solana", SOLANA_RANGE, tmp_path))
    method, params = fake_rpc.calls[0]
    assert method == "getBlocks"
    assert params[:2] == [SOLANA_RANGE.start, SOLANA_RANGE.end - 1]  # inclusive end
    requested = [params[0] for method, params in fake_rpc.calls if method == "getBlock"]
    assert requested == [459564295, 459564296]  # 459564297 was skipped
    options = fake_rpc.calls[1][1][1]
    assert options["encoding"] == "jsonParsed"
    assert options["maxSupportedTransactionVersion"] == 1
    assert options["commitment"] == "finalized"


def _solana_blocks() -> list[tuple[int, dict]]:
    return [
        (slot, json.loads((RPC_FIXTURES / f"solana_getBlock_{slot}.json").read_text())["result"])
        for slot in (459564295, 459564296)
    ]


def _decoded(name: str) -> list[dict]:
    rows = []
    for slot, block in _solana_blocks():
        rows += solana.decode_block(slot, block, "solana")[name]
    return rows


def test_solana_transactions():
    txs = _decoded("solana_transactions")
    assert len(txs) == 12
    assert {tx["transaction_version"] for tx in txs} == {"legacy", "0", "1"}
    assert sum(tx["is_vote"] for tx in txs) == 2
    failed = [tx for tx in txs if not tx["success"]]
    assert failed and all(json.loads(tx["error"]) for tx in failed)


def test_solana_transfers_resolve_mint_and_owners():
    transfers = _decoded("solana_transfers")
    token = [t for t in transfers if t["program"] != "system" and t["success"]]
    assert {t["program"] for t in token} == {"spl-token", "spl-token-2022"}
    assert {t["instruction_type"] for t in token} == {"transfer", "transferChecked"}
    for t in token:
        assert t["mint"] and t["source_owner"] and t["destination_owner"], t
        assert t["decimals"] is not None, t
    # Some token accounts are created and closed inside their transaction, so they
    # appear in no token balance; their mint and owner come from initializeAccount.
    balance_accounts = set()
    for _, block in _solana_blocks():
        for tx in block["transactions"]:
            keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
            for b in tx["meta"]["preTokenBalances"] + tx["meta"]["postTokenBalances"]:
                balance_accounts.add(keys[b["accountIndex"]])
    assert any(
        t["source"] not in balance_accounts or t["destination"] not in balance_accounts
        for t in token
    )


def test_solana_sol_transfers_use_wallets_directly():
    sol = [t for t in _decoded("solana_transfers") if t["program"] == "system"]
    assert sol
    for t in sol:
        assert t["mint"] is None and t["source_owner"] is None and t["decimals"] == 9
        assert int(t["amount"]) > 0


def test_solana_failed_transactions_keep_transfers_marked():
    transfers = _decoded("solana_transfers")
    txs = {
        (t["slot"], t["transaction_index"]): t["success"] for t in _decoded("solana_transactions")
    }
    assert any(not t["success"] for t in transfers)
    for t in transfers:
        assert t["success"] == txs[(t["slot"], t["transaction_index"])]


def test_inner_instruction_positions():
    positions = {t["instruction_position"] for t in _decoded("solana_transfers")}
    assert any("." in p for p in positions)
    assert any("." not in p for p in positions)
