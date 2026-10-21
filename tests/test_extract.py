import shutil
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from chainstar import extract
from chainstar.chains import get_chain
from chainstar.config import BlockRange, RunSettings
from chainstar.datasets import (
    BLOCKS,
    DEX_POOLS,
    ERC20_METADATA,
    ERC20_TRANSFERS,
    dataset_dir,
    parquet_files,
)
from chainstar.targets import DuckDBTarget


def _settings(data_dir: Path) -> RunSettings:
    return RunSettings(
        chain=get_chain("ethereum"),
        blocks=BlockRange(26239738, 26239740),
        rpc_url="https://rpc.example",
        target=DuckDBTarget(data_dir / "w.duckdb"),
        data_dir=data_dir,
        requests_per_second=None,
        chunk_size=100,
    )


def test_count_rows_in_range_is_end_exclusive(data_dir: Path):
    files = parquet_files(data_dir, "ethereum", ERC20_TRANSFERS)
    both = extract.count_rows_in_range(files, 26239738, 26239740)
    first_only = extract.count_rows_in_range(files, 26239738, 26239739)
    assert both == 562
    assert 0 < first_only < both
    assert extract.count_rows_in_range([], 0, 10) == 0


@pytest.fixture
def recorded_cryo_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(extract, "run_cryo", lambda dataset, **kwargs: calls.append(kwargs))
    return calls


def test_decode_skips_tokens_with_metadata_on_disk(data_dir: Path, recorded_cryo_calls):
    # The fixture's pools trade tokens its transfers never moved; without them,
    # every token has metadata on disk.
    shutil.rmtree(dataset_dir(data_dir, "ethereum", DEX_POOLS))
    result = extract.decode_token_metadata(_settings(data_dir))
    assert result.tokens_seen == 11
    assert result.tokens_fetched == 0
    assert recorded_cryo_calls == []


def test_decode_reads_the_tokens_of_pools_too(data_dir: Path, recorded_cryo_calls):
    result = extract.decode_token_metadata(_settings(data_dir))
    assert (result.tokens_seen, result.tokens_fetched) == (38, 27)
    fetched = recorded_cryo_calls[0]["args"]
    assert "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" not in fetched  # WETH: on disk


def test_decode_fetches_missing_tokens_in_labelled_batches(
    data_dir: Path, recorded_cryo_calls, monkeypatch
):
    for file in parquet_files(data_dir, "ethereum", ERC20_METADATA):
        file.unlink()
    shutil.rmtree(dataset_dir(data_dir, "ethereum", DEX_POOLS))
    monkeypatch.setattr(extract, "METADATA_BATCH_SIZE", 4)

    result = extract.decode_token_metadata(_settings(data_dir))

    assert result.tokens_fetched == 11
    assert len(recorded_cryo_calls) == 3  # 4 + 4 + 3
    labels = set()
    fetched = []
    for call in recorded_cryo_calls:
        args = call["args"]
        labels.add(args[args.index("--label") + 1])
        fetched += args[args.index("--address") + 1 : args.index("--columns")]
        # Metadata is read at the chain head, never at the historical range.
        assert "--blocks" not in args
    assert len(labels) == 3
    assert sorted(fetched) == sorted(
        extract.distinct_values(parquet_files(data_dir, "ethereum", ERC20_TRANSFERS), "erc20")
    )


def test_extract_passes_range_and_chunk_size_to_cryo(data_dir: Path, recorded_cryo_calls):
    rows = extract.extract_dataset(replace(_settings(data_dir), chunk_size=10), BLOCKS)
    args = recorded_cryo_calls[0]["args"]
    assert args[args.index("--blocks") + 1] == "26239738:26239740"
    assert args[args.index("--chunk-size") + 1] == "10"
    assert rows == 2


def test_wallet_transfers_use_one_filtered_run_per_direction(data_dir: Path, recorded_cryo_calls):
    settings = replace(_settings(data_dir), wallet="0xabc", blocks_per_request=50)
    rows = extract.extract_wallet_transfers(settings, "0xabc")
    flags = []
    for call in recorded_cryo_calls:
        args = call["args"]
        assert args[args.index("--inner-request-size") + 1] == "50"
        label = args[args.index("--label") + 1]
        flag = "--from-address" if "--from-address" in args else "--to-address"
        assert args[args.index(flag) + 1] == "0xabc"
        flags.append((label, flag))
    # topic1 and topic2 are AND-ed by eth_getLogs, so never both in one run.
    assert flags == [("from", "--from-address"), ("to", "--to-address")]
    assert rows == 562  # the fixture files stand in for what cryo wrote


def test_wallet_blocks_are_fetched_only_where_transfers_are(data_dir: Path, recorded_cryo_calls):
    assert extract.extract_blocks_with_transfers(_settings(data_dir)) == 0
    assert recorded_cryo_calls == []

    for file in parquet_files(data_dir, "ethereum", BLOCKS):
        file.unlink()
    assert extract.extract_blocks_with_transfers(_settings(data_dir)) == 2
    args = recorded_cryo_calls[0]["args"]
    assert args[args.index("--blocks") + 1 : args.index("--columns")] == ["26239738", "26239739"]
    assert "--label" in args


def test_a_wallet_also_fetches_the_blocks_of_its_pricing_swaps(data_dir: Path, recorded_cryo_calls):
    # Only the second block has a transfer; the first only a swap. Both are fetched:
    # a swap needs its block's time to be priced and valued.
    for file in parquet_files(data_dir, "ethereum", BLOCKS):
        file.unlink()
    transfers = parquet_files(data_dir, "ethereum", ERC20_TRANSFERS)[0]
    duckdb.execute(
        f"COPY (SELECT * FROM read_parquet('{transfers}') WHERE block_number = 26239739) "
        f"TO '{transfers}.tmp' (FORMAT parquet)"
    )
    Path(f"{transfers}.tmp").replace(transfers)
    assert extract.extract_blocks_with_transfers(_settings(data_dir)) == 2
    args = recorded_cryo_calls[0]["args"]
    assert args[args.index("--blocks") + 1 : args.index("--columns")] == ["26239738", "26239739"]
