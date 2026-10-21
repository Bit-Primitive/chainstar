from pathlib import Path

import duckdb
import pytest

from chainstar import chunks
from chainstar.chunks import chunk_path, chunk_ranges, write_parquet
from chainstar.config import BlockRange
from chainstar.datasets import BITCOIN_BLOCKS, SOLANA_TRANSFERS


def test_chunks_split_like_cryo():
    # cryo 0.3.2 steps from START by the chunk size; the last chunk may be short.
    assert chunk_ranges(BlockRange(26239900, 26239940), 10) == [
        (26239900, 26239909),
        (26239910, 26239919),
        (26239920, 26239929),
        (26239930, 26239939),
    ]
    assert chunk_ranges(BlockRange(5, 8), 100) == [(5, 7)]
    assert chunk_ranges(BlockRange(0, 25), 10)[-1] == (20, 24)


def test_chunk_names_are_padded_like_cryo(tmp_path: Path):
    path = chunk_path(tmp_path, "bitcoin", BITCOIN_BLOCKS, 972460, 972462)
    assert (
        path
        == tmp_path / "bitcoin/bitcoin_blocks/bitcoin__bitcoin_blocks__00972460_to_00972462.parquet"
    )


def test_write_parquet_keeps_types_and_big_amounts(tmp_path: Path):
    path = tmp_path / "out.parquet"
    row = {"chain": "solana", "slot": 1, "amount": "18446744073709551615", "success": True}
    write_parquet(path, SOLANA_TRANSFERS, [row])
    with duckdb.connect() as connection:
        types = {
            row[0]: row[1]
            for row in connection.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
        }
        stored = connection.execute(f"SELECT amount, mint FROM '{path}'").fetchone()
    assert types["slot"] == "BIGINT"
    assert types["amount"] == "VARCHAR"
    assert stored == ("18446744073709551615", None)  # u64 max survives; missing keys are NULL
    assert list(tmp_path.iterdir()) == [path]  # no staging files left behind


def test_empty_chunk_still_gets_a_file(tmp_path: Path):
    path = tmp_path / "empty.parquet"
    write_parquet(path, SOLANA_TRANSFERS, [])
    with duckdb.connect() as connection:
        assert connection.execute(f"SELECT count(*) FROM '{path}'").fetchone() == (0,)


def test_failed_write_leaves_no_chunk(tmp_path: Path, monkeypatch):
    def broken_replace(source, destination):
        raise OSError("disk full")

    monkeypatch.setattr(chunks.os, "replace", broken_replace)
    path = tmp_path / "chunk.parquet"
    with pytest.raises(OSError):
        write_parquet(path, BITCOIN_BLOCKS, [{"chain": "bitcoin", "block_number": 1}])
    assert list(tmp_path.iterdir()) == []


def test_extract_chunks_fetches_only_missing_chunks(tmp_path: Path):
    fetched = []

    def fetch(first: int, last: int):
        fetched.append((first, last))
        return {BITCOIN_BLOCKS.name: [{"chain": "bitcoin", "block_number": first}]}

    def run():
        return chunks.extract_chunks(
            data_dir=tmp_path,
            chain="bitcoin",
            datasets=(BITCOIN_BLOCKS,),
            blocks=BlockRange(10, 13),
            chunk_size=2,
            fetch_chunk=fetch,
        )

    assert run() == {"bitcoin_blocks": 2}
    assert fetched == [(10, 11), (12, 12)]
    chunk_path(tmp_path, "bitcoin", BITCOIN_BLOCKS, 12, 12).unlink()
    run()
    assert fetched[2:] == [(12, 12)]
