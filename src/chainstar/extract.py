"""The extract and decode stages: cryo pulls chain data into Parquet files.

extract: blocks, transactions and ERC-20 Transfer events for the block range.
         cryo decodes each Transfer log into token, from, to and value as it goes.
decode:  for every token contract seen, cryo calls name(), symbol() and decimals().
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.config import RunSettings
from chainstar.cryo import run_cryo
from chainstar.datasets import (
    ERC20_METADATA,
    ERC20_TRANSFERS,
    Dataset,
    dataset_dir,
    parquet_files,
)

# Keeps each cryo command line a reasonable length; there is no protocol limit.
METADATA_BATCH_SIZE = 500


def extract_dataset(settings: RunSettings, dataset: Dataset) -> int:
    """Extract one ranged dataset and return its row count within the range.

    cryo skips chunk files that already exist, so re-running a range reuses what
    is on disk instead of hitting the RPC again.
    """
    run_cryo(
        dataset.name,
        rpc_url=settings.rpc_url,
        output_dir=dataset_dir(settings.data_dir, settings.chain.name, dataset),
        args=[
            "--blocks",
            str(settings.blocks),
            "--chunk-size",
            str(settings.chunk_size),
            "--columns",
            *dataset.cryo_columns,
        ],
        requests_per_second=settings.requests_per_second,
    )
    return count_rows_in_range(
        parquet_files(settings.data_dir, settings.chain.name, dataset),
        settings.blocks.start,
        settings.blocks.end,
    )


@dataclass(frozen=True)
class MetadataResult:
    tokens_seen: int
    tokens_fetched: int


def decode_token_metadata(settings: RunSettings) -> MetadataResult:
    """Fetch metadata for token contracts that have no metadata on disk yet.

    Metadata is read at the latest block, not at the historical range: most public
    and non-archive RPC nodes cannot answer calls against old state, and cryo
    records a failed call as NULL without raising, which would silently empty the
    token dimension.
    """
    chain = settings.chain.name
    transfer_files = parquet_files(settings.data_dir, chain, ERC20_TRANSFERS)
    seen = distinct_values(transfer_files, "erc20")
    known = distinct_values(parquet_files(settings.data_dir, chain, ERC20_METADATA), "erc20")
    missing = sorted(seen - known)
    for batch_start in range(0, len(missing), METADATA_BATCH_SIZE):
        batch = missing[batch_start : batch_start + METADATA_BATCH_SIZE]
        run_cryo(
            ERC20_METADATA.name,
            rpc_url=settings.rpc_url,
            output_dir=dataset_dir(settings.data_dir, chain, ERC20_METADATA),
            args=[
                "--address",
                *batch,
                "--columns",
                *ERC20_METADATA.cryo_columns,
                # cryo names files by block; a label unique to the address batch
                # stops two batches fetched in the same block from colliding.
                "--label",
                _batch_label(batch),
            ],
            requests_per_second=settings.requests_per_second,
        )
    return MetadataResult(tokens_seen=len(seen), tokens_fetched=len(missing))


def count_rows_in_range(files: Sequence[Path], start: int, end: int) -> int:
    if not files:
        return 0
    query = "SELECT count(*) FROM read_parquet(?) WHERE block_number >= ? AND block_number < ?"
    with duckdb.connect() as connection:
        row = connection.execute(query, [[str(f) for f in files], start, end]).fetchone()
    return int(row[0]) if row else 0


def distinct_values(files: Sequence[Path], column: str) -> set[str]:
    if not files:
        return set()
    with duckdb.connect() as connection:
        rows = connection.execute(
            f"SELECT DISTINCT {column} FROM read_parquet(?) WHERE {column} IS NOT NULL",
            [[str(f) for f in files]],
        ).fetchall()
    return {row[0] for row in rows}


def _batch_label(addresses: Sequence[str]) -> str:
    return hashlib.sha256("".join(addresses).encode()).hexdigest()[:12]
