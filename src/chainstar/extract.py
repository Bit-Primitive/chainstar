"""The extract and decode stages for EVM chains: cryo pulls chain data into Parquet files.

extract: blocks, transactions, ERC-20 Transfer events and DEX Swap events for the
         block range. cryo decodes each Transfer log into token, from, to and
         value as it goes; swaps are decoded by chainstar (swaps.py). For one
         wallet (`chainstar wallet`), only the Transfer events it sent or
         received, the blocks those happened in, and the swaps of the pools
         that price its tokens.
decode:  for every pool seen, its tokens and factory (swaps.py); for every token
         contract seen in a transfer or a pool, cryo calls name(), symbol() and
         decimals().
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from chainstar.config import RunSettings
from chainstar.cryo import run_cryo
from chainstar.datasets import (
    BLOCKS,
    DEX_SWAPS,
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
    if dataset is DEX_SWAPS:
        # Imported here: swaps.py imports chunks.py, which imports this module.
        from chainstar.swaps import extract_swaps

        return extract_swaps(settings)
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


# cryo 0.3.2 turns --from-address into the Transfer filter's topic1 and --to-address
# into topic2. Topics are AND-ed in eth_getLogs, so passing both would match only
# transfers to oneself; "sent or received" takes one run per direction. The label
# keeps the two runs' files apart and lets re-runs skip what is already on disk.
WALLET_DIRECTIONS = (("from", "--from-address"), ("to", "--to-address"))


def extract_wallet_transfers(settings: RunSettings, wallet: str) -> int:
    """Extract only the ERC-20 transfers `wallet` sent or received; return their count.

    The node does the filtering, so only matching events are downloaded. With
    blocks_per_request above 1, one eth_getLogs request covers several blocks;
    many endpoints refuse that for filters without a contract address (the free
    publicnode endpoints accept one block per request).
    """
    for label, flag in WALLET_DIRECTIONS:
        run_cryo(
            ERC20_TRANSFERS.name,
            rpc_url=settings.rpc_url,
            output_dir=dataset_dir(settings.data_dir, settings.chain.name, ERC20_TRANSFERS),
            args=[
                "--blocks",
                str(settings.blocks),
                "--chunk-size",
                str(settings.chunk_size),
                "--inner-request-size",
                str(settings.blocks_per_request),
                flag,
                wallet,
                "--label",
                label,
                "--columns",
                *ERC20_TRANSFERS.cryo_columns,
            ],
            requests_per_second=settings.requests_per_second,
        )
    files = parquet_files(settings.data_dir, settings.chain.name, ERC20_TRANSFERS)
    if not files:
        return 0
    # A transfer to oneself is in both directions' files; count it once.
    with duckdb.connect() as connection:
        row = connection.execute(
            "SELECT count(*) FROM (SELECT DISTINCT transaction_hash, log_index "
            "FROM read_parquet(?) WHERE block_number >= ? AND block_number < ?)",
            [[str(f) for f in files], settings.blocks.start, settings.blocks.end],
        ).fetchone()
    return int(row[0]) if row else 0


def extract_blocks_with_transfers(settings: RunSettings) -> int:
    """Fetch the blocks that contain a saved transfer or swap and are not on disk yet.

    The star schema needs each transfer's and swap's block for its timestamp; in
    wallet mode that is usually a small fraction of the range. Returns the number
    fetched.
    """
    chain = settings.chain.name
    transfer_files = parquet_files(settings.data_dir, chain, ERC20_TRANSFERS)
    swap_files = parquet_files(settings.data_dir, chain, DEX_SWAPS)
    needed = distinct_values(transfer_files, "block_number") | distinct_values(
        swap_files, "block_number"
    )
    have = distinct_values(parquet_files(settings.data_dir, chain, BLOCKS), "block_number")
    missing = sorted(needed - have)
    for batch_start in range(0, len(missing), METADATA_BATCH_SIZE):
        batch = [str(number) for number in missing[batch_start : batch_start + METADATA_BATCH_SIZE]]
        run_cryo(
            BLOCKS.name,
            rpc_url=settings.rpc_url,
            output_dir=dataset_dir(settings.data_dir, chain, BLOCKS),
            # Separate values are separate blocks to cryo, not a range. It names
            # the file after the lowest and highest, so the label tells batches apart.
            args=[
                "--blocks",
                *batch,
                "--columns",
                *BLOCKS.cryo_columns,
                "--label",
                _batch_label(batch),
            ],
            requests_per_second=settings.requests_per_second,
        )
    return len(missing)


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
    from chainstar.swaps import pool_tokens  # see extract_dataset

    chain = settings.chain.name
    transfer_files = parquet_files(settings.data_dir, chain, ERC20_TRANSFERS)
    # Pool tokens too: a swap's amounts need both tokens' decimals, and a wallet's
    # pricing pools trade tokens the wallet never moved.
    seen = distinct_values(transfer_files, "erc20") | pool_tokens(settings.data_dir, chain)
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


def count_rows_in_range(
    files: Sequence[Path], start: int, end: int, column: str = "block_number"
) -> int:
    if not files:
        return 0
    query = f"SELECT count(*) FROM read_parquet(?) WHERE {column} >= ? AND {column} < ?"
    with duckdb.connect() as connection:
        row = connection.execute(query, [[str(f) for f in files], start, end]).fetchone()
    return int(row[0]) if row else 0


def distinct_values(files: Sequence[Path], column: str) -> set:
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
