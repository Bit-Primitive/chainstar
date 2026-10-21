"""A fake chain for sync tests, served from Parquet fixtures instead of an RPC node.

FixtureSource answers what a sync asks a node (head, block hashes) from a folder
laid out like tests/fixtures, and stands in for extraction by writing each
chunk's rows into the data folder under the names cryo and chainstar's own
extractors use. Tests move its head forward between syncs, or rewrite a block to
simulate a reorganisation.
"""

import shutil
from pathlib import Path

import duckdb

from chainstar.chains import EVM, Chain
from chainstar.chunks import chunk_path, chunk_ranges
from chainstar.config import RunSettings
from chainstar.datasets import (
    ALL_DATASETS,
    BLOCK_TIME_COLUMN,
    ERC20_METADATA,
    blocks_dataset,
    dataset_dir,
    parquet_files,
    ranged_datasets_for,
)
from chainstar.heads import ChainBlock
from chainstar.rpc import NodeIdentity
from chainstar.sync import Stage
from conftest import FIXTURES

# The blocks (Solana: slots) each chain's fixtures hold, after add_bitcoin_block.
FIXTURE_BLOCKS = {
    "ethereum": (26239738, 26239739),
    "base": (52892500, 52892501),
    "bitcoin": (972460, 972461),
    "solana": (459564295, 459564296),
}


DATASETS = {dataset.name: dataset for dataset in ALL_DATASETS}


def _files(paths: list[Path]) -> list[str]:
    return [str(path) for path in paths]


def make_source_dir(tmp_path: Path) -> Path:
    """A copy of the fixtures with a second Bitcoin block, so every chain has two."""
    source = tmp_path / "source"
    shutil.copytree(FIXTURES, source)
    add_bitcoin_block(source)
    return source


def add_bitcoin_block(source: Path) -> None:
    """Block 972461: block 972460's transactions under new txids, one day later.

    Same addresses, so wallets appear in both blocks and the incremental
    dimensions have to merge them; a new day, so dim_dates grows.
    """
    with duckdb.connect() as connection:
        for dataset in (
            "bitcoin_blocks",
            "bitcoin_transactions",
            "bitcoin_inputs",
            "bitcoin_outputs",
        ):
            folder = source / "bitcoin" / dataset
            original = parquet_files(source, "bitcoin", DATASETS[dataset])[0]
            replacements = ["block_number + 1 AS block_number"]
            if dataset == "bitcoin_blocks":
                replacements += [
                    f"'{'0' * 58}972461' AS block_hash",
                    '"timestamp" + 86400 AS "timestamp"',
                    "median_time + 86400 AS median_time",
                ]
            else:
                replacements.append("'ffff' || substr(transaction_hash, 5) AS transaction_hash")
            target = folder / f"bitcoin__{dataset}__00972461_to_00972461.parquet"
            connection.execute(
                f"COPY (SELECT * REPLACE ({', '.join(replacements)}) FROM read_parquet(?)) "
                f"TO '{target}' (FORMAT parquet)",
                [str(original)],
            )


def rewrite_block(source: Path, chain: Chain, number: int, new_hash: str) -> None:
    """Simulate a reorganisation: block `number` gets a new hash and loses half its rows."""
    with duckdb.connect() as connection:
        for dataset in ranged_datasets_for(chain):
            for path in parquet_files(source, chain.name, dataset):
                column = dataset.block_column
                if dataset is blocks_dataset(chain):
                    query = (
                        f"SELECT * REPLACE (CASE WHEN {column} = {number} THEN '{new_hash}' "
                        f"ELSE block_hash END AS block_hash) FROM read_parquet(?)"
                    )
                else:
                    query = (
                        f"SELECT * FROM read_parquet(?) WHERE {column} <> {number} "
                        "OR transaction_index % 2 = 0"
                    )
                staging = path.with_suffix(".rewrite")
                connection.execute(f"COPY ({query}) TO '{staging}' (FORMAT parquet)", [str(path)])
                staging.replace(path)


class FixtureSource:
    """A chain node and extractor backed by a fixture folder."""

    def __init__(self, chain: Chain, source: Path, head: int) -> None:
        self.chain = chain
        self.source = source
        self.head = head
        self.extracted: list[tuple[int, int]] = []

    def identity(self) -> NodeIdentity:
        return NodeIdentity(True, f"fixture head {self.head:,}", self.chain.name, self.head)

    def finalized_block(self) -> int:
        return self.head

    def block(self, number: int) -> ChainBlock | None:
        dataset = blocks_dataset(self.chain)
        files = parquet_files(self.source, self.chain.name, dataset)
        with duckdb.connect() as connection:
            row = connection.execute(
                f'SELECT block_hash, "{BLOCK_TIME_COLUMN[self.chain.family]}" '
                f"FROM read_parquet(?) WHERE {dataset.block_column} = ?",
                [_files(files), number],
            ).fetchone()
        return ChainBlock(number, row[0], row[1]) if row else None

    def extract(self, settings: RunSettings, report) -> None:
        assert settings.blocks.end - 1 <= self.head, "a sync asked for blocks past the head"
        self.extracted.append((settings.blocks.start, settings.blocks.end - 1))
        counts: dict[str, int] = {}
        with duckdb.connect() as connection:
            for dataset in ranged_datasets_for(self.chain):
                files = _files(parquet_files(self.source, self.chain.name, dataset))
                counts[dataset.name] = 0
                for first, last in chunk_ranges(settings.blocks, settings.chunk_size):
                    path = chunk_path(settings.data_dir, self.chain.name, dataset, first, last)
                    query = (
                        f"SELECT * FROM read_parquet(?) "
                        f"WHERE {dataset.block_column} BETWEEN {first} AND {last}"
                    )
                    if not path.exists():
                        path.parent.mkdir(parents=True, exist_ok=True)
                        connection.execute(f"COPY ({query}) TO '{path}' (FORMAT parquet)", [files])
                    row = connection.execute(f"SELECT count(*) FROM ({query})", [files]).fetchone()
                    counts[dataset.name] += row[0]
        if self.chain.family == EVM:
            metadata = dataset_dir(settings.data_dir, self.chain.name, ERC20_METADATA)
            if not metadata.exists():
                shutil.copytree(dataset_dir(self.source, self.chain.name, ERC20_METADATA), metadata)
        report(Stage("extract", counts, 0.0))
