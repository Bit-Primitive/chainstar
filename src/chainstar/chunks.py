"""Chunked extraction to Parquet for chains cryo does not support (Bitcoin, Solana).

Files follow cryo's layout and naming, `<chain>__<dataset>__<first>_to_<last>.parquet`
with both ends inclusive and zero-padded to 8 digits, and chunks step from the
range start by the chunk size, exactly as cryo 0.3.2 splits a range. Re-runs skip
chunks whose files are all on disk, so a failed run resumes where it stopped.
"""

import json
import os
import tempfile
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import duckdb

from chainstar.config import BlockRange
from chainstar.datasets import Dataset, dataset_dir, parquet_files
from chainstar.extract import count_rows_in_range

Rows = list[dict[str, Any]]
# Fetches one chunk (first and last block, inclusive) and returns rows per dataset name.
FetchChunk = Callable[[int, int], Mapping[str, Rows]]


class ExtractError(RuntimeError):
    pass


def chunk_ranges(blocks: BlockRange, chunk_size: int) -> list[tuple[int, int]]:
    """Inclusive (first, last) pairs covering the half-open range, as cryo splits it."""
    return [
        (first, min(first + chunk_size, blocks.end) - 1)
        for first in range(blocks.start, blocks.end, chunk_size)
    ]


def chunk_path(data_dir: Path, chain: str, dataset: Dataset, first: int, last: int) -> Path:
    name = f"{chain}__{dataset.name}__{first:0>8}_to_{last:0>8}.parquet"
    return dataset_dir(data_dir, chain, dataset) / name


def extract_chunks(
    *,
    data_dir: Path,
    chain: str,
    datasets: tuple[Dataset, ...],
    blocks: BlockRange,
    chunk_size: int,
    fetch_chunk: FetchChunk,
) -> dict[str, int]:
    """Fetch every chunk not yet on disk, then return each dataset's rows in the range."""
    for first, last in chunk_ranges(blocks, chunk_size):
        paths = {d.name: chunk_path(data_dir, chain, d, first, last) for d in datasets}
        if all(path.exists() for path in paths.values()):
            continue
        rows = fetch_chunk(first, last)
        for dataset in datasets:
            write_parquet(paths[dataset.name], dataset, rows[dataset.name])
    return {
        dataset.name: count_rows_in_range(
            parquet_files(data_dir, chain, dataset),
            blocks.start,
            blocks.end,
            column=dataset.block_column,
        )
        for dataset in datasets
    }


def write_parquet(path: Path, dataset: Dataset, rows: Iterable[Mapping[str, Any]]) -> None:
    """Write rows to `path` with the dataset's exact column types.

    Rows pass through newline-delimited JSON so DuckDB, already a dependency,
    can write the Parquet file. The file appears under its final name only once
    complete, so a crash never leaves a chunk that later runs would skip. An empty
    chunk still gets a file: it records that the chunk was fetched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = {name: sql_type for name, sql_type in dataset.columns}
    with tempfile.NamedTemporaryFile(
        "w", suffix=".ndjson", dir=path.parent, delete=False
    ) as handle:
        for row in rows:
            handle.write(json.dumps({name: row.get(name) for name in columns}) + "\n")
        staging = Path(handle.name)
    partial = path.with_name(path.name + ".partial")
    try:
        select_list = ", ".join(f'"{name}"' for name in columns)
        with duckdb.connect() as connection:
            connection.execute(
                f"COPY (SELECT {select_list} FROM read_json(?, format = 'newline_delimited', "
                f"columns = {_struct_literal(columns)})) TO '{_escape(partial)}' (FORMAT parquet)",
                [str(staging)],
            )
        os.replace(partial, path)
    finally:
        staging.unlink(missing_ok=True)
        partial.unlink(missing_ok=True)


def _struct_literal(columns: Mapping[str, str]) -> str:
    return "{" + ", ".join(f"'{name}': '{sql_type}'" for name, sql_type in columns.items()) + "}"


def _escape(path: Path) -> str:
    return str(path).replace("'", "''")
