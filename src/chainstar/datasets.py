"""The cryo datasets chainstar uses, their raw-table schemas, and where their files live.

Column names mirror cryo's Parquet output (as produced with `--hex --u256-types
string`) so the raw layer stays traceable to cryo. Types are chosen to be exact
and to mean the same thing in DuckDB and Postgres: cryo writes unsigned integers,
which Postgres lacks, so 64-bit wei amounts become DECIMAL(20,0) rather than
risking a BIGINT overflow.
"""

from dataclasses import dataclass
from pathlib import Path

U256_STRING_SUFFIX = "_string"


@dataclass(frozen=True)
class Dataset:
    name: str
    columns: tuple[tuple[str, str], ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    @property
    def cryo_columns(self) -> tuple[str, ...]:
        """Names to pass to cryo's --columns: cryo writes a U256 column `x` as `x_string`."""
        return tuple(name.removesuffix(U256_STRING_SUFFIX) for name in self.column_names)


BLOCKS = Dataset(
    name="blocks",
    columns=(
        ("block_number", "BIGINT"),
        ("block_hash", "VARCHAR"),
        ("timestamp", "BIGINT"),
        ("base_fee_per_gas", "DECIMAL(20,0)"),
        ("gas_used", "BIGINT"),
        ("chain_id", "BIGINT"),
    ),
)

TRANSACTIONS = Dataset(
    name="transactions",
    columns=(
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("from_address", "VARCHAR"),
        ("to_address", "VARCHAR"),
        ("value_string", "VARCHAR"),
        ("gas_limit", "BIGINT"),
        ("gas_used", "BIGINT"),
        ("gas_price", "DECIMAL(20,0)"),
        ("transaction_type", "BIGINT"),
        ("max_priority_fee_per_gas", "DECIMAL(20,0)"),
        ("max_fee_per_gas", "DECIMAL(20,0)"),
        ("success", "BOOLEAN"),
        ("chain_id", "BIGINT"),
    ),
)

ERC20_TRANSFERS = Dataset(
    name="erc20_transfers",
    columns=(
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("log_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("erc20", "VARCHAR"),
        ("from_address", "VARCHAR"),
        ("to_address", "VARCHAR"),
        ("value_string", "VARCHAR"),
        ("chain_id", "BIGINT"),
    ),
)

ERC20_METADATA = Dataset(
    name="erc20_metadata",
    columns=(
        ("block_number", "BIGINT"),
        ("erc20", "VARCHAR"),
        ("name", "VARCHAR"),
        ("symbol", "VARCHAR"),
        ("decimals", "BIGINT"),
        ("chain_id", "BIGINT"),
    ),
)

# Datasets extracted for the block range. Token metadata is not one of them: it is
# read at the chain head for the tokens the transfers mention.
# Transfers go first: some RPC providers (including free public endpoints) answer
# log queries only for recent blocks, so they are fetched while the range is newest.
RANGED_DATASETS = (ERC20_TRANSFERS, BLOCKS, TRANSACTIONS)
ALL_DATASETS = (*RANGED_DATASETS, ERC20_METADATA)


def dataset_dir(data_dir: Path, chain: str, dataset: Dataset) -> Path:
    return data_dir / chain / dataset.name


def parquet_files(data_dir: Path, chain: str, dataset: Dataset) -> list[Path]:
    return sorted(dataset_dir(data_dir, chain, dataset).glob("*.parquet"))
