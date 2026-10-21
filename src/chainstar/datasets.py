"""The raw datasets chainstar extracts, their raw-table schemas, and where their files live.

EVM column names mirror cryo's Parquet output (as produced with `--hex --u256-types
string`) so the raw layer stays traceable to cryo. Types are chosen to be exact
and to mean the same thing in DuckDB and Postgres: cryo writes unsigned integers,
which Postgres lacks, so 64-bit wei amounts become DECIMAL(20,0) rather than
risking a BIGINT overflow.

Bitcoin and Solana datasets are written by chainstar's own extractors. Their
column names follow the node's JSON-RPC field names where one exists, and each
row carries the chain name, as EVM rows carry the chain id.
"""

from dataclasses import dataclass
from pathlib import Path

from chainstar.chains import BITCOIN, EVM, SOLANA, Chain

U256_STRING_SUFFIX = "_string"


@dataclass(frozen=True)
class Dataset:
    name: str
    columns: tuple[tuple[str, str], ...]
    family: str = EVM
    # The column that places a row in the extracted range.
    block_column: str = "block_number"
    # False for datasets read once per item rather than per block (token and pool
    # metadata): they are replaced in full on every load and never rolled back.
    ranged: bool = True

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
    ranged=False,
)

# One row per DEX swap event (see venues.py for the three event layouts), decoded
# by chainstar from the logs cryo extracts. The pool's own amounts are kept as
# exact base-10 strings; zero_for_one, amount_in and amount_out are the net
# direction: token0 in and token1 out when zero_for_one, NULL when the event's
# amounts show no single direction (both net in, or both net out).
DEX_SWAPS = Dataset(
    name="dex_swaps",
    columns=(
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("log_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("pool", "VARCHAR"),
        ("event_family", "VARCHAR"),
        ("sender", "VARCHAR"),
        ("recipient", "VARCHAR"),
        ("amount0_in", "VARCHAR"),
        ("amount1_in", "VARCHAR"),
        ("amount0_out", "VARCHAR"),
        ("amount1_out", "VARCHAR"),
        ("zero_for_one", "BOOLEAN"),
        ("amount_in", "VARCHAR"),
        ("amount_out", "VARCHAR"),
        # Uniswap v3-style pools only: the pool's state after the swap.
        ("sqrt_price_x96", "VARCHAR"),
        ("liquidity", "VARCHAR"),
        ("tick", "BIGINT"),
        ("chain_id", "BIGINT"),
    ),
)

# One row per pool, read with eth_call at block_number (the chain head when read),
# once per pool, like token metadata. factory_confirms: the factory the pool names
# returns this pool's address for the same tokens (and fee, tick spacing or
# stable flag), so the pool really is one of that factory's.
DEX_POOLS = Dataset(
    name="dex_pools",
    columns=(
        ("block_number", "BIGINT"),
        ("pool", "VARCHAR"),
        ("event_family", "VARCHAR"),
        ("factory", "VARCHAR"),
        ("token0", "VARCHAR"),
        ("token1", "VARCHAR"),
        ("fee", "BIGINT"),
        ("tick_spacing", "BIGINT"),
        ("stable", "BOOLEAN"),
        ("factory_confirms", "BOOLEAN"),
        ("chain_id", "BIGINT"),
    ),
    ranged=False,
)

# Datasets extracted for the block range. Token and pool metadata are not: they
# are read at the chain head for the tokens and pools the range mentions.
# Logs go first: some RPC providers (including free public endpoints) answer log
# queries only for recent blocks, so they are fetched while the range is newest.
RANGED_DATASETS = (ERC20_TRANSFERS, DEX_SWAPS, BLOCKS, TRANSACTIONS)
METADATA_DATASETS = (ERC20_METADATA, DEX_POOLS)
EVM_DATASETS = (*RANGED_DATASETS, *METADATA_DATASETS)

# Bitcoin amounts are whole satoshis. The largest possible (21 million BTC) is
# about 2.1e15, well inside BIGINT.
BITCOIN_BLOCKS = Dataset(
    name="bitcoin_blocks",
    family=BITCOIN,
    columns=(
        ("chain", "VARCHAR"),
        ("block_number", "BIGINT"),
        ("block_hash", "VARCHAR"),
        # The header time the miner set. It can run up to two hours ahead and need
        # not increase from block to block; median_time does increase.
        ("timestamp", "BIGINT"),
        ("median_time", "BIGINT"),
        ("transaction_count", "BIGINT"),
        ("size", "BIGINT"),
        ("weight", "BIGINT"),
    ),
)

BITCOIN_TRANSACTIONS = Dataset(
    name="bitcoin_transactions",
    family=BITCOIN,
    columns=(
        ("chain", "VARCHAR"),
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("is_coinbase", "BOOLEAN"),
        ("input_count", "BIGINT"),
        ("output_count", "BIGINT"),
        # The fee as the node reports it; NULL for the coinbase. Kept to check the
        # fee chainstar computes from input and output values.
        ("fee_sat", "BIGINT"),
        ("size", "BIGINT"),
        ("vsize", "BIGINT"),
        ("weight", "BIGINT"),
    ),
)

# One row per spent output. Coinbase inputs spend nothing and are left out.
BITCOIN_INPUTS = Dataset(
    name="bitcoin_inputs",
    family=BITCOIN,
    columns=(
        ("chain", "VARCHAR"),
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("input_index", "BIGINT"),
        ("spent_transaction_hash", "VARCHAR"),
        ("spent_output_index", "BIGINT"),
        ("address", "VARCHAR"),
        ("script_type", "VARCHAR"),
        ("value_sat", "BIGINT"),
    ),
)

BITCOIN_OUTPUTS = Dataset(
    name="bitcoin_outputs",
    family=BITCOIN,
    columns=(
        ("chain", "VARCHAR"),
        ("block_number", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("transaction_hash", "VARCHAR"),
        ("output_index", "BIGINT"),
        # NULL for scripts without an address: OP_RETURN data, bare multisig,
        # pay-to-pubkey and non-standard scripts.
        ("address", "VARCHAR"),
        ("script_type", "VARCHAR"),
        ("value_sat", "BIGINT"),
    ),
)

BITCOIN_DATASETS = (BITCOIN_BLOCKS, BITCOIN_TRANSACTIONS, BITCOIN_INPUTS, BITCOIN_OUTPUTS)

# Solana rows are placed by slot. Lamport fees fit BIGINT; token amounts are u64,
# which can exceed it, so they are kept as base-10 strings like EVM values.
SOLANA_BLOCKS = Dataset(
    name="solana_blocks",
    family=SOLANA,
    block_column="slot",
    columns=(
        ("chain", "VARCHAR"),
        ("slot", "BIGINT"),
        ("block_height", "BIGINT"),
        ("block_hash", "VARCHAR"),
        ("parent_slot", "BIGINT"),
        # Estimated production time; the RPC returns null when it has none.
        ("block_time", "BIGINT"),
        ("transaction_count", "BIGINT"),
    ),
)

SOLANA_TRANSACTIONS = Dataset(
    name="solana_transactions",
    family=SOLANA,
    block_column="slot",
    columns=(
        ("chain", "VARCHAR"),
        ("slot", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("signature", "VARCHAR"),
        ("fee_payer", "VARCHAR"),
        ("fee_lamports", "BIGINT"),
        ("success", "BOOLEAN"),
        # meta.err as JSON text; NULL when the transaction succeeded.
        ("error", "VARCHAR"),
        ("transaction_version", "VARCHAR"),
        ("is_vote", "BOOLEAN"),
        ("compute_units_consumed", "BIGINT"),
    ),
)

# One row per parsed transfer instruction, top-level or inner, including those of
# failed transactions (marked by `success`) so the raw layer stays complete.
SOLANA_TRANSFERS = Dataset(
    name="solana_transfers",
    family=SOLANA,
    block_column="slot",
    columns=(
        ("chain", "VARCHAR"),
        ("slot", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("signature", "VARCHAR"),
        # "3" for the fourth top-level instruction, "3.1" for its second inner one.
        ("instruction_position", "VARCHAR"),
        # system, spl-token or spl-token-2022
        ("program", "VARCHAR"),
        ("instruction_type", "VARCHAR"),
        # NULL for native SOL.
        ("mint", "VARCHAR"),
        ("source", "VARCHAR"),
        ("destination", "VARCHAR"),
        # Wallets owning the token accounts; NULL for native SOL, where source and
        # destination are the wallets themselves.
        ("source_owner", "VARCHAR"),
        ("destination_owner", "VARCHAR"),
        ("amount", "VARCHAR"),
        ("decimals", "BIGINT"),
        ("success", "BOOLEAN"),
    ),
)

# One row per swap chainstar derives from a successful transaction that calls a
# known DEX program (venues.SOLANA_DEX_PROGRAMS): what one signer gave and got,
# from the transaction's balance changes (see solana.py). SOL and wrapped SOL are
# one asset, "native". Amounts are base-10 strings in the asset's smallest unit.
SOLANA_SWAPS = Dataset(
    name="solana_swaps",
    family=SOLANA,
    block_column="slot",
    columns=(
        ("chain", "VARCHAR"),
        ("slot", "BIGINT"),
        ("transaction_index", "BIGINT"),
        ("signature", "VARCHAR"),
        ("trader", "VARCHAR"),
        # The known DEX programs the transaction called, e.g. "jupiter_v6+raydium_clmm".
        ("venues", "VARCHAR"),
        ("token_in", "VARCHAR"),
        ("amount_in", "VARCHAR"),
        ("decimals_in", "BIGINT"),
        ("token_out", "VARCHAR"),
        ("amount_out", "VARCHAR"),
        ("decimals_out", "BIGINT"),
    ),
)

SOLANA_DATASETS = (SOLANA_BLOCKS, SOLANA_TRANSACTIONS, SOLANA_TRANSFERS, SOLANA_SWAPS)

ALL_DATASETS = (*EVM_DATASETS, *BITCOIN_DATASETS, *SOLANA_DATASETS)


def datasets_for(chain: Chain) -> tuple[Dataset, ...]:
    return tuple(dataset for dataset in ALL_DATASETS if dataset.family == chain.family)


def ranged_datasets_for(chain: Chain) -> tuple[Dataset, ...]:
    """The chain's datasets whose rows belong to a block: all but token and pool metadata."""
    return tuple(dataset for dataset in datasets_for(chain) if dataset.ranged)


def metadata_datasets_for(chain: Chain) -> tuple[Dataset, ...]:
    """The chain's datasets read once per token or pool, replaced in full on each load."""
    return tuple(dataset for dataset in datasets_for(chain) if not dataset.ranged)


def blocks_dataset(chain: Chain) -> Dataset:
    """The chain's raw blocks: their numbers (Solana: slots), hashes and times."""
    return {EVM: BLOCKS, BITCOIN: BITCOIN_BLOCKS, SOLANA: SOLANA_BLOCKS}[chain.family]


# The column of blocks_dataset() holding the block's Unix time.
BLOCK_TIME_COLUMN = {EVM: "timestamp", BITCOIN: "timestamp", SOLANA: "block_time"}


def coverage_dataset(chain: Chain) -> Dataset:
    """The dataset whose chunk files show which blocks (Solana: slots) were fetched.

    ERC-20 transfers on EVM chains, because they are extracted for the whole range
    in every mode, while `chainstar wallet` fetches blocks only where transfers are.
    Blocks on Bitcoin and Solana, whose chunks are written even when empty.
    """
    return {EVM: ERC20_TRANSFERS, BITCOIN: BITCOIN_BLOCKS, SOLANA: SOLANA_BLOCKS}[chain.family]


def dataset_dir(data_dir: Path, chain: str, dataset: Dataset) -> Path:
    return data_dir / chain / dataset.name


def parquet_files(data_dir: Path, chain: str, dataset: Dataset) -> list[Path]:
    return sorted(dataset_dir(data_dir, chain, dataset).glob("*.parquet"))
