"""`chainstar quickstart`: pick a small recent range from the chain head and explain what next.

The guided flow itself (questions, confirmation) lives in cli.py; this module holds
the parts worth testing on their own.
"""

from pathlib import Path

from chainstar.chains import BITCOIN, EVM, Chain
from chainstar.config import BlockRange, ConfigError
from chainstar.targets import DuckDBTarget, Target

# Bitcoin blocks are ~15 MB of JSON each, so one is enough for a first look.
DEFAULT_RECENT = {"ethereum": 20, "base": 20, "bitcoin": 1, "solana": 20}
# EVM ranges end this many blocks behind the head: chainstar does not handle
# reorganisations, and load-balanced public endpoints can lag a block or two.
# Bitcoin's head is used as is, so "the latest block" means the latest; Solana's
# head is already its latest finalized slot, which cannot be rolled back.
EVM_HEAD_MARGIN = 5

DOCKER_IMAGE = "ghcr.io/bit-primitive/chainstar"


def recent_range(chain: Chain, head: int, count: int) -> BlockRange:
    """The `count` most recent blocks (Solana: slots) up to the head, END exclusive."""
    if count < 1:
        raise ConfigError(f"the number of recent {chain.block_noun} must be at least 1")
    end = head + 1 - (EVM_HEAD_MARGIN if chain.family == EVM else 0)
    return BlockRange(max(end - count, 0), end)


def cryo_missing_message(chain: Chain) -> str:
    return (
        f"{chain.name.capitalize()} needs cryo, a small tool that reads Ethereum-style chains, "
        "and it is not installed. Either:\n"
        "  - install it: get Rust from https://rustup.rs, then run `cargo install cryo_cli`; or\n"
        "  - use the Docker image, which includes it (no Rust or Python needed):\n"
        f'      docker run --rm -v "$PWD:/work" {DOCKER_IMAGE} '
        f"quickstart --chain {chain.name} --yes\n"
        "Bitcoin and Solana work without cryo: try `chainstar quickstart --chain solana`."
    )


def next_steps(chain: Chain, target: Target, data_dir: Path) -> list[str]:
    lines = [
        "Next steps",
        "",
        "  1. Save the token transfers as a spreadsheet:",
        f"       chainstar export transfers --chain {chain.name} "
        f"--output {chain.name}-transfers.xlsx",
        "     (or .csv / .parquet; add --wallet ADDRESS for one address)",
        "",
    ]
    if isinstance(target, DuckDBTarget):
        lines += [
            "  2. Ask your own questions in SQL with the DuckDB command-line tool",
            "     (https://duckdb.org/docs/installation/), for example the most moved tokens:",
            f'       duckdb {target.path} "select k.symbol, count(*) as transfers, '
            "sum(t.amount) as amount from chainstar.fct_transfers t join chainstar.dim_tokens k "
            f"using (token_id) where t.chain = '{chain.name}' "
            'group by 1 order by 2 desc limit 10"',
            "",
            "  3. Where your files are:",
            f"       {target.path}  the warehouse (tables in schema chainstar)",
        ]
    else:
        lines += ["  2. Where your files are:", f"       {target.describe()}  the warehouse"]
    lines += [
        f"       {data_dir / chain.name}/  the raw Parquet files",
        "",
        f"  Fetch a bigger range: chainstar run --chain {chain.name} --blocks START:END",
    ]
    if chain.family == BITCOIN:
        lines.append("  (each Bitcoin block is ~6 MB to download, so start small)")
    return lines
