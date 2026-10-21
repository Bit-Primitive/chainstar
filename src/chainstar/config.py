"""Run settings: block ranges, the chainstar.yml file, and how options are resolved.

Precedence for every setting is: command-line flag, then environment variable,
then chainstar.yml, then the built-in default.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from chainstar.chains import SUPPORTED_CHAINS, Chain, get_chain
from chainstar.public_rpc import public_endpoint
from chainstar.targets import Target, parse_target

CONFIG_FILENAME = "chainstar.yml"
DEFAULT_RPC_ENV = "CHAINSTAR_RPC_URL"
TARGET_ENV = "CHAINSTAR_TARGET"
# Not "chainstar.duckdb": DuckDB names the catalog after the file, and a catalog
# called chainstar would make the schema reference chainstar.fct_transfers ambiguous.
DEFAULT_TARGET = "duckdb:warehouse.duckdb"
DEFAULT_DATA_DIR = "./data"
# Blocks per Parquet file. cryo saves and later skips whole files, so a failed
# request costs at most one chunk of re-fetching. cryo's own default is 1000.
DEFAULT_CHUNK_SIZE = 100


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class BlockRange:
    """Half-open block range [start, end), matching cryo's `START:END` syntax."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end <= self.start:
            raise ConfigError(f"block range {self} is empty; END must be greater than START")

    def __str__(self) -> str:
        return f"{self.start}:{self.end}"

    def __len__(self) -> int:
        return self.end - self.start


_RANGE_PATTERN = re.compile(r"^\s*([\d_]+)\s*:\s*([\d_]+)\s*$")


def parse_block_range(text: str) -> BlockRange:
    match = _RANGE_PATTERN.match(str(text))
    if not match:
        raise ConfigError(
            f"blocks must be START:END with whole numbers (END exclusive), got {text!r}"
        )
    start, end = (int(group.replace("_", "")) for group in match.groups())
    return BlockRange(start, end)


@dataclass(frozen=True)
class RunSettings:
    chain: Chain
    blocks: BlockRange
    rpc_url: str
    target: Target
    data_dir: Path
    requests_per_second: int | None
    chunk_size: int
    # True when no RPC URL was configured and the chain's free public endpoint is used.
    rpc_is_public: bool = False
    # Set by `chainstar wallet`: on EVM chains, extract only this address's transfers.
    wallet: str | None = None
    # Blocks per eth_getLogs request when extracting one wallet's transfers.
    blocks_per_request: int = 1


@dataclass(frozen=True)
class RpcChoice:
    url: str
    # True when nothing was configured and chainstar fell back to a public endpoint.
    public: bool


def load_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    if "rpc_url" in data:
        # Refuse rather than silently ignore: a key in this file would end up in git.
        raise ConfigError(
            f"{path} contains rpc_url; keep RPC URLs out of config files and set "
            f"the {data.get('rpc_url_env', DEFAULT_RPC_ENV)} environment variable instead"
        )
    return data


def rpc_env_name(file_config: dict[str, Any]) -> str:
    return file_config.get("rpc_url_env", DEFAULT_RPC_ENV)


def chain_rpc_env_name(file_config: dict[str, Any], chain: Chain) -> str:
    """e.g. CHAINSTAR_RPC_URL_BASE: one endpoint per chain, for syncing several at once."""
    return f"{rpc_env_name(file_config)}_{chain.name.upper()}"


def resolve_rpc(flag: str | None, file_config: dict[str, Any], chain: Chain) -> RpcChoice:
    """The --rpc-url flag, then the chain's own environment variable, then the shared one,
    then the chain's public endpoint."""
    rpc_url = (
        flag
        or os.environ.get(chain_rpc_env_name(file_config, chain), "")
        or os.environ.get(rpc_env_name(file_config), "")
    )
    if rpc_url:
        return RpcChoice(rpc_url, public=False)
    return RpcChoice(public_endpoint(chain).url, public=True)


def rpc_secrets(flag: str | None, file_config: dict[str, Any]) -> list[str]:
    """Every RPC URL chainstar might use, so all of them are redacted from its output."""
    base = rpc_env_name(file_config)
    names = [base, *(f"{base}_{name.upper()}" for name in SUPPORTED_CHAINS)]
    return [url for url in (flag, *(os.environ.get(name) for name in names)) if url]


@dataclass(frozen=True)
class Connection:
    """How chainstar talks to one chain's RPC endpoint."""

    rpc: RpcChoice
    requests_per_second: int | None
    chunk_size: int


def resolve_connection(
    *,
    file_config: dict[str, Any],
    chain: Chain,
    rpc_url: str | None,
    requests_per_second: int | None,
    chunk_size: int | None,
) -> Connection:
    rpc = resolve_rpc(rpc_url, file_config, chain)
    rps = requests_per_second or file_config.get("requests_per_second")
    chunk = chunk_size or file_config.get("chunk_size")
    if rpc.public:
        # A shared endpoint gets the gentle defaults unless the user chose otherwise.
        endpoint = public_endpoint(chain)
        rps = rps or endpoint.requests_per_second
        chunk = chunk or endpoint.chunk_size
    chunk = int(chunk or DEFAULT_CHUNK_SIZE)
    if chunk < 1:
        raise ConfigError(f"chunk_size must be at least 1, got {chunk}")
    return Connection(rpc, int(rps) if rps else None, chunk)


def target_spec(flag: str | None, file_config: dict[str, Any]) -> str:
    return flag or os.environ.get(TARGET_ENV) or file_config.get("target", DEFAULT_TARGET)


_DURATION = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    """'30s', '5m', '2h' or '1d' as seconds."""
    match = _DURATION.match(str(text).lower())
    if not match or int(match[1]) < 1:
        raise ConfigError(
            f"duration must be a whole number with s, m, h or d, like 30s or 5m; got {text!r}"
        )
    return int(match[1]) * _SECONDS[match[2]]


def resolve_settings(
    *,
    file_config: dict[str, Any],
    chain: str | None,
    blocks: str | None,
    rpc_url: str | None,
    target: str | None,
    data_dir: Path | None,
    requests_per_second: int | None,
    chunk_size: int | None,
    wallet: str | None = None,
    blocks_per_request: int = 1,
) -> RunSettings:
    chain_name = chain or file_config.get("chain")
    if not chain_name:
        raise ConfigError("no chain: pass --chain or set `chain` in chainstar.yml")
    block_text = blocks or file_config.get("blocks")
    if not block_text:
        raise ConfigError(
            "no block range: pass --blocks START:END or set `blocks` in chainstar.yml"
        )
    resolved_chain = get_chain(chain_name)
    connection = resolve_connection(
        file_config=file_config,
        chain=resolved_chain,
        rpc_url=rpc_url,
        requests_per_second=requests_per_second,
        chunk_size=chunk_size,
    )
    if blocks_per_request < 1:
        raise ConfigError(f"blocks per request must be at least 1, got {blocks_per_request}")
    return RunSettings(
        chain=resolved_chain,
        blocks=parse_block_range(block_text),
        rpc_url=connection.rpc.url,
        target=parse_target(target_spec(target, file_config)),
        data_dir=resolve_data_dir(data_dir, file_config),
        requests_per_second=connection.requests_per_second,
        chunk_size=connection.chunk_size,
        rpc_is_public=connection.rpc.public,
        wallet=wallet,
        blocks_per_request=blocks_per_request,
    )


def resolve_data_dir(flag: Path | None, file_config: dict[str, Any]) -> Path:
    return Path(flag or file_config.get("data_dir", DEFAULT_DATA_DIR)).expanduser()


CONFIG_TEMPLATE = """\
# chainstar configuration. Command-line flags override anything set here.
# https://github.com/Bit-Primitive/chainstar

# Chain to read: ethereum, base, bitcoin or solana.
chain: ethereum

# Block range START:END. END is exclusive, so 20000000:20000100 is 100 blocks.
# On Solana the range counts slots; skipped slots simply have no rows.
# Uncomment and set, or pass --blocks on the command line.
# blocks: "20000000:20000100"

# Where the tables are written. One of:
#   duckdb:PATH
#   postgres:postgresql://USER@HOST:PORT/DBNAME
# For Postgres, leave the password out of this file and set PGPASSWORD instead.
# The CHAINSTAR_TARGET environment variable overrides this value.
target: duckdb:warehouse.duckdb

# Where Parquet files are kept. Files already here are reused on re-runs.
data_dir: ./data

# The RPC URL is never stored in this file, because hosted RPC URLs usually
# contain an API key. chainstar reads it from the environment variable named here.
# If it is not set, chainstar uses a free shared public endpoint for the chain,
# which is rate limited: fine for trying chainstar, slow for large ranges.
rpc_url_env: CHAINSTAR_RPC_URL

# Optional: cap requests per second sent to your RPC provider.
# requests_per_second: 25

# Optional: blocks (Solana: slots) per Parquet file (default 100). If your provider often fails
# requests, smaller chunks let retries make progress.
# chunk_size: 100

# For `chainstar sync`: the chains to keep up to date when no --chain is given.
# With several chains, give each its own RPC URL in CHAINSTAR_RPC_URL_<CHAIN>,
# for example CHAINSTAR_RPC_URL_BASE.
# chains: [ethereum, base]

# For `chainstar sync`: confirmations a block needs before it is synced, per chain
# (defaults: ethereum 12, base 60, bitcoin 6; Solana always syncs finalized slots).
# `finalized` on Ethereum or Base waits for the node's finalized block.
# confirmations:
#   ethereum: 12
#   base: 60
#   bitcoin: 6
"""
