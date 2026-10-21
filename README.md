# chainstar

Raw blockchain data to clean dimensional tables in your own warehouse, from one command.

chainstar pulls blocks, transactions and ERC-20 token transfers from an RPC endpoint
you provide, saves them as Parquet, loads them into DuckDB or Postgres, and builds a
small star schema with dbt: facts for transfers and fees, dimensions for wallets,
tokens, contracts and dates.

```
extract  →  decode  →  load  →  model
 cryo        cryo      DuckDB    dbt
```

- **extract**: [cryo](https://github.com/paradigmxyz/cryo) fetches blocks, transactions
  and ERC-20 `Transfer` events for your block range into Parquet files. cryo decodes
  each `Transfer` log into token, sender, recipient and value as it goes.
- **decode**: for every token contract seen, cryo calls `name()`, `symbol()` and
  `decimals()`.
- **load**: the Parquet files are copied into raw tables (schema `chainstar_raw`).
- **model**: a bundled dbt project builds the star schema (schema `chainstar`) and
  tests it.

This is an early release (0.1, unreleased). It supports **Ethereum** and **Base**, and
**DuckDB** and **Postgres** as targets.

## Install

chainstar needs Python 3.11 or newer, and the `cryo` command-line tool.

1. Install cryo (it is written in Rust; get Rust from https://rustup.rs first):

   ```sh
   cargo install cryo_cli
   ```

   This installs cryo 0.3.2 from crates.io, the version chainstar is tested with.

2. Install chainstar:

   ```sh
   pipx install chainstar     # or: pip install chainstar
   ```

   chainstar is not on PyPI yet. Until it is, install from a checkout:

   ```sh
   git clone https://github.com/Bit-Primitive/chainstar
   pipx install ./chainstar
   ```

## Quickstart

```sh
export CHAINSTAR_RPC_URL="https://your-ethereum-rpc.example"   # bring your own
chainstar doctor --chain ethereum
chainstar run --chain ethereum --blocks 20000000:20000100
```

That writes Parquet files to `./data/ethereum/` and the tables to `./warehouse.duckdb`.
Query them with DuckDB:

```sh
duckdb warehouse.duckdb "
  select d.token_address, d.symbol, count(*) as transfers, sum(f.amount) as volume
  from chainstar.fct_transfers f
  join chainstar.dim_tokens d using (token_id)
  group by 1, 2 order by 3 desc limit 10"
```

### Commands

| Command | What it does |
| --- | --- |
| `chainstar doctor` | Checks that cryo is installed, the RPC answers with the expected chain id, the target is reachable, and dbt is installed. |
| `chainstar init` | Writes a commented `chainstar.yml` you can edit. |
| `chainstar run` | Runs extract, decode, load and model for a block range, then prints row counts. |

`chainstar run` options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--chain` | from `chainstar.yml` | `ethereum` or `base` |
| `--blocks START:END` | from `chainstar.yml` | Block range. **END is exclusive**, as in cryo: `100:200` is 100 blocks. |
| `--rpc-url` | `$CHAINSTAR_RPC_URL` | JSON-RPC endpoint for the chain |
| `--target` | `$CHAINSTAR_TARGET`, then `chainstar.yml`, then `duckdb:warehouse.duckdb` | `duckdb:PATH` or `postgres:postgresql://USER@HOST:PORT/DBNAME` |
| `--data-dir` | `./data` | Where Parquet files are kept |
| `--requests-per-second` | no limit | Cap on requests cryo sends to your RPC |
| `--chunk-size` | `100` | Blocks per Parquet file. Lower it if your RPC often fails requests (see below). |
| `--config` | `./chainstar.yml` | Config file, if you use one |

Command-line flags override environment variables, which override `chainstar.yml`.

### Re-running and adding ranges

Runs are safe to repeat. cryo skips Parquet files it has already written (files hold
100 blocks each by default), and each load replaces a chain's raw rows with everything in its data
directory. So you can:

- re-run the same command after a failure; only the missing chunks are fetched again;
- run further ranges into the same warehouse; they add up;
- load Ethereum and Base into the same warehouse; every table has a `chain` column.

dbt rebuilds the star-schema tables in full on each run.

### Your RPC URL stays private

Hosted RPC URLs usually contain an API key, so chainstar:

- never writes the URL to `chainstar.yml` (it refuses to read one that contains it);
- passes it to cryo through an environment variable, not the command line;
- turns off cryo's report files;
- prints only the scheme and host, e.g. `https://eth-mainnet.g.alchemy.com/<redacted>`.

chainstar sends no telemetry, and turns off dbt's anonymous usage statistics.

## Targets

**DuckDB**: `--target duckdb:path/to/file.duckdb`. The file is created if it does not
exist. Avoid naming it `chainstar.duckdb`: DuckDB names the database after the file, so
`chainstar.fct_transfers` would become ambiguous and you would have to write
`chainstar.chainstar.fct_transfers`.

**Postgres**: `--target postgres:postgresql://user@host:5432/dbname`. Put the password in
the `PGPASSWORD` environment variable rather than the URL, so it stays out of your shell
history. chainstar loads raw tables through DuckDB's `postgres` extension, which DuckDB
downloads from its extension repository the first time it is used. The database user
needs permission to create schemas.

Both targets get three schemas: `chainstar_raw` (cryo's columns, as loaded),
`chainstar_staging` (dbt views and the `chains` seed) and `chainstar` (the star schema).

## The star schema

Keys are readable strings that start with the chain, for example
`ethereum:0xdac17f958d2ee523a2206206994597c13d831ec7`, so rows from different chains never
collide. Addresses and hashes are lowercase `0x` hex. Times are UTC.

### fct_transfers

One row per ERC-20 `Transfer` event.

| Column | Notes |
| --- | --- |
| `transfer_id` | `chain:transaction_hash:log_index` |
| `chain`, `block_number`, `block_timestamp`, `date_day` | `date_day` joins to `dim_dates` |
| `transaction_hash`, `transaction_index`, `log_index` | |
| `token_id` | joins to `dim_tokens` and `dim_contracts` |
| `from_wallet_id`, `to_wallet_id` | join to `dim_wallets`; the zero address means a mint (sender) or burn (recipient) |
| `token_address`, `from_address`, `to_address` | |
| `amount_raw` | the exact value in the token's smallest unit, as a whole-number string |
| `amount` | `amount_raw / 10^decimals` as a floating-point number (about 15 significant digits); NULL when the token's decimals are unknown |

`amount_raw` is text because ERC-20 values are 256-bit integers, larger than any numeric
type DuckDB and Postgres share. Cast it yourself when you need exact arithmetic, for
example `amount_raw::numeric` in Postgres.

### fct_fees

One row per transaction, with the fee its sender paid for execution.

| Column | Notes |
| --- | --- |
| `fee_id` | `chain:transaction_hash` |
| `chain`, `block_number`, `block_timestamp`, `date_day` | |
| `transaction_hash`, `transaction_index` | |
| `payer_wallet_id` | the sender; joins to `dim_wallets` |
| `from_address`, `to_address` | `to_address` is NULL for contract creation |
| `transaction_type` | 0 legacy, 1 access list, 2 EIP-1559, 3 blob, 4 set-code, 126 OP-stack deposit (Base) |
| `success` | |
| `gas_limit`, `gas_used` | |
| `effective_gas_price_wei` | price paid per unit of gas |
| `base_fee_per_gas_wei` | the block's base fee |
| `execution_fee_wei` | `gas_used × effective_gas_price_wei`, exact |
| `native_symbol`, `execution_fee_native` | the fee in ETH, as a floating-point number |

`execution_fee_wei` is not always the whole fee: on Ethereum it leaves out the separate
blob fee paid by blob (type 3) transactions, and on Base it leaves out the L1 data fee.
cryo does not extract either.

### dim_wallets

Every address that sent or received an ERC-20 transfer, or sent a transaction.

`wallet_id`, `chain`, `address`, `is_zero_address`, `first_seen_block`, `first_seen_at`,
`last_seen_block`, `last_seen_at`, `transfers_out`, `transfers_in`, `transactions_sent`

"Wallet" here means any address. Some of them are contracts; this release does not tell
the two apart.

### dim_tokens

Every token contract that emitted an ERC-20 `Transfer` in the loaded data.

`token_id`, `chain`, `token_address`, `name`, `symbol`, `decimals`,
`metadata_block_number`, `has_metadata`

### dim_contracts

Contracts chainstar can identify. In this release that is only the token contracts that
emitted `Transfer` events, so it has one row per row of `dim_tokens`.

`contract_id`, `chain`, `contract_address`, `contract_type` (always `erc20` for now),
`first_seen_block`, `last_seen_block`, `transfer_events`

### dim_dates

One row per UTC day from the earliest to the latest loaded block.

`date_day`, `year`, `quarter`, `month`, `day_of_month`, `iso_day_of_week` (1 = Monday),
`iso_week`, `is_weekend`

### Tests

`chainstar run` finishes with `dbt build`, which runs these checks and fails the run if
any fail: unique and not-null keys on every table, every fact row joining to its date,
token and wallet dimensions, and every token joining to `dim_contracts`.

## Limitations of this release

- **Chains**: Ethereum and Base only.
- **Events**: ERC-20 transfers only. NFT transfers are not extracted: cryo's ERC-20
  dataset skips ERC-721 `Transfer` events (they have an extra indexed topic), and
  ERC-1155 uses different events. Swaps are not modelled yet.
- **Token names lose their spaces.** cryo 0.3.2 strips spaces and control characters
  from `name()` and `symbol()` results, so "Wrapped Ether" arrives as `WrappedEther`.
  Tokens that return `bytes32` names, or that do not implement these functions, may
  have odd or empty values.
- **Symbols are not identities.** Any contract can call itself "USDC". Real data is
  full of impostor tokens with look-alike or identical symbols, and chainstar does not
  filter them. Identify tokens by `token_address`.
- **Token metadata is read at the latest block**, not at the time of the transfers. Most
  RPC nodes cannot answer calls against old state, and cryo stores a failed call as an
  empty value without reporting an error. Metadata is fetched once per token and then
  reused; delete `data/<chain>/erc20_metadata/` to fetch it again.
- **Fees** exclude Ethereum blob fees and Base's L1 data fee (see `fct_fees`).
- **Full rebuilds**: dbt rebuilds every star-schema table on each run, so very large
  warehouses will get slower to update. Incremental models are planned.
- **Block reorganisations** are not handled. Stay a few blocks behind the chain head.
- **Your RPC provider sets the limits.** Free public endpoints often refuse some
  requests, cap request rates, or answer only for recent blocks. cryo fetches ERC-20
  transfers with one request per block, and one refused request fails its whole chunk.
  chainstar retries a failed extraction up to three times, keeping completed chunks; if
  a run still fails, run the same command again, and on a flaky endpoint use a smaller
  `--chunk-size` (for example 10) so each retry makes progress.

## Roadmap

Not built yet, in rough order:

- swaps fact table (`fct_swaps`)
- more EVM chains
- Bitcoin and Solana
- BigQuery and Snowflake targets through dbt adapters
- Homebrew formula and Docker image

## Development

```sh
uv sync
uv run pytest            # unit and end-to-end tests; no network or cryo needed
uv run ruff check . && uv run ruff format --check .
```

The tests use small Parquet files cut from real cryo output (`tests/fixtures/`). The
Postgres end-to-end test runs only when `CHAINSTAR_TEST_POSTGRES_DSN` points at a
throwaway database.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
