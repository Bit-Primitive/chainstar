# chainstar

Raw blockchain data to clean dimensional tables in your own warehouse, from one command.

chainstar pulls blocks, transactions and token transfers from an RPC endpoint you
provide, saves them as Parquet, loads them into DuckDB or Postgres, and builds a small
star schema with dbt: facts for transfers and fees, dimensions for wallets, tokens,
contracts and dates. All chains share the same tables, so one warehouse can hold
Ethereum, Base, Bitcoin and Solana side by side.

```
extract  →  decode  →  load  →  model
 cryo        cryo      DuckDB    dbt        (Ethereum, Base)
 JSON-RPC    -         DuckDB    dbt        (Bitcoin, Solana)
```

- **extract**: on Ethereum and Base, [cryo](https://github.com/paradigmxyz/cryo) fetches
  blocks, transactions and ERC-20 `Transfer` events for your block range into Parquet
  files, decoding each `Transfer` log into token, sender, recipient and value. cryo does
  not support Bitcoin or Solana, so for those chainstar reads blocks over plain JSON-RPC
  itself and writes Parquet files in the same layout (see [Chains](#chains)).
- **decode** (Ethereum and Base): for every token contract seen, cryo calls `name()`,
  `symbol()` and `decimals()`.
- **load**: the Parquet files are copied into raw tables (schema `chainstar_raw`).
- **model**: a bundled dbt project builds the star schema (schema `chainstar`) and
  tests it.

This is an early release (0.1, unreleased). It supports **Ethereum**, **Base**,
**Bitcoin** and **Solana**, and **DuckDB** and **Postgres** as targets.

## Install

chainstar needs Python 3.11 or newer. For Ethereum and Base it also needs the `cryo`
command-line tool; Bitcoin and Solana need nothing else.

1. For Ethereum and Base, install cryo (it is written in Rust; get Rust from
   https://rustup.rs first):

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
Bitcoin and Solana work the same way, with an RPC endpoint for that chain:

```sh
CHAINSTAR_RPC_URL="https://your-bitcoin-node.example" \
  chainstar run --chain bitcoin --blocks 972460:972463
CHAINSTAR_RPC_URL="https://your-solana-rpc.example" \
  chainstar run --chain solana --blocks 459564290:459564310   # slots, see below
```

Query the tables with DuckDB:

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
| `chainstar doctor` | Checks that cryo is installed (Ethereum and Base only), the RPC serves the expected chain (EVM chain id, Bitcoin network `main`, or Solana mainnet's genesis hash), the target is reachable, and dbt is installed. |
| `chainstar init` | Writes a commented `chainstar.yml` you can edit. |
| `chainstar run` | Runs extract, decode, load and model for a block range, then prints row counts. |

`chainstar run` options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--chain` | from `chainstar.yml` | `ethereum`, `base`, `bitcoin` or `solana` |
| `--blocks START:END` | from `chainstar.yml` | Block range. **END is exclusive**, as in cryo: `100:200` is 100 blocks. On Solana the numbers are slots. |
| `--rpc-url` | `$CHAINSTAR_RPC_URL` | JSON-RPC endpoint for the chain |
| `--target` | `$CHAINSTAR_TARGET`, then `chainstar.yml`, then `duckdb:warehouse.duckdb` | `duckdb:PATH` or `postgres:postgresql://USER@HOST:PORT/DBNAME` |
| `--data-dir` | `./data` | Where Parquet files are kept |
| `--requests-per-second` | no limit | Cap on requests sent to your RPC |
| `--chunk-size` | `100` | Blocks (Solana: slots) per Parquet file. Lower it if your RPC often fails requests (see below). |
| `--config` | `./chainstar.yml` | Config file, if you use one |

Command-line flags override environment variables, which override `chainstar.yml`.

### Re-running and adding ranges

Runs are safe to repeat. Parquet files already written are skipped, by cryo on Ethereum
and Base and by chainstar on Bitcoin and Solana (files hold 100 blocks each by default),
and each load replaces a chain's raw rows with everything in its data directory. So you
can:

- re-run the same command after a failure; only the missing chunks are fetched again;
- run further ranges into the same warehouse; they add up;
- load all four chains into the same warehouse; every table has a `chain` column.

dbt rebuilds the star-schema tables in full on each run.

### Your RPC URL stays private

Hosted RPC URLs usually contain an API key, so chainstar:

- never writes the URL to `chainstar.yml` (it refuses to read one that contains it);
- passes it to cryo through an environment variable, not the command line, and sends
  it nowhere but your endpoint for Bitcoin and Solana;
- turns off cryo's report files;
- prints only the scheme and host, e.g. `https://eth-mainnet.g.alchemy.com/<redacted>`.

chainstar sends no telemetry, and turns off dbt's anonymous usage statistics.

## Chains

| Chain | Extracted by | RPC you need | `--blocks` counts | Identity check |
| --- | --- | --- | --- | --- |
| Ethereum | cryo 0.3.2 | Ethereum JSON-RPC | blocks | chain id 1 |
| Base | cryo 0.3.2 | Ethereum JSON-RPC | blocks | chain id 8453 |
| Bitcoin | chainstar | Bitcoin Core JSON-RPC, version 23 or later, not pruned below your range | blocks (heights) | `getblockchaininfo` reports chain `main` |
| Solana | chainstar | Solana JSON-RPC that serves `getBlock` for your slots | slots | `getGenesisHash` is mainnet's, `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d` |

`chainstar run` checks the identity before extracting anything and stops if the endpoint
serves a different chain (a testnet, devnet or another EVM chain).

**Bitcoin.** For each block chainstar calls `getblockhash` and then `getblock` with
verbosity 3, which includes the spent output (`prevout`: value, address, script type)
for every input. That is what lets chainstar compute fees and name senders without one
extra call per input. Bitcoin Core returns it only for blocks it still has undo data for,
so a pruned node fails for older blocks, with an error that says so. Amounts arrive as BTC
with 8 decimals and are converted to whole satoshis exactly.

**Solana.** `START:END` counts slots. Not every slot has a block: for each chunk
chainstar asks `getBlocks` which slots have a finalized block, then fetches only those
with `getBlock` (`jsonParsed`, full transaction details, `maxSupportedTransactionVersion`
1, commitment `finalized`). Skipped slots produce no rows and cost no failed requests. The
end of the range must not be past the latest finalized slot, which `chainstar run` checks.
Version-1 transactions are live on mainnet: an endpoint or setting that accepts only
version 0 refuses every block that contains one.

Both extractors send one request at a time, honour `--requests-per-second`, make up to
three attempts per request with a growing delay between them, and ask for gzip (it shrank a
Bitcoin block about 2.5 times and a Solana block about 7 times). Tested in October 2026
against `https://bitcoin-rpc.publicnode.com` (Bitcoin Core 29.3) and
`https://solana-rpc.publicnode.com` (solana-core 4.3.0);
`https://api.mainnet-beta.solana.com` answered the same identity and slot calls. Public
endpoints are fine for trying chainstar on a few blocks; a Bitcoin block with input
values is about 15 MB of JSON (6 MB compressed) and a Solana block about 4 to 8 MB.

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

Both targets get three schemas: `chainstar_raw` (the extracted columns, as loaded: cryo's
for Ethereum and Base, and `bitcoin_*` and `solana_*` tables), `chainstar_staging` (dbt
views and the `chains` seed) and `chainstar` (the star schema).

## The star schema

Keys are readable strings that start with the chain, for example
`ethereum:0xdac17f958d2ee523a2206206994597c13d831ec7` or `bitcoin:native`, so rows from
different chains never collide. Addresses and hashes are as each chain writes them:
lowercase `0x` hex on Ethereum and Base, Bitcoin addresses and txids as the node returns
them, base58 public keys and signatures on Solana. Times are UTC.

The shared columns mean the same on every chain: `chain`; `block_number` (the Bitcoin
block height, the Solana slot); `block_timestamp` and `date_day`; `transaction_hash` (the
Bitcoin txid, the Solana transaction's first signature); and the `_id` keys. A column
that does not apply to a chain is NULL there, as noted below.

### fct_transfers

One row per value movement:

- **Ethereum, Base**: one row per ERC-20 `Transfer` event.
- **Bitcoin**: one row per transaction output, coinbase outputs included, in BTC.
  Bitcoin has no tokens, and a transaction can spend coins from many addresses at once,
  so no single input is picked as "the" sender. `from_address` is set only when every
  input came from the same address, which is the common case; when inputs came from
  several addresses it is NULL and `input_address_count` says how many. The coinbase
  spends nothing: `from_address` NULL, `input_address_count` 0. Outputs without an
  address (OP_RETURN data, bare multisig, pay-to-pubkey, non-standard scripts) are kept
  with `to_address` NULL and their `script_type`; in October 2026 almost half of all
  outputs were zero-value OP_RETURN outputs.
- **Solana**: one row per parsed SPL Token or Token-2022 `transfer` / `transferChecked`
  instruction and System Program SOL `transfer` instruction, top-level or inner (CPI), in
  successful transactions only. A failed transaction (`meta.err` set) moves nothing; its
  fee is still in `fct_fees`. For SPL tokens, `from_address` and `to_address` are the
  wallets that own the token accounts, and the token accounts are in
  `from_token_account` and `to_token_account`.

| Column | Notes |
| --- | --- |
| `transfer_id` | `chain:transaction_hash:transfer_position` (on EVM chains the same as `chain:transaction_hash:log_index`) |
| `chain`, `block_number`, `block_timestamp`, `date_day` | `date_day` joins to `dim_dates` |
| `transaction_hash`, `transaction_index` | |
| `transfer_position` | log index (EVM), output index (Bitcoin), instruction position (Solana: `3` is the fourth top-level instruction, `3.1` its second inner instruction) |
| `log_index` | EVM only |
| `token_id` | joins to `dim_tokens`, and on EVM chains to `dim_contracts` |
| `from_wallet_id`, `to_wallet_id` | join to `dim_wallets`; on EVM the zero address means a mint (sender) or burn (recipient); NULL on Bitcoin where there is no single sender or no address |
| `token_address` | ERC-20 contract, SPL mint, or `native` for BTC and SOL |
| `from_address`, `to_address` | |
| `input_address_count` | Bitcoin only: distinct addresses among the inputs |
| `from_token_account`, `to_token_account` | Solana SPL only |
| `script_type` | Bitcoin only: the output script type as the node names it (`witness_v0_keyhash`, `nulldata`, ...) |
| `amount_raw` | the exact value in the asset's smallest unit (satoshis, lamports, token base units), as a whole-number string |
| `amount` | `amount_raw / 10^decimals` as a floating-point number (about 15 significant digits); NULL when the token's decimals are unknown |

`amount_raw` is text because ERC-20 values are 256-bit integers and SPL amounts 64-bit
unsigned, larger than any numeric type DuckDB and Postgres share. Cast it yourself when
you need exact arithmetic, for example `amount_raw::numeric` in Postgres.

Why Solana transfers come from instructions rather than from token balance changes:
balance changes (`preTokenBalances` / `postTokenBalances`) net out every movement in a
transaction and cannot say who paid whom. Checked against real blocks, the parsed
instructions account for every token balance change except wrapped-SOL wrapping and
unwrapping (`syncNative`, `closeAccount`) and the fee that Token-2022 mints with the
transfer-fee extension withhold from the recipient: for those mints `amount_raw` is the
amount sent, and the recipient receives less. Token accounts that are created and closed
within one transaction appear in no balance list; their mint and owner are read from the
`initializeAccount` instruction that created them.

### fct_fees

One row per fee-paying transaction, with what its payer paid.

- **Ethereum, Base**: every transaction; the execution fee, `gas_used × effective gas
  price`. On Ethereum this leaves out the separate blob fee paid by blob (type 3)
  transactions, and on Base the L1 data fee. cryo does not extract either.
- **Bitcoin**: every transaction except the coinbase, which pays no fee; the fee is the
  value of the outputs spent minus the value of the outputs created. A dbt test checks
  it against the fee the node reports for every transaction. The payer is known only
  when one address funded the whole transaction; otherwise `payer_wallet_id` is NULL.
- **Solana**: every transaction, failed and vote transactions included (`is_vote` flags
  validator votes, which are most transactions); the fee is `meta.fee` (base plus
  priority fee), paid by the transaction's first account.

| Column | Notes |
| --- | --- |
| `fee_id` | `chain:transaction_hash` |
| `chain`, `block_number`, `block_timestamp`, `date_day` | |
| `transaction_hash`, `transaction_index` | |
| `payer_wallet_id` | who paid; joins to `dim_wallets` |
| `from_address` | the payer's address |
| `to_address` | EVM only; NULL for contract creation |
| `transaction_type` | EVM only: 0 legacy, 1 access list, 2 EIP-1559, 3 blob, 4 set-code, 126 OP-stack deposit (Base) |
| `success` | always true on Bitcoin |
| `is_vote` | Solana only |
| `gas_limit`, `gas_used`, `effective_gas_price_wei`, `base_fee_per_gas_wei` | EVM only |
| `execution_fee_wei`, `execution_fee_native` | EVM only, kept from earlier releases; equal to `fee_raw` and `fee_native` |
| `fee_raw` | the fee in the chain's smallest unit (wei, satoshis, lamports), exact |
| `native_symbol`, `fee_native` | the fee in ETH, BTC or SOL, as a floating-point number |

### dim_wallets

Every address that sent or received a transfer, or paid for a transaction.

`wallet_id`, `chain`, `address`, `is_zero_address`, `first_seen_block`, `first_seen_at`,
`last_seen_block`, `last_seen_at`, `transfers_out`, `transfers_in`, `transactions_sent`

"Wallet" here means any address. Some of them are contracts or programs; this release
does not tell them apart. On Bitcoin, `transfers_out` counts inputs spent from the
address and `transactions_sent` the transactions it helped fund, since an output does
not always have a single sender. `is_zero_address` applies to EVM chains only.

### dim_tokens

Every asset moved in the loaded data.

`token_id`, `chain`, `token_address`, `token_standard`, `name`, `symbol`, `decimals`,
`metadata_block_number`, `has_metadata`

- **Ethereum, Base**: `token_standard` `erc20`; name, symbol and decimals as cryo read
  them from the contract.
- **Bitcoin**: one row, `bitcoin:native`, symbol `BTC`, 8 decimals.
- **Solana**: one row per SPL mint (`spl-token` or `spl-token-2022`) with decimals from
  the transactions, plus `solana:native` for SOL (9 decimals). Mint names and symbols
  live in separate metadata programs that chainstar does not read, so they are NULL
  rather than guessed. Wrapped SOL (`So111...112`) is an SPL mint like any other, kept
  apart from native SOL.

### dim_contracts

Contracts chainstar can identify. In this release that is only the Ethereum and Base
token contracts that emitted `Transfer` events, so it has one row per `erc20` row of
`dim_tokens`. Bitcoin has no contracts, and Solana programs are not extracted.

`contract_id`, `chain`, `contract_address`, `contract_type` (always `erc20` for now),
`first_seen_block`, `last_seen_block`, `transfer_events`

### dim_dates

One row per UTC day from the earliest to the latest loaded block.

`date_day`, `year`, `quarter`, `month`, `day_of_month`, `iso_day_of_week` (1 = Monday),
`iso_week`, `is_weekend`

### Tests

`chainstar run` finishes with `dbt build`, which runs these checks and fails the run if
any fail: unique and not-null keys on every table, every fact row joining to its date,
token and wallet dimensions, and every ERC-20 token joining to `dim_contracts`. Where a
column does not apply to a chain (a Bitcoin output with no single sender, for example),
its not-null test skips that chain.

## Limitations of this release

- **Chains**: Ethereum, Base, Bitcoin mainnet and Solana mainnet-beta.
- **Events**: ERC-20 transfers only on Ethereum and Base. NFT transfers are not
  extracted: cryo's ERC-20 dataset skips ERC-721 `Transfer` events (they have an extra
  indexed topic), and ERC-1155 uses different events. Swaps are not modelled yet.
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
- **Bitcoin** needs a node that serves `getblock` verbosity 3 with undo data: Bitcoin
  Core 23 or later, not pruned below your range. Other Bitcoin RPC implementations were
  not tested. The coinbase transactions in blocks 91842 and 91880 repeat txids of earlier
  coinbases (BIP 30 grandfathers them); loading those blocks would fail the unique key
  tests.
- **Solana transfers** cover SPL `transfer` and `transferChecked` and System Program
  `transfer`. Not included: SPL mints and burns (unlike ERC-20, they are separate
  instructions, not transfers from or to a zero address), Token-2022
  `transferCheckedWithFee`, SOL moved by `createAccount`, `transferWithSeed` or by
  programs that change lamport balances directly, and staking rewards. SPL token names
  and symbols are not read.
- **Solana block times** are the RPC's estimate; a block without one fails the
  `block_timestamp` not-null test.
- **Full rebuilds**: dbt rebuilds every star-schema table on each run, so very large
  warehouses will get slower to update. Incremental models are planned.
- **Block reorganisations** are not handled. Stay a few blocks behind the chain head
  (Solana reads only finalized slots, which cannot be rolled back).
- **Your RPC provider sets the limits.** Free public endpoints often refuse some
  requests, cap request rates, or answer only for recent blocks. cryo fetches ERC-20
  transfers with one request per block, and one refused request fails its whole chunk.
  chainstar retries a failed extraction (Bitcoin and Solana: each failed request), making
  up to three attempts and keeping completed chunks; if a run still fails, run the same command
  again, and on a flaky endpoint use a smaller `--chunk-size` (for example 10) so each
  retry makes progress.

## Roadmap

Not built yet, in rough order:

- swaps fact table (`fct_swaps`)
- more EVM chains
- SPL mints and burns, and Solana token names from on-chain metadata
- BigQuery and Snowflake targets through dbt adapters
- Homebrew formula and Docker image

## Development

```sh
uv sync
uv run pytest            # unit and end-to-end tests; no network or cryo needed
uv run ruff check . && uv run ruff format --check .
```

The tests use small Parquet files cut from real cryo output (`tests/fixtures/`), and real
Bitcoin and Solana RPC responses sliced to a few transactions (`tests/rpc_fixtures/`),
from which the Bitcoin and Solana Parquet fixtures are produced by chainstar's own
extractors. The end-to-end tests load all four chains into one warehouse. The Postgres
end-to-end test runs only when `CHAINSTAR_TEST_POSTGRES_DSN` points at a throwaway
database.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
