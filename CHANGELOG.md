# Changelog

All notable changes to chainstar are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `chainstar quickstart`: a guided first run. Asks for a chain and how many of the latest
  blocks (default 20, or the latest Bitcoin block), reads the range from the chain head,
  builds `warehouse.duckdb`, and prints next steps (an export command, a ready-to-paste
  DuckDB query, where the files are). `--yes`, or no terminal, skips the questions. If
  cryo is missing for Ethereum or Base it explains how to install it or use Docker.
- Works without your own RPC: each chain has a default free public endpoint
  (`*-rpc.publicnode.com`), used when neither `--rpc-url` nor `CHAINSTAR_RPC_URL` is
  set, with a one-line notice and gentle defaults (5 requests/s, Bitcoin 2; 10-block
  chunks).
- `chainstar export`: any star-schema table, or a `transfers` view with token symbols
  and names, to CSV, Excel (.xlsx, through DuckDB's `excel` extension) or Parquet, with
  `--chain`, `--wallet`, `--token` and `--limit`. `amount_raw` stays exact text. Exports
  that do not fit in an Excel sheet are refused with a suggestion.
- `chainstar wallet`: all token transfers to or from one address in a range, as a
  spreadsheet, in its own folder and warehouse. On Ethereum and Base the node filters
  by sender and recipient (cryo `--from-address` / `--to-address`), and only blocks with
  a matching transfer are fetched; `--blocks-per-request` for providers that accept
  ranged log filters.
- Docker image with cryo 0.3.2 bundled (`Dockerfile`), and a workflow publishing it to
  `ghcr.io/bit-primitive/chainstar` for amd64 and arm64 on pushes to `main` and version
  tags.
- Release workflow publishing to PyPI with trusted publishing on version tags.
- Friendlier errors: one `error:` line and a `hint:` line for common failures
  (rate limits, unreachable RPC, wrong chain, range past the head, Postgres password or
  connection, locked DuckDB file, unwritable folders, full disk, extension downloads);
  `--debug` shows the traceback, with RPC URLs redacted. Examples in every command's
  `--help`.

- `chainstar run`: extract, decode, load and model a block range in one command.
- `chainstar doctor`: checks cryo, the RPC endpoint and chain id, the target and dbt.
- `chainstar init`: writes a commented `chainstar.yml`.
- Chains: Ethereum and Base.
- Targets: DuckDB and Postgres.
- Extraction with cryo 0.3.2: blocks, transactions, ERC-20 transfers, and token
  metadata (name, symbol, decimals) for every token seen.
- dbt star schema: `fct_transfers`, `fct_fees`, `dim_wallets`, `dim_tokens`,
  `dim_contracts`, `dim_dates`, with key, not-null and relationship tests.
- Re-runnable loads: repeated or overlapping ranges do not duplicate rows, and
  Ethereum and Base can share one warehouse.
- Failed cryo runs are retried; completed chunks are kept.
- RPC URLs are redacted in all output and never written to disk by chainstar.
- Chains: Bitcoin and Solana, extracted by chainstar itself over JSON-RPC (cryo does not
  support them), into Parquet files with cryo's chunk layout and naming. Each request is
  tried up to three times, honours `--requests-per-second`, and asks for gzip.
  - Bitcoin: `getblock` verbosity 3 for input values and addresses; raw tables
    `bitcoin_blocks`, `bitcoin_transactions`, `bitcoin_inputs`, `bitcoin_outputs`.
  - Solana: `--blocks` counts slots; skipped slots are found with `getBlocks` and never
    requested; `getBlock` with `jsonParsed` and `maxSupportedTransactionVersion` 1; raw
    tables `solana_blocks`, `solana_transactions`, `solana_transfers`.
- `chainstar doctor` and `chainstar run` check the network of Bitcoin (`main`) and Solana
  (mainnet genesis hash) endpoints, and doctor no longer requires cryo for them.
  `chainstar run` refuses a Bitcoin or Solana range that ends past the node's head.
- Star schema across all four chains: Bitcoin outputs and Solana SPL and SOL transfers in
  `fct_transfers`; Bitcoin (inputs minus outputs) and Solana (`meta.fee`) fees in
  `fct_fees`, with new `fee_raw` and `fee_native` columns for every chain; SPL mints and
  native BTC and SOL in `dim_tokens`, with a new `token_standard` column. New
  `fct_transfers` columns: `transfer_position`, `input_address_count`,
  `from_token_account`, `to_token_account`, `script_type`; new `fct_fees` column
  `is_vote`. A dbt test checks every Bitcoin fee against the fee the node reports.

### Changed

- `chainstar run` without an RPC URL now uses the chain's public endpoint instead of
  stopping with an error.
- After loading, `chainstar run` says which blocks (Solana: slots) the warehouse holds
  for the chain, and notes when that includes earlier runs' chunks: each load takes every
  chunk saved in the chain's data directory, not only the range just extracted.
- `chainstar run` now refuses an Ethereum or Base range that ends past the chain head,
  as it already did for Bitcoin and Solana.
- RPC errors name the HTTP status (for example 429, rate limited).
- Package metadata: keywords, classifiers and project URLs (website, live demo, docs,
  changelog).
