# Changelog

All notable changes to chainstar are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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
