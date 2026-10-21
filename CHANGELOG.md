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
