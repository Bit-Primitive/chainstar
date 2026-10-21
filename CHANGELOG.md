# Changelog

All notable changes to chainstar are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **DEX swaps and USD prices**, from on-chain data only (no price service, nothing to
  sign up for); see the README's "Swaps and USD prices".
  - Ethereum and Base: Uniswap v2-style, Uniswap v3-style and Aerodrome `Swap` events,
    extracted with cryo's `logs` dataset and decoded exactly by chainstar (raw tables
    `chainstar_raw.dex_swaps`). Each pool's tokens and factory are read once with
    `eth_call` (`chainstar_raw.dex_pools`), and a pool counts as a venue's (Uniswap v2
    and v3, SushiSwap v2 and v3, Aerodrome, Aerodrome Slipstream) only when that factory
    returns the pool for the same tokens and fee. Other pools with the same events are
    kept as venue `unknown` and never priced.
  - Solana: one swap per successful transaction that calls Raydium (AMM v4, CLMM,
    CPMM), Orca Whirlpool, Meteora DLMM, Jupiter v6, Pump.fun or PumpSwap, derived from
    the signer's token and SOL balance changes (`chainstar_raw.solana_swaps`).
  - `fct_swaps`, `dim_pools` and `fct_token_prices`: hourly and daily USD prices per
    asset, with USD stablecoins (by address) at 1 USD, other assets from their swaps
    against stablecoins and the native asset (WETH, wrapped SOL), dust swaps dropped,
    outliers more than 10% from the median rejected, the lower median as the price, and
    observations, volume, sources and a confidence per row. Bitcoin is priced from
    wrapped BTC (WBTC, cbBTC) on Ethereum and Base in the same hour, labelled
    `cross_chain_wrapped_btc`, at most medium confidence. Low-confidence or missing
    prices are NULL.
  - `fct_transfers.amount_usd`, `fct_fees.fee_usd` and `fct_swaps.usd_value`, valued at
    the hourly price, with the price, method, confidence and hour beside each. The
    `transfers` export and `chainstar wallet` spreadsheets carry the same columns.
  - `chainstar wallet` on Ethereum and Base looks up each token's pools against WETH and
    USDC and fetches only those pools' swaps (filtered by address), so wallet histories
    get USD values without reading every swap.
  - All incremental under `sync`: each build recomputes prices from the start of the UTC
    day of the earliest new block and revalues the newest hour's transfers, fees and
    swaps; a rollback deletes swaps and prices too. Two syncs equal a full rebuild on
    DuckDB, Postgres, ClickHouse and Parquet lakes; the models run on the BigQuery
    emulator and compile for Snowflake.
  - dbt tests for the new tables (keys, relationships to `dim_pools` and `dim_dates`,
    accepted values, positive prices within their observed range), and two warnings:
    a stablecoin more than 2% off its peg, and ETH more than 2% apart on Ethereum and
    Base in the same hour.
  - dbt variables for the pricing thresholds (`price_min_swap_usd`,
    `price_outlier_band`, `price_high_*`, `price_medium_*`) and seeds `dex_venues` and
    `price_assets` with every factory and anchor and where each address was published.
- More targets. Each works with `run`, `sync` (progress, reorganisation rollbacks and
  incremental builds), `status` and `export`; see the README's Targets table for what was
  tested how.
  - **Parquet data lake**, `--target parquet:PATH` or `parquet:s3://BUCKET/PREFIX` (AWS S3,
    Cloudflare R2, MinIO and other S3-compatible storage, with `?endpoint=`). The star
    schema is built in a local DuckDB file (`?engine=`, default `lake-engine.duckdb`)
    exactly as for a DuckDB target, then published as Hive-partitioned Parquet: facts by
    `chain` and `date_day`, dimensions by `chain`, plus `chainstar_state/sync_progress`.
    Publishing rewrites only partitions whose rows changed (a manifest in
    `_chainstar/published.parquet` keeps each partition's row count and hash) and empties
    partitions that are gone. `status` and `export` read the lake itself, so they work
    without the engine file. S3 keys come from `AWS_ACCESS_KEY_ID` and
    `AWS_SECRET_ACCESS_KEY`, else the AWS credential chain.
  - **ClickHouse**, `--target clickhouse:http://USER@HOST:8123` (or `https://`), password
    in `CLICKHOUSE_PASSWORD`. Raw rows are loaded as Parquet (`INSERT ... FORMAT
    Parquet`); dbt-clickhouse builds the tables with `delete+insert` on lightweight
    deletes. ClickHouse has no multi-statement transactions, so a sync writes its
    progress last, as a new version of the row, and a sync that stopped half-way loads
    the same blocks again on the next run.
  - **MotherDuck**, `--target motherduck:DB` or `duckdb:md:DB`, token in
    `motherduck_token`.
  - **BigQuery**, `--target bigquery:PROJECT[?location=EU]`, with Application Default
    Credentials. Raw rows are loaded with Parquet load jobs; dbt-bigquery's `merge` on
    each table's key. Progress is versioned as on ClickHouse.
  - **Snowflake**, `--target snowflake:USER@ACCOUNT/DATABASE?warehouse=WH&role=ROLE`,
    password in `SNOWFLAKE_PASSWORD` or a key pair in `SNOWFLAKE_PRIVATE_KEY_PATH`
    (and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`), or `&authenticator=externalbrowser`. Raw
    rows go through the table stage (PUT, then COPY INTO from Parquet) inside the
    batch's transaction.
- Optional extras for the drivers: `chainstar[clickhouse]`, `chainstar[bigquery]`,
  `chainstar[snowflake]`, `chainstar[all]`. The base install is unchanged; the Docker
  image includes every extra, and DuckDB's `httpfs`, `aws` and `motherduck` extensions.
- `chainstar doctor` checks the target for each kind: ClickHouse and Snowflake versions, a
  lake's folder or bucket listing and its engine file, the MotherDuck token, and the dbt
  adapter the target needs, naming the extra to install when it is missing.
- A `publish` step in sync logs and in `run`'s summary for Parquet lakes.
- Passwords, tokens and keys of every target (from the URL or the environment) are
  removed from errors, `--debug` tracebacks, `doctor` and sync log warnings and errors.
- CI runs the ClickHouse, S3 and BigQuery-SQL tests against
  `clickhouse/clickhouse-server:25.8`, `adobe/s3mock:5.2.3` and
  `ghcr.io/goccy/bigquery-emulator:0.8.1`, and installs every extra.

### Changed

- The star-schema tables have new columns (USD values) and there are three new tables:
  after upgrading, run `chainstar sync --full-refresh` once (or `chainstar run`), since
  incremental builds refuse a changed schema. `chainstar run` now also extracts swaps
  for its range; chunks extracted before have no swaps until extracted again.
- `chainstar export --wallet` and `--token` filters are written as `col = ?` ORs, which
  ClickHouse accepts where it refused `? IN (col, ...)`.
- The dbt models are written for all seven targets: the SQL that differs (type names,
  Unix times, date parts and series, a quoted column) goes through dispatched macros, and
  qualified columns carry explicit names. On DuckDB and Postgres the tables, their
  columns and types are as before.
- `uv.lock` now resolves certifi to 2025.1.31: every dbt-snowflake release requires
  `certifi<2025.4.26`. chainstar's own RPC requests use the system's certificates.

- `chainstar sync`: keeps a warehouse up to date. Each run continues from where the
  last stopped, for one or more chains (`--chain`, repeatable; default `chains` in
  `chainstar.yml`, else the chains already synced), up to a safe head, then exits; with
  `--every 5m` it keeps running until Ctrl-C or SIGTERM, which stop at the next safe
  point: between batches, or (Ethereum, Base) between a batch's extraction and its
  token-metadata step, before its load. A second one stops at once; both are safe. A
  chain's first sync needs `--from BLOCK` or `--from-recent N`. Blocks are fetched in batches of ten chunks, each
  loaded in one transaction together with its progress. One sync per data folder at a
  time (a lock file).
- Safe head: a block is synced once it has enough confirmations. Defaults: Ethereum 12,
  Base 60, Bitcoin 6; Solana syncs finalized slots as before. `--confirmations N`, or
  `finalized` on Ethereum and Base for the node's finalized block; per chain in
  `chainstar.yml` (`confirmations:`).
- Reorganisations deeper than that: before each sync the last synced block's hash is
  checked against the chain. On a mismatch chainstar finds the newest stored block that
  still matches (checking up to 128), deletes everything after it (chunk files, raw
  rows, fact rows), syncs that range again and rebuilds the star schema in full, saying
  so in the log.
- Sync progress in the warehouse, `chainstar_state.sync_progress`: per chain the first
  and last synced block, the hash and time of the newest synced block, the confirmations
  used, how far the star schema is built, the last run and its error, and the chainstar
  version.
- `chainstar status`: per chain, synced to, head and safe head, lag in blocks and time,
  last run, star-schema state and rows per table; `--json`, and `--max-lag DURATION`
  exits 1 when any chain is further behind, for health checks.
- `chainstar wallet ADDRESS --sync`: keeps one address's history current in its own
  folder and warehouse, rewriting the spreadsheet after each run; `--every`, `--from`,
  `--confirmations` and `--json-logs` as for sync.
- Logs for sync: one line per stage (head, check, extract, decode, load, model, export,
  reorg, rollback) with block range, row counts and time; `--json-logs` writes each as
  a JSON object.
- `CHAINSTAR_RPC_URL_<CHAIN>` (for example `CHAINSTAR_RPC_URL_BASE`): an RPC URL per
  chain, used before `CHAINSTAR_RPC_URL`, so several chains can sync in one command.
- Examples for running sync as a service: `examples/cron/crontab`,
  `examples/systemd/chainstar-sync.service` and `.timer`, and
  `examples/docker-compose.yml` with Postgres and a health check.
- `dim_tokens.first_seen_block` and `last_seen_block`.

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

- The star-schema tables are dbt incremental models (`delete+insert` on their unique
  key, which dbt-duckdb and dbt-postgres both support). `chainstar sync` builds only rows
  above what each table already holds, per chain; `chainstar run`, `quickstart` and
  `wallet` (without `--sync`) still rebuild them in full every time, as before. An
  incremental build after several syncs gives the same rows as a full rebuild (tested on
  DuckDB and Postgres). `dim_dates` stays a table, now built from a one-row-per-chain
  summary (`chainstar_staging.int_chain_coverage`), and `fct_transfers` takes token
  decimals from `dim_tokens`. `dbt build` now reports 23 models.
- The `int_*` views are defined by macros the incremental models share;
  `int_bitcoin_senders` gains `block_number`, and `int_tokens` gains `first_seen_block`
  and `last_seen_block`.
- On Postgres, `chainstar sync` indexes the raw tables by chain and block, and the
  incremental tables are indexed by key and by chain and block, so syncs do not scan
  whole tables.
- `chainstar.yml` template: commented `chains` and `confirmations` settings for sync.
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
