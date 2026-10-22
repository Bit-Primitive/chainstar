# chainstar

Raw blockchain data to clean dimensional tables in your own warehouse, from one command.

[Website](https://bitprimitive.com/projects/chainstar) ·
[Live demo](https://bitprimitive.com/projects/chainstar/demo) ·
[Source](https://github.com/Bit-Primitive/chainstar)

chainstar reads blocks, transactions, token transfers and DEX swaps from Ethereum, Base,
Bitcoin and Solana, and turns them into tidy tables you can open in a spreadsheet or query
with SQL, with USD values from prices it derives from on-chain swaps.
It runs on your own computer: nothing is sent anywhere except requests to the blockchain
node, and it needs no account or sign-up.

## Start here: your first data in 2 minutes

You need a terminal (Terminal on macOS, PowerShell on Windows) and one of two things:
**Docker** (easiest, works for every chain) or **uv** (no Docker; Ethereum and Base then
also need a tool called cryo).

**With Docker.** Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
and start it. Make an empty folder, open a terminal in it, and run:

```sh
docker run --rm -it -v "$PWD:/work" ghcr.io/bit-primitive/chainstar quickstart
```

On Windows PowerShell write `"${PWD}:/work"` instead of `"$PWD:/work"`. On Linux add
`--user "$(id -u):$(id -g)"` after `--rm` so the files belong to you.

**With uv.** Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```sh
uv tool install git+https://github.com/Bit-Primitive/chainstar
chainstar quickstart
```

(Once chainstar is on PyPI this becomes `uv tool install chainstar`.) Bitcoin and Solana
work straight away. Ethereum and Base also need cryo; if it is missing, quickstart says
how to get it, or use the Docker command above, which includes it.

**What quickstart does.** It asks which chain and how many of the latest blocks
(default: 20, or the single latest Bitcoin block), fetches them from a free public
endpoint, builds the tables in a file called `warehouse.duckdb` in your folder, and
prints what to do next. It takes about a minute on Solana and Bitcoin and a few minutes
on Ethereum and Base (most of it reading token names). To run it without questions, add
`--chain base --yes`.

**Open it in a spreadsheet.** Save the transfers as an Excel file and double-click it:

```sh
chainstar export transfers --output transfers.xlsx
```

(with Docker: `docker run --rm -v "$PWD:/work" ghcr.io/bit-primitive/chainstar export transfers --output transfers.xlsx`)

Each row is one transfer: when, which token (`token_symbol`), from whom, to whom, and how
much (`amount`), and what that was worth in US dollars (`amount_usd`, when the token has
an on-chain price that hour; see [Swaps and USD prices](#swaps-and-usd-prices)).
`amount_raw` is the exact amount in the token's smallest unit. Prefer
`.xlsx` over `.csv` for spreadsheets: a spreadsheet opening a CSV rounds long numbers
like `amount_raw`.

**Your own wallet's history.** All token transfers to or from one address, as a
spreadsheet:

```sh
chainstar wallet 0xYourAddress --chain base --recent 1000
```

That writes `base-0xyouraddress.xlsx`. On Ethereum and Base the node filters by address,
so this is quick; see [Wallet history](#wallet-history) for what it costs on each chain.

**Good to know.** The free public endpoints are shared by everyone and rate limited, so
chainstar asks them slowly (see [Public endpoints](#public-endpoints)). They are fine for
trying things out and for a few thousand blocks; for more, get an RPC URL from a provider
and pass it with `--rpc-url` or the `CHAINSTAR_RPC_URL` environment variable. If
something goes wrong, chainstar says what happened and what to try, in one or two lines.

The rest of this page is the full reference.

## How it works

chainstar pulls blocks, transactions and token transfers from an RPC endpoint (yours, or
a free public one), saves them as Parquet, loads them into your warehouse (DuckDB,
Postgres, MotherDuck, ClickHouse, BigQuery or Snowflake) or a Parquet data lake, and builds
a small star schema with dbt: facts for transfers, fees, swaps and USD prices, dimensions
for wallets, tokens, contracts, pools and dates. All chains share the same tables, so one
warehouse can hold Ethereum, Base, Bitcoin and Solana side by side.

```
extract  →  decode  →  load  →  model
 cryo        cryo      DuckDB    dbt        (Ethereum, Base)
 JSON-RPC    -         DuckDB    dbt        (Bitcoin, Solana)
```

- **extract**: on Ethereum and Base, [cryo](https://github.com/paradigmxyz/cryo) fetches
  blocks, transactions, ERC-20 `Transfer` events and DEX `Swap` events for your block
  range into Parquet files, decoding each `Transfer` log into token, sender, recipient
  and value; chainstar decodes the swaps. cryo does not support Bitcoin or Solana, so
  for those chainstar reads blocks over plain JSON-RPC itself and writes Parquet files in
  the same layout (see [Chains](#chains)), deriving Solana swaps from the transactions.
- **decode** (Ethereum and Base): for every pool seen, chainstar reads its tokens and
  factory and asks the factory to confirm it; for every token contract seen, cryo calls
  `name()`, `symbol()` and `decimals()`.
- **load**: the Parquet files are copied into raw tables (schema `chainstar_raw`).
- **model**: a bundled dbt project builds the star schema (schema `chainstar`) and
  tests it.

This is an early release (0.1, unreleased). It supports **Ethereum**, **Base**,
**Bitcoin** and **Solana**, and as targets **DuckDB**, **Postgres**, **MotherDuck**,
**ClickHouse**, **BigQuery**, **Snowflake** and a **Parquet data lake** on local disk or
S3-compatible storage; see [Targets](#targets) for how each was tested.

## Install

There are three ways; pick one.

**Docker image** (no Python or Rust needed, cryo included). Published to the GitHub
Container Registry by this repository's `Docker image` workflow, for amd64 and arm64
(Apple silicon and other 64-bit ARM machines):

```sh
docker run --rm -v "$PWD:/work" ghcr.io/bit-primitive/chainstar --help
```

`ghcr.io/bit-primitive/chainstar:latest` follows `main`; version tags such as `:0.1.0`
follow releases. To build the image yourself: `docker build -t chainstar .` in a
checkout (the cryo compile takes about 5 minutes).

How the container sees your files: the folder you mount at `/work` is chainstar's
working folder, so `warehouse.duckdb`, `data/` and exports appear in it, and paths you
pass (`--output mine.xlsx`, `--target duckdb:my.duckdb`) are relative to it. Pass your
own RPC URL with `-e CHAINSTAR_RPC_URL` (it is read from your environment, not written
into the command). Add `-it` for quickstart's questions; without a terminal it uses the
defaults, as with `--yes`. The image runs as an unprivileged user (uid 1000); on Linux,
`--user "$(id -u):$(id -g)"` makes the files yours. DuckDB's `postgres`, `excel`, `httpfs`,
`aws` and `motherduck` extensions and the ClickHouse driver are included, so DuckDB,
Postgres, MotherDuck, ClickHouse and the Parquet lake all work without downloads, in an
image of about 1 GB unpacked. The BigQuery and Snowflake drivers would nearly double that,
so they are left out of the published image; to write to those, build your own:
`docker build --build-arg EXTRAS="all" -t chainstar .` (or `EXTRAS="bigquery"`,
`EXTRAS="snowflake"`, or `EXTRAS=""` for no drivers).

**uv or pipx** (Python 3.11 or newer):

```sh
uv tool install git+https://github.com/Bit-Primitive/chainstar
# or: pipx install git+https://github.com/Bit-Primitive/chainstar
```

chainstar is not on PyPI yet; once it is, `uv tool install chainstar` or
`pipx install chainstar`.

DuckDB, Postgres, MotherDuck and Parquet lakes need nothing more. ClickHouse, BigQuery
and Snowflake need their drivers, as an extra: `clickhouse`, `bigquery`, `snowflake`, or
`all`:

```sh
uv tool install "chainstar[clickhouse] @ git+https://github.com/Bit-Primitive/chainstar"
```

The plug-ins for dlt and Airbyte have their own extras, `dlt` and `airbyte`, which `all`
does not include; see [Using chainstar from dlt and Airbyte](#using-chainstar-from-dlt-and-airbyte).

**cryo**, for Ethereum and Base outside Docker. It is written in Rust; get Rust from
https://rustup.rs, then:

```sh
cargo install cryo_cli
```

This installs cryo 0.3.2 from crates.io, the version chainstar is tested with. Bitcoin
and Solana need nothing else.

## Using chainstar

```sh
chainstar quickstart --chain ethereum --yes            # the latest 20 blocks
chainstar run --chain ethereum --blocks 20000000:20000100
```

`run` writes Parquet files to `./data/ethereum/` and the tables to `./warehouse.duckdb`.
Without `--rpc-url` or `CHAINSTAR_RPC_URL` both use the chain's free public endpoint;
with your own:

```sh
export CHAINSTAR_RPC_URL="https://your-ethereum-rpc.example"
chainstar doctor --chain ethereum
chainstar run --chain ethereum --blocks 20000000:20000100
```

Bitcoin and Solana work the same way:

```sh
chainstar run --chain bitcoin --blocks 972460:972463
chainstar run --chain solana --blocks 459564290:459564310   # slots, see below
```

Query the tables with the [DuckDB command-line tool](https://duckdb.org/docs/installation/):

```sh
duckdb warehouse.duckdb "
  select d.token_address, d.symbol, count(*) as transfers, sum(f.amount) as volume
  from chainstar.fct_transfers f
  join chainstar.dim_tokens d using (token_id)
  group by 1, 2 order by 3 desc limit 10"
```

Every command has `--help` with examples. Add `--debug` before the command
(`chainstar --debug run ...`) to see the full traceback when something fails.

### Commands

| Command | What it does |
| --- | --- |
| `chainstar quickstart` | Guided first run: asks for a chain and how many of the latest blocks, reads the range from the chain head, runs everything, and prints next steps. `--yes` for scripts. |
| `chainstar run` | Runs extract, decode, load and model for a block range, then prints row counts. |
| `chainstar sync` | Brings the warehouse up to date from where the last sync stopped, for one or more chains, up to the newest block with enough confirmations; detects and rolls back reorganisations; builds the star schema incrementally. Once, or `--every 5m` until stopped. See [Keeping a warehouse in sync](#keeping-a-warehouse-in-sync). |
| `chainstar status` | Per synced chain: synced to, head, lag in blocks and time, last run, rows per table. `--json`; `--max-lag 15m` exits 1 when a chain is further behind. |
| `chainstar export` | Saves a table, or the `transfers` view with token symbols, as CSV, Excel or Parquet, with optional filters. |
| `chainstar wallet` | All token transfers sent or received by one address in a range, as a spreadsheet. `--sync` keeps it current. |
| `chainstar doctor` | Checks that cryo is installed (Ethereum and Base only), the RPC serves the expected chain (EVM chain id, Bitcoin network `main`, or Solana mainnet's genesis hash), the target is reachable, and dbt is installed. |
| `chainstar init` | Writes a commented `chainstar.yml` you can edit. |

`chainstar run` options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--chain` | from `chainstar.yml` | `ethereum`, `base`, `bitcoin` or `solana` |
| `--blocks START:END` | from `chainstar.yml` | Block range. **END is exclusive**, as in cryo: `100:200` is 100 blocks. On Solana the numbers are slots. |
| `--rpc-url` | `$CHAINSTAR_RPC_URL`, then the chain's [public endpoint](#public-endpoints) | JSON-RPC endpoint for the chain |
| `--target` | `$CHAINSTAR_TARGET`, then `chainstar.yml`, then `duckdb:warehouse.duckdb` | `duckdb:PATH` or `postgres:postgresql://USER@HOST:PORT/DBNAME` |
| `--data-dir` | `./data` | Where Parquet files are kept |
| `--requests-per-second` | no limit; the public endpoint's rate when using one | Cap on requests sent to the RPC |
| `--chunk-size` | `100`; `10` on public endpoints | Blocks (Solana: slots) per Parquet file. Lower it if your RPC often fails requests (see below). |
| `--config` | `./chainstar.yml` | Config file, if you use one |

Command-line flags override environment variables, which override `chainstar.yml`.

`chainstar quickstart` options: `--chain` (default `base`), `--recent N` (default 20, or
1 on Bitcoin), `--rpc-url`, `--target` (default `duckdb:warehouse.duckdb`), `--data-dir`
(default `./data`) and `--yes`. On Ethereum and Base the range ends 5 blocks behind the
head, since `quickstart` and `run` do not handle reorganisations (`sync` does); on
Bitcoin it is the latest block, and on Solana the latest finalized slots.

### Re-running and adding ranges

Runs are safe to repeat. Parquet files already written are skipped, by cryo on Ethereum
and Base and by chainstar on Bitcoin and Solana, and each load replaces a chain's raw rows
with **everything in its data directory**, not only the range you just asked for. So you
can:

- re-run the same command after a failure; only the missing chunks are fetched again;
- run further ranges into the same warehouse; they add up;
- load all four chains into the same warehouse; every table has a `chain` column.

The extract lines count the range you asked for; the load line counts everything loaded,
and the line under it says which blocks the warehouse now holds. When that is more than
this run's range, chainstar says so:

```
extract  erc20_transfers  4,640 rows                            0.4s
...
load     chainstar_raw    4 tables, 18,844 rows                 0.1s
         ethereum now in the warehouse: blocks 26,240,971 to 26,240,990 (20 blocks)
         (each run loads every chunk saved in data/ethereum, so earlier runs' blocks are included)
```

To keep a range on its own, give it its own `--data-dir` and `--target`. `run` rebuilds
the star-schema tables in full each time; `chainstar sync` builds them incrementally.

### Public endpoints

When no RPC URL is set, chainstar uses these free shared endpoints and prints a one-line
notice saying so. They are run by [PublicNode](https://www.publicnode.com/), are shared
by everyone, and limit request rates, so chainstar asks them gently:

| Chain | Endpoint | Requests per second | Blocks per chunk |
| --- | --- | --- | --- |
| Ethereum | `https://ethereum-rpc.publicnode.com` | 5 | 10 |
| Base | `https://base-rpc.publicnode.com` | 5 | 10 |
| Bitcoin | `https://bitcoin-rpc.publicnode.com` | 2 | 10 |
| Solana | `https://solana-rpc.publicnode.com` | 5 | 10 (slots) |

All four ran `chainstar quickstart` in October 2026 without a refused request: 20
Ethereum blocks in 4.8 minutes, 20 Base blocks in 3.4 minutes (most of both reading
names and symbols of 300 to 400 tokens, three calls each), 20 Solana slots in 26 seconds,
and one Bitcoin block in 6 seconds. In earlier runs the Ethereum endpoint refused a few
percent of requests at 5 per second with 100-block chunks, and none with 10-block chunks.
`--requests-per-second` and `--chunk-size` override these, and your own `--rpc-url` or
`CHAINSTAR_RPC_URL` replaces them (with no rate cap unless you set one). Public
endpoints can change their limits or go away at any time; if they refuse requests,
chainstar's hint says to wait and run the same command again, which resumes from the
chunks already saved.

### Exporting

`chainstar export [TABLE]` saves a table to a file. `TABLE` is `transfers` (the default)
or any star-schema table: `fct_transfers`, `fct_fees`, `fct_swaps`, `fct_token_prices`,
`dim_wallets`, `dim_tokens`, `dim_contracts`, `dim_pools`, `dim_dates`.

```sh
chainstar export                                         # transfers.csv
chainstar export transfers --output transfers.xlsx
chainstar export transfers --chain base --wallet 0xYourAddress --output mine.xlsx
chainstar export transfers --token 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 --limit 1000
chainstar export fct_fees --format parquet --target postgres:postgresql://me@localhost:5432/chain
```

| Option | Meaning |
| --- | --- |
| `--output`, `-o` | File to write. Default `<table>.<format>` in the current folder. |
| `--format` | `csv`, `xlsx` or `parquet`. Default: from `--output`'s extension, else `csv`. |
| `--chain` | Only this chain's rows. |
| `--wallet ADDRESS` | Only rows where the address sends or receives (transfers and fees: `from_address` or `to_address`; swaps: `trader`, `sender` or `recipient`; `dim_wallets`: `address`). Ethereum and Base addresses match in any letter case. |
| `--token ADDRESS` | Only this token: ERC-20 contract, SPL mint, or `native` for BTC and SOL (swaps and pools: either side). |
| `--limit N` | At most N rows. |
| `--target`, `--config` | Which warehouse, as for `run`. |

`transfers` is `fct_transfers` joined to `dim_tokens`, ordered by chain, block and
position: `block_timestamp`, `chain`, `block_number`, `transaction_hash`,
`transfer_position`, `token_address`, `token_symbol`, `token_name`, `from_address`,
`to_address`, `amount`, `amount_raw`, and for bookkeeping `amount_usd`, `usd_price` (the
token's on-chain USD price used), `price_method` (how it was derived),
`price_confidence` and `price_bucket_start` (the UTC hour it is the price of), so each
USD value can be traced to `fct_token_prices`. A transfer without a usable price has
these empty.

In every format `amount_raw` stays text, so 256-bit token amounts keep every digit, and
`amount` is a number. Excel files are written by DuckDB's
[`excel` extension](https://duckdb.org/docs/stable/core_extensions/excel), which DuckDB
downloads the first time (the Docker image includes it), so chainstar needs no extra
Python package. An Excel sheet holds at most 1,048,576 rows including the header; a
larger export is refused before anything is written, with a suggestion to filter it or
use CSV or Parquet, which have no limit. Excel files store numbers as floating point, so
`block_number` and the like arrive as numbers with up to 15 significant digits, which
covers every block number and index.

On Bitcoin, `--wallet` matches outputs the address received, and outputs of transactions
the address funded alone (see `from_address` in [fct_transfers](#fct_transfers)); a
transaction funded by several addresses has no single sender.

### Wallet history

`chainstar wallet ADDRESS` builds a warehouse for one address and writes its transfers
to a spreadsheet:

```sh
chainstar wallet 0xYourAddress --chain base --recent 1000
chainstar wallet 0xYourAddress --chain ethereum --blocks 20000000:20010000 \
  --rpc-url "$MY_RPC" --blocks-per-request 1000
chainstar wallet YourSolanaAddress --chain solana --recent 100 --output mine.csv
```

Give either `--blocks START:END` or `--recent N`. The output defaults to
`<chain>-<address>.xlsx`; the data goes to `data/wallet-<chain>-<address>/`, with its own
`warehouse.duckdb`, so it never mixes with your main warehouse.

What it costs, honestly:

- **Ethereum and Base**: the node does the filtering. cryo 0.3.2 can filter ERC-20
  `Transfer` events by sender (`--from-address`, the event's topic1) or by recipient
  (`--to-address`, topic2); `eth_getLogs` combines topics with AND, so chainstar makes one
  filtered pass for each direction. Only matching events are downloaded, then only the
  blocks that contain one (for their timestamps), then the token names. Transactions,
  and therefore `fct_fees`, are skipped. With `--blocks-per-request 1` (the default) that
  is two requests per block: the free public endpoints refuse a filtered `eth_getLogs`
  over more than one block without a contract address. Many paid providers accept
  hundreds or thousands of blocks per request, which makes a long history quick; check
  your provider's `eth_getLogs` block-range limit. Checked on 20 Base blocks: the same 8
  transfers, byte for byte, as a full run filtered with `export --wallet`, in 18 seconds
  instead of 202 (before USD values were added). For USD values it also looks up each
  token's pools against WETH and USDC (10 calls per token on Ethereum, 14 on Base, once),
  fetches only those pools' swaps, one filtered request per chunk, and the blocks they are
  in; see [Swaps and USD prices](#swaps-and-usd-prices). Checked on 20 Base blocks on 21
  October 2026: 16 transfers of one address, every one valued (WETH at 2,489.92 USD from 7
  pricing pools, against 2,489.87 from all of the range's swaps), in 37 seconds.
- **Bitcoin and Solana**: their RPC interfaces cannot filter by address, so every block
  in the range is downloaded in full (a Bitcoin block is about 6 MB compressed, a Solana
  block 4 to 8 MB of JSON before compression) and the export keeps the address's rows. It costs the same as
  `chainstar run` on that range.

### When something goes wrong

chainstar prints one `error:` line saying what happened and, for common problems, a
`hint:` line saying what to try, instead of a traceback. For example:

```
error: getblockchaininfo: could not reach https://no-such-host.invalid/<redacted> (URLError)
hint:  check your internet connection and the RPC URL; a free public endpoint may also be down for a while, so try again later or use your own RPC URL
```

Covered: cryo missing (with install and Docker instructions), RPC unreachable or rate
limited (HTTP 429), the RPC serving another chain, a range past the chain head, Postgres
refusing the password or not running, a DuckDB file another program has open, folders
that cannot be written, a full disk, and DuckDB extensions that cannot be downloaded.
Anything else is reported as a likely bug, with a pointer to `--debug` and the issue
tracker. Ctrl-C stops cleanly; finished chunks are kept.

### Your RPC URL stays private

Hosted RPC URLs usually contain an API key, so chainstar:

- never writes the URL to `chainstar.yml` (it refuses to read one that contains it);
- reads one URL per chain from `CHAINSTAR_RPC_URL_<CHAIN>` (for example
  `CHAINSTAR_RPC_URL_BASE`) when set, before `CHAINSTAR_RPC_URL`;
- passes it to cryo through an environment variable, not the command line, and sends
  it nowhere but your endpoint for Bitcoin and Solana;
- turns off cryo's report files;
- prints only the scheme and host, e.g. `https://eth-mainnet.g.alchemy.com/<redacted>`,
  also in `--debug` tracebacks.

chainstar sends no telemetry, and turns off dbt's anonymous usage statistics.

## Keeping a warehouse in sync

`chainstar run` fetches a range you name. `chainstar sync` keeps a warehouse current:
each time it runs, it continues from where the last sync stopped, up to the newest block
that is safe to take, and exits; with `--every`, it keeps going until you stop it.

### Your wallet, kept current

```sh
chainstar wallet 0xYourAddress --chain base --recent 1000 --sync
```

The first run fetches the latest 1,000 blocks and writes `base-0xyouraddress.xlsx`, as
`chainstar wallet` always does. Run the same command again tomorrow or next month and it
fetches only the blocks since then, and rewrites the spreadsheet; `--recent` matters only
the first time. To have it update by itself while the terminal stays open, add
`--every 1h`; Ctrl-C stops it. To run it on a schedule, see
[Running it as a service](#running-it-as-a-service). Each wallet keeps its own folder and
warehouse, as before, and costs the same per block as
[`chainstar wallet`](#wallet-history).

### A warehouse, kept current

```sh
chainstar sync --chain base --from-recent 1000      # first sync: the latest 1,000 blocks
chainstar sync --chain base                         # later: continue where it stopped
chainstar sync --chain ethereum --chain base --every 5m
```

A chain's first sync into a warehouse needs a starting point: `--from BLOCK`, or
`--from-recent N` for the latest N blocks (Solana: slots). After that, every sync resumes
from the warehouse's own record of its progress, so the same command works every time;
`--from` and `--from-recent` are then ignored, with a note. Several chains are synced one
after the other, and the star schema is built once at the end. Without `--chain`, sync
takes the chains listed under `chains:` in `chainstar.yml`, or else every chain already
synced into the target.

Each chain needs an endpoint for that chain, so with several give each its own URL in
`CHAINSTAR_RPC_URL_<CHAIN>`, for example `CHAINSTAR_RPC_URL_BASE` and
`CHAINSTAR_RPC_URL_SOLANA`; chainstar uses it before `CHAINSTAR_RPC_URL`, and the public
endpoint when neither is set. `--rpc-url` works for a single chain.

A real first sync of Base on the public endpoint, 21 October 2026:

```
$ chainstar sync --chain base --from-recent 10
2026-10-21T22:55:38Z all      start    chainstar 0.1.0.dev0; target duckdb:warehouse.duckdb; data data
2026-10-21T22:55:38Z base     rpc      https://base-rpc.publicnode.com (free shared public endpoint)
2026-10-21T22:55:38Z base     note     no RPC URL set, so chainstar uses a free shared public endpoint (5 requests/s, fine for trying small ranges); for more, use your own with --rpc-url or CHAINSTAR_RPC_URL
2026-10-21T22:55:38Z base     head     chain id 8453, latest block 52,916,995; safe head 52,916,936 (60 confirmations)
2026-10-21T22:55:54Z base     extract  52,916,927-52,916,936: erc20_transfers 3,412, dex_swaps 250, blocks 10, transactions 1,856  15.2s
2026-10-21T23:00:03Z base     decode   52,916,927-52,916,936: pools 96, pools fetched 96, tokens 203, fetched 203  249.4s
2026-10-21T23:00:03Z base     load     52,916,927-52,916,936: 5,827 raw rows; synced to 52,916,936  0.1s
2026-10-21T23:00:09Z all      model    dbt build (full refresh: first sync or rollback of base): 30 models, 93 tests passed  5.3s
```

and, from the release before swaps, a few minutes after an earlier first sync, the same
command without `--from-recent` continuing from the next block (the decode step now also
reads each new pool, and the build has 30 models and 93 tests):

```
$ chainstar sync --chain base
2026-10-21T17:31:49Z all      start    chainstar 0.1.0.dev0; target duckdb:warehouse.duckdb; data data
2026-10-21T17:31:49Z base     rpc      https://base-rpc.publicnode.com (free shared public endpoint)
2026-10-21T17:31:49Z base     note     no RPC URL set, so chainstar uses a free shared public endpoint (5 requests/s, fine for trying small ranges); for more, use your own with --rpc-url or CHAINSTAR_RPC_URL
2026-10-21T17:31:49Z base     head     chain id 8453, latest block 52,907,281; safe head 52,907,222 (60 confirmations)
2026-10-21T17:31:50Z base     check    block 52,907,136 still has hash 0x2e6f0107...c0b1bc; continuing from 52,907,137
2026-10-21T17:37:07Z base     extract  52,907,137-52,907,222: erc20_transfers 26,352, blocks 86, transactions 18,783  70.0s
2026-10-21T17:37:07Z base     decode   52,907,137-52,907,222: tokens 586, fetched 411  247.2s
2026-10-21T17:37:07Z base     load     52,907,137-52,907,222: 45,807 raw rows; synced to 52,907,222  0.5s
2026-10-21T17:37:14Z all      model    dbt build (incremental): 23 models, 54 tests passed  6.6s
```

`chainstar sync` options, besides `--target`, `--data-dir`, `--requests-per-second`,
`--chunk-size` and `--config`, which work as for `run`:

| Option | Default | Meaning |
| --- | --- | --- |
| `--chain` | `chains` in `chainstar.yml`, else the chains already synced | Chain to sync; repeat for several |
| `--from BLOCK` | | First sync only: start here (one chain at a time) |
| `--from-recent N` | | First sync only: start N blocks before the safe head |
| `--every DURATION` | run once | Keep running, syncing every `30s`, `5m`, `2h` or `1d` |
| `--confirmations` | per chain, see below | Confirmations a block needs, or `finalized` (Ethereum, Base) |
| `--full-refresh` | off | Rebuild the star-schema tables in full instead of incrementally |
| `--json-logs` | off | One JSON object per log line |
| `--rpc-url` | `CHAINSTAR_RPC_URL_<CHAIN>`, `CHAINSTAR_RPC_URL`, the public endpoint | For a single chain |

What one sync does, per chain:

1. **Head.** Asks the node for its latest block and works out the safe head (below).
2. **Check.** Asks the chain for the hash of the last block it synced and compares it with
   the one it stored. If they differ, see [Reorganisations](#reorganisations).
3. **Extract and load**, in batches of ten chunks: 1,000 blocks with the default chunk
   size, 100 on public endpoints, whose chunks are 10. Each batch is loaded in **one
   transaction together with the progress it makes**, so a crash or a power cut never
   leaves the warehouse claiming blocks it does not hold, or holding raw rows its record
   does not cover. Only the batch's blocks are replaced in
   the raw tables; nothing else is reloaded.
4. **Model.** After all chains, dbt builds the star schema incrementally: each table
   processes only rows it does not have yet (see [Incremental tables](#incremental-tables)).

Stopping is safe at any point. The first Ctrl-C or SIGTERM (what `docker stop` and
`systemctl stop` send) stops at the next safe point: between batches, or on Ethereum and
Base between a batch's extraction and its token-metadata step, in which case that batch
is not loaded and the next sync loads it, reusing the chunk files already saved. The dbt
build is skipped too; the next sync does it. So stopping waits for at most one
extraction stage, which on a public endpoint can take a minute or two. A second Ctrl-C
or SIGTERM stops at once, which is also safe: chunk files appear only when complete, and
an unfinished transaction is rolled back.

### Can a sync keep up?

Only if your endpoint answers faster than the chain makes blocks. Measured on the free
public endpoints on 21 October 2026:

- **Solana** makes about 2.5 blocks a second. At the public endpoint's 5 requests per
  second, chainstar fetched 99 slots in 87.5 seconds, about 1.1 a second, so a sync
  `--every` few minutes falls further behind each time. To follow Solana's head, use your
  own RPC and raise `--requests-per-second`.
- **Base** makes one block every 2 seconds. On the public endpoint 86 blocks took 70
  seconds to extract, faster than the chain, plus 247 seconds reading names of 411 new
  tokens; that second part shrinks as tokens repeat, because metadata is fetched once.
  Swaps add the reading of each new pool, about 6 calls in batches of 10, also once per
  pool: the first 10 blocks above met 96 pools. The next 100 blocks met 357, and after a
  day of test runs from the same machine the public endpoint refused those batches (HTTP
  403) a few minutes in, on three tries over 20 minutes. Pools are saved in files of 50, so
  a try keeps what it read, and the blocks wait, unloaded, for the next sync. A Base sync
  with swaps wants your own RPC.
- **Ethereum** makes one block every 12 seconds, well within the public endpoint's pace.
- **Bitcoin** makes one block about every 10 minutes; one block takes seconds.

`chainstar status` shows the lag; `--max-lag` turns a growing lag into a failing health
check.

### How far behind the head: confirmations

A block's confirmations are the blocks built on top of it, plus one, so the newest block
has one. Sync takes a block only once it has enough, so the short reorganisations every
chain has never reach the warehouse:

| Chain | Default | Behind the head | Why |
| --- | --- | --- | --- |
| Ethereum | 12 | 11 blocks, about 2.2 minutes | Well past the short reorganisations late blocks cause; a margin, not a guarantee. `--confirmations finalized` waits for the node's finalized block instead, which cannot be reverted without a third of all stake being slashed: 92 blocks (about 18 minutes) behind on 21 October 2026. |
| Base | 60 | 59 blocks, about 2 minutes | Past the node's `safe` head, the newest block derived from data already posted to Ethereum, which was 38 blocks (76 seconds) behind on 21 October 2026. `finalized` (581 blocks, about 19 minutes behind that day) also waits for Ethereum to finalize that data. |
| Bitcoin | 6 | 5 blocks, about 50 minutes | The long-standing convention: five blocks of work on top. Bitcoin has no finality, so there is no `finalized`. |
| Solana | finalized | to the latest finalized slot | The latest finalized slot, which the cluster has rooted and cannot roll back; a number of confirmations does not apply. |

Set them per chain in `chainstar.yml`, or with `--confirmations` for every chain in a
command:

```yaml
confirmations:
  ethereum: finalized
  base: 120
```

### Reorganisations

A reorganisation deeper than the confirmation depth is rare but possible, so before each
sync chainstar checks that the last synced block still has the hash it had. If not, it:

1. compares the blocks it stored, newest first, with the chain until one still matches
   (blocks link to their parents by hash, so everything before a matching block is
   unchanged); it checks at most 128 and otherwise stops and asks you to look;
2. deletes everything after that block: its chunk files (whole files, so the rollback may
   start a few blocks earlier), its raw rows, and its rows in `fct_transfers` and
   `fct_fees`;
3. syncs that range again from the chain as it is now, and rebuilds the star schema in
   full so the dimensions are recounted.

The log says what happened; from the tests' simulated reorganisation:

```
ethereum reorg    warning: block 26,239,739 now has hash 0xabababab...ababab, not 0x5f3cfd2d...77e8c1 as synced: the chain reorganised deeper than 1 confirmation
ethereum rollback warning: rolled back from block 26,239,739: deleted 3 chunk files, 47 raw rows and 0 fact rows; syncing them again, then rebuilding the star schema in full
```

This is tested with a simulated reorganisation on DuckDB, Postgres, ClickHouse and
Parquet lakes (local and S3-compatible): afterwards the tables equal a full rebuild from
the new chain, and the block before the fork is
untouched. Files are deleted before the database, so if the process stops half-way the
hash check fails again next time and the rollback repeats.

### Incremental tables

The star-schema tables are dbt incremental models. Each keeps, per chain, the highest
block it already holds, and a sync builds only rows above that: new transfers and fees
are appended, and wallets, tokens and contracts seen again are updated (first seen kept,
last seen and counts moved on). Each table is updated by its unique key: with dbt's
`delete+insert` strategy on DuckDB, Postgres, MotherDuck, ClickHouse and Snowflake, and
with `merge` on BigQuery, whose adapter has no `delete+insert` (see [Targets](#targets)).
`dim_dates`, a few hundred
rows a year, is rebuilt each time from a one-row-per-chain summary
(`chainstar_staging.int_chain_coverage`) instead of from every block.

The tables are rebuilt in full instead whenever older blocks may have changed: on a
chain's first sync (a `run` may have loaded older blocks), after a rollback, with
`--full-refresh`, and on every `chainstar run`, which therefore behaves as before. An
incremental build after two syncs gives exactly the rows a full rebuild gives, on
DuckDB, Postgres, ClickHouse and Parquet lakes, for all four chains; the tests check it
table by table.

On Postgres, sync also indexes the raw tables by chain and block, and the incremental
tables by key and by chain and block, so a sync reads only its own blocks. DuckDB needs
no indexes: it skips data by its min/max statistics. On ClickHouse the raw tables are
ordered by chain and block, and on BigQuery clustered by them.

Two things a full rebuild would recompute but an incremental build leaves as they were:
an ERC-20 token's name, symbol or decimals fetched again later (only if you delete
`data/<chain>/erc20_metadata/`) reach older rows' `amount` only with `--full-refresh`;
the same holds for a Solana mint whose decimals were unknown in its first transfers.

### The progress record

Sync keeps its progress in the warehouse, in `chainstar_state.sync_progress`, one row
per chain, written in the same transaction as the data it describes (on ClickHouse and
BigQuery, which have no such transactions, after it, as a new version: see
[Targets](#targets)):

| Column | Meaning |
| --- | --- |
| `chain` | |
| `first_block` | where the first sync started |
| `last_block` | every block from `first_block` to here is loaded |
| `checkpoint_block`, `checkpoint_hash`, `checkpoint_time` | the newest loaded block with a known hash (Solana: the last slot that had a block), checked before each sync |
| `confirmations` | as used by the last sync |
| `modelled_block` | the star schema includes raw data up to here |
| `rebuild_pending` | the next build is a full rebuild (after a first sync or a rollback) |
| `updated_at` | when progress was last recorded |
| `last_run_at`, `last_run_status`, `last_error` | the last sync of the chain, `ok` or `error`, and why (RPC URLs redacted) |
| `chainstar_version` | the version that last recorded progress |

Times are UTC. Block numbers on Solana are slots.

### Checking on it: chainstar status

```
$ chainstar status --max-lag 10m
chainstar status: duckdb:warehouse.duckdb at 2026-10-21 17:37:16 UTC

base
  synced to    block 52,907,222, 2026-10-21 17:29:51 UTC (first synced block 52,907,127)
  head         52,907,444; safe head 52,907,385 (60 confirmations)
  lag          222 blocks, 7m 25s
  last run     2026-10-21 17:37:07 UTC, ok
  star schema  built to block 52,907,222
  rows         fct_transfers 32,861, fct_fees 20,916, dim_wallets 19,673, dim_tokens 586, dim_contracts 586

healthy: every chain is within 10m
```

`--json` prints the same as JSON for scripts. `--max-lag DURATION` makes it a health
check: it exits with status 1 when any chain's newest synced block is older than that.
The lag in time is measured from that block's own time, so it works without the node and
includes the confirmation depth: about 2 minutes on Base, an hour on Bitcoin. Allow for
that and for the sync interval, for example `--max-lag 15m` for Base synced every 5
minutes, and `--chain bitcoin --max-lag 3h`. The lag in blocks needs the node; when it
cannot be reached, status says so and still reports the rest.

### Running it as a service

Ready-to-use files are in [`examples/`](examples/):

- **cron** ([`examples/cron/crontab`](examples/cron/crontab)): `chainstar sync` every 5
  minutes. Runs never overlap: a sync refuses to start while another uses the same data
  folder.
- **systemd** ([`examples/systemd/`](examples/systemd/)): a service that runs one sync,
  and a timer that starts it 5 minutes after the previous run finished. Logs go to the
  journal (`journalctl -u chainstar-sync`).
- **Docker**, one container that keeps syncing:

  ```sh
  docker run -d --name chainstar-sync --restart unless-stopped --stop-timeout 180 \
    -v "$PWD:/work" -e CHAINSTAR_RPC_URL_BASE \
    ghcr.io/bit-primitive/chainstar sync --chain base --from-recent 100 --every 5m
  docker logs -f chainstar-sync
  docker exec chainstar-sync chainstar status
  ```

  `--stop-timeout` gives the current extraction stage time to finish on `docker stop`
  (the default is 10 seconds, after which Docker kills it, which is safe but wastes the
  stage's work).
- **Docker Compose with Postgres** ([`examples/docker-compose.yml`](examples/docker-compose.yml)):
  Postgres plus a sync container every 5 minutes, with `chainstar status --max-lag 15m`
  as its health check. `docker compose up -d` in that folder; put a password and RPC URLs
  in a `.env` file next to it.

Run the first sync by hand, or keep `--from-recent` in the command as the Docker examples
do: it applies only until the first sync has happened.

### Logs

One line per step, with the block range, row counts and time taken:

```
2026-10-21T17:30:53Z base     extract  52,907,127-52,907,136: erc20_transfers 6,509, blocks 10, transactions 2,133  9.1s
```

Steps: `head`, `check`, `extract`, `decode`, `load`, `model`, `publish` (Parquet lakes),
`export` (wallets), `reorg`, `rollback`, `wait`, `stop`. Warnings and errors say so after the step name. With
`--json-logs` each line is a JSON object with `time`, `level`, `chain`, `stage`,
`message`, `seconds` where timed, and the step's numbers (`first_block`, `last_block`,
`rows`, `head`, `safe_head`, ...), for log shippers. RPC URLs are redacted in both, and
warnings and errors are cleared of warehouse passwords, tokens and keys.

### Good to know

- **One sync per data folder.** A second one refuses to start. Do not point syncs with
  different data folders, or on different machines, at the same warehouse and chain.
- **`sync` and `run` on the same chain and warehouse** work together when they share a
  data folder: `run` reloads every chunk on disk and rebuilds in full. With different
  data folders, `run` would replace the rows sync recorded; keep them apart.
- **Errors do not stop the others.** A chain that fails (node down, rate limited) is
  logged and recorded in `last_error`; the other chains are synced, the exit status is 1,
  and with `--every` the next round tries again.
- **Wallets on Ethereum and Base** store blocks only where the wallet has transfers, so
  their checkpoint hash is asked of the node at the end of each batch rather than taken
  from the data.
- **dbt tests run on every build**, over whole tables, as with `run`; on a large
  warehouse they take a growing share of each sync.
- **A DuckDB file has one writer.** `chainstar status` waits up to 15 seconds while a sync
  step holds it; other tools that open the file can make a sync fail with a hint, as for
  `run`. For a warehouse others query while it syncs, use one of the server warehouses,
  or a Parquet lake, which readers query from its files.
- **Public endpoints** are fine for `--every 5m` on one or two chains. A Bitcoin block is
  about 6 MB to download from them, so a long Bitcoin catch-up wants your own node.

## Swaps and USD prices

chainstar reads DEX swaps on Ethereum, Base and Solana into `fct_swaps`, derives USD
prices from them into `fct_token_prices`, and values transfers (`amount_usd`), fees
(`fee_usd`) and swaps (`usd_value`) at those prices. Every price comes from on-chain
swaps: no price service is asked, nothing needs an account, and nothing is assumed
except that the USD stablecoins listed below are worth 1 USD. Where a token has no
reliable price, its USD columns are NULL rather than a guess.

### Venues

**Ethereum and Base.** cryo's `logs` dataset, filtered to three `Swap` events by
topic0, one `eth_getLogs` request per block as for transfers. chainstar decodes them
itself, exactly, because their amounts are 256-bit integers (signed in Uniswap v3):

| Event layout | Signature | topic0 | Source |
| --- | --- | --- | --- |
| `uniswap_v2` | `Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)` | `0xd78ad95f...159d822` | Uniswap/v2-core `IUniswapV2Pair.sol` |
| `uniswap_v3` | `Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)` | `0xc42079f9...fbcca67` | Uniswap/v3-core `IUniswapV3PoolEvents.sol`; aerodrome-finance/slipstream `ICLPoolEvents.sol` (identical) |
| `solidly_v2` | `Swap(address indexed sender, address indexed to, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out)` | `0xb3e27736...efffaf75b` | aerodrome-finance/contracts `IPool.sol` |

Any contract can emit these events, so for every pool chainstar reads `token0()`,
`token1()` and `factory()` (plus `fee()` and `tickSpacing()`, or `stable()`), then asks
that factory for the pool of the same tokens (`getPair`, `getPool(tokens, fee)`,
`getPool(tokens, tickSpacing)` or `getPool(tokens, stable)`). A pool is **verified** when
the factory answers with the pool's own address and the factory is one of these:

| Chain | Venue | Factory | Published in |
| --- | --- | --- | --- |
| Ethereum | `uniswap_v2` | `0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f` | Uniswap v2 deployments page |
| Ethereum | `uniswap_v3` | `0x1f98431c8ad98523631ae4a59f267346ea31f984` | Uniswap v3 Ethereum deployments page |
| Ethereum | `sushiswap_v2` | `0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac` | sushi-labs/sushi `sushiswap-v2.ts` |
| Ethereum | `sushiswap_v3` | `0xbaceb8ec6b9355dfc0269c18bac9d6e2bdc29c4f` | sushi-labs/sushi `sushiswap-v3.ts` |
| Base | `uniswap_v2` | `0x8909dc15e40173ff4699343b6eb8132c65e18ec6` | Uniswap v2 deployments page |
| Base | `uniswap_v3` | `0x33128a8fc17869897dce68ed026d694621f6fdfd` | Uniswap v3 Base deployments page |
| Base | `sushiswap_v2` | `0x71524b4f93c58fcbf659783284e38825f0622859` | sushi-labs/sushi `sushiswap-v2.ts` |
| Base | `sushiswap_v3` | `0xc35dadb65012ec5796536bd9864ed8773abc74c4` | sushi-labs/sushi `sushiswap-v3.ts` |
| Base | `aerodrome_v2` | `0x420dd381b31aef6683db6b902084cb0ffece40da` | aerodrome-finance/contracts README |
| Base | `aerodrome_slipstream` | `0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a`, `0xade65c38cd4849adba595a4323a8c7ddfe89716a`, `0xf8f2eb4940cfe7d13603dddd87f123820fc061ef` | aerodrome-finance/slipstream README (three deployments) |

Each factory was also checked on chain (it has code; the Uniswap and SushiSwap v3 ones
enable fee tiers 100, 500, 3000 and 10000), and every topic and function selector is
recomputed from its signature by the tests. Swaps in other pools with the same events
(forks such as BaseSwap, whose factory address could not be found in its own
documentation) are kept with venue `unknown` and `pool_verified` false; they are never
used for prices and get no `usd_value`. Pool metadata is read once per pool at the chain
head and cached in `data/<chain>/dex_pools/`, like token metadata: reading factories'
`PairCreated`/`PoolCreated` logs instead would need every block since each factory was
deployed. A swap whose pool does not answer `token0()` and `token1()` is left out of
`fct_swaps` (it stays in the raw table).

Not covered: Uniswap v4 (one contract for all pools, whose tokens are known only from
its `Initialize` events), PancakeSwap v3 (its `Swap` event has two more fields, so a
different topic0), Curve, Balancer, and aggregators' own events (their swaps show up as
the pool swaps they route through).

**Solana.** A swap through an aggregator crosses several pools and programs; what
matters is what the trader gave and got, so chainstar derives one swap per transaction
from balance changes. A successful transaction counts when it calls one of these
programs (each id from the program's own source or IDL, and checked on chain to be an
executable program):

| Venue | Program id | Source |
| --- | --- | --- |
| `raydium_amm_v4` | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | raydium-io/raydium-amm |
| `raydium_clmm` | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | raydium-io/raydium-clmm |
| `raydium_cpmm` | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | raydium-io/raydium-cp-swap |
| `orca_whirlpool` | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | orca-so/whirlpools |
| `meteora_dlmm` | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | MeteoraAg/dlmm-sdk IDL |
| `jupiter_v6` | `JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4` | jup-ag/jupiter-cpi |
| `pump_fun` | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | pump-fun/pump-public-docs IDL |
| `pump_swap` | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | pump-fun/pump-public-docs IDL |

For the first signer whose balances changed like a swap, token changes are summed over
the token accounts it owns (`preTokenBalances`/`postTokenBalances`), and SOL and wrapped
SOL count as one asset, `native`: its lamport change, plus the fee it paid, plus the
lamport changes of its token accounts, which nets out wrapping, unwrapping and the rent of
accounts it opens and closes. Exactly one token down and one up is a swap (SOL that
moved only for fees, tips or rent is ignored); one token against SOL moving the other way
is a swap with SOL; anything else (several tokens each way, an arbitrage that ends where
it started) is left out. Limitations: a SOL amount includes tips and rent paid to
accounts the signer does not own in the same transaction; programs not listed are not
read; a transaction gives at most one swap; Solana swaps have no pool, sender or
recipient, and `dim_pools` has no Solana rows.

### How prices are derived

For each chain, asset and UTC hour (and, separately, UTC day), in `fct_token_prices`:

1. **USD stablecoins are 1 USD** (`usd_stablecoin_peg`), identified by address or mint,
   never by symbol. `observed_price` shows what their swaps against other stablecoins
   said, and a dbt test warns when it strays more than 2% from 1.
2. **Swaps against a stablecoin** give one observation each: the stablecoin amount (its
   USD value) divided by the other asset's amount. ETH and SOL are priced this way from
   WETH's and wrapped SOL's swaps, as the asset `native`.
3. **Swaps against the native asset** price other tokens through the native asset's
   price in the same bucket, when that price is high or medium confidence. Nothing is
   priced through a third token: a token traded only against other tokens has no price.
4. **Outliers**: observations worth less than 10 USD are dropped (mostly rounding);
   those more than 10% from the bucket's median are rejected. The price is the median of
   the rest. Medians here are lower medians (for an even count, the lower middle value),
   always one of the observed prices, so a price is the same to the last digit on every
   warehouse and in incremental and full builds.
5. **Confidence**: `high` with at least 5 observations kept, 10,000 USD of volume and 2
   sources (pools, or Solana traders); `medium` with at least 3 and 1,000 USD; `low`
   otherwise, when `usd_price` is NULL and `observed_price` still shows the median. Every
   chain has a row for its native asset and its stablecoins in each bucket with blocks
   (`none` when there were no observations); other tokens only where they were traded.
6. **Bitcoin** has no on-chain DEX. Its price is that of wrapped BTC (WBTC and cbBTC) on
   Ethereum and Base in the same bucket (`cross_chain_wrapped_btc`), at most `medium`,
   and only when those chains' swaps for that hour are in the same warehouse. Wrapped
   BTC can trade slightly away from BTC; otherwise BTC values are NULL.

**Checked on real data**, `chainstar quickstart` of 20 blocks of Base and Ethereum and 20
Solana slots into one warehouse on 21 October 2026 (22:36 to 22:51 UTC): 360, 470 and
1,285 swaps (separately, each of the 84 swaps in the test fixtures' Ethereum and Base
blocks was checked against its pool's `Transfer` events, to the unit). ETH came out at
2,489.33 USD on Ethereum (97 observations, 6 pools, high) and 2,489.87 on Base (21, 7
pools, high), 0.02% apart; each WETH/stablecoin Uniswap v3 and Slipstream pool's own price
after its last swap (from `sqrtPriceX96`) was within 0.14% of the hour's price, and the
medians of pools with two or more swaps spread 0.15% (Ethereum, 4 pools) and 0.03% (Base,
3). SOL came out at 107.37 USD (medium: 3 swaps of at least 10 USD, through Jupiter,
Orca and Meteora, 0.9% apart; a fourth, at 50,319, was rejected). USD values were found
for 52% of Ethereum's transfers, 48% of Base's and 78% of Solana's, and for every fee.
Incremental builds over the same data, in two halves, equalled a full rebuild.

Only verified pools count on Ethereum and Base. The thresholds are dbt variables in
`dbt_project.yml` (`price_min_swap_usd`, `price_outlier_band`, `price_high_*`,
`price_medium_*`).

The anchors:

| Chain | USD stablecoins (1 USD) | Priced as the native asset | Prices Bitcoin |
| --- | --- | --- | --- |
| Ethereum | USDC `0xa0b86991...3606eb48`, USDT `0xdac17f95...831ec7`, DAI `0x6b175474...71d0f` | WETH `0xc02aaa39...756cc2` | WBTC `0x2260fac5...c2c599`, cbBTC `0xcbb7c000...eed33bf` |
| Base | USDC `0x833589fc...da02913`, USDbC `0xd9aaec86...710b6ca`, USDT `0xfde4c96c...2699bb2`, DAI `0x50c57259...17db0cb` | WETH `0x42000000...00000006` | cbBTC `0xcbb7c000...eed33bf` |
| Solana | USDC `EPjFWdd5...TDt1v`, USDT `Es9vMFrz...enwNYB` | wrapped SOL `So111...112` | |

(full addresses in the `price_assets` seed). They come from Circle's USDC addresses
page, Tether's supported-protocols page and the Uniswap Labs default token list; their
decimals were read on chain.

### What gets a USD value

- `fct_transfers.amount_usd`, `fct_fees.fee_usd` and `fct_swaps.usd_value` use the
  **hourly** price of the block's UTC hour, when its confidence is `peg`, `high` or
  `medium`, and are NULL otherwise. Beside each: the price (`usd_price`,
  `native_usd_price`), `price_method`, `price_confidence` and `price_bucket_start`, the
  hour the price is for, so every value can be checked against `fct_token_prices`.
- A swap is valued by its stablecoin side if it has one, else its native side, else
  `token_in`.
- The `transfers` export and `chainstar wallet` spreadsheets carry the same columns.

**Wallet histories.** `chainstar wallet` does not read every swap of the range. For each
token the wallet moved, it asks the Uniswap v2 and v3 factories (and Aerodrome's on Base)
for that token's pools against WETH and USDC, once per token (cached in
`data/<chain>/.pricing_pools.json`), and fetches only those pools' swaps, with
`eth_getLogs` filtered by address, which the public endpoints answer for a whole chunk
per request, and the blocks they are in. The lookup asks Uniswap v2, Uniswap v3 (fee
tiers 100, 500, 3000 and 10000) and, on Base, Aerodrome v2 (stable and volatile); it does
not find SushiSwap or Aerodrome Slipstream pools. Tokens without such a pool stay
unpriced, and a wallet's prices rest on fewer swaps than a full run's, so their
confidence is often lower. A Bitcoin wallet's warehouse has
no Ethereum or Base swaps, so its BTC values are NULL; run Bitcoin and Ethereum into one
warehouse to price them.

### Prices under sync

`fct_token_prices` is incremental like the other tables. A bucket's price is computed
from all its swaps, so new blocks recompute every bucket from the start of the UTC day of
the earliest new block, for every chain at once (Bitcoin's price depends on Ethereum's
and Base's swaps). Transfers, fees and swaps are revalued for the hour of their chain's
first new block (Bitcoin's: the earliest of Bitcoin's, Ethereum's and Base's), whose
price may have changed. The tests check that two syncs give exactly the rows of a full
rebuild, and a rollback deletes a chain's swaps and prices with its other facts.

After upgrading from a release without swaps, rebuild once with
`chainstar sync --full-refresh` (or any `chainstar run`): the tables have new columns.
Blocks extracted before the upgrade have no swaps until they are extracted again: a
`chainstar run` over such a range fetches its swaps and reuses the other chunk files.

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
values is about 15 MB of JSON (6 MB compressed) and a Solana block about 4 to 8 MB. These
publicnode endpoints are the defaults when no RPC URL is set (see
[Public endpoints](#public-endpoints)).

## Targets

`--target` (or `CHAINSTAR_TARGET`, or `target:` in `chainstar.yml`) says where the
tables go. Every target works with `run`, `sync` (its progress record, reorganisation
rollbacks and incremental builds), `status` and `export`. Passwords, tokens and keys never
go in the target or in `chainstar.yml`: each comes from an environment variable or the
warehouse's own standard mechanism, never appears in dbt's profile (dbt reads it from a
`DBT_ENV_SECRET_*` variable, which dbt keeps out of its logs), and chainstar removes it
from every error.

| Target | `--target` | Credentials | Raw rows loaded by | Incremental strategy | Tested how |
| --- | --- | --- | --- | --- | --- |
| DuckDB | `duckdb:PATH` | none | `INSERT ... SELECT` from the Parquet files | `delete+insert` | live: every end-to-end test |
| Postgres | `postgres:postgresql://USER@HOST:5432/DB` | `PGPASSWORD` | DuckDB's `postgres` extension | `delete+insert` | live: `postgres:16-alpine` in Docker |
| MotherDuck | `motherduck:DB` or `duckdb:md:DB` | `motherduck_token` | `INSERT ... SELECT` from the Parquet files, through DuckDB | `delete+insert` (dbt-duckdb) | unit only: URL, profile, load path with DuckDB standing in. **Not run against MotherDuck** (no token) |
| Parquet lake | `parquet:PATH`, `parquet:s3://BUCKET/PREFIX` | S3: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN`), else the AWS credential chain | built in a local DuckDB engine, published with `COPY ... TO` Parquet | `delete+insert` in the engine; publishing rewrites changed partitions | live: local disk; S3 API on `adobe/s3mock:5.2.3` in Docker (multipart uploads and listing checked). **Not run against AWS S3, R2 or MinIO** |
| ClickHouse | `clickhouse:http://USER@HOST:8123` (or `https://...:8443`) | `CLICKHOUSE_PASSWORD` | `INSERT ... FORMAT Parquet` over HTTP | `delete+insert` (dbt-clickhouse's `delete_insert`, lightweight deletes) | live: `clickhouse/clickhouse-server:25.8` in Docker |
| BigQuery | `bigquery:PROJECT` (`?location=EU`) | Application Default Credentials | load jobs, source format Parquet | `merge` on the unique key | unit, and the models' SQL on the BigQuery emulator. **Not run against BigQuery**; dbt-bigquery and load jobs not run at all |
| Snowflake | `snowflake:USER@ACCOUNT/DB?warehouse=WH&role=ROLE` | `SNOWFLAKE_PASSWORD`, or a key pair in `SNOWFLAKE_PRIVATE_KEY_PATH` (and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`), or `&authenticator=externalbrowser` | PUT to the table's stage, then `COPY INTO ... MATCH_BY_COLUMN_NAME` from Parquet | `delete+insert` | unit only: statements and profile, and the models compiled and parsed as Snowflake SQL. **Not run against Snowflake** |

ClickHouse, BigQuery and Snowflake need their extra (`chainstar[clickhouse]`,
`chainstar[bigquery]`, `chainstar[snowflake]`; see [Install](#install)); the Docker image
has them all. `chainstar doctor --target ...` checks the target without changing it: it
connects (ClickHouse and Snowflake report their versions), lists a lake's files, checks
the MotherDuck token, and names the extra to install when the dbt adapter is missing.

What "tested" means above: *live* is the full suite (load, build and its 93 dbt tests,
two syncs equal to a full rebuild, a reorganisation rolled back, exports) against the
real thing in a throwaway container, and on ClickHouse the star schema equals DuckDB's
row for row and type for type. *Unit* checks what chainstar sends: the SQL, its
parameters and their types, the Parquet files given to the bulk loader, and the dbt
profile. For BigQuery and Snowflake the bundled dbt project is also compiled offline,
for a first build and an incremental one, and every model and test must parse as that
warehouse's SQL. On the BigQuery emulator (`ghcr.io/goccy/bigquery-emulator:0.8.1`),
which uses BigQuery's own SQL front end, the 30 models build from the compiled SQL and
the star schema equals DuckDB's; the emulator cannot run dbt-bigquery (its `create
schema` job fails) or Parquet load jobs (it crashes), so neither was tested, and a bug in
it (`SELECT x, x AS y ... UNION ALL SELECT 2, 3` returns 2 for `y`) blanks the Bitcoin
and Solana fees, which that test leaves out.

Every target gets the same schemas: `chainstar_raw` (the extracted columns, as loaded:
cryo's for Ethereum and Base, and `bitcoin_*` and `solana_*` tables), `chainstar_staging`
(dbt views and the `chains` seed) and `chainstar` (the star schema). `chainstar sync` adds
a fourth, `chainstar_state`, for its [progress record](#the-progress-record). ClickHouse
calls them databases and BigQuery datasets.

**DuckDB**: `--target duckdb:path/to/file.duckdb`. The file is created if it does not
exist. Avoid naming it `chainstar.duckdb`: DuckDB names the database after the file, so
`chainstar.fct_transfers` would become ambiguous and you would have to write
`chainstar.chainstar.fct_transfers`.

**Postgres**: `--target postgres:postgresql://user@host:5432/dbname`. Put the password in
the `PGPASSWORD` environment variable rather than the URL, so it stays out of your shell
history. chainstar loads raw tables through DuckDB's `postgres` extension, which DuckDB
downloads from its extension repository the first time it is used. The database user
needs permission to create schemas.

**MotherDuck**: `--target motherduck:my_db` (or `duckdb:md:my_db`), with your token in
`motherduck_token`. It runs exactly as a DuckDB file does, through DuckDB's `md:` paths
and its `motherduck` extension, which DuckDB downloads the first time (the Docker image
includes it). Untested against MotherDuck itself (no token was available), so these are
open: whether the database must exist first, and whether MotherDuck's transactions cover
a sync batch as DuckDB's do.

**Parquet data lake**: `--target parquet:./lake` on local disk, or
`parquet:s3://bucket/prefix` on S3-compatible storage. The star schema and the progress
record are written in the Hive layout that DuckDB, Spark, Athena and Trino read:

```
lake/chainstar/fct_transfers/chain=base/date_day=2026-10-21/data_0.parquet
lake/chainstar/fct_fees/chain=base/date_day=2026-10-21/data_0.parquet
lake/chainstar/fct_swaps/chain=base/date_day=2026-10-21/data_0.parquet   (and fct_token_prices)
lake/chainstar/dim_wallets/chain=base/data_0.parquet        (and dim_tokens, dim_contracts, dim_pools)
lake/chainstar/dim_dates/data_0.parquet
lake/chainstar_state/sync_progress/data_0.parquet
lake/_chainstar/published.parquet                           (what was published)
```

The partition columns are in the folder names only, as those engines expect, so read
the files with Hive partitioning, for example in DuckDB
`select * from read_parquet('lake/chainstar/fct_transfers/**/*.parquet', hive_partitioning = true)`.

How it is built: in a local DuckDB file, the engine (`lake-engine.duckdb` in the current
folder; choose another with `?engine=PATH`), exactly as a DuckDB target, then published
to the lake after each successful build. dbt-duckdb can write Parquet directly (its
`external` materialization), but it rewrites each table in full every time and cannot
build incrementally; building in the engine keeps incremental builds, the transactional
progress record and reorganisation rollbacks the same as on DuckDB, and the lake only
ever receives tables that were built and tested. Publishing is incremental too: each
partition's row count and a hash of its rows are kept in `_chainstar/published.parquet`,
and only partitions whose rows changed are rewritten. A partition that no longer exists
(after a rollback, for example) is deleted on local disk; on object storage, which DuckDB
writes but cannot delete from, its file is replaced by an empty one, which readers see as
no rows. The manifest is written last, so a publish that stops half-way is completed by
the next one. Each file is replaced in one upload, but a reader that lists the lake while
a publish runs can see some partitions new and others not yet. Keep the engine file with
the data folder: it holds the raw tables and the progress that later syncs continue
from. `status` and `export` read the lake itself, so they work on any machine that can
reach it.

For S3, set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN` for
temporary keys); without them chainstar uses the AWS credential chain (shared config and
credentials files, SSO, instance and container roles). The region comes from `?region=`,
`AWS_REGION` or `AWS_DEFAULT_REGION`. For other S3-compatible storage name its endpoint;
`http://` turns TLS off, and requests use path-style URLs unless `url_style=vhost`:

```sh
--target "parquet:s3://my-bucket/chainstar?region=eu-west-1"                                   # AWS S3
--target "parquet:s3://my-bucket/chainstar?endpoint=https://ACCOUNT_ID.r2.cloudflarestorage.com&region=auto"   # Cloudflare R2
--target "parquet:s3://my-bucket/chainstar?endpoint=http://127.0.0.1:9000&region=us-east-1"    # MinIO
```

**ClickHouse**: `--target clickhouse:http://user@host:8123`, or `https://` for TLS (port
8443 by default), with the password in `CLICKHOUSE_PASSWORD`. chainstar's schemas become
ClickHouse databases, so the user needs to create databases and tables. Raw tables are
`MergeTree` tables ordered by chain and block, with nullable columns. dbt-clickhouse
builds the star schema with `delete+insert` (its `delete_insert`), which deletes with
lightweight `DELETE`, so the user must be allowed to set
`allow_nondeterministic_mutations`; dbt sessions also set `join_use_nulls` and
`cast_keep_nullable`, which the models need. ClickHouse has no transaction spanning
several statements, so a sync batch is not atomic there: the raw rows are replaced first
and the progress row is written last, as a new version of the row (the progress table has
a `_version` column, and readers take the newest). If a sync stops between the two, the
next one finds the old progress and loads the same blocks again, replacing them rather
than adding them twice (the tests simulate this). A rollback deletes raw and fact rows
before it records the new progress, so if it stops half-way the hash check fails again
and it repeats.

**BigQuery**: `--target bigquery:my-project`, optionally `?location=EU` for where new
datasets are made. Credentials are Google's Application Default Credentials: the key file
named by `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or
the metadata server on Google Cloud. chainstar's schemas become datasets of the project;
the identity needs to create datasets and run load and query jobs. Raw rows go up as
Parquet load jobs and the raw tables are clustered by chain and block; the star schema
uses dbt-bigquery's `merge` on each table's key, which replaces the same rows
`delete+insert` would. BigQuery's multi-statement transactions need sessions, which
chainstar does not use, so progress is versioned as on ClickHouse. Every sync runs DML and
load jobs, which BigQuery bills or counts against quotas. Query results for `export`
arrive as Arrow; whole-number `BIGNUMERIC` columns come back as 38-digit decimals.

**Snowflake**: `--target "snowflake:user@account/DATABASE?warehouse=WH&role=ROLE"`. The
account is your account identifier (for example `myorg-myaccount`). The password comes
from `SNOWFLAKE_PASSWORD`; for key-pair authentication set `SNOWFLAKE_PRIVATE_KEY_PATH`
(and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` if the key is encrypted) instead; for single
sign-on add `&authenticator=externalbrowser`. The database must exist; chainstar's
schemas are created in it, in upper case, as Snowflake stores unquoted names. Raw rows are
uploaded to each table's stage with PUT and loaded with `COPY INTO ... FILE_FORMAT =
(TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, each file under a new name and
with `FORCE = TRUE`, so a repeated load is never skipped as already loaded, and inside the
batch's transaction together with its progress, as on DuckDB and Postgres.

## Using chainstar from dlt and Airbyte

If your team already moves data with [dlt](https://dlthub.com) or
[Airbyte](https://airbyte.com), chainstar plugs into them as a source. The tables they
deliver are the finished star schema: the same tested, priced tables `chainstar sync`
builds, not raw blocks to model yourself. That is chainstar's value: decoding, pool
verification, on-chain USD prices, incremental builds and reorganisation handling. Raw
records would leave all of that to you.

```sh
pip install "chainstar[dlt] @ git+https://github.com/Bit-Primitive/chainstar"       # dlt 1.31
pip install "chainstar[airbyte] @ git+https://github.com/Bit-Primitive/chainstar"   # airbyte-cdk 7.38
```

### How it works

Each run does two things:

1. **Sync.** One `chainstar sync` pass into a *working folder*: Parquet chunks in
   `data/` and a DuckDB working warehouse, `warehouse.duckdb`. Everything in
   [Keeping a warehouse in sync](#keeping-a-warehouse-in-sync) applies: confirmations,
   reorganisation rollbacks, incremental builds, the progress record. Keep the folder
   between runs; it holds the progress and the token and pool metadata fetched once.
2. **Emit what changed.** Each table is compared, by key and a hash of the whole row,
   with what this destination was last given (kept in the working warehouse, schema
   `chainstar_emit`, per destination). New and changed rows are emitted, and so are
   deletes for keys that left the star schema. All of them carry a new
   `_chainstar_generation`, the cursor.

Watching only new blocks would not be enough. An incremental build also rewrites older
rows: the newest hour's USD values, prices from the start of the UTC day, wallets and
tokens seen again. A rollback deletes rows and rebuilds everything. Comparing rows catches
all of it. After every run the destination's tables equal the working warehouse's, row for
row. The comparison reads every table each run, which DuckDB does at millions of rows a
second; on a warehouse of hundreds of millions of rows it becomes the slow part.

**The cursor lives in the framework's state.** dlt keeps it in the pipeline state and
Airbyte in its state messages, and both move it only after a load commits. A run passes
the destination's generation in, changes are computed against that generation, and an
uncommitted run is simply repeated. When the state is empty (a first load, or a full
refresh), everything is emitted.

**Reorganisations.** Blocks are taken only with enough confirmations (Ethereum 12, Base 60,
Bitcoin 6, Solana finalized), so the short reorganisations every chain has never reach the
warehouse. A deeper one is rolled back as described in
[Reorganisations](#reorganisations). The next emission then deletes the rows that are gone
from the destination (dlt: `_chainstar_deleted`, a `hard_delete` column; Airbyte:
`_ab_cdc_deleted_at`) and upserts the rebuilt ones. This is tested with a simulated
reorganisation for both.

The emitted tables, keys and cursor:

| Table | Primary key |
| --- | --- |
| `fct_transfers` | `transfer_id` |
| `fct_fees` | `fee_id` |
| `fct_swaps` | `swap_id` |
| `fct_token_prices` | `price_id` |
| `dim_wallets` | `wallet_id` |
| `dim_tokens` | `token_id` |
| `dim_contracts` | `contract_id` |
| `dim_pools` | `pool_id` |
| `dim_dates` | `date_day` |

Every table has the columns of [The star schema](#the-star-schema), plus:
- `_chainstar_generation` (bigint): the cursor, increasing per table;
- `_chainstar_deleted` (bool): true on deletes;
- on Airbyte, `_ab_cdc_deleted_at`.

`amount_raw` and the other `*_raw` amounts stay text.

### dlt

```python
import dlt
from chainstar.integrations.dlt import chainstar_source


def source():
    return chainstar_source(
        chains=["base", "solana"],
        working_dir="chainstar-work",  # keep it between runs
        from_recent=100,  # a chain's first sync: the latest 100 blocks
        # rpc_urls={"base": "https://..."},  # else CHAINSTAR_RPC_URL_<CHAIN>, then public endpoints
        # tables=["fct_transfers", "dim_tokens"],
    )


# DuckDB
pipeline = dlt.pipeline("chainstar", destination="duckdb", dataset_name="chainstar")
print(pipeline.run(source()))

# Postgres (dlt reads the credentials from .dlt/secrets.toml or the environment, e.g.
# DESTINATION__POSTGRES__CREDENTIALS=postgresql://user:password@localhost:5432/db)
pipeline = dlt.pipeline("chainstar_pg", destination="postgres", dataset_name="chainstar")
print(pipeline.run(source()))
```

Run the same script again, for example from cron, and only what changed is loaded. Each
table is one dlt resource with:
- `write_disposition="merge"` on its primary key;
- column hints for every column, with the `DECIMAL(38,0)` wei amounts as exact decimals;
- a `dlt.sources.incremental` cursor on `_chainstar_generation`, kept in dlt's state.

Arguments:

| Argument | Meaning |
| --- | --- |
| `chains` | `ethereum`, `base`, `bitcoin`, `solana` |
| `working_dir` | default `chainstar-work` |
| `from_block` / `from_recent` | a chain's first sync |
| `rpc_urls` | per chain |
| `tables` | which tables |
| `confirmations` | per chain: a number or `"finalized"` |
| `requests_per_second`, `chunk_size` | as for `chainstar sync` |
| `sync=False` | emit from the working folder without syncing, for a folder another process keeps current, while that process is not running |

dlt can also read the arguments from its own configuration, for example
`[sources.chainstar] rpc_urls = {base = "..."}` in `.dlt/secrets.toml`.

- **Full reload:** `pipeline.run(source, refresh="drop_resources")` resets the cursor and
  reloads everything. Do not use `write_disposition="replace"` without it, since only
  changes would be loaded.
- **Several pipelines** can share one working folder. Each pipeline and dataset gets its
  own record of what it was given, and the second to run finds the sync already done.
- **Telemetry:** importing `chainstar.integrations.dlt` sets
  `RUNTIME__DLTHUB_TELEMETRY=false` unless you set it yourself, so dlt's anonymous
  telemetry is off.

### Airbyte

[`airbyte/source-chainstar/`](airbyte/source-chainstar/) holds the connector. It is
built with Airbyte's Python CDK, packaged as a Docker image on Airbyte's
`python-connector-base`, and comes with its tests. Every stream offers
`Incremental | Append + Deduped`, using the primary keys above and the cursor
`_chainstar_generation`, and `Full refresh`. Exact `DECIMAL` amounts are sent as strings,
because Airbyte's number types lose digits in some destinations. Its README covers the
configuration, building the image, and adding it to a self-managed Airbyte.

**One caveat to know first:** Airbyte runs every sync in a fresh container, so the working
folder must be on storage that outlives it. On self-managed Airbyte that is the optional
`/local` volume; on Airbyte Cloud there is none, so Cloud is not supported.

```sh
docker build -f airbyte/source-chainstar/Dockerfile -t source-chainstar:dev .
docker run --rm source-chainstar:dev spec
```

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
| `amount_usd` | `amount × usd_price`; NULL when the token has no `peg`, `high` or `medium` price for the block's UTC hour (see [Swaps and USD prices](#swaps-and-usd-prices)) |
| `usd_price`, `price_method`, `price_confidence`, `price_bucket_start` | the hourly price used, from `fct_token_prices`, and the start of its hour |

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
| `fee_usd` | `fee_native` at the native asset's hourly USD price; NULL when unpriced |
| `native_usd_price`, `price_method`, `price_confidence`, `price_bucket_start` | the hourly price used, and the start of its hour |

### fct_swaps

One row per DEX swap (see [Swaps and USD prices](#swaps-and-usd-prices) for the venues):
on Ethereum and Base a decoded `Swap` event, on Solana what one signer gave and got in
a transaction that called a known DEX program.

| Column | Notes |
| --- | --- |
| `swap_id` | `chain:transaction_hash:swap_position` |
| `chain`, `block_number`, `block_timestamp`, `date_day` | |
| `transaction_hash`, `transaction_index` | |
| `swap_position`, `log_index` | the log index (EVM); `0` on Solana, one swap per transaction |
| `venue` | EVM: the pool's factory's venue (`uniswap_v2`, `uniswap_v3`, `sushiswap_v2`, `sushiswap_v3`, `aerodrome_v2`, `aerodrome_slipstream`) or `unknown`; Solana: the known programs called, joined with `+` |
| `swap_method` | the event layout (`uniswap_v2`, `uniswap_v3`, `solidly_v2`), or `balance_changes` on Solana |
| `pool_id`, `pool_address`, `pool_verified` | EVM only; joins to `dim_pools`. Only verified pools are priced and valued |
| `trader` | EVM: the transaction's sender (NULL in a wallet history); Solana: the signer |
| `sender`, `recipient` | EVM only: the event's fields, usually a router and the receiver |
| `token_in_id`, `token_out_id`, `token_in_address`, `token_out_address` | what the trader paid and received; `native` for SOL and wrapped SOL on Solana |
| `amount_in_raw`, `amount_out_raw` | exact amounts in the tokens' smallest units, as text |
| `amount_in`, `amount_out` | divided by 10^decimals; NULL when decimals are unknown |
| `usd_value`, `usd_value_side` | the value at the hourly price of one side (`in` or `out`): a stablecoin side first, then the native asset, then `token_in` |
| `price_method`, `price_confidence`, `price_bucket_start` | of the price used |

On EVM, `trader` is whoever sent the transaction, often a bot or a relayer, and one
transaction through a router can hold several swaps (one per pool crossed).

### fct_token_prices

One row per chain, asset, granularity (`hour` or `day`) and UTC bucket; see
[How prices are derived](#how-prices-are-derived).

| Column | Notes |
| --- | --- |
| `price_id` | `chain:token_address:granularity:bucket_start_unix` |
| `chain`, `token_address`, `token_id` | `token_address` is `native` for ETH, BTC and SOL, which WETH and wrapped SOL are priced as |
| `granularity`, `bucket_start`, `bucket_start_unix`, `date_day` | the UTC hour or day |
| `usd_price` | what chainstar values things at: 1 for stablecoins; the observed price at `high` or `medium` confidence; otherwise NULL |
| `observed_price` | the lower median of the observations kept (for stablecoins: from swaps against other stablecoins) |
| `price_low`, `price_high` | lowest and highest observation kept, the spread within the bucket |
| `price_method` | `usd_stablecoin_peg`, `swaps_vs_stablecoins`, `swaps_vs_native`, `swaps_vs_stablecoins_and_native` or `cross_chain_wrapped_btc` |
| `confidence` | `peg`, `high`, `medium`, `low` or `none` |
| `observations`, `rejected_observations` | swaps kept, and those rejected as more than 10% from the median |
| `volume_usd` | USD volume of the swaps kept, rounded to cents |
| `sources` | distinct pools (EVM) or traders (Solana) among them |
| `last_block_number` | the chain's newest block in the bucket |

### dim_pools

Every Ethereum and Base pool that emitted a swap event, as its contract and factory
described it when read (once, at the chain head).

`pool_id`, `chain`, `pool_address`, `venue`, `event_family`, `factory_address`,
`factory_confirms`, `is_verified` (the factory confirms the pool and is a known venue),
`token0_address`, `token1_address`, `token0_id`, `token1_id`, `fee` (Uniswap v3-style:
hundredths of a basis point, so 500 is 0.05%), `tick_spacing`, `is_stable` (Aerodrome),
`metadata_block_number`, `first_seen_block`, `last_seen_block`, `swap_events`

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
`metadata_block_number`, `has_metadata`, `first_seen_block`, `last_seen_block` (the first
and last block in the loaded data where the asset moved)

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
token and wallet dimensions, every EVM swap joining to `dim_pools`, every ERC-20 token
joining to `dim_contracts`, accepted values for chains, granularities, methods and
confidences, and every USD price and value positive with each price between its lowest
and highest observation. Where a column does not apply to a chain (a Bitcoin output with
no single sender, for example), its not-null test skips that chain. Two checks warn
instead of failing, because the chain can be right and they still fire: a stablecoin
whose swaps against other stablecoins put it more than 2% from 1 USD in an hour (at least
3 swaps), and ETH priced more than 2% apart on Ethereum and Base in the same hour (both
high confidence).

## Limitations of this release

- **Chains**: Ethereum, Base, Bitcoin mainnet and Solana mainnet-beta.
- **Targets not run against the real service**: MotherDuck, BigQuery and Snowflake were
  built from their documentation and adapter source and checked as the
  [Targets](#targets) table says, not against an account. ClickHouse was tested on a
  single 25.8 server, not on ClickHouse Cloud or a cluster (chainstar sets no
  `ON CLUSTER` or replicated engines); S3 lakes on s3mock, not on AWS S3, R2 or MinIO.
- **ClickHouse and BigQuery batches are not atomic**: a sync that stops between loading
  a batch and recording it loads the batch again next time (see [Targets](#targets)).
- **Events**: ERC-20 transfers and the swap events of
  [Swaps and USD prices](#swaps-and-usd-prices) only on Ethereum and Base. NFT transfers
  are not extracted: cryo's ERC-20 dataset skips ERC-721 `Transfer` events (they have an
  extra indexed topic), and ERC-1155 uses different events.
- **USD prices** come only from the swaps in the warehouse, so a short range prices only
  what traded in it, and a token traded only against other tokens is unpriced. Prices are
  per UTC hour; values use the hourly price only. Stablecoins are taken at 1 USD (a test
  warns if their swaps disagree); Bitcoin is priced from wrapped BTC on Ethereum and Base
  and only when those are in the same warehouse. Uniswap v4, PancakeSwap v3, Curve and
  Balancer are not read. Solana swaps are derived from balance changes, one per
  transaction, with the limits described there.
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
- **Full rebuilds with `run`**: `chainstar run` rebuilds every star-schema table each
  time, so very large warehouses get slower to update that way; `chainstar sync` builds
  them incrementally (see [Incremental tables](#incremental-tables)).
- **Block reorganisations** are handled by `chainstar sync` (see
  [Reorganisations](#reorganisations)), not by `run`, `quickstart` or `wallet` without
  `--sync`: with those, stay a few blocks behind the chain head (Solana reads only
  finalized slots, which cannot be rolled back).
- **Your RPC provider sets the limits.** Free public endpoints often refuse some
  requests, cap request rates, or answer only for recent blocks (the default publicnode
  endpoints are asked at 2 to 5 requests per second, see
  [Public endpoints](#public-endpoints)). cryo fetches ERC-20
  transfers with one request per block, and one refused request fails its whole chunk.
  chainstar retries a failed extraction (Bitcoin and Solana: each failed request), making
  up to three attempts and keeping completed chunks; if a run still fails, run the same command
  again, and on a flaky endpoint use a smaller `--chunk-size` (for example 10) so each
  retry makes progress.

## Roadmap

Not built yet, in rough order:

- more DEX venues (Uniswap v4, Curve, Balancer, PancakeSwap v3)
- more EVM chains
- SPL mints and burns, and Solana token names from on-chain metadata
- BigQuery and Snowflake verified against real accounts, and MotherDuck with a token
- Homebrew formula

## Development

```sh
uv sync --all-extras     # every warehouse driver, so every test can run
uv run pytest            # unit and end-to-end tests; no RPC or cryo needed
uv run ruff check . && uv run ruff format --check .
docker build -t chainstar .   # the image; the cryo compile takes about 5 minutes
```

The tests use small Parquet files cut from real cryo output (`tests/fixtures/`), and real
Bitcoin and Solana RPC responses sliced to a few transactions (`tests/rpc_fixtures/`),
from which the Bitcoin and Solana Parquet fixtures are produced by chainstar's own
extractors. The end-to-end tests load all four chains into one warehouse. Some need a
server, and run only when it is named; give each a throwaway one, as CI does:

| Variable | Example | Runs |
| --- | --- | --- |
| `CHAINSTAR_TEST_POSTGRES_DSN` | `postgresql://chainstar:chainstar@127.0.0.1:5432/chainstar_test` (`postgres:16-alpine`) | Postgres end to end |
| `CHAINSTAR_TEST_CLICKHOUSE_URL` | `http://chainstar:chainstar@127.0.0.1:8123` (`clickhouse/clickhouse-server:25.8`) | ClickHouse end to end |
| `CHAINSTAR_TEST_S3_ENDPOINT`, `CHAINSTAR_TEST_S3_BUCKET` | `http://127.0.0.1:9090`, `chainstar-test` (`adobe/s3mock:5.2.3`) | Parquet lakes on S3 |
| `CHAINSTAR_TEST_BIGQUERY_EMULATOR` | `http://127.0.0.1:9050` (`ghcr.io/goccy/bigquery-emulator:0.8.1`, port published as the same number) | BigQuery SQL on the emulator |

Each test file's docstring has the `docker run` line. The Excel export test downloads
DuckDB's `excel` extension the first time and is skipped when it cannot.

## Releasing

Two workflows publish chainstar; neither needs a stored secret.

- **Docker image** (`.github/workflows/docker.yml`): on every push to `main` and every
  `v*` tag, builds `ghcr.io/bit-primitive/chainstar` for amd64 and arm64 on native
  runners (`ubuntu-24.04` and `ubuntu-24.04-arm`) and pushes it with the workflow's own
  `GITHUB_TOKEN` (`packages: write`). Tags: `latest` and `main` from `main`; `X.Y.Z` and
  `X.Y` from a `vX.Y.Z` tag. One-time step after the first run: GitHub creates new
  container packages as private, so open the package's settings
  (github.com/orgs/Bit-Primitive/packages, then `chainstar`, then Package settings,
  Danger Zone, Change visibility) and make it public, so `docker run` works without
  logging in. GitHub notes this cannot be undone.
- **PyPI** (`.github/workflows/release.yml`): on a `v*` tag, checks that the tag matches
  the version in `pyproject.toml`, builds the sdist and wheel with `uv build`, and
  publishes them with [trusted publishing](https://docs.pypi.org/trusted-publishers/)
  (`pypa/gh-action-pypi-publish`, `id-token: write`): no API token is stored anywhere.

One-time PyPI setup, before the first release:

1. On pypi.org, sign in to the account that should own the project, open
   *Your account → Publishing*, and add a **pending publisher** for GitHub Actions:
   PyPI project name `chainstar`, owner `Bit-Primitive`, repository `chainstar`, workflow
   `release.yml`, environment `pypi`. A pending publisher does not reserve the name; the
   first successful release does (the name was free in October 2026).
2. In the GitHub repository, *Settings → Environments → New environment*, named `pypi`.
   Optionally add yourself as a required reviewer, so each release waits for approval.

To release: set `version` in `pyproject.toml` (for example `0.1.0`), move the
`Unreleased` notes in `CHANGELOG.md` under that version, commit, then
`git tag v0.1.0 && git push origin v0.1.0`.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
