# source-chainstar: chainstar as an Airbyte source

An Airbyte source connector that delivers chainstar's star schema (transfers, fees, swaps
and on-chain USD prices, with their wallet, token, contract, pool and date dimensions) for
Ethereum, Base, Bitcoin and Solana to any Airbyte destination, and keeps it current.

It is built with Airbyte's Python CDK (`airbyte-cdk` 7.38). Airbyte's Connector Builder and
low-code framework are for HTTP APIs; this connector runs chainstar's own pipeline (cryo,
JSON-RPC extractors, DuckDB and dbt), so it needs the Python CDK. The code lives in
chainstar itself, `chainstar.integrations.airbyte`; this folder holds the image and
Airbyte's test files.

Not published to Airbyte's registry or a container registry. Build the image yourself
(below) or push it where your Airbyte can pull it.

## What it emits

One stream per star-schema table: `fct_transfers`, `fct_fees`, `fct_swaps`,
`fct_token_prices`, `dim_wallets`, `dim_tokens`, `dim_contracts`, `dim_pools`, `dim_dates`.
Columns are as in [the main README](../../README.md#the-star-schema), plus:

| Column | Meaning |
| --- | --- |
| `_chainstar_generation` | the stream's change number when the row was emitted; the incremental cursor |
| `_chainstar_deleted` | true on a record that deletes its key |
| `_ab_cdc_deleted_at` | set on those records: Airbyte's convention for a delete |

| Stream | Primary key | Cursor |
| --- | --- | --- |
| `fct_transfers` | `transfer_id` | `_chainstar_generation` |
| `fct_fees` | `fee_id` | `_chainstar_generation` |
| `fct_swaps` | `swap_id` | `_chainstar_generation` |
| `fct_token_prices` | `price_id` | `_chainstar_generation` |
| `dim_wallets` | `wallet_id` | `_chainstar_generation` |
| `dim_tokens` | `token_id` | `_chainstar_generation` |
| `dim_contracts` | `contract_id` | `_chainstar_generation` |
| `dim_pools` | `pool_id` | `_chainstar_generation` |
| `dim_dates` | `date_day` | `_chainstar_generation` |

Each sync first runs one `chainstar sync` pass into a working warehouse (see
[Working directory](#working-directory)). It then emits, per stream, what changed since
the generation in the stream's state:

- **Incremental | Append + Deduped** (use this): new and changed rows, and a delete
  record for every key that left the star schema. Rows below the newest block also change:
  the newest hour's USD values, wallets and tokens seen again, and everything after a
  reorganisation rollback. The destination keeps the newest record per key by
  `_chainstar_generation` and drops keys whose newest record has `_ab_cdc_deleted_at`.
  Airbyte's Postgres, BigQuery and Snowflake destinations delete only when the column is
  in the stream's schema (it is) and their CDC deletion mode is hard delete (their
  default). Afterwards the destination's tables equal chainstar's, row for row.
- **Incremental | Append**: every change is appended, deletes included. Filter on
  `_ab_cdc_deleted_at`, or deduplicate yourself.
- **Full refresh | Overwrite**: every row of the table, with `_chainstar_generation` 0.

State is per stream (`{"_chainstar_generation": N}`). It is emitted only after a stream's
last record, never part-way, so a generation is either delivered whole or sent again. If
a sync fails after records but before Airbyte commits its state, the next sync starts from
the old generation and sends the same changes again.

**Exact amounts.** `amount_raw`, `amount_in_raw` and `amount_out_raw` are text, as
everywhere in chainstar. The DECIMAL columns of `fct_fees` (`fee_raw`,
`execution_fee_wei`, `effective_gas_price_wei`, `base_fee_per_gas_wei`) are sent as
strings too. Airbyte's `integer` is 64-bit in most destinations; Postgres nulls larger
values, and BigQuery's INT64 overflows. `number` becomes a float in Snowflake by default,
and BigQuery's NUMERIC keeps only 29 integer digits. Text is the only type that keeps every
digit in every destination. Cast it there, for example `fee_raw::numeric` in Postgres.

**Reorganisations.** chainstar takes a block only once it has enough confirmations
(Ethereum 12, Base 60, Bitcoin 6; Solana finalized slots), so the usual short
reorganisations never reach the warehouse. A deeper one is detected, rolled back and
re-synced (see the main README's [Reorganisations](../../README.md#reorganisations)). The
next emission then carries deletes for rows that are gone, and the rebuilt rows as
upserts.

## Configuration

| Field | Required | Meaning |
| --- | --- | --- |
| `chains` | yes | `ethereum`, `base`, `bitcoin`, `solana` (one or more) |
| `working_directory` | yes | chainstar's working folder; must outlive the container (below) |
| `from_recent` | no, default 100 | a chain's first sync starts this many blocks (Solana: slots) before its safe head |
| `from_block` | no | instead, start here; one chain only |
| `rpc_urls` | no | per chain, secret; without one the chain's free public endpoint is used (rate limited) |
| `confirmations` | no | per chain: a number, or `finalized` on Ethereum and Base |
| `streams` | no | which tables to offer; default all nine |
| `requests_per_second`, `chunk_size` | no | as for `chainstar sync` |

```json
{
  "chains": ["base", "solana"],
  "working_directory": "/local/chainstar",
  "from_recent": 100,
  "rpc_urls": {"base": "https://base-mainnet.example/v2/KEY"},
  "confirmations": {"base": "finalized"}
}
```

RPC URLs are marked `airbyte_secret`. chainstar prints only their scheme and host and
never writes them to disk.

`check` validates the options and confirms the working directory can be written. It also
checks that cryo is present for Ethereum and Base, and that each chain's RPC serves the
expected chain (one or two calls per chain).

## Working directory

chainstar keeps its progress, its raw tables and the token and pool metadata it fetched
(once per token) in the working directory: `data/` and `warehouse.duckdb`. It also keeps
there what each destination was last given (schema `chainstar_emit`). **That folder must
survive between syncs.** Airbyte starts each sync in a fresh container whose own folders
(`/config`, `/source`) are emptied every time. Without a lasting folder, every sync would
start again from `from_recent` and re-read every token's metadata.

- **Self-managed Airbyte (Kubernetes, Helm):** turn on the local volume, the platform's
  `JOB_KUBE_LOCAL_VOLUME_ENABLED` setting (`kube.localVolume.enabled` in the v2 Helm
  chart's configuration; off by default). Sync containers, sources included, then mount
  the persistent volume claim `airbyte-local-pvc` at `/local`, which must exist; abctl was
  not found to create it. Use `"working_directory": "/local/chainstar"`, one folder per
  connection. This was read in Airbyte's platform source (`VolumeFactory.kt`), not tried
  on a running Airbyte.
- **Running the image yourself** (scripts, Dagster, Airflow, cron): mount a volume, as
  below.
- **Airbyte Cloud:** no persistent storage for sources was found, so it is not supported.

The working directory is an ordinary chainstar folder. You can seed it with a long first
sync from the command line, which avoids one very long Airbyte sync:

```sh
chainstar sync --chain base --from-recent 100000 \
  --data-dir /local/chainstar/data --target duckdb:/local/chainstar/warehouse.duckdb
```

## Build and run the image

From the repository root:

```sh
docker build -f airbyte/source-chainstar/Dockerfile -t source-chainstar:dev .
docker run --rm source-chainstar:dev spec
mkdir -p work && cp airbyte/source-chainstar/integration_tests/sample_config.json work/config.json
# point working_directory at /work in work/config.json, then:
docker run --rm -v "$PWD/work:/work" --user "$(id -u):$(id -g)" source-chainstar:dev check --config /work/config.json
docker run --rm -v "$PWD/work:/work" --user "$(id -u):$(id -g)" source-chainstar:dev discover --config /work/config.json
docker run --rm -v "$PWD/work:/work" --user "$(id -u):$(id -g)" \
  -v "$PWD/airbyte/source-chainstar/integration_tests:/tests:ro" source-chainstar:dev \
  read --config /work/config.json --catalog /tests/configured_catalog.json
```

The image is Airbyte's `python-connector-base` 4.1.1 (Python 3.13, pinned by digest).
cryo 0.3.2 is compiled in a build stage, and chainstar with the `airbyte` extra is pinned
by `uv.lock`. The entrypoint is `python -m chainstar.integrations.airbyte`, and
`AIRBYTE_ENTRYPOINT` holds the same command, as the platform requires. The image runs as
the user `airbyte` (uid 1000).

To use it in self-managed Airbyte, push the image to a registry your Airbyte can pull
from, or on abctl run `kind load docker-image source-chainstar:dev -n airbyte-abctl`. Then
go to *Workspace settings → Sources → New connector → Add a new Docker connector* and give
the image name and tag.

Without Docker, in an environment with `chainstar[airbyte]` installed:

```sh
python main.py spec
python main.py read --config config.json --catalog integration_tests/configured_catalog.json --state state.json
```

## Tests

- `tests/test_airbyte_source.py` in the repository runs `spec`, `check`, `discover` and
  `read` through the connector's entrypoint against chainstar's fixture chains:
  - two incremental reads, then a read of an uncommitted state;
  - a read with nothing new;
  - a simulated reorganisation, which must arrive as deletes;
  - a full refresh;
  - a failed sync, which must emit no record and no state.

  Each read is applied the way a deduped destination would apply it, and the result must
  equal chainstar's tables.
- Airbyte's standard tests (`airbyte-cdk connector test`, the CDK's replacement for the
  connector acceptance tests) use `acceptance-test-config.yml` and `integration_tests/`.
  They read one Bitcoin block from the public endpoint. Run the in-process tests from
  this folder, with it on the Python path so `source_chainstar` imports. Run the
  image-level ones against the image built above: the CDK's own image build expects a
  Poetry project in the folder, which this connector is not.

  ```sh
  cd airbyte/source-chainstar
  PYTHONPATH=$PWD uv run --project ../.. --all-extras airbyte-cdk connector test --pytest-arg=-k --pytest-arg="not docker"
  PYTHONPATH=$PWD uv run --project ../.. --all-extras airbyte-cdk image test --image source-chainstar:dev
  ```

  On 22 October 2026 (airbyte-cdk 7.38.0): 6 passed in-process (spec, check of a valid
  and an invalid config, discover, basic read, read with a bad catalog) and 4 passed
  against the image (spec, check of both configs, discover). The rest were skipped by
  the suite itself: scenarios whose config is not listed for that command.

## Telemetry

None. chainstar sends nothing, the image turns off dbt's anonymous usage statistics, and
`airbyte-cdk` itself makes no analytics calls.
