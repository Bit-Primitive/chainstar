"""The change feed behind chainstar's dlt and Airbyte plug-ins.

What the plug-ins emit: the finished star-schema tables, the same tested, priced
rows `chainstar sync` builds, not raw extracted records. Each run:

1. **Syncs a working warehouse.** chainstar's own sync, unchanged, into a working
   folder: `data/` (the Parquet chunks, token and pool metadata) and
   `warehouse.duckdb` (raw tables, sync progress, the star schema). Blocks are
   taken only once they have enough confirmations, a reorganisation deeper than
   that is rolled back, and the star schema is built incrementally (see sync.py).
   The folder must persist between runs: it holds the progress later syncs
   continue from, and metadata fetched once per token and pool.
2. **Emits what changed, per table.** Block watermarks alone would miss changes:
   an incremental build also rewrites older rows (the newest hour's USD values,
   prices from the start of the UTC day, wallets and tokens seen again), and a
   rollback deletes rows and rebuilds everything. So each table is compared, by
   key and a hash of the whole row, with what the destination was last given,
   kept in the working warehouse (`chainstar_emit`, per consumer: one working
   folder can feed several destinations). New and changed rows are
   emitted as upserts and keys that disappeared as deletes, all marked with a new
   generation number, the stream's cursor. After every run the destination's
   tables equal the working warehouse's, row for row, deletes included.

Generations and the framework's state. Which generation the destination holds is
the framework's to say, because only it knows whether a load was committed: dlt
keeps the cursor in its pipeline state, Airbyte in the state messages it persists
after the records before them. A run passes in that committed generation C:

- the ledger keeps C and drops anything newer (a run whose load never committed)
  or older; changes are computed against C and recorded as C + 1;
- when the framework says 0 (a first load, or a full refresh), everything is
  emitted;
- when the framework names a generation the ledger does not have (the working
  folder was replaced), every row is emitted again, which merge makes safe, with
  a warning: rows deleted meanwhile cannot be found and need a full refresh.

Rows are compared in full each run, which DuckDB does at millions of rows a
second; a warehouse of hundreds of millions of rows makes this the slow part.
"""

import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from chainstar.chains import Chain, get_chain
from chainstar.config import ConfigError, resolve_connection, rpc_secrets
from chainstar.heads import confirmations_for
from chainstar.model import MART_SCHEMA
from chainstar.sync import ChainPlan, StopFlag, Syncer, SyncPlan, rpc_sources
from chainstar.synclog import SyncLog
from chainstar.targets import DuckDBTarget

from .streams import DELETED_COLUMN, GENERATION_COLUMN, Stream

WAREHOUSE_FILE = "warehouse.duckdb"
DATA_FOLDER = "data"
EMIT_SCHEMA = "chainstar_emit"
# One row per key and hash per stream and generation: what the destination was given.
LEDGER_TABLE = "emitted"
# One row per stream and generation, so a generation that emptied a table is known too.
GENERATIONS_TABLE = "generations"
BATCH_ROWS = 10_000
# Who the ledger is kept for. Each destination fed from one working folder needs
# its own: the dlt source uses its pipeline and dataset, the Airbyte source "airbyte".
DEFAULT_CONSUMER = "default"

logger = logging.getLogger("chainstar")


class FeedError(RuntimeError):
    pass


@dataclass(frozen=True)
class FeedSettings:
    """What to sync. Mirrors `chainstar sync`'s options."""

    chains: tuple[str, ...]
    working_dir: Path
    # Where a chain's first sync starts: a block (one chain only), or the latest N.
    from_block: int | None = None
    from_recent: int | None = None
    # Per chain; else CHAINSTAR_RPC_URL_<CHAIN>, CHAINSTAR_RPC_URL, the public endpoint.
    rpc_urls: Mapping[str, str] = field(default_factory=dict)
    # Per chain: a number or `finalized`; else chainstar's defaults.
    confirmations: Mapping[str, Any] = field(default_factory=dict)
    requests_per_second: int | None = None
    chunk_size: int | None = None


class Workspace:
    """The working folder: chainstar's data folder and its DuckDB working warehouse."""

    def __init__(self, working_dir: str | Path) -> None:
        self.path = Path(working_dir).expanduser()
        self.data_dir = self.path / DATA_FOLDER
        self.target = DuckDBTarget(self.path / WAREHOUSE_FILE)

    # Syncing ---------------------------------------------------------------------

    def sync(self, settings: FeedSettings, write: Callable[[str], None] = logger.info) -> None:
        """Bring the working warehouse up to date, as one `chainstar sync` pass would.

        Raises FeedError when a chain or the build failed: the star schema is then
        not emitted, and the next run starts from where this one got to.
        """
        plan = self.plan(settings)
        secrets = [*settings.rpc_urls.values(), *rpc_secrets(None, {})]
        log = _CollectingLog(write, secrets=secrets)
        self.path.mkdir(parents=True, exist_ok=True)
        syncer = Syncer(plan, rpc_sources(plan), log, StopFlag())
        if not syncer.run():
            raise FeedError(
                "chainstar sync failed, so nothing was emitted; the next run continues "
                "from where it stopped: " + "; ".join(log.errors)
            )

    def plan(self, settings: FeedSettings) -> SyncPlan:
        chains = [get_chain(name) for name in settings.chains]
        if not chains:
            raise ConfigError("no chains: name at least one of ethereum, base, bitcoin, solana")
        chains = list(dict.fromkeys(chains))
        if settings.from_block is not None and settings.from_recent is not None:
            raise ConfigError("give from_block or from_recent, not both")
        if settings.from_block is not None and len(chains) > 1:
            raise ConfigError("from_block is a block of one chain; with several, use from_recent")
        if settings.from_recent is not None and settings.from_recent < 1:
            raise ConfigError(f"from_recent must be at least 1, got {settings.from_recent}")
        unknown = set(settings.rpc_urls) | set(settings.confirmations)
        unknown -= {chain.name for chain in chains}
        if unknown:
            raise ConfigError(
                f"RPC URLs or confirmations given for {', '.join(sorted(unknown))}, "
                "which are not among the chains"
            )
        return SyncPlan(
            target=self.target,
            data_dir=self.data_dir,
            chains=tuple(self._chain_plan(chain, settings) for chain in chains),
            from_block=settings.from_block,
            from_recent=settings.from_recent,
        )

    @staticmethod
    def _chain_plan(chain: Chain, settings: FeedSettings) -> ChainPlan:
        return ChainPlan(
            chain=chain,
            connection=resolve_connection(
                file_config={},
                chain=chain,
                rpc_url=settings.rpc_urls.get(chain.name) or None,
                requests_per_second=settings.requests_per_second,
                chunk_size=settings.chunk_size,
            ),
            confirmations=confirmations_for(chain, None, settings.confirmations),
        )

    # Emitting --------------------------------------------------------------------

    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        # A plain connect, as load.attach does: it shares the database dbt-duckdb
        # may still hold open in this process.
        if not self.target.path.exists():
            raise FeedError(
                f"there is no working warehouse at {self.target.path} yet; a run with syncing "
                "on builds it"
            )
        connection = duckdb.connect(str(self.target.path))
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def changes(
        self, stream: Stream, committed: int, consumer: str = DEFAULT_CONSUMER
    ) -> Iterator["Changes"]:
        """What the destination, holding generation `committed` of `stream`, lacks.

        Records the new generation in the ledger before yielding, so the caller
        reports `changes.generation` as the stream's new cursor.
        """
        with self.connect() as connection:
            yield _compute_changes(connection, stream, int(committed or 0), consumer)

    @contextmanager
    def snapshot(self, stream: Stream) -> Iterator["Changes"]:
        """Every row of `stream`, for a full refresh; the ledger is not touched."""
        with self.connect() as connection:
            if not _has_table(connection, MART_SCHEMA, stream.name):
                yield Changes(connection, stream, 0, 0, 0, upsert_source=None)
                return
            count = connection.execute(
                f"SELECT count(*) FROM {_table(MART_SCHEMA, stream.name)}"
            ).fetchone()[0]
            yield Changes(
                connection, stream, 0, int(count), 0, upsert_source=_table(MART_SCHEMA, stream.name)
            )


class _CollectingLog(SyncLog):
    """A sync log that also keeps its errors, to raise them after the pass."""

    def __init__(self, write: Callable[[str], None], *, secrets: Sequence[str]) -> None:
        super().__init__(write, secrets=secrets)
        self.errors: list[str] = []

    def error(self, chain: str, stage: str, message: str, **fields: Any) -> None:
        self.errors.append(f"{chain} {stage}: {self.redact(message)}")
        super().error(chain, stage, message, **fields)


@dataclass
class Changes:
    """One stream's rows to emit: upserts (whole rows) and deletes (keys)."""

    connection: duckdb.DuckDBPyConnection
    stream: Stream
    # The generation every emitted row carries, and the stream's cursor afterwards.
    generation: int
    upserts: int
    deletes: int
    # A table or query whose rows are the upserts; None when there are none.
    upsert_source: str | None
    delete_source: str | None = None

    @property
    def empty(self) -> bool:
        return self.upserts == 0 and self.deletes == 0

    def _queries(self) -> Iterator[str]:
        stream = self.stream
        columns = ", ".join(_ident(name) for name in stream.column_names)
        marks = f"CAST({self.generation} AS BIGINT) AS {_ident(GENERATION_COLUMN)}"
        if self.upserts and self.upsert_source:
            yield (
                f"SELECT {columns}, {marks}, false AS {_ident(DELETED_COLUMN)} "
                f"FROM {self.upsert_source} ORDER BY {_ident(stream.primary_key)}"
            )
        if self.deletes and self.delete_source:
            # The key, and every other column NULL, so both kinds share one schema.
            values = ", ".join(
                f"CAST(key AS {sql_type}) AS {_ident(name)}"
                if name == stream.primary_key
                else f"CAST(NULL AS {sql_type}) AS {_ident(name)}"
                for name, sql_type in stream.columns
            )
            yield (
                f"SELECT {values}, {marks}, true AS {_ident(DELETED_COLUMN)} "
                f"FROM {self.delete_source} ORDER BY key"
            )

    def arrow_tables(self, batch_rows: int = BATCH_ROWS) -> Iterator[Any]:
        """The rows as pyarrow tables of at most `batch_rows` rows (needs pyarrow)."""
        import pyarrow as pa

        for query in self._queries():
            result = self.connection.execute(query)
            # to_arrow_reader() replaced fetch_record_batch() in newer DuckDB releases.
            reader = (
                result.to_arrow_reader(batch_rows)
                if hasattr(result, "to_arrow_reader")
                else result.fetch_record_batch(batch_rows)
            )
            for batch in reader:
                if batch.num_rows:
                    yield pa.Table.from_batches([batch])

    def records(self, batch_rows: int = BATCH_ROWS) -> Iterator[dict[str, Any]]:
        """The rows as dicts of Python values (str, int, float, bool, Decimal, date, datetime)."""
        for query in self._queries():
            cursor = self.connection.execute(query)
            names = [column[0] for column in cursor.description]
            while rows := cursor.fetchmany(batch_rows):
                for row in rows:
                    yield dict(zip(names, row, strict=True))


def _compute_changes(
    connection: duckdb.DuckDBPyConnection, stream: Stream, committed: int, consumer: str
):
    ledger = _table(EMIT_SCHEMA, LEDGER_TABLE)
    generations = _table(EMIT_SCHEMA, GENERATIONS_TABLE)
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(EMIT_SCHEMA)}")
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {ledger} "
        "(consumer VARCHAR, stream VARCHAR, generation BIGINT, key VARCHAR, row_hash UBIGINT)"
    )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {generations} "
        "(consumer VARCHAR, stream VARCHAR, generation BIGINT, created_at TIMESTAMP, "
        "row_count BIGINT, "
        "upserts BIGINT, deletes BIGINT)"
    )
    name = stream.name
    key = _ident(stream.primary_key)
    source = _table(MART_SCHEMA, stream.name)
    if not _has_table(connection, MART_SCHEMA, stream.name):
        # Not built yet (no block synced): nothing to emit, and the cursor stays.
        return Changes(connection, stream, committed, 0, 0, upsert_source=None)

    connection.execute("BEGIN TRANSACTION")
    try:
        known = connection.execute(
            f"SELECT count(*) FROM {generations} "
            "WHERE consumer = ? AND stream = ? AND generation = ?",
            [consumer, name, committed],
        ).fetchone()[0]
        if committed > 0 and not known:
            logger.warning(
                "%s: the destination holds generation %s, which this working folder never "
                "emitted (was it replaced?); emitting every row again. Rows deleted meanwhile "
                "stay in the destination until a full refresh.",
                name,
                committed,
            )
        # Keep only the committed generation: anything else was superseded, or never
        # reached the destination.
        keep = committed if known else -1
        for table in (ledger, generations):
            connection.execute(
                f"DELETE FROM {table} WHERE consumer = ? AND stream = ? AND generation <> ?",
                [consumer, name, keep],
            )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE previous AS "
            f"SELECT key, row_hash FROM {ledger} "
            "WHERE consumer = ? AND stream = ? AND generation = ?",
            [consumer, name, keep],
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE current AS "
            f"SELECT CAST({key} AS VARCHAR) AS key, hash(t) AS row_hash FROM {source} t"
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE changed AS SELECT c.key FROM current c "
            "LEFT JOIN previous p ON p.key = c.key "
            "WHERE p.key IS NULL OR p.row_hash <> c.row_hash"
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE gone AS SELECT p.key FROM previous p "
            "ANTI JOIN current c ON c.key = p.key"
        )
        upserts = connection.execute("SELECT count(*) FROM changed").fetchone()[0]
        deletes = connection.execute("SELECT count(*) FROM gone").fetchone()[0]
        generation = committed
        if upserts or deletes:
            generation = committed + 1
            connection.execute(
                f"INSERT INTO {ledger} SELECT ?, ?, ?, key, row_hash FROM current",
                [consumer, name, generation],
            )
            connection.execute(
                f"INSERT INTO {generations} SELECT ?, ?, ?, now()::TIMESTAMP, "
                "(SELECT count(*) FROM current), ?, ?",
                [consumer, name, generation, upserts, deletes],
            )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    return Changes(
        connection,
        stream,
        generation,
        int(upserts),
        int(deletes),
        upsert_source=(
            f"(SELECT t.* FROM {source} t "
            f"SEMI JOIN changed ON changed.key = CAST(t.{key} AS VARCHAR))"
            if upserts
            else None
        ),
        delete_source="gone" if deletes else None,
    )


def _has_table(connection: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database() "
        "AND schema_name = ? AND table_name = ?",
        [schema, name],
    ).fetchone()
    return bool(row and row[0])


def _table(schema: str, name: str) -> str:
    return f"{_ident(schema)}.{_ident(name)}"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
