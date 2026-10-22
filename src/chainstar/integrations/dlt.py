"""chainstar as a dlt source: the star-schema tables, kept current, in any dlt destination.

    import dlt
    from chainstar.integrations.dlt import chainstar_source

    pipeline = dlt.pipeline("chainstar", destination="duckdb", dataset_name="chainstar")
    pipeline.run(chainstar_source(chains=["base"], from_recent=100))

Each run syncs a working warehouse with chainstar's own pipeline (see feed.py),
then emits, per table, the rows that changed since the destination's last load:
one dlt resource per star-schema table, merged on the table's key, with deletes
(`_chainstar_deleted`, dlt's `hard_delete` hint) for keys that left the star
schema, such as after a reorganisation rollback. The cursor, `_chainstar_generation`,
is a dlt incremental, so it lives in the pipeline's state and moves only with a
committed load; `refresh="drop_resources"` reloads tables from scratch.

Needs `chainstar[dlt]`. dlt's anonymous telemetry is turned off on import unless
RUNTIME__DLTHUB_TELEMETRY is already set: chainstar sends none, and neither should
the tools it runs inside.
"""

import os

# Before dlt reads its runtime configuration, which happens when a pipeline starts.
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

from collections.abc import Iterator
from typing import Any

try:
    import dlt
    import pyarrow  # noqa: F401  (dlt receives the rows as Arrow tables)
    from dlt.common.schema.typing import TColumnSchema
    from dlt.extract.resource import DltResource
except ImportError as error:  # pragma: no cover - depends on the install
    raise ImportError(
        f"chainstar's dlt source needs the dlt extra: pip install 'chainstar[dlt]' ({error})"
    ) from error

from .feed import FeedSettings, Workspace
from .streams import (
    DELETED_COLUMN,
    GENERATION_COLUMN,
    Stream,
    select_streams,
)

DEFAULT_WORKING_DIR = "chainstar-work"
SOURCE_NAME = "chainstar"


def column_hints(stream: Stream) -> dict[str, TColumnSchema]:
    """dlt column schemas: types as in the working warehouse, the key, and the delete flag.

    Exact amounts stay exact: `amount_raw` and the other `*_raw` text columns are
    text, and DECIMAL(38,0) wei amounts are decimals of the same precision.
    """
    hints: dict[str, TColumnSchema] = {}
    for name, sql_type in stream.columns:
        hint = _dlt_type(sql_type)
        if name == stream.primary_key:
            hint["nullable"] = False
        hints[name] = hint
    hints[GENERATION_COLUMN] = {"data_type": "bigint", "nullable": False}
    hints[DELETED_COLUMN] = {"data_type": "bool", "nullable": False, "hard_delete": True}
    return hints


def _dlt_type(sql_type: str) -> TColumnSchema:
    if sql_type.startswith("DECIMAL("):
        precision, scale = sql_type[len("DECIMAL(") : -1].split(",")
        return {"data_type": "decimal", "precision": int(precision), "scale": int(scale)}
    if sql_type == "TIMESTAMP":
        # Naive UTC in the working warehouse; dlt stores timestamps as UTC.
        return {"data_type": "timestamp", "precision": 6}
    return {
        "data_type": {
            "VARCHAR": "text",
            "BIGINT": "bigint",
            "INTEGER": "bigint",
            "DOUBLE": "double",
            "BOOLEAN": "bool",
            "DATE": "date",
        }[sql_type]
    }


class _Run:
    """One extraction: syncs the working warehouse once, before the first table is read."""

    def __init__(self, workspace: Workspace, settings: FeedSettings | None) -> None:
        self.workspace = workspace
        self.settings = settings
        self.synced = False

    def ensure_synced(self) -> None:
        if not self.synced and self.settings is not None:
            self.workspace.sync(self.settings)
        self.synced = True


def _resource(stream: Stream, run: _Run) -> DltResource:
    def rows(
        # dlt's own idiom: the incremental is declared as the argument's default.
        generation: dlt.sources.incremental[int] = dlt.sources.incremental(  # noqa: B008
            GENERATION_COLUMN,
            initial_value=0,
            # Every row of a run carries one new generation, above the last: an open
            # start needs no de-duplication, so dlt keeps no per-row hashes in state.
            range_start="open",
        ),
    ) -> Iterator[Any]:
        run.ensure_synced()
        pipeline = dlt.current.pipeline()
        consumer = f"dlt:{pipeline.pipeline_name}:{pipeline.dataset_name}"
        with run.workspace.changes(stream, generation.start_value or 0, consumer) as changes:
            yield from changes.arrow_tables()

    return dlt.resource(
        rows,
        name=stream.name,
        primary_key=stream.primary_key,
        write_disposition="merge",
        columns=column_hints(stream),
        max_table_nesting=0,
    )


@dlt.source(name=SOURCE_NAME, max_table_nesting=0)
def chainstar_source(
    chains: list[str] | None = None,
    working_dir: str = DEFAULT_WORKING_DIR,
    from_block: int | None = None,
    from_recent: int | None = None,
    rpc_urls: dict[str, str] | None = None,
    tables: list[str] | None = None,
    confirmations: dict[str, Any] | None = None,
    requests_per_second: int | None = None,
    chunk_size: int | None = None,
    sync: bool = True,
) -> Any:
    """chainstar's star-schema tables, one resource per table.

    Args:
        chains: ethereum, base, bitcoin and/or solana.
        working_dir: chainstar's working folder (Parquet chunks and the DuckDB working
            warehouse). Keep it between runs: it holds the sync progress. A folder used
            by `chainstar sync --data-dir DIR/data --target duckdb:DIR/warehouse.duckdb`
            works too.
        from_block: a chain's first sync starts here (one chain only).
        from_recent: or this many blocks (Solana: slots) before the safe head.
        rpc_urls: per chain, e.g. {"base": "https://..."}; else CHAINSTAR_RPC_URL_<CHAIN>,
            CHAINSTAR_RPC_URL, then the chain's free public endpoint. Never logged.
        tables: which star-schema tables to emit (default: all nine).
        confirmations: per chain, a number or "finalized"; chainstar's defaults
            otherwise (ethereum 12, base 60, bitcoin 6, Solana finalized).
        requests_per_second, chunk_size: as for `chainstar sync`.
        sync: False emits from the working warehouse as it is, without syncing (for a
            folder another process keeps current; not while that process runs).
    """
    workspace = Workspace(working_dir)
    settings = None
    if sync:
        settings = FeedSettings(
            chains=tuple(chains or ()),
            working_dir=workspace.path,
            from_block=from_block,
            from_recent=from_recent,
            rpc_urls=dict(rpc_urls or {}),
            confirmations=dict(confirmations or {}),
            requests_per_second=requests_per_second,
            chunk_size=chunk_size,
        )
        # Check the options now, before dlt starts extracting.
        workspace.plan(settings)
    run = _Run(workspace, settings)
    return [_resource(stream, run) for stream in select_streams(tables)]
