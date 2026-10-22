"""chainstar as an Airbyte source connector, built on the Python CDK (`airbyte-cdk`).

    python -m chainstar.integrations.airbyte spec
    python -m chainstar.integrations.airbyte check --config config.json
    python -m chainstar.integrations.airbyte discover --config config.json
    python -m chainstar.integrations.airbyte read --config config.json \\
        --catalog configured_catalog.json [--state state.json]

The Docker image Airbyte runs is built from airbyte/source-chainstar/. Each `read`
syncs chainstar's working warehouse once (see feed.py), then emits each selected
star-schema table:

- **Incremental** (`Incremental | Append + Deduped`): the rows that changed since
  the generation in the stream's state, with `_chainstar_generation` as the cursor
  and the table's key as the primary key; keys that left the star schema (after a
  reorganisation rollback) as records with `_ab_cdc_deleted_at` set, which
  Airbyte's destinations delete in deduped mode. State is per stream
  (`{"_chainstar_generation": N}`), emitted only after a stream's last record.
- **Full refresh**: every row, with `_chainstar_generation` 0; nothing recorded.

DECIMAL columns (wei amounts) are sent as strings: Airbyte's `integer` is 64-bit
in most destinations and `number` is a float in some, so a string is the only type
that keeps every digit everywhere, as `amount_raw` always has.

Airbyte runs each sync in a fresh container. The working folder must be on storage
that outlives it (on self-managed Airbyte, the `/local` volume; see the connector's
README), or every sync starts from scratch.
"""

import logging
import os
import sys
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    from airbyte_cdk.entrypoint import launch
    from airbyte_cdk.models import (
        AirbyteMessage,
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
        ConnectorSpecification,
        FailureType,
        SyncMode,
    )
    from airbyte_cdk.sources import AbstractSource
    from airbyte_cdk.sources.streams import CheckpointMixin, Stream
    from airbyte_cdk.utils.traced_exception import AirbyteTracedException
except ImportError as error:  # pragma: no cover - depends on the install
    raise ImportError(
        "chainstar's Airbyte source needs the airbyte extra: "
        f"pip install 'chainstar[airbyte]' ({error})"
    ) from error

from chainstar import __version__
from chainstar.chains import EVM, SUPPORTED_CHAINS, get_chain
from chainstar.config import ConfigError, resolve_rpc
from chainstar.cryo import INSTALL_HINT, find_cryo
from chainstar.errors import describe
from chainstar.redact import redact_text
from chainstar.rpc import identify_node

from .feed import FeedError, FeedSettings, Workspace
from .streams import (
    DELETED_COLUMN,
    GENERATION_COLUMN,
    STREAMS,
    UnknownStreamError,
    select_streams,
)
from .streams import (
    Stream as TableStream,
)

CDC_DELETED_AT = "_ab_cdc_deleted_at"
# The working folder's ledger is kept per consumer; one Airbyte connection per folder.
AIRBYTE_CONSUMER = "airbyte"
DEFAULT_FROM_RECENT = 100
DOCUMENTATION_URL = (
    "https://github.com/Bit-Primitive/chainstar/tree/main/airbyte/source-chainstar#readme"
)


def connection_specification() -> dict[str, Any]:
    chain_names = list(SUPPORTED_CHAINS)
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "chainstar",
        "type": "object",
        "required": ["chains", "working_directory"],
        "additionalProperties": True,
        "properties": {
            "chains": {
                "title": "Chains",
                "description": "The chains to sync into the star schema.",
                "type": "array",
                "items": {"type": "string", "enum": chain_names},
                "minItems": 1,
                "uniqueItems": True,
                "order": 0,
            },
            "working_directory": {
                "title": "Working directory",
                "description": "chainstar's working folder: its Parquet chunks and DuckDB working "
                "warehouse, which hold the sync progress. It must outlive the container, e.g. "
                "the /local volume on self-managed Airbyte; otherwise every sync starts again.",
                "type": "string",
                "examples": ["/local/chainstar"],
                "order": 1,
            },
            "from_recent": {
                "title": "Start: latest N blocks",
                "description": "A chain's first sync starts this many blocks (Solana: slots) "
                "before its safe head. Ignored once a chain has synced, and when a start "
                "block is set.",
                "type": "integer",
                "minimum": 1,
                "default": DEFAULT_FROM_RECENT,
                "order": 2,
            },
            "from_block": {
                "title": "Start: block",
                "description": "Instead: a chain's first sync starts at this block (Solana: "
                "slot). One chain only.",
                "type": "integer",
                "minimum": 0,
                "order": 3,
            },
            "rpc_urls": {
                "title": "RPC URLs",
                "description": "Your JSON-RPC endpoint per chain. Without one, chainstar uses the "
                "chain's free shared public endpoint, which is rate limited.",
                "type": "object",
                "properties": {
                    name: {"title": name.title(), "type": "string", "airbyte_secret": True}
                    for name in chain_names
                },
                "additionalProperties": False,
                "order": 4,
            },
            "confirmations": {
                "title": "Confirmations",
                "description": "Confirmations a block needs before it is synced, per chain: a "
                "number, or `finalized` on Ethereum and Base. Defaults: ethereum 12, base 60, "
                "bitcoin 6; Solana always takes finalized slots.",
                "type": "object",
                "properties": {
                    "ethereum": {"type": "string", "pattern": "^([1-9][0-9]*|finalized)$"},
                    "base": {"type": "string", "pattern": "^([1-9][0-9]*|finalized)$"},
                    "bitcoin": {"type": "string", "pattern": "^[1-9][0-9]*$"},
                },
                "additionalProperties": False,
                "order": 5,
            },
            "streams": {
                "title": "Tables",
                "description": "The star-schema tables to offer. Default: all nine.",
                "type": "array",
                "items": {"type": "string", "enum": list(STREAMS)},
                "uniqueItems": True,
                "order": 6,
            },
            "requests_per_second": {
                "title": "Requests per second",
                "description": "Cap on RPC requests per second. Default: none for your own RPC, "
                "a gentle rate for public endpoints.",
                "type": "integer",
                "minimum": 1,
                "order": 7,
            },
            "chunk_size": {
                "title": "Blocks per chunk",
                "description": "Blocks (Solana: slots) per Parquet file. Default 100, 10 on "
                "public endpoints.",
                "type": "integer",
                "minimum": 1,
                "order": 8,
            },
        },
    }


def feed_settings(config: Mapping[str, Any]) -> FeedSettings:
    from_block = config.get("from_block")
    return FeedSettings(
        chains=tuple(config.get("chains") or ()),
        working_dir=Path(str(config.get("working_directory") or "")),
        from_block=from_block,
        from_recent=None
        if from_block is not None
        else config.get("from_recent", DEFAULT_FROM_RECENT),
        rpc_urls={name: url for name, url in (config.get("rpc_urls") or {}).items() if url},
        confirmations=dict(config.get("confirmations") or {}),
        requests_per_second=config.get("requests_per_second"),
        chunk_size=config.get("chunk_size"),
    )


def json_schema(stream: TableStream) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for name, sql_type in stream.columns:
        schema = _json_type(sql_type)
        if name != stream.primary_key:
            schema["type"] = ["null", schema["type"]]
        properties[name] = schema
    properties[GENERATION_COLUMN] = {"type": "integer"}
    properties[DELETED_COLUMN] = {"type": "boolean"}
    properties[CDC_DELETED_AT] = {
        "type": ["null", "string"],
        "format": "date-time",
        "airbyte_type": "timestamp_with_timezone",
    }
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": True,
        "properties": properties,
    }


def _json_type(sql_type: str) -> dict[str, Any]:
    if sql_type.startswith("DECIMAL("):
        # Exact whole numbers of up to 38 digits: as text (see the module docstring).
        return {"type": "string"}
    return {
        "VARCHAR": {"type": "string"},
        "BIGINT": {"type": "integer"},
        "INTEGER": {"type": "integer"},
        "DOUBLE": {"type": "number"},
        "BOOLEAN": {"type": "boolean"},
        "DATE": {"type": "string", "format": "date"},
        "TIMESTAMP": {
            "type": "string",
            "format": "date-time",
            "airbyte_type": "timestamp_without_timezone",
        },
    }[sql_type]


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def to_record(row: Mapping[str, Any], deleted_at: str) -> dict[str, Any]:
    record = {name: _json_value(value) for name, value in row.items()}
    record[CDC_DELETED_AT] = deleted_at if row[DELETED_COLUMN] else None
    return record


class _Run:
    """One `read`: syncs the working warehouse once, before the first stream is read."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.settings = feed_settings(config)
        self.workspace = Workspace(self.settings.working_dir)
        self.synced = False

    def ensure_synced(self, logger: logging.Logger) -> None:
        if self.synced:
            return
        try:
            self.workspace.sync(self.settings, logger.info)
        except ConfigError as error:
            raise AirbyteTracedException(
                message=str(error), failure_type=FailureType.config_error, exception=error
            ) from None
        except FeedError as error:
            # RPC trouble, mostly: the next sync continues from where this one got to.
            raise AirbyteTracedException(
                message=str(error), failure_type=FailureType.transient_error, exception=error
            ) from None
        self.synced = True


class ChainstarStream(Stream, CheckpointMixin):
    """One star-schema table, as an Airbyte stream."""

    # Never checkpoint inside a stream: a generation is complete only after its last row.
    state_checkpoint_interval = None
    source_defined_cursor = True

    def __init__(self, table: TableStream, run: _Run) -> None:
        self.table = table
        self.run = run
        self._generation = 0

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def primary_key(self) -> str:
        return self.table.primary_key

    @property
    def cursor_field(self) -> str:
        return GENERATION_COLUMN

    @property
    def supports_incremental(self) -> bool:
        return True

    def get_json_schema(self) -> Mapping[str, Any]:
        return json_schema(self.table)

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {GENERATION_COLUMN: self._generation}

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._generation = int((value or {}).get(GENERATION_COLUMN) or 0)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        deleted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        if sync_mode == SyncMode.full_refresh:
            with self.run.workspace.snapshot(self.table) as changes:
                yield from (to_record(row, deleted_at) for row in changes.records())
            return
        with self.run.workspace.changes(self.table, self._generation, AIRBYTE_CONSUMER) as changes:
            self.logger.info(
                f"{self.name}: generation {changes.generation}, {changes.upserts:,} new or "
                f"changed rows, {changes.deletes:,} deleted (destination had {self._generation})"
            )
            yield from (to_record(row, deleted_at) for row in changes.records())
            # Only now: the state that follows says the destination has all of it.
            self._generation = changes.generation


class SourceChainstar(AbstractSource):
    """The finished star schema of chainstar, kept current by its own sync."""

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl=DOCUMENTATION_URL,
            connectionSpecification=connection_specification(),
        )

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Any]:
        settings = feed_settings(config)
        secrets = list(settings.rpc_urls.values())
        try:
            select_streams(config.get("streams"))
            workspace = Workspace(settings.working_dir)
            if not str(config.get("working_directory") or "").strip():
                raise ConfigError("working_directory is required")
            workspace.plan(settings)
            workspace.path.mkdir(parents=True, exist_ok=True)
            probe = workspace.path / ".chainstar-write-check"
            probe.write_text("ok")
            probe.unlink()
            for name in dict.fromkeys(settings.chains):
                chain = get_chain(name)
                if chain.family == EVM and find_cryo() is None:
                    raise ConfigError(
                        f"cryo is needed for {chain.name} but not installed; {INSTALL_HINT}"
                    )
                rpc = resolve_rpc(settings.rpc_urls.get(chain.name), {}, chain)
                node = identify_node(chain, rpc.url)
                if not node.matches:
                    raise ConfigError(
                        f"the RPC for {chain.name} reports {node.detail}, expected {node.expected}"
                    )
                logger.info(f"{chain.name}: {node.detail}")
        except (ConfigError, UnknownStreamError, OSError) as error:
            return False, redact_text(str(error), secrets)
        except Exception as error:
            return False, redact_text(describe(error), secrets)
        return True, None

    def __init__(self) -> None:
        super().__init__()
        self._run: _Run | None = None

    def streams(self, config: Mapping[str, Any]) -> list[Stream]:
        run = self._run or _Run(config)
        return [ChainstarStream(table, run) for table in select_streams(config.get("streams"))]

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterator[AirbyteMessage]:
        logger.info(f"chainstar {__version__}")
        # Sync once, before any stream: a failure stops the read here, with nothing
        # emitted and no state moved, and the next read continues the sync.
        self._run = _Run(config)
        try:
            if catalog.streams:
                self._run.ensure_synced(logger)
            yield from super().read(logger, config, catalog, state)
        finally:
            self._run = None


def run(args: list[str] | None = None) -> None:
    # dbt runs inside a sync; it sends no usage statistics (chainstar turns them off).
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    launch(SourceChainstar(), sys.argv[1:] if args is None else args)


if __name__ == "__main__":
    run()
