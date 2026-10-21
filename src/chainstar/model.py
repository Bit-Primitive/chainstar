"""The model stage: build the star schema with the dbt project bundled in this package."""

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from chainstar.targets import (
    BigQueryTarget,
    ClickHouseTarget,
    DuckDBTarget,
    LakeTarget,
    MotherDuckTarget,
    PostgresTarget,
    SnowflakeTarget,
    Target,
)

DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
MART_SCHEMA = "chainstar"
MART_TABLES = [
    "fct_transfers",
    "fct_fees",
    "fct_swaps",
    "fct_token_prices",
    "dim_wallets",
    "dim_tokens",
    "dim_contracts",
    "dim_pools",
    "dim_dates",
]

# dbt scrubs the values of DBT_ENV_SECRET_* variables from its logs and artifacts,
# so passwords reach the profile through these and never through profiles.yml.
PASSWORD_ENV = "DBT_ENV_SECRET_CHAINSTAR_PG_PASSWORD"
CLICKHOUSE_PASSWORD_ENV = "DBT_ENV_SECRET_CHAINSTAR_CLICKHOUSE_PASSWORD"
SNOWFLAKE_PASSWORD_ENV = "DBT_ENV_SECRET_CHAINSTAR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_PASSPHRASE_ENV = "DBT_ENV_SECRET_CHAINSTAR_SNOWFLAKE_KEY_PASSPHRASE"

# Every ClickHouse session dbt opens: SQL's NULLs for unmatched outer-join rows
# (ClickHouse otherwise fills in empty values), and CAST keeping NULLs. The models
# rely on both.
CLICKHOUSE_SETTINGS = {"join_use_nulls": 1, "cast_keep_nullable": 1}


def _secret(env: str) -> str:
    return f"{{{{ env_var('{env}', '') }}}}"


class ModelError(RuntimeError):
    pass


@dataclass
class ModelResult:
    models: int = 0
    seeds: int = 0
    tests_passed: int = 0
    warnings: int = 0
    failures: list[str] = field(default_factory=list)
    log_path: Path | None = None


def profile_for(target: Target) -> dict:
    """A dbt profiles.yml body for `target`. It never contains a password or token."""
    if isinstance(target, DuckDBTarget | LakeTarget):
        path = target.path if isinstance(target, DuckDBTarget) else target.engine
        output = {
            "type": "duckdb",
            "path": str(path.resolve()),
            "schema": MART_SCHEMA,
            "threads": 4,
        }
    elif isinstance(target, MotherDuckTarget):
        # The motherduck extension reads the token from motherduck_token itself.
        output = {"type": "duckdb", "path": target.path, "schema": MART_SCHEMA, "threads": 4}
    elif isinstance(target, PostgresTarget):
        output = {
            "type": "postgres",
            "host": target.host,
            "port": target.port,
            "user": target.user,
            "password": _secret(PASSWORD_ENV),
            "dbname": target.dbname,
            "schema": MART_SCHEMA,
            "threads": 4,
        }
    elif isinstance(target, ClickHouseTarget):
        output = {
            "type": "clickhouse",
            "driver": "http",
            "host": target.host,
            "port": target.port,
            "user": target.user,
            "password": _secret(CLICKHOUSE_PASSWORD_ENV),
            "secure": target.secure,
            "schema": MART_SCHEMA,
            "threads": 4,
            # The delete+insert strategy deletes with lightweight DELETE.
            "use_lw_deletes": True,
            "custom_settings": dict(CLICKHOUSE_SETTINGS),
        }
    elif isinstance(target, BigQueryTarget):
        output = {
            "type": "bigquery",
            # Application Default Credentials.
            "method": "oauth",
            "project": target.project,
            "dataset": MART_SCHEMA,
            "threads": 4,
            "job_retries": 1,
            "priority": "interactive",
        }
        if target.location:
            output["location"] = target.location
    elif isinstance(target, SnowflakeTarget):
        output = {
            "type": "snowflake",
            "account": target.account,
            "user": target.user,
            "database": target.database,
            "schema": MART_SCHEMA,
            "threads": 4,
        }
        if target.warehouse:
            output["warehouse"] = target.warehouse
        if target.role:
            output["role"] = target.role
        if target.authenticator:
            output["authenticator"] = target.authenticator
        elif target.private_key_path:
            output["private_key_path"] = target.private_key_path
            output["private_key_passphrase"] = _secret(SNOWFLAKE_PASSPHRASE_ENV)
        else:
            output["password"] = _secret(SNOWFLAKE_PASSWORD_ENV)
    else:
        raise ModelError(f"unsupported target {target!r}")
    return {"chainstar": {"target": "chainstar", "outputs": {"chainstar": output}}}


def secret_environment(target: Target) -> dict[str, str]:
    """The DBT_ENV_SECRET_* values the profile for `target` reads."""
    if isinstance(target, PostgresTarget):
        return {PASSWORD_ENV: target.password}
    if isinstance(target, ClickHouseTarget):
        return {CLICKHOUSE_PASSWORD_ENV: target.password}
    if isinstance(target, SnowflakeTarget):
        return {
            SNOWFLAKE_PASSWORD_ENV: target.password,
            SNOWFLAKE_PASSPHRASE_ENV: target.private_key_passphrase,
        }
    return {}


def build_models(target: Target, work_dir: Path, *, full_refresh: bool = False) -> ModelResult:
    """Run `dbt build` (seeds, models, tests) and summarise the outcome.

    The star-schema tables are incremental: without full_refresh, each processes
    only rows above what it already holds (see the dbt project's
    macros/incremental.sql); with it, every table is rebuilt from the raw tables.

    dbt's own output is silenced and written to a log file in `work_dir`, so the
    terminal shows chainstar's concise summary; failures are reported in full.
    """
    # Imported here because dbt is slow to import and only this stage needs it.
    from dbt.cli.main import dbtRunner

    # dbt resolves relative paths against the project directory, which lives inside
    # the installed package; anchor them to the caller's working directory instead.
    work_dir = work_dir.resolve()
    target_path = work_dir / "target"
    log_path = work_dir / "logs"
    os.environ.update(secret_environment(target))
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"

    with tempfile.TemporaryDirectory(prefix="chainstar-profiles-") as profiles_dir:
        (Path(profiles_dir) / "profiles.yml").write_text(yaml.safe_dump(profile_for(target)))
        outcome = dbtRunner().invoke(
            [
                "build",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                profiles_dir,
                "--target-path",
                str(target_path),
                "--log-path",
                str(log_path),
                "--log-level",
                "none",
                "--log-level-file",
                "debug",
                "--no-send-anonymous-usage-stats",
                "--no-use-colors",
                *(["--full-refresh"] if full_refresh else []),
            ]
        )

    result = ModelResult(log_path=log_path / "dbt.log")
    if outcome.exception is not None:
        raise ModelError(f"dbt could not run: {outcome.exception}")
    for node_result in outcome.result or []:
        _tally(result, node_result)
    return result


def _tally(result: ModelResult, node_result) -> None:
    resource = str(node_result.node.resource_type)
    status = str(node_result.status)
    name = node_result.node.name
    # "skipped" means an upstream node failed, so the table is stale or missing.
    if status in ("error", "fail", "skipped"):
        message = (node_result.message or "").strip()
        result.failures.append(f"{resource} {name}: {status}. {message}".strip())
    elif status == "warn":
        result.warnings += 1
    elif resource == "model":
        result.models += 1
    elif resource == "seed":
        result.seeds += 1
    elif resource == "test":
        result.tests_passed += 1
