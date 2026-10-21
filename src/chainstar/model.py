"""The model stage: build the star schema with the dbt project bundled in this package."""

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from chainstar.targets import DuckDBTarget, PostgresTarget, Target

DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
MART_SCHEMA = "chainstar"
MART_TABLES = [
    "fct_transfers",
    "fct_fees",
    "dim_wallets",
    "dim_tokens",
    "dim_contracts",
    "dim_dates",
]

# dbt scrubs the values of DBT_ENV_SECRET_* variables from its logs and artifacts.
PASSWORD_ENV = "DBT_ENV_SECRET_CHAINSTAR_PG_PASSWORD"


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
    """A dbt profiles.yml body for `target`. It never contains a password."""
    if isinstance(target, DuckDBTarget):
        output = {
            "type": "duckdb",
            "path": str(target.path.resolve()),
            "schema": MART_SCHEMA,
            "threads": 4,
        }
    elif isinstance(target, PostgresTarget):
        output = {
            "type": "postgres",
            "host": target.host,
            "port": target.port,
            "user": target.user,
            "password": f"{{{{ env_var('{PASSWORD_ENV}', '') }}}}",
            "dbname": target.dbname,
            "schema": MART_SCHEMA,
            "threads": 4,
        }
    else:
        raise ModelError(f"unsupported target {target!r}")
    return {"chainstar": {"target": "chainstar", "outputs": {"chainstar": output}}}


def build_models(target: Target, work_dir: Path) -> ModelResult:
    """Run `dbt build` (seeds, models, tests) and summarise the outcome.

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
    if isinstance(target, PostgresTarget):
        os.environ[PASSWORD_ENV] = target.password
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
