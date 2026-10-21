"""Compile the bundled dbt project for a target without connecting to it.

    python compile_offline.py TARGET OUT_DIR full|incremental

`full`: no table exists yet, as on a first build. `incremental`: every table
exists and holds no rows, so the incremental branches and the watermark lookups
compile too. Relation lookups and queries are answered here instead of by the
warehouse; queries are written to OUT_DIR/introspection.sql. Run in its own
process by test_compile_warehouses.py, because it patches dbt's adapters.
"""

import contextlib
import sys
from pathlib import Path

import agate
import yaml
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.sql.impl import SQLAdapter
from dbt.cli.main import dbtRunner

from chainstar.model import DBT_PROJECT_DIR, profile_for
from chainstar.targets import parse_target

spec, out, mode = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
out.mkdir(parents=True, exist_ok=True)
(out / "profiles.yml").write_text(yaml.safe_dump(profile_for(parse_target(spec))))

classes = [BaseAdapter, SQLAdapter]
for module, name in (
    ("dbt.adapters.bigquery.impl", "BigQueryAdapter"),
    ("dbt.adapters.snowflake.impl", "SnowflakeAdapter"),
):
    with contextlib.suppress(ImportError):
        classes.append(getattr(__import__(module, fromlist=[name]), name))
try:
    # dbt-bigquery looks up Application Default Credentials when it starts.
    import dbt.adapters.bigquery.credentials as bigquery_credentials
    from google.auth.credentials import AnonymousCredentials

    bigquery_credentials._create_bigquery_defaults = lambda scopes=None: (
        AnonymousCredentials(),
        "offline",
    )
except ImportError:
    pass


def get_relation(self, database, schema, identifier, **_):
    if mode == "full":
        return None
    return self.Relation.create(
        database=database, schema=schema, identifier=identifier, type="table"
    )


def execute(self, sql, auto_begin=False, fetch=False, limit=None):
    with (out / "introspection.sql").open("a") as handle:
        handle.write(sql.strip() + ";\n")
    return AdapterResponse("OK"), agate.Table([], ["chain", "mark"])


for cls in classes:
    cls.get_relation = get_relation
    cls.execute = execute

result = dbtRunner().invoke(
    [
        "compile",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(out),
        "--target-path",
        str(out / "target"),
        "--log-path",
        str(out / "logs"),
        "--no-populate-cache",
        "--no-send-anonymous-usage-stats",
        "--threads",
        "1",
        "--quiet",
    ]
)
sys.exit(0 if result.success else 1)
