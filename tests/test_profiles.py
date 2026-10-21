"""dbt profiles for every target: the right adapter settings, and never a secret."""

from pathlib import Path

import yaml

from chainstar.model import (
    CLICKHOUSE_PASSWORD_ENV,
    PASSWORD_ENV,
    SNOWFLAKE_PASSPHRASE_ENV,
    SNOWFLAKE_PASSWORD_ENV,
    profile_for,
    secret_environment,
)
from chainstar.targets import parse_target


def _output(spec: str) -> dict:
    profile = profile_for(parse_target(spec))
    return profile["chainstar"]["outputs"]["chainstar"]


def test_duckdb_family_profiles(tmp_path: Path):
    lake = _output(f"parquet:s3://bucket/lake?engine={tmp_path}/engine.duckdb")
    assert lake == {
        "type": "duckdb",
        "path": str(tmp_path / "engine.duckdb"),
        "schema": "chainstar",
        "threads": 4,
    }
    motherduck = _output("motherduck:analytics")
    assert (motherduck["type"], motherduck["path"]) == ("duckdb", "md:analytics")


def test_clickhouse_profile(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "ch-secret")
    target = parse_target("clickhouse:https://ana@ch.cloud:8443")
    output = profile_for(target)["chainstar"]["outputs"]["chainstar"]
    assert output["type"] == "clickhouse"
    assert (output["host"], output["port"], output["user"], output["secure"]) == (
        "ch.cloud",
        8443,
        "ana",
        True,
    )
    assert output["driver"] == "http"
    # delete+insert needs lightweight deletes; the models need SQL's outer-join NULLs.
    assert output["use_lw_deletes"] is True
    assert output["custom_settings"] == {"join_use_nulls": 1, "cast_keep_nullable": 1}
    assert output["password"] == f"{{{{ env_var('{CLICKHOUSE_PASSWORD_ENV}', '') }}}}"
    assert secret_environment(target) == {CLICKHOUSE_PASSWORD_ENV: "ch-secret"}


def test_bigquery_profile_uses_application_default_credentials():
    output = _output("bigquery:my-project?location=EU")
    assert output == {
        "type": "bigquery",
        "method": "oauth",
        "project": "my-project",
        "dataset": "chainstar",
        "location": "EU",
        "threads": 4,
        "job_retries": 1,
        "priority": "interactive",
    }
    assert secret_environment(parse_target("bigquery:my-project")) == {}


def test_snowflake_profiles(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "sf-secret")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
    spec = "snowflake:ana@xy12345.eu-west-1/ANALYTICS?warehouse=WH&role=LOADER"
    output = _output(spec)
    assert {key: output[key] for key in ("account", "user", "database", "warehouse", "role")} == {
        "account": "xy12345.eu-west-1",
        "user": "ana",
        "database": "ANALYTICS",
        "warehouse": "WH",
        "role": "LOADER",
    }
    assert output["password"] == f"{{{{ env_var('{SNOWFLAKE_PASSWORD_ENV}', '') }}}}"
    assert secret_environment(parse_target(spec))[SNOWFLAKE_PASSWORD_ENV] == "sf-secret"

    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/rsa.p8")
    key_pair = _output(spec)
    assert key_pair["private_key_path"] == "/keys/rsa.p8"
    assert (
        key_pair["private_key_passphrase"] == f"{{{{ env_var('{SNOWFLAKE_PASSPHRASE_ENV}', '') }}}}"
    )
    assert "password" not in key_pair

    browser = _output(spec + "&authenticator=externalbrowser")
    assert browser["authenticator"] == "externalbrowser"
    assert "password" not in browser and "private_key_path" not in browser


def test_no_profile_contains_a_secret(monkeypatch, tmp_path: Path):
    secrets = {
        "PGPASSWORD": "pg-secret-1",
        "CLICKHOUSE_PASSWORD": "ch-secret-2",
        "SNOWFLAKE_PASSWORD": "sf-secret-3",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "key-secret-4",
        "motherduck_token": "md-secret-5",
        "AWS_SECRET_ACCESS_KEY": "aws-secret-6",
    }
    for name, value in secrets.items():
        monkeypatch.setenv(name, value)
    for spec in (
        "postgres:postgresql://ana@localhost/dw",
        "clickhouse:http://ana@localhost:8123",
        "snowflake:ana@acct/DB",
        "motherduck:analytics",
        f"parquet:s3://bucket/lake?engine={tmp_path}/e.duckdb",
        "bigquery:my-project",
    ):
        text = yaml.safe_dump(profile_for(parse_target(spec)))
        assert not [value for value in secrets.values() if value in text], spec
    assert secret_environment(parse_target("postgres:postgresql://ana@localhost/dw")) == {
        PASSWORD_ENV: "pg-secret-1"
    }
