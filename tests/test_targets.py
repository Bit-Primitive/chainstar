from pathlib import Path

import pytest

from chainstar.targets import (
    DEFAULT_LAKE_ENGINE,
    BigQueryTarget,
    ClickHouseTarget,
    DuckDBTarget,
    LakeTarget,
    MotherDuckTarget,
    PostgresTarget,
    S3Options,
    TargetError,
    credential_secrets,
    parse_target,
)


def test_duckdb_target():
    assert parse_target("duckdb:warehouse/chain.duckdb") == DuckDBTarget(
        Path("warehouse/chain.duckdb")
    )


def test_postgres_target_decodes_url_parts():
    target = parse_target("postgres:postgresql://ana%40corp:p%40ss%20word@db.local:6543/dw")
    assert isinstance(target, PostgresTarget)
    assert (target.user, target.password, target.host, target.port, target.dbname) == (
        "ana@corp",
        "p@ss word",
        "db.local",
        6543,
        "dw",
    )


def test_postgres_password_falls_back_to_pgpassword(monkeypatch):
    monkeypatch.setenv("PGPASSWORD", "from-env")
    target = parse_target("postgres:postgresql://ana@localhost/dw")
    assert target.password == "from-env"
    assert target.port == 5432


def test_password_never_in_describe_or_repr():
    target = parse_target("postgres:postgresql://ana:topsecret@localhost/dw")
    assert "topsecret" not in target.describe()
    assert "topsecret" not in repr(target)


def test_libpq_dsn_quotes_values():
    target = PostgresTarget(host="h", port=5432, user="u", dbname="d", password="it's \\ hard")
    assert "password='it\\'s \\\\ hard'" in target.libpq_dsn()


@pytest.mark.parametrize(
    "spec",
    [
        "chain.duckdb",
        "duckdb:",
        "snowflake:account",
        "postgres:mysql://u@h/d",
        "postgres:postgresql://localhost/dw",
        "postgres:postgresql://u@localhost",
    ],
)
def test_invalid_targets(spec):
    with pytest.raises(TargetError):
        parse_target(spec)


def test_motherduck_targets(monkeypatch):
    assert parse_target("motherduck:analytics") == MotherDuckTarget("analytics")
    assert parse_target("duckdb:md:analytics") == MotherDuckTarget("analytics")
    assert parse_target("motherduck:analytics").path == "md:analytics"
    monkeypatch.setenv("motherduck_token", "md-token-value")
    assert "md-token-value" in credential_secrets()
    assert "md-token-value" not in parse_target("motherduck:analytics").describe()


def test_local_lake_target(tmp_path):
    target = parse_target(f"parquet:{tmp_path}/lake")
    assert isinstance(target, LakeTarget)
    assert (target.location, target.remote) == (str(tmp_path / "lake"), False)
    assert target.engine == Path(DEFAULT_LAKE_ENGINE)
    assert target.path("chainstar", "dim_dates") == f"{tmp_path}/lake/chainstar/dim_dates"
    other = parse_target(f"parquet:{tmp_path}/lake?engine={tmp_path}/e.duckdb")
    assert other.engine == tmp_path / "e.duckdb"


def test_s3_lake_target_options(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    target = parse_target("parquet:s3://bucket/chainstar/")
    assert target.location == "s3://bucket/chainstar"
    assert target.s3 == S3Options(endpoint=None, region="eu-west-1", url_style=None, use_ssl=True)
    minio = parse_target("parquet:s3://lake/x?endpoint=http://127.0.0.1:9000&region=us-east-1")
    # The scheme of the endpoint says whether to use TLS.
    assert minio.s3 == S3Options("127.0.0.1:9000", "us-east-1", None, use_ssl=False)
    r2 = parse_target(
        "parquet:s3://lake?endpoint=https://abc.r2.cloudflarestorage.com&region=auto&url_style=vhost"
    )
    assert r2.s3 == S3Options("abc.r2.cloudflarestorage.com", "auto", "vhost", use_ssl=True)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    assert parse_target("parquet:s3://lake").s3.endpoint == "minio:9000"


def test_clickhouse_target(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "from-env")
    target = parse_target("clickhouse:http://ana@ch.local:8123")
    assert target == ClickHouseTarget("ch.local", 8123, "ana", secure=False, password="from-env")
    secure = parse_target("clickhouse:https://ana:p%40ss@ch.cloud")
    assert (secure.port, secure.secure, secure.password) == (8443, True, "p@ss")
    assert "p@ss" not in secure.describe() and "p@ss" not in repr(secure)
    assert parse_target("clickhouse:http://localhost").user == "default"


def test_bigquery_target():
    assert parse_target("bigquery:my-project") == BigQueryTarget("my-project")
    assert parse_target("bigquery://my-project?location=EU") == BigQueryTarget("my-project", "EU")
    assert parse_target("bigquery:my-project?location=EU").describe() == (
        "bigquery:my-project?location=EU"
    )


def test_snowflake_target(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "sf-secret")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
    target = parse_target("snowflake:ana@xy12345.eu-west-1/ANALYTICS?warehouse=WH&role=LOADER")
    assert (target.account, target.user, target.database) == (
        "xy12345.eu-west-1",
        "ana",
        "ANALYTICS",
    )
    assert (target.warehouse, target.role, target.password) == ("WH", "LOADER", "sf-secret")
    assert "sf-secret" not in target.describe() and "sf-secret" not in repr(target)
    assert parse_target("snowflake://ana@acct/DB").database == "DB"
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/rsa.p8")
    assert parse_target("snowflake:ana@acct/DB").private_key_path == "/keys/rsa.p8"


@pytest.mark.parametrize(
    "spec",
    [
        "motherduck:",
        "motherduck:a/b",
        "parquet:",
        "parquet:gs://bucket/x",
        "parquet:s3:///x",
        "parquet:./lake?endpoint=localhost:9000",
        "parquet:s3://b?use_ssl=maybe",
        "parquet:s3://b?colour=red",
        "clickhouse:tcp://localhost:9000",
        "clickhouse:http://localhost:8123/db",
        "bigquery:",
        "bigquery:p?dataset=x",
        "snowflake:ana@acct",
        "snowflake:ana:pw@acct/DB",
        "snowflake:ana@acct/DB?colour=red",
    ],
)
def test_invalid_new_targets(spec):
    with pytest.raises(TargetError):
        parse_target(spec)


def test_credential_secrets_cover_every_target(monkeypatch):
    for name, value in {
        "PGPASSWORD": "pg-secret",
        "CLICKHOUSE_PASSWORD": "ch-secret",
        "SNOWFLAKE_PASSWORD": "sf-secret",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "key-secret",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "AWS_SESSION_TOKEN": "aws-token",
    }.items():
        monkeypatch.setenv(name, value)
    found = credential_secrets(parse_target("clickhouse:http://ana:url-secret@localhost"))
    assert set(found) >= {
        "pg-secret",
        "ch-secret",
        "sf-secret",
        "key-secret",
        "aws-secret",
        "aws-token",
        "url-secret",
    }
