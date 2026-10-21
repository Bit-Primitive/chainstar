from pathlib import Path

import pytest

from chainstar.targets import DuckDBTarget, PostgresTarget, TargetError, parse_target


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
