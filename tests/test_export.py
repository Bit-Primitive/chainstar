"""Exports from a warehouse built from the fixtures of all four chains."""

import csv
import shutil
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from chainstar import export as export_module
from chainstar.cli import app
from chainstar.export import ExportError, ExportRequest, export, normalize_address, resolve_format
from chainstar.targets import DuckDBTarget
from conftest import FIXTURES
from e2e_checks import ALL_CHAINS, load_and_build

# Sends three transfers and receives three in the Base fixture slice.
BASE_WALLET = "0x80a4e4754e586e67e8a1a5a83b4024c15c94806f"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> DuckDBTarget:
    root = tmp_path_factory.mktemp("export")
    shutil.copytree(FIXTURES, root / "data")
    target = DuckDBTarget(root / "warehouse.duckdb")
    load_and_build(target, root / "data", root / "dbt", chains=ALL_CHAINS)
    return target


def _rows(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def test_transfers_view_has_symbols_and_exact_amounts(warehouse, tmp_path: Path):
    path = tmp_path / "t.csv"
    count = export(warehouse, ExportRequest("transfers", path, "csv", chain="ethereum"))
    rows = _rows(path)
    assert count == len(rows) == 562
    usdt = next(r for r in rows if r["token_symbol"] == "USDT" and r["amount_raw"] == "26240000")
    assert float(usdt["amount"]) == pytest.approx(26.24)
    assert list(rows[0])[:3] == ["block_timestamp", "chain", "block_number"]
    assert "transaction_index" not in rows[0]


def test_wallet_filter_matches_sender_or_recipient_any_case(warehouse, tmp_path: Path):
    path = tmp_path / "w.csv"
    export(warehouse, ExportRequest("transfers", path, "csv", wallet=BASE_WALLET.upper()[2:]))
    assert _rows(path) == []  # not an address without its 0x
    count = export(
        warehouse, ExportRequest("transfers", path, "csv", wallet="0x" + BASE_WALLET[2:].upper())
    )
    rows = _rows(path)
    assert count == len(rows) == 6
    assert all(BASE_WALLET in (r["from_address"], r["to_address"]) for r in rows)
    assert {r["from_address"] == BASE_WALLET for r in rows} == {True, False}


def test_token_filter_and_limit(warehouse, tmp_path: Path):
    path = tmp_path / "native.csv"
    count = export(
        warehouse,
        ExportRequest("transfers", path, "csv", chain="bitcoin", token="native", limit=5),
    )
    rows = _rows(path)
    assert count == 5
    assert {r["token_symbol"] for r in rows} == {"BTC"}
    # Ordered by output index as a number, not as text.
    assert [r["transfer_position"] for r in rows][:2] == ["0", "1"]


def test_parquet_keeps_types(warehouse, tmp_path: Path):
    path = tmp_path / "t.parquet"
    export(warehouse, ExportRequest("fct_transfers", path, "parquet"))
    with duckdb.connect() as connection:
        types = dict(
            connection.execute(
                f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '{path}')"
            ).fetchall()
        )
    assert types["amount_raw"] == "VARCHAR"
    assert types["amount"] == "DOUBLE"


def test_excel_refuses_more_rows_than_a_sheet_holds(warehouse, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(export_module, "EXCEL_MAX_ROWS", 10)
    path = tmp_path / "t.xlsx"
    with pytest.raises(ExportError, match="do not fit in an Excel sheet"):
        export(warehouse, ExportRequest("transfers", path, "xlsx"))
    assert not path.exists()


def test_excel_file(warehouse, tmp_path: Path):
    path = tmp_path / "t.xlsx"
    try:
        export(warehouse, ExportRequest("transfers", path, "xlsx", chain="base", limit=3))
    except duckdb.IOException as error:  # the extension is downloaded on first use
        pytest.skip(f"DuckDB excel extension unavailable: {error}")
    with duckdb.connect() as connection:
        connection.execute("LOAD excel")
        rows = connection.execute(f"SELECT amount_raw, amount FROM read_xlsx('{path}')").fetchall()
    assert len(rows) == 3
    assert all(isinstance(raw, str) and isinstance(amount, float) for raw, amount in rows)


def test_filters_that_do_not_apply_are_refused(warehouse, tmp_path: Path):
    with pytest.raises(ExportError, match="--wallet does not apply to dim_tokens"):
        export(warehouse, ExportRequest("dim_tokens", tmp_path / "x.csv", "csv", wallet="0x1"))
    with pytest.raises(ExportError, match="unknown table"):
        export(warehouse, ExportRequest("fct_nft_transfers", tmp_path / "x.csv", "csv"))


def test_missing_warehouse_is_explained_and_not_created(tmp_path: Path):
    target = DuckDBTarget(tmp_path / "none.duckdb")
    with pytest.raises(ExportError, match="chainstar quickstart"):
        export(target, ExportRequest("transfers", tmp_path / "x.csv", "csv"))
    assert not target.path.exists()


def test_format_from_flag_or_extension():
    assert resolve_format(None, None, "transfers") == (Path("transfers.csv"), "csv")
    assert resolve_format(Path("a.xlsx"), None, "transfers") == (Path("a.xlsx"), "xlsx")
    assert resolve_format(None, "parquet", "fct_fees") == (Path("fct_fees.parquet"), "parquet")
    with pytest.raises(ExportError, match="does not match"):
        resolve_format(Path("a.csv"), "xlsx", "transfers")
    with pytest.raises(ExportError, match="unknown format"):
        resolve_format(Path("a.json"), None, "transfers")


def test_addresses_are_normalized_only_on_evm():
    assert normalize_address(" 0x" + BASE_WALLET[2:].upper() + " ") == BASE_WALLET
    solana = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    assert normalize_address(solana) == solana


def test_cli_export(warehouse, tmp_path: Path):
    path = tmp_path / "fees.csv"
    result = CliRunner().invoke(
        app,
        [
            "export",
            "fct_fees",
            "--chain",
            "solana",
            "--output",
            str(path),
            "--target",
            f"duckdb:{warehouse.path}",
            "--config",
            str(tmp_path / "none.yml"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"rows to {path}" in result.output
    assert {r["chain"] for r in _rows(path)} == {"solana"}
