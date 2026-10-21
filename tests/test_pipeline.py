from dataclasses import replace
from pathlib import Path

import pytest

from chainstar import pipeline
from chainstar.chains import get_chain
from chainstar.config import BlockRange, RunSettings
from chainstar.model import ModelResult
from chainstar.pipeline import PipelineError, run_pipeline
from chainstar.rpc import NodeIdentity
from chainstar.targets import DuckDBTarget


def _settings(chain: str, blocks: BlockRange, tmp_path: Path) -> RunSettings:
    return RunSettings(
        chain=get_chain(chain),
        blocks=blocks,
        rpc_url="https://rpc.example/key/abc123",
        target=DuckDBTarget(tmp_path / "w.duckdb"),
        data_dir=tmp_path / "data",
        requests_per_second=None,
        chunk_size=100,
    )


@pytest.fixture
def node(monkeypatch):
    identity = {"value": None}
    monkeypatch.setattr(pipeline, "identify_node", lambda chain, url: identity["value"])
    monkeypatch.setitem(
        pipeline.JSON_RPC_EXTRACTORS,
        "solana",
        lambda settings: pytest.fail("extraction must not start"),
    )
    return identity


def test_refuses_the_wrong_network(node, tmp_path: Path):
    node["value"] = NodeIdentity(
        False, "genesis EtWT..., finalized slot 1", "solana (genesis 5eyk...)", 1
    )
    with pytest.raises(PipelineError, match="RPC reports genesis EtWT"):
        run_pipeline(_settings("solana", BlockRange(1, 2), tmp_path), lambda line: None)


def test_refuses_a_range_past_the_head(node, tmp_path: Path):
    # getBlocks lists only finalized slots, so slots past the head would be saved
    # as empty chunks and skipped by every later run.
    node["value"] = NodeIdentity(True, "genesis 5eyk..., finalized slot 1,000", "solana", 1000)
    with pytest.raises(PipelineError, match="at most 1001"):
        run_pipeline(_settings("solana", BlockRange(990, 1002), tmp_path), lambda line: None)


def test_header_names_slots_and_hides_the_key(node, tmp_path: Path):
    node["value"] = NodeIdentity(False, "x", "y", 0)
    lines: list[str] = []
    with pytest.raises(PipelineError):
        run_pipeline(_settings("solana", BlockRange(5, 25), tmp_path), lines.append)
    assert "slots    5:25 (20 slots, end exclusive)" in lines
    assert all("abc123" not in line for line in lines)


def test_refuses_an_evm_range_past_the_head(node, tmp_path: Path):
    node["value"] = NodeIdentity(True, "chain id 1, latest block 100", "ethereum (1)", 100)
    with pytest.raises(PipelineError, match="at most 101"):
        run_pipeline(_settings("ethereum", BlockRange(90, 105), tmp_path), lambda line: None)


def _chunk(data_dir: Path, name: str) -> None:
    path = data_dir / "ethereum" / "erc20_transfers" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


@pytest.fixture
def stub_stages(node, monkeypatch):
    """Everything after the identity check is stubbed; held_ranges reads real file names."""
    node["value"] = NodeIdentity(True, "chain id 1, latest block 10,000", "ethereum (1)", 10_000)
    monkeypatch.setattr(pipeline, "_extract_evm", lambda settings, echo: None)
    monkeypatch.setattr(pipeline, "load_raw", lambda target, data_dir, chain: {"blocks": 40})
    monkeypatch.setattr(
        pipeline, "build_models", lambda target, work_dir, full_refresh: ModelResult(models=1)
    )
    monkeypatch.setattr(
        pipeline, "table_row_counts", lambda target, schema, tables: dict.fromkeys(tables, 0)
    )


def test_load_says_which_blocks_the_warehouse_holds(stub_stages, tmp_path: Path):
    # Earlier runs left 1000-1019 and 1040-1059 on disk; this run asks for 1020-1039.
    data_dir = tmp_path / "data"
    _chunk(data_dir, "ethereum__erc20_transfers__00001000_to_00001019.parquet")
    _chunk(data_dir, "ethereum__erc20_transfers__00001020_to_00001039.parquet")
    _chunk(data_dir, "ethereum__erc20_transfers__from__00001040_to_00001059.parquet")
    _chunk(data_dir, "ethereum__erc20_transfers__00002000_to_00002009.parquet")
    lines: list[str] = []
    run_pipeline(_settings("ethereum", BlockRange(1020, 1040), tmp_path), lines.append)
    text = "\n".join(lines)
    assert (
        "ethereum now in the warehouse: blocks 1,000 to 1,059, 2,000 to 2,009 (70 blocks)" in text
    )
    assert "earlier runs' blocks are included" in text


def test_load_line_is_quiet_when_disk_matches_the_range(stub_stages, tmp_path: Path):
    _chunk(tmp_path / "data", "ethereum__erc20_transfers__00001020_to_00001039.parquet")
    lines: list[str] = []
    run_pipeline(_settings("ethereum", BlockRange(1020, 1040), tmp_path), lines.append)
    text = "\n".join(lines)
    assert "blocks 1,020 to 1,039 (20 blocks)" in text
    assert "earlier runs" not in text


def test_public_endpoint_notice(node, tmp_path: Path):
    node["value"] = NodeIdentity(False, "x", "y", 0)
    lines: list[str] = []
    settings = replace(_settings("solana", BlockRange(5, 25), tmp_path), rpc_is_public=True)
    with pytest.raises(PipelineError):
        run_pipeline(settings, lines.append)
    assert any("free shared public endpoint (5 requests/s" in line for line in lines)
