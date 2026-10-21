from pathlib import Path

import pytest

from chainstar import pipeline
from chainstar.chains import get_chain
from chainstar.config import BlockRange, RunSettings
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
