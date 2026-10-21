from pathlib import Path

import pytest

from chainstar.config import (
    CONFIG_TEMPLATE,
    BlockRange,
    ConfigError,
    load_config_file,
    parse_block_range,
    resolve_settings,
)
from chainstar.targets import DuckDBTarget


def test_block_range_is_end_exclusive():
    blocks = parse_block_range("20_000_000:20000100")
    assert blocks == BlockRange(20_000_000, 20_000_100)
    assert len(blocks) == 100
    assert str(blocks) == "20000000:20000100"


@pytest.mark.parametrize("text", ["100", "100:100", "200:100", "a:b", "100:+50", "-5:10"])
def test_bad_block_ranges(text):
    with pytest.raises(ConfigError):
        parse_block_range(text)


def _resolve(file_config=None, **overrides):
    options = {
        "chain": None,
        "blocks": None,
        "rpc_url": None,
        "target": None,
        "data_dir": None,
        "requests_per_second": None,
        "chunk_size": None,
    }
    options.update(overrides)
    return resolve_settings(file_config=file_config or {}, **options)


def test_flags_override_env_and_file(monkeypatch):
    monkeypatch.setenv("CHAINSTAR_RPC_URL", "https://env.example")
    monkeypatch.setenv("CHAINSTAR_TARGET", "duckdb:env.duckdb")
    file_config = {"chain": "base", "blocks": "1:2", "target": "duckdb:file.duckdb"}

    from_file_and_env = _resolve(file_config)
    assert from_file_and_env.chain.name == "base"
    assert from_file_and_env.rpc_url == "https://env.example"
    assert from_file_and_env.target == DuckDBTarget(Path("env.duckdb"))

    from_flags = _resolve(
        file_config,
        chain="ethereum",
        blocks="5:6",
        rpc_url="https://flag.example",
        target="duckdb:flag.duckdb",
    )
    assert from_flags.chain.name == "ethereum"
    assert str(from_flags.blocks) == "5:6"
    assert from_flags.rpc_url == "https://flag.example"
    assert from_flags.target == DuckDBTarget(Path("flag.duckdb"))


def test_custom_rpc_env_name(monkeypatch):
    monkeypatch.delenv("CHAINSTAR_RPC_URL", raising=False)
    monkeypatch.setenv("MY_NODE", "https://mine.example")
    settings = _resolve({"rpc_url_env": "MY_NODE"}, chain="base", blocks="1:2")
    assert settings.rpc_url == "https://mine.example"


def test_missing_rpc_names_the_env_var(monkeypatch):
    monkeypatch.delenv("CHAINSTAR_RPC_URL", raising=False)
    with pytest.raises(ConfigError, match="CHAINSTAR_RPC_URL"):
        _resolve(chain="base", blocks="1:2")


def test_config_file_refuses_rpc_url(tmp_path):
    path = tmp_path / "chainstar.yml"
    path.write_text("rpc_url: https://eth.example/v2/key\n")
    with pytest.raises(ConfigError, match="keep RPC URLs out"):
        load_config_file(path)


def test_template_is_valid_yaml_with_defaults(tmp_path):
    path = tmp_path / "chainstar.yml"
    path.write_text(CONFIG_TEMPLATE)
    config = load_config_file(path)
    assert config["chain"] == "ethereum"
    assert config["rpc_url_env"] == "CHAINSTAR_RPC_URL"
    assert "blocks" not in config


def test_chunk_size_defaults_and_validates(monkeypatch):
    monkeypatch.setenv("CHAINSTAR_RPC_URL", "https://env.example")
    assert _resolve(chain="base", blocks="1:2").chunk_size == 100
    assert _resolve({"chunk_size": 10}, chain="base", blocks="1:2").chunk_size == 10
    with pytest.raises(ConfigError, match="chunk_size"):
        _resolve(chain="base", blocks="1:2", chunk_size=-1)
