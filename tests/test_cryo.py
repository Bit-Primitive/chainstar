import subprocess
from pathlib import Path

import pytest

from chainstar import cryo
from chainstar.cryo import CryoError, run_cryo

SECRET_URL = "https://eth.example.com/v2/supersecretkey"


class RecordedCalls(list):
    """Calls made to the fake cryo, plus what it should return.

    `returncodes` is consumed one per call; once empty, `outcome` applies.
    """

    def __init__(self) -> None:
        super().__init__()
        self.outcome = {"returncode": 0, "stdout": ""}
        self.returncodes: list[int] = []


@pytest.fixture
def fake_cryo(monkeypatch):
    """Replace the cryo binary with a recorder; returns the list of recorded calls."""
    calls = RecordedCalls()

    def fake_run(command, env, capture_output, text, check):
        calls.append({"command": command, "env": env})
        code = calls.returncodes.pop(0) if calls.returncodes else calls.outcome["returncode"]
        return subprocess.CompletedProcess(command, code, calls.outcome["stdout"], "")

    monkeypatch.setattr(cryo, "find_cryo", lambda: "/usr/local/bin/cryo")
    monkeypatch.setattr(cryo.subprocess, "run", fake_run)
    return calls


def test_rpc_url_goes_in_env_not_argv(fake_cryo, tmp_path: Path):
    run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=["--blocks", "1:2"])
    call = fake_cryo[0]
    assert SECRET_URL not in " ".join(call["command"])
    assert call["env"]["ETH_RPC_URL"] == SECRET_URL
    assert call["env"]["MESC_MODE"] == "DISABLED"
    for flag in ("--hex", "--no-report", "--u256-types"):
        assert flag in call["command"]


def test_requests_per_second_is_passed_through(fake_cryo, tmp_path: Path):
    run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=[], requests_per_second=7)
    command = fake_cryo[0]["command"]
    assert command[command.index("--requests-per-second") + 1] == "7"


def test_failure_shows_error_summary_without_secret(fake_cryo, tmp_path: Path):
    # Shape of real cryo 0.3.2 verbose output when a chunk fails.
    fake_cryo.outcome.update(
        returncode=1,
        stdout=(
            "cryo parameters\n───────────────\n"
            f"    - rpc url: {SECRET_URL}\n\n\n"
            "error summary\n─────────────\n(errors in 1 chunks)\n"
            f"- Failed to get block from {SECRET_URL}: archive requests unsupported (1x)\n\n\n"
            "collection summary\n──────────────────\n- rows written: 0\n"
        ),
    )
    with pytest.raises(CryoError) as raised:
        run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=[], attempts=1)
    message = str(raised.value)
    assert "archive requests unsupported" in message
    assert "supersecretkey" not in message
    assert "collection summary" not in message


def test_missing_cryo_gives_install_hint(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(cryo, "find_cryo", lambda: None)
    with pytest.raises(CryoError, match="cargo install cryo_cli"):
        run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=[])


def test_failed_run_is_retried(fake_cryo, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cryo.time, "sleep", lambda seconds: None)
    fake_cryo.returncodes = [1, 1, 0]
    run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=[], attempts=3)
    assert len(fake_cryo) == 3


def test_gives_up_after_all_attempts(fake_cryo, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cryo.time, "sleep", lambda seconds: None)
    fake_cryo.outcome["returncode"] = 1
    with pytest.raises(CryoError, match="after 2 attempts"):
        run_cryo("blocks", rpc_url=SECRET_URL, output_dir=tmp_path, args=[], attempts=2)
    assert len(fake_cryo) == 2
