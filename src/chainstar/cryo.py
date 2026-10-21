"""Thin wrapper around the `cryo` command-line tool.

chainstar shells out to the cryo binary (installed with `cargo install cryo_cli`)
rather than importing the `cryo` Python package, because that package has not
been released for current Python versions.
"""

import os
import shutil
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path

from chainstar.redact import redact_text

INSTALL_HINT = (
    "install it with `cargo install cryo_cli` (needs Rust: https://rustup.rs), "
    "or use the Docker image ghcr.io/bit-primitive/chainstar, which includes it"
)
ATTEMPTS = 3
RETRY_DELAY_SECONDS = 3.0


class CryoError(RuntimeError):
    pass


def find_cryo() -> str | None:
    return shutil.which("cryo")


def cryo_version() -> str:
    executable = _require_cryo()
    result = subprocess.run([executable, "--version"], capture_output=True, text=True, check=False)
    return result.stdout.strip() or "unknown version"


def run_cryo(
    dataset: str,
    *,
    rpc_url: str,
    output_dir: Path,
    args: Sequence[str],
    requests_per_second: int | None = None,
    attempts: int = ATTEMPTS,
) -> None:
    """Run one cryo extraction into `output_dir` as Parquet.

    The RPC URL travels in the environment, not argv, so API keys embedded in it
    never show up in process listings. MESC is switched off so a user's MESC config
    cannot silently redirect cryo to a different endpoint than the one we checked.

    A failed run is retried: cryo keeps the chunk files it completed and skips them
    next time, so a retry only re-requests the chunks that failed. cryo's own retry
    covers rate limiting only, while RPC providers also fail requests transiently
    in other ways (load-balanced nodes that lack a block, for example).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        _require_cryo(),
        dataset,
        *args,
        "--output-dir",
        str(output_dir),
        # Lowercase 0x-prefixed hex for hashes and addresses, and U256 values as
        # exact base-10 strings, so nothing downstream has to decode binary.
        "--hex",
        "--u256-types",
        "string",
        # cryo's report file records its command and arguments; chainstar has no use
        # for it, and skipping it keeps run artefacts free of endpoint details.
        "--no-report",
    ]
    if requests_per_second:
        command += ["--requests-per-second", str(requests_per_second)]
    env = {**os.environ, "ETH_RPC_URL": rpc_url, "MESC_MODE": "DISABLED"}
    for attempt in range(1, attempts + 1):
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return
        if attempt < attempts:
            time.sleep(RETRY_DELAY_SECONDS * attempt)
    detail = _error_summary(result.stdout + result.stderr)
    raise CryoError(
        f"cryo {dataset} failed after {attempts} attempts (exit {result.returncode}):\n"
        + redact_text(detail, [rpc_url])
        + "\nCompleted chunks are kept; re-running the same command retries only the rest."
    )


def _require_cryo() -> str:
    executable = find_cryo()
    if executable is None:
        raise CryoError(f"cryo was not found on PATH; {INSTALL_HINT}")
    return executable


def _error_summary(output: str) -> str:
    """Pull cryo's 'error summary' block out of its verbose output.

    cryo prints its parameters (including the RPC URL) before collecting, so we
    show only the part that explains the failure.
    """
    lines = output.splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "error summary":
            block: list[str] = []
            for following in lines[index + 2 :]:
                if not following.strip():
                    break
                block.append(following)
            return "\n".join(block)
    tail = [line for line in lines if line.strip()][-5:]
    return "\n".join(tail) or "(cryo printed no error details)"
