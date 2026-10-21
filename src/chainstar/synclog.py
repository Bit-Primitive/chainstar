"""Log lines for `chainstar sync`: one per stage, readable or as JSON.

Readable lines suit a terminal and journald:

    2026-10-21T10:00:13Z base     extract  52,905,801-52,905,900: blocks 100, ...  11.8s

With --json-logs each line is one JSON object with the same facts as fields
(time, level, chain, stage, message, seconds, and per-stage counts), for log
shippers and monitoring. Every message passes through RPC URL redaction, and
warnings and errors are also cleared of warehouse passwords, tokens and keys.
"""

import json
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from chainstar.redact import redact_text, redact_values
from chainstar.targets import credential_secrets

Write = Callable[[str], None]
# For lines that concern every chain, such as the dbt build.
ALL_CHAINS = "all"


def _utc_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class SyncLog:
    def __init__(
        self,
        write: Write,
        *,
        json_lines: bool = False,
        secrets: Iterable[str | None] = (),
        clock: Callable[[], str] = _utc_timestamp,
    ) -> None:
        self._write = write
        self._json = json_lines
        self._secrets = [secret for secret in secrets if secret]
        self._clock = clock

    def redact(self, text: str) -> str:
        """RPC URLs, and warehouse credentials, which only a failure could show."""
        return redact_values(redact_text(text, self._secrets), credential_secrets())

    def info(self, chain: str, stage: str, message: str, **fields: Any) -> None:
        self._event("info", chain, stage, message, fields)

    def warning(self, chain: str, stage: str, message: str, **fields: Any) -> None:
        self._event("warning", chain, stage, message, fields)

    def error(self, chain: str, stage: str, message: str, **fields: Any) -> None:
        self._event("error", chain, stage, message, fields)

    def _event(
        self, level: str, chain: str, stage: str, message: str, fields: dict[str, Any]
    ) -> None:
        # chainstar's own messages never contain a credential, so only warnings and
        # errors, which can quote a driver, are scrubbed of them; a password that is
        # also an ordinary word then never blanks that word out of every line.
        scrub = (lambda text: redact_text(text, self._secrets)) if level == "info" else self.redact
        message = scrub(message)
        seconds = fields.pop("seconds", None)
        if self._json:
            record = {"time": self._clock(), "level": level, "chain": chain, "stage": stage}
            record["message"] = message
            if seconds is not None:
                record["seconds"] = round(seconds, 2)
            record.update(fields)
            self._write(json.dumps(record, default=str))
            return
        prefix = "" if level == "info" else f"{level}: "
        line = f"{self._clock()} {chain:<8} {stage:<8} {prefix}{message}"
        if seconds is not None:
            line += f"  {seconds:.1f}s"
        self._write(line)


def block_span(first: int, last: int) -> str:
    return f"{first:,}" if first == last else f"{first:,}-{last:,}"


def describe_counts(counts: dict[str, int], prefix: str = "") -> str:
    """'blocks 100, transactions 9,000' with an optional table-name prefix removed."""
    return ", ".join(f"{name.removeprefix(prefix)} {rows:,}" for name, rows in counts.items())
