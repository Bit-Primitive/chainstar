import pytest

from chainstar.redact import redact_text, redact_url, redact_values


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "https://eth-mainnet.g.alchemy.com/v2/abc123",
            "https://eth-mainnet.g.alchemy.com/<redacted>",
        ),
        ("https://rpc.example.com/?apikey=abc123", "https://rpc.example.com/<redacted>"),
        ("https://user:pw@rpc.example.com", "https://rpc.example.com/<redacted>"),
        ("http://127.0.0.1:8545", "http://127.0.0.1:8545"),
        ("https://ethereum-rpc.publicnode.com", "https://ethereum-rpc.publicnode.com"),
        ("not a url", "<redacted>"),
    ],
)
def test_redact_url(url, expected):
    assert redact_url(url) == expected


def test_redact_text_removes_every_occurrence():
    secret = "https://base.example.com/key/abc123"
    text = f"rpc url: {secret}\nretrying {secret}"
    redacted = redact_text(text, [secret])
    assert "abc123" not in redacted
    assert redacted.count("https://base.example.com/<redacted>") == 2


def test_redact_values_removes_secrets_and_url_passwords():
    text = (
        "failed for http://ana:hunter22@ch.local:8123 with token md-0123456789; again md-0123456789"
    )
    redacted = redact_values(text, ["md-0123456789"])
    assert "hunter22" not in redacted and "md-0123456789" not in redacted
    assert "http://ana:<redacted>@ch.local:8123" in redacted
    assert redacted.count("<redacted>") == 3


def test_sync_log_scrubs_credentials_from_warnings_and_errors_only(monkeypatch):
    from chainstar.synclog import SyncLog

    # A password that is also an ordinary word must not blank that word out of
    # every line; chainstar's own messages never contain a credential.
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "chainstar")
    lines: list[str] = []
    log = SyncLog(lines.append, clock=lambda: "T")
    log.info("all", "start", "chainstar 0.1.0; target clickhouse:http://ana@ch:8123")
    log.error("all", "model", "login failed: user ana, password chainstar")
    assert lines[0].endswith("chainstar 0.1.0; target clickhouse:http://ana@ch:8123")
    assert lines[1].endswith("login failed: user ana, password <redacted>")
    assert log.redact("password chainstar") == "password <redacted>"
