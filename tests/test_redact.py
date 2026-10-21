import pytest

from chainstar.redact import redact_text, redact_url


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
