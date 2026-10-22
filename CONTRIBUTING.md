# Contributing to chainstar

chainstar turns raw blockchain data into clean, tested star-schema tables in your own warehouse.

Issues and pull requests are welcome. For anything larger than a small fix, please open
an issue first so we can agree on the approach before you spend time on it.

## Setting up

See **Development** in the [README](README.md#development) for installing the
dependencies with [uv](https://docs.astral.sh/uv/) and running the tests and linters.
Before opening a pull request:

```sh
uv run ruff check . && uv run ruff format --check .
uv run pytest
```

## What we look for

- Tests for new behaviour, built from real data (small fixtures sliced from real
  responses or real output, never invented values).
- No guessing: protocol details, API fields and units are checked against their source.
- No telemetry, and secrets never printed, logged or written to files.
- Plain, accurate docs: if behaviour changes, the README and `CHANGELOG.md` change with it.

## Licence

chainstar is licensed under Apache-2.0. By contributing you agree that your contribution
is licensed under the same terms.
