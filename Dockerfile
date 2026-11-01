# chainstar with cryo bundled, so nobody needs Rust or Python installed.
#
#   docker build -t chainstar .
#   docker run --rm -v "$PWD:/work" chainstar quickstart --chain base --yes
#
# Files (Parquet, the DuckDB warehouse, exports) are written to /work, the
# directory you mount.

# Stage 1: build cryo 0.3.2 from crates.io, as the README's install step does.
# Not --locked: the Cargo.lock shipped with the crate pins ethnum 1.5.0, which
# no longer compiles on current Rust (E0512 in ethnum's error.rs with Rust 1.97).
FROM rust:1.99.1-slim-bookworm AS cryo
RUN cargo install cryo_cli --version 0.3.2

# Stage 2: chainstar and its Python dependencies, pinned by uv.lock. The published
# image includes ClickHouse (DuckDB, Postgres, MotherDuck and the Parquet lake need no
# extra), which keeps it under 1 GB for everyone who just wants to try chainstar. For
# BigQuery or Snowflake build your own, which adds about 800 MB:
#   docker build --build-arg EXTRAS="all" .     (or "bigquery", "snowflake", "")
FROM python:3.13-slim-bookworm AS venv
ARG EXTRAS="clickhouse"
COPY --from=ghcr.io/astral-sh/uv:0.11.24 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/chainstar
WORKDIR /src
COPY pyproject.toml uv.lock README.md LICENSE ./
# Dependencies first, so editing chainstar's code does not reinstall dbt.
RUN uv sync --locked --no-dev $(for extra in $EXTRAS; do echo --extra "$extra"; done) \
        --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev $(for extra in $EXTRAS; do echo --extra "$extra"; done) \
        --no-editable
# uv.lock pins certifi to 2025.1.31 only because dbt-snowflake requires
# certifi<2025.4.26. An image without Snowflake gets the current CA bundle.
RUN case " $EXTRAS " in *" snowflake "*|*" all "*) ;; \
      *) uv pip install --python /opt/chainstar/bin/python --upgrade certifi ;; esac

# Stage 3: the runtime image, without compilers or uv.
FROM python:3.13-slim-bookworm
LABEL org.opencontainers.image.source="https://github.com/Bit-Primitive/chainstar" \
      org.opencontainers.image.description="Raw blockchain data to clean dimensional tables, with cryo bundled." \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=cryo /usr/local/cargo/bin/cryo /usr/local/bin/cryo
COPY --from=venv /opt/chainstar /opt/chainstar
# HOME is fixed so DuckDB finds the extensions installed below even when the
# container runs as another user (docker run --user "$(id -u):$(id -g)").
ENV PATH=/opt/chainstar/bin:$PATH \
    HOME=/home/chainstar \
    PYTHONUNBUFFERED=1
RUN useradd --uid 1000 --create-home chainstar \
    && mkdir /work && chown chainstar:chainstar /work
USER chainstar
# DuckDB downloads extensions on first use; bake in the ones chainstar uses
# (postgres targets, Excel export, s3:// lakes and the AWS credential chain,
# MotherDuck) so the image works without that download.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL postgres; INSTALL excel; INSTALL httpfs; INSTALL aws; INSTALL motherduck')" \
    && chmod -R a+rX /home/chainstar
WORKDIR /work
ENTRYPOINT ["chainstar"]
CMD ["--help"]
