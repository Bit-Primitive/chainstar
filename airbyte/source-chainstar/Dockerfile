# The chainstar source connector for Airbyte, built from the repository root:
#
#   docker build -f airbyte/source-chainstar/Dockerfile -t source-chainstar:dev .
#   docker run --rm source-chainstar:dev spec
#
# As Airbyte requires of a connector image: the entrypoint is also in
# AIRBYTE_ENTRYPOINT (the platform runs the commands through it), it runs as the
# user `airbyte` (uid 1000), and /airbyte exists and belongs to that user. The
# base image is Airbyte's own Python connector base, pinned by digest.

# Stage 1: cryo 0.3.2, for Ethereum and Base, as in chainstar's own Dockerfile.
FROM rust:1.97.1-slim-bookworm AS cryo
RUN cargo install cryo_cli --version 0.3.2

# Stage 2: chainstar with the `airbyte` extra, pinned by uv.lock, on the same Python
# as the runtime image (3.13 in python-connector-base 4.1.1).
FROM docker.io/airbyte/python-connector-base:4.1.1@sha256:a268b44c733ae699a60f5fbc06a324945dba98945c6e2ab7f8609f2f895b0d28 AS venv
COPY --from=ghcr.io/astral-sh/uv:0.11.24 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/chainstar \
    UV_PYTHON_DOWNLOADS=never UV_PYTHON=/usr/local/bin/python3
WORKDIR /src
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --extra airbyte --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --extra airbyte --no-editable

# Stage 3: the connector.
FROM docker.io/airbyte/python-connector-base:4.1.1@sha256:a268b44c733ae699a60f5fbc06a324945dba98945c6e2ab7f8609f2f895b0d28
LABEL io.airbyte.name="ghcr.io/bit-primitive/source-chainstar" \
      io.airbyte.version="0.1.0" \
      org.opencontainers.image.source="https://github.com/Bit-Primitive/chainstar" \
      org.opencontainers.image.description="chainstar's star schema of Ethereum, Base, Bitcoin and Solana, as an Airbyte source." \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=cryo /usr/local/cargo/bin/cryo /usr/local/bin/cryo
COPY --from=venv /opt/chainstar /opt/chainstar
# No usage statistics from anything chainstar runs (dbt); chainstar itself sends none.
ENV PATH=/opt/chainstar/bin:$PATH \
    HOME=/airbyte \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    AIRBYTE_ENTRYPOINT="/opt/chainstar/bin/python -m chainstar.integrations.airbyte"
RUN mkdir -p /airbyte/integration_code && chown -R airbyte:airbyte /airbyte
WORKDIR /airbyte/integration_code
USER airbyte
ENTRYPOINT ["/opt/chainstar/bin/python", "-m", "chainstar.integrations.airbyte"]
