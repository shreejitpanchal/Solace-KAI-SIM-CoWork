# One image for every service (publisher, the five consumers, provision);
# compose picks the entry point per service.
#
# Lives under docker/ rather than the repo root on purpose: tag.yml and the dev
# scripts auto-detect a root Dockerfile as "this project ships an image". This
# one exists only to run the simulation locally. Move it to the root (and keep
# SHIP_IMAGE unset) if the image should be published to GHCR.
#
# Build context is the repo root: docker build -f docker/Dockerfile .

FROM ghcr.io/astral-sh/uv:0.11.21 AS uv

FROM python:3.12-slim AS build
COPY --from=uv /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Dependencies first, so a source edit does not invalidate this layer.
# --locked fails loudly if uv.lock is missing or stale: run `uv lock` first.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

FROM python:3.12-slim AS runtime
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
# The project is installed editable, so src/ must sit at the same path.
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/src /app/src
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1
USER app
# compose overrides this per service (kai-sim-orchestrator, kai-sim-publisher, ...).
ENTRYPOINT ["kai-sim-audit-pipeline"]
