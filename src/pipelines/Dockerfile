FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /opt/pipelines

# System deps in an early stable layer
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential && \
    apt-get clean

# Copy only dependency metadata first
COPY pyproject.toml uv.lock ./

# Cache uv downloads/wheels across builds
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Copy app code last
COPY . .
