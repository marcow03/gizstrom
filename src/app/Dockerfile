FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /opt/app

# Copy only dependency metadata first
COPY pyproject.toml uv.lock ./

# Cache uv downloads/wheels across builds
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Copy app code last
COPY . .

ENTRYPOINT [ "uv", "run", "fastapi", "run", "main.py" ]
