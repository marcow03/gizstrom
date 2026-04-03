FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /opt/app
COPY . /opt/app
RUN uv sync --no-cache

ENTRYPOINT [ "uv", "run", "fastapi", "run", "main.py" ]
