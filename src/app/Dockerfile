FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY . /opt/app

WORKDIR /opt/app

RUN uv sync --no-cache

ENTRYPOINT [ "uv", "run", "main.py" ]