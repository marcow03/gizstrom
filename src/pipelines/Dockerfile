FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY . /opt/pipelines

WORKDIR /opt/pipelines

RUN uv sync --no-cache