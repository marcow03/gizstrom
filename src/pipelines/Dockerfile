FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /opt/pipelines
COPY . /opt/pipelines

RUN uv sync --no-cache