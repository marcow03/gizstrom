FROM python:3.12-slim

WORKDIR /opt/feast

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "feast[aws,redis]"
