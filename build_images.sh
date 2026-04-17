#!/usr/bin/env bash

docker build -t ghcr.io/marcow03/gizstrom/app:latest ./src/app/
docker build -t ghcr.io/marcow03/gizstrom/pipelines:latest ./src/pipelines/
