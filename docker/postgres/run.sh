#!/bin/bash

set -eu -o pipefail

HOST_UID=$(id -u)
HOST_GID=$(id -g)
export HOST_UID
export HOST_GID

# Run in the foreground (i.e., not -d).
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
docker compose \
    --project-directory "$SCRIPT_DIR" \
    -f "$SCRIPT_DIR/docker-compose.yml" \
    up --build
