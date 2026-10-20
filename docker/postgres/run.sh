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

# docker compose ps
# docker compose logs autosubmit
# docker compose logs postgres
# docker compose down --volumes
# docker compose exec autosubmit autosubmit --version
# docker compose exec autosubmit /bin/bash
# docker compose exec postgres psql -U autosubmit -d autosubmit
