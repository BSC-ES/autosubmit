#!/usr/bin/env bash

set -euo pipefail

sudo mkdir -p /as
sudo chown "$(id -u):$(id -g)" /as
sudo chmod 775 /as

exec "$@"
