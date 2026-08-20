#!/usr/bin/env bash

set -eo pipefail
set -xuve

# Login to DockerHub and GH Container registry first:
# docker login
# docker login ghcr.io

IMAGES=(
  "lscr.io/linuxserver/openssh-server:latest"
  "autosubmit/linuxserverio-ssh-2fa-x11:latest"
  "giovtorres/slurm-docker:25.11.2-v0.1.7"
  "githttpd/githttpd:latest"
  "elleflorio/svn-server:latest"
)

GHCR_NAMESPACE="ghcr.io/bsc-es"

for SRC in "${IMAGES[@]}"; do
    IMAGE_NAME="${SRC##*/}"               # e.g. openssh-server:latest
    DEST="${GHCR_NAMESPACE}/${IMAGE_NAME}"

    echo "==> Pulling ${SRC}"
    docker pull "${SRC}"

    echo "==> Tagging as ${DEST}"
    docker tag "${SRC}" "${DEST}"

    echo "==> Pushing ${DEST}"
    docker push "${DEST}"

    echo
done

echo "Done!"
