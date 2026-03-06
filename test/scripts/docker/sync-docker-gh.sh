#!/bin/bash
set -euo pipefail

# This script syncs DockerHub images with GitHUb Registry.
# We use this when we need new versions from DockerHub uploaded
# to our GitHub Registry.
#
# The rationale behind using both, is that we can easily pull
# with Docker in Kubernetes and for tests, but for our GitHub
# Actions pipelines it is much better to fetch images from
# GitHub Registry than going over the Internet to fetch them
# from DockerHub.
#
# Note; GitHub Actions already does some caching of images like
# python and ubuntu. There is also a way to cache images with
# GitHub Actions. Said all this, if we use Runners some day in
# the future that are located elsewhere like Spain, then it could
# make more sense to sync the images to GitLab.

# The script only runs if CR_PAT is defined. Create a GitHUb personal
# token (classic) with write/read/delete permissions for package. Then,
# use `  export CR_PAT=ghp_....`. Note the space at the beginning. This
# way your token is not saved to your Linux Bash history.
: "${CR_PAT:?CR_PAT is not defined)}"

echo "$CR_PAT" | docker login ghcr.io -u USERNAME --password-stdin

ORG=bsc-es

# List of images used in our tests. ATOW they are listed on
# test/integration/test_utils_
#
# The versions used:
#
# githttpd: sha256:74040afe2d252a556b12f17cbabab8827cc0b37ec3fc1a37580a774571c7ee51
# linuxserverio-ssh-2fa-x11: sha256:c66cfd36c0f366d78bb20b97528769df20754c4795d157fbee1672cfff27b15a
# slurm-openssh-container: sha256:86fe9888739930d3e49a0bcd36aa107bb6cd8c74604cbf80746d7e7ad414134d
# openssh: sha256:fdd9783e0fa4c52592c8405765c5de04dcf657db1b4d6ab073c14c4b998ca1fa

IMAGES=(
  githttpd/githttpd:latest
  autosubmit/linuxserverio-ssh-2fa-x11:latest
  autosubmit/slurm-openssh-container:25-05-0-1
  lscr.io/linuxserver/openssh-server:latest
)

for IMAGE in "${IMAGES[@]}"; do
  NAME=$(echo "$IMAGE" | awk -F/ '{print $NF}')
  docker pull "$IMAGE"
  docker tag "$IMAGE" ghcr.io/$ORG/"$NAME"
  docker push ghcr.io/$ORG/"$NAME"
done

echo "Docker images synced!"
