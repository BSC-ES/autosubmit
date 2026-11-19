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

IMAGES=(
  autosubmit/autosubmit-ci:latest
  linuxserver/openssh-server:10.3_p1-r2-ls243
  autosubmit/linuxserverio-ssh-2fa-x11@sha256:c66cfd36c0f366d78bb20b97528769df20754c4795d157fbee1672cfff27b15a
  giovtorres/slurm-docker:25.11.2-v0.1.7
  githttpd/githttpd@sha256:74040afe2d252a556b12f17cbabab8827cc0b37ec3fc1a37580a774571c7ee51
  elleflorio/svn-server@sha256:7b900bb84f106f4b8d97dcf02514288c856f06388f7f01c2961f4ad74a092172
)

for IMAGE in "${IMAGES[@]}"; do
  NAME=$(echo "$IMAGE" | awk -F/ '{print $NF}')
  docker pull "$IMAGE"
  docker tag "$IMAGE" ghcr.io/$ORG/"$NAME"
  docker push ghcr.io/$ORG/"$NAME"
done

echo "Docker images synced!"
