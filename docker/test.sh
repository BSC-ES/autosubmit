#!/bin/bash

# TODO: this is a temporary test script, to help others to
#       review our Docker files. Replace it by a proper test
#       once our CICD adds support to containers.

# This file contains scripts identical or very similar to
# what is described in README.md. The main differences are:
#
# - It builds the image with --no-cache (to force rebuild)
# - It builds the image with the tag :test (and removes it)
# - It runs the container with --detached
# - Tries to set up and clean before/after running all tests
#
# Trap errors and print a message that can be grep'ed.

set -eux

setup() {
  # Create directories for database and experiments.
  rm -rf /tmp/autosubmit/
  mkdir -pv /tmp/autosubmit/{database,experiments}
}

teardown() {
  echo "*** CLEANING UP ***"
  docker rm -f -v test-autosubmit || true
  rm -rf /tmp/autosubmit/
  docker image rm -f ${USER}/autosubmit:test || true
}

handle_errors() {
  echo "*** ERROR TESTING DOCKER COMMANDS ***" 1>&2
  teardown
  exit 1
}

trap handle_errors ERR

setup

# Test commands.

# Build.

docker build \
  --no-cache \
  -t ${USER}/autosubmit:test \
  .

# Print version.

docker run --rm --init ${USER}/autosubmit:test \
  autosubmit --version

# Create external DB.

docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:test \
  autosubmit install

# Create a dummy experiment.

docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:test \
  autosubmit expid -H local -d test --dummy

# List experiments.

docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:test \
  autosubmit describe

# Delete the experiment created.

docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:test \
  autosubmit delete -f a000

# Start the container with SSH (no terminal, dettached).

docker run --init \
  --detach \
  --name test-autosubmit \
  -ti \
  -p 2222:22 \
  -e DISPLAY=$DISPLAY \
  -v $(pwd -P)/id_rsa:/home/autosubmit/.ssh/id_rsa \
  -v $(pwd -P)/id_rsa.pub:/home/autosubmit/.ssh/id_rsa.pub \
  -v $(pwd -P)/authorized_keys:/home/autosubmit/.ssh/authorized_keys \
  -v /tmp/.X11-unix/:/tmp/.X11-unix/ \
  ${USER}/autosubmit:test /bin/bash

# Create a dummy experiment again.

docker exec \
  test-autosubmit \
  autosubmit expid -H local -d test --dummy

teardown

echo "*** BYE ***"

exit 0
