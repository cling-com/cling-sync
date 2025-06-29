#!/usr/bin/env bash
# Run `.test.sh` in a Docker container.

set -eu

cd "$(dirname "$0")"

docker run --rm --name cling-sync-test \
    -v "$(pwd)/..:/opt/cling-sync" \
    -w /opt/cling-sync \
    --entrypoint /bin/bash \
    golang:1.24.2-bullseye \
    -c "test/test.sh"
