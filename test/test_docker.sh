#!/usr/bin/env bash
# Run the integration tests in a Docker container using `secret-tool` to
# test keychain support.

set -eu

cd "$(dirname "$0")"

podman build -t cling-sync-test - <<EOF
FROM golang:1.24.4-bullseye
RUN apt-get update && apt-get install -y \
    dbus-x11 \
    gnome-keyring \
    xvfb \
    libsecret-tools
EOF

podman run --rm --name cling-sync-test \
    -v "$(pwd)/..:/opt/cling-sync" \
    -w /opt/cling-sync \
    --entrypoint /bin/bash \
    --privileged \
    cling-sync-test \
    -c "\
        export CS_TEST_NO_MOCK=1 && \
        export DISPLAY=:99; \
        Xvfb :99 & \
        eval $(dbus-launch) && \
        echo '\n' | gnome-keyring-daemon --unlock && \
        groupadd testgroup && \
        usermod -a -G testgroup \$(whoami) && \
        ./build.sh test test -v && \
        ./build.sh test integration-bash \
        "
