#!/usr/bin/env bash
# Run the tests on Linux in a container against a real `secret-tool`/gnome-keyring
# so the Linux keychain code paths are actually exercised (they cannot be on macOS).

set -eu

cd "$(dirname "$0")"

podman build -t cling-sync-test - <<EOF
FROM golang:1.26.1
RUN apt-get update && apt-get install -y \
    dbus-x11 \
    gnome-keyring \
    xvfb \
    libsecret-tools \
    unzip
EOF

# The script passed to bash is single-quoted on purpose: \$(dbus-launch), \$(whoami)
# and friends must expand inside the container, not on the host. Only the volume
# mount path is expanded here on the host.
podman run --rm --name cling-sync-test \
    -v "$(pwd)/..:/opt/cling-sync" \
    -w /opt/cling-sync \
    --entrypoint /bin/bash \
    --privileged \
    cling-sync-test \
    -euc '
        export CS_TEST_NO_MOCK=1
        export DISPLAY=:99
        Xvfb :99 >/dev/null 2>&1 &
        eval "$(dbus-launch --sh-syntax)"
        eval "$(printf "\n" | gnome-keyring-daemon --unlock --components=secrets)"
        # TestChmodChtimeChown needs the user to belong to a second group.
        groupadd testgroup
        usermod -aG testgroup "$(whoami)"
        ./build.sh test cli
        ./build.sh test test -v
        ./build.sh test integration-bash
    '
