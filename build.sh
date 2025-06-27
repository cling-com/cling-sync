#!/bin/sh
# Build parts of the project.

set -eu

if [ $# -ne 1 ]; then
    echo "Usage: $0 target"
    echo "  tagetts:"
    echo "    cli - build the CLI as `./cling-sync`"
    exit 1
fi

case "$1" in
    cli)
        go build -o cling-sync ./cli
        ;;
    *)
        echo "Unknown target: $1"
        exit 1
        ;;
esac
