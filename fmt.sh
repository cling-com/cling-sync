#!/bin/sh
# Format code.

set -eu

if [ $# -ne 1 ]; then
    echo "Usage: $0 project"
    exit 1
fi

cd "$1"
go tool golangci-lint fmt
