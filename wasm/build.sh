#!/bin/sh
# CLI to build the wasm binary.

set -eu
root=$(cd $(dirname $0) && pwd)
cd "$root"


if [ $# -eq 0 ]; then
    echo "Usage: $0 build|dev|deps|fmt|lint"
    echo
    echo "  build [--tinygo]"
    echo "      Build the wasm binary."
    echo "      --tinygo - use the TinyGo compiler to produce a much smaller binary"
    echo "                 (TinyGo has to be installed)"
    echo
    echo "  dev [--tinygo]"
    echo "      Build and serve the wasm binary and supporting files."
    echo "      --tinygo - use the TinyGo compiler to produce a much smaller binary"
    echo "                 (TinyGo has to be installed)"
    echo
    echo "  deps [--tinygo]"
    echo "      Show what ends up in the wasm binary, by size."
    echo "      Default lists the largest symbols of the Go build (needs twiggy)."
    echo "      --tinygo - per-package sizes of the TinyGo build (needs TinyGo)."
    echo
    echo "  fmt"
    echo "      Run golangci-lint fmt with wasm build tags."
    echo
    echo "  lint"
    echo "      Run golangci-lint with wasm build tags."
    exit 1
fi

# Build the wasm binary.
# Input:
#   - $1 (optional): `--tinygo` to use the TinyGo compiler
build_wasm() {
    echo ">>> Building Wasm"
    rm -rf build
    mkdir -p build
    if [ $# -gt 0 ] ; then
        case $1 in
            "--tinygo")
                echo "    Using TinyGo compiler"
                cp "$(tinygo env TINYGOROOT)/targets/wasm_exec.js" build
                GOOS=js GOARCH=wasm tinygo build -no-debug -o build/main.wasm .
                ;;
            *)
                echo "Unknown build mode: $1"
                exit 1
                ;;
        esac
    else
        echo "    Using Go compiler with debug symbols in dev mode"
        cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" build
        GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o build/main.wasm .
    fi
    wasm_size=$(wc -c < build/main.wasm)
    if [ $wasm_size -gt 1048576 ]; then
        echo "Warning: main.wasm is larger than 1MB"
    fi
    echo "main.wasm: $((wasm_size / 1024)) KB"
}

# Show what ends up in the wasm binary, using each compiler's own size report.
# Input:
#   - $1 (optional): `--tinygo` to analyse the TinyGo build
deps_wasm() {
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    if [ "${1:-}" = "--tinygo" ]; then
        command -v tinygo >/dev/null 2>&1 || { echo "TinyGo is not installed"; exit 1; }
        echo ">>> Wasm package sizes (TinyGo build)"
        GOOS=js GOARCH=wasm tinygo build -size=full -o "$tmp/main.wasm" .
    elif [ -n "${1:-}" ]; then
        echo "Unknown flag: $1"
        exit 1
    else
        command -v twiggy >/dev/null 2>&1 || { echo "twiggy is not installed (cargo install twiggy)"; exit 1; }
        echo ">>> Largest wasm symbols (Go build)"
        GOOS=js GOARCH=wasm go build -o "$tmp/main.wasm" .
        twiggy top -n 25 "$tmp/main.wasm"
    fi
}

cmd=$1
shift
case "$cmd" in
    build)
        build_wasm "$@"
        ;;
    deps)
        deps_wasm "$@"
        ;;
    dev)
        build_wasm "$@"
        echo ">>> Serving wasm"
        python3 -m http.server 8000 --directory .
        ;;
    fmt)
        echo "wasm: golangci-lint does not support formatting with build constraints."
        ;;
    lint)
        echo ">>> Linting wasm code"
        GOOS=js GOARCH=wasm ../tools/golangci-lint run --build-tags=wasm
        ;;
    *)
        echo "Unknown target: $cmd"
        exit 1
        ;;
esac
