#!/bin/sh
# CLI to build the wasm binary.

set -eu
root=$(cd $(dirname $0) && pwd)
cd "$root"


if [ $# -eq 0 ]; then
    echo "Usage: $0 build|serve"
    echo
    echo "  build [--optimize]"
    echo "      Build the wasm binary."
    echo "      --optimize - use the TinyGo compiler to produce a much smaller binary (~10%)"
    echo "                   (TinyGo has to be installed)"
    echo
    echo "  dev [--optimize]"
    echo "      Build and serve the wasm binary and supporting files."
    echo "      --optimize - use the TinyGo compiler to produce a much smaller binary (~10%)"
    echo "                   (TinyGo has to be installed)"
    exit 1
fi

# Build the wasm binary.
# Input:
#   - $1 (optional): `optimize` to use the TinyGo compiler
build_wasm() {
    echo ">>> Building Wasm"
    rm -rf build
    mkdir -p build
    if [ $# -gt 0 ] ; then
        case $1 in
            "--optimize")
                echo "    Using TinyGo compiler and optimize the output"
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
    wasm_size=$(stat -c%s build/main.wasm)
    if [ $wasm_size -gt 1048576 ]; then
        echo "Warning: main.wasm is larger than 1MB"
    fi
    echo "main.wasm: $((wasm_size / 1024)) KB"
}

cmd=$1
shift
case "$cmd" in
    build)
        build_wasm "$@"
        ;;
    dev)
        build_wasm "$@"
        echo ">>> Serving wasm"
        python3 -m http.server 8000 --directory .
        ;;
    *)
        echo "Unknown target: $cmd"
        exit 1
        ;;
esac
