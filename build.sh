#!/bin/sh
# CLI to build, test, and develop the project.

set -eu
root=$(cd $(dirname $0) && pwd)

# Read .env file if it exists.
[ -f "$root/.env" ] && . "$root/.env"

if [ $# -eq 0 ]; then
    echo "Usage: $0 build|fmt|lint|test"
    echo
    echo "  build [target]"
    echo "      Build the target. If no target is specified, build all targets."
    echo "      Available targets:"
    echo "        cli - build the CLI as \`./cling-sync\`"
    echo "        wasm - build the wasm binary"
    echo
    echo "  fmt [project]"
    echo "      Format code. If no project is specified, format all projects."
    echo
    echo "  lint [project]"
    echo "      Lint code. If no project is specified, lint all projects."
    echo
    echo "  precommit"
    echo "      Run all checks before committing. This is the same as running \`fmt\`, \`lint\`, and \`test\`."
    echo
    echo "  test [project|integration-bash]"
    echo "      Run tests. If no project is specified, run all tests including integration tests."
    echo
    echo "  tools"
    echo "      Build tools needed for development."
    exit 1
fi

projects="lib workspace http cli wasm test"

# Build all or a specific target.
# Input:
#   - $1: target (optional, if not specified, build all targets)
run_build() {
    local targets="cli wasm"
    if [ $# -gt 0 ]; then
        targets="$1"
        shift
    fi
    for target in $targets; do
        case "$target" in
            cli)
                echo ">>> Building CLI"
                go build "$@" -o cling-sync ./cli
                if [ -n "${CS_DARWIN_CODESIGN:-}" ] && [ "$(uname -s)" = "Darwin" ]; then
                    echo "Codesigning CLI"
                    codesign --sign "${CS_DARWIN_CODESIGN}" --force --options runtime ./cling-sync
                fi
                ;;
            wasm)
                bash wasm/build.sh build "$@"
                ;;
            *)
                echo "Unknown target: $target"
                exit 1
                ;;
        esac
    done
}

# Run a command for all or a specific project.
# Input:
#   - $1: command
#   - $2: project (optional, if not specified, run for all projects)
run_project_cmd() {
    local cmd="$1"
    if [ $# -gt 1 ]; then
        projects="$2"
    fi
    for project in $projects; do
        cd "$project"
        echo "$project"
        eval "$cmd"
        cd ..
    done
}

build_tools() {
    local expected_version="2.1.2"
    
    # Check if tools/golangci-lint exists and has the correct version.
    if [ -f tools/golangci-lint ]; then
        local current_version=$(tools/golangci-lint --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
        if [ "$current_version" = "$expected_version" ]; then
            return
        fi
    fi

    echo ">>> Downloading golangci-lint v$expected_version"
    local os arch
    case "$(uname -s)" in
        Darwin) os="darwin" ;;
        Linux) os="linux" ;;
        *) echo "Unsupported OS: $(uname -s)"; exit 1 ;;
    esac
    
    case "$(uname -m)" in
        x86_64) arch="amd64" ;;
        arm64|aarch64) arch="arm64" ;;
        *) echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
    esac
    
    local filename="golangci-lint-${expected_version}-${os}-${arch}.tar.gz"
    local url="https://github.com/golangci/golangci-lint/releases/download/v${expected_version}/${filename}"
    
    # Download and extract directly to tools/golangci-lint.
    curl -SsL "$url" | tar xzO --wildcards '*/golangci-lint' > "$root/tools/golangci-lint"
    chmod +x "$root/tools/golangci-lint"
}

cmd=$1
shift
case "$cmd" in
    build)
        run_build "$@"
        ;;
    tools)
        build_tools
        ;;
    fmt)
        if [ $# -gt 0 ] && [ "$1" = "wasm" ]; then
            shift
            bash wasm/build.sh fmt "$@"
        else
            build_tools
            echo ">>> Formatting code"
            run_project_cmd "$root/tools/golangci-lint fmt" "$@"
            bash wasm/build.sh fmt
        fi
        ;;
    lint)
        if [ $# -gt 0 ] && [ "$1" = "wasm" ]; then
            shift
            bash wasm/build.sh lint "$@"
        else
            build_tools
            echo ">>> Linting code"
            run_project_cmd "$root/tools/golangci-lint run" "$@"
            bash wasm/build.sh lint
        fi
        ;;
    test)
        echo ">>> Running tests"
        if [ $# -gt 0 ] && [ "$1" = "integration-bash" ]; then
            bash test/test.sh
            exit 0
        fi
        run_project_cmd "go test ./... -count 1" "$@"
        ;;
    precommit)
        bash $0 fmt "$@"
        bash $0 lint "$@"
        bash $0 test "$@"
        # Run the integration bash tests if there is no target project specified.
        # We run the bash tests even though they are not the main integration tests
        # (the Go ones are) just to make sure they work.
        if [ $# -eq 0 ]; then
            bash $0 test integration-bash
        fi
        echo
        echo "Looks perfect, go ahead and commit this beauty."
        ;;
    *)
        echo "Unknown target: $cmd"
        exit 1
        ;;
esac
