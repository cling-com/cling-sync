#!/bin/sh
# CLI to build, test, and develop the project.

set -eu
root=$(cd $(dirname $0) && pwd)

if [ $# -eq 0 ]; then
    echo "Usage: $0 build|fmt|lint|test"
    echo
    echo "  build [target]"
    echo "      Build the target. If no target is specified, build all targets."
    echo "      Available targets:"
    echo "        cli - build the CLI as \`./cling-sync\`"
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
    echo "  test [project|integration]"
    echo "      Run tests. If no project is specified, run all tests including integration tests."
    echo 
    echo "  tools"
    echo "      Build tools needed for development."
    exit 1
fi

projects="lib workspace http cli test"

# Build all or a specific target.
# Input:
#   - $1: target (optional, if not specified, build all targets)
run_build() {
    local targets=cli
    if [ $# -gt 0 ]; then
        targets="$1"
        shift
    fi
    for target in $targets; do
        case "$target" in
            cli)
                echo ">>> Building CLI"
                go build -o cling-sync ./cli
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
    if [ -f tools/golangci-lint ]; then
        return
    fi
    echo ">>> Building golangci-lint"
    local tmp_dir=$(mktemp -d)
    cp tools/golangci-lint-*.tar.gz "$tmp_dir"
    cd "$tmp_dir"
    tar xzf golangci-lint-*.tar.gz
    cd golangci-lint-*
    go build -o "$root/tools/golangci-lint" ./cmd/golangci-lint
    cd "$root"
    rm -rf "$tmp_dir"
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
        build_tools
        echo ">>> Formatting code"
        run_project_cmd "$root/tools/golangci-lint fmt" "$@"
        ;;
    lint)
        build_tools
        echo ">>> Linting code"
        run_project_cmd "$root/tools/golangci-lint run" "$@"
        ;;
    test)
        echo ">>> Running tests"
        if [ $# -gt 0 ] && [ "$1" = "integration-bash" ]; then
            bash test/test.sh
            exit 0
        fi
        run_project_cmd "go test -mod vendor ./... -count 1" "$@"
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
