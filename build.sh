#!/bin/sh
# CLI to build, test, and develop the project.

set -eu

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
    echo "  test [project|integration]"
    echo "      Run tests. If no project is specified, run all tests including integration tests."
    exit 1
fi

projects="lib workspace cli"

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

cmd=$1
shift
case "$cmd" in
    build)
        run_build "$@"
        ;;
    fmt)
        echo ">>> Formatting code"
        run_project_cmd "go tool golangci-lint fmt" "$@"
        ;;
    lint)
        echo ">>> Linting code"
        run_project_cmd "go tool golangci-lint run" "$@"
        ;;
    test)
        echo ">>> Running tests"
        if [ $# -gt 0 ]; then
            if [ "$1" == "integration" ]; then
                bash test/test.sh
            fi
            exit 0
        fi
        run_project_cmd "go test ./... -count 1" "$@"
        ;;
    precommit)
        bash $0 fmt "$@"
        bash $0 lint "$@"
        bash $0 test "$@"
        # Run the integration tests if the target project is `cli` or none.
        if [ $# -eq 0 ] || [ "$1" == "cli" ]; then
            bash $0 test integration
        fi
        echo
        echo "Looks perfect, go ahead and commit this beauty."
        ;;
    *)
        echo "Unknown target: $cmd"
        exit 1
        ;;
esac
