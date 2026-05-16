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
    echo "  gen [project]"
    echo "      Run \`go generate\`. If no project is specified, generate all projects."
    echo
    echo "  fmt [project]"
    echo "      Format code. If no project is specified, format all projects."
    echo
    echo "  lint [project]"
    echo "      Lint code. If no project is specified, lint all projects."
    echo
    echo "  precommit"
    echo "      Run all checks before committing. This is the same as running \`gen\`, \`fmt\`, \`lint\`, and \`test\`."
    echo
    echo "  test [project|integration-bash]"
    echo "      Run tests. If no project is specified, run all tests including integration tests."
    echo
    echo "  bench [project]"
    echo "      Run all benchmarks. If no project is specified, run for all projects."
    echo
    echo "  fuzz [project] [duration]"
    echo "      Run each fuzz target sequentially for the given duration (default 30s)."
    echo "      If no project is specified, run for all projects with fuzz targets."
    echo
    echo "  tools"
    echo "      Build tools needed for development."
    exit 1
fi

projects="lib workspace http cli wasm test"

# Reformat raw `go test -bench` output into an aligned table with humanized
# units (ns/us/ms/s, B/KB/MB/GB) and comma-separated counters. Non-benchmark
# lines are dropped so the file diffs cleanly run-to-run.
format_bench() {
    awk '
function commify(n,   s, i) {
    s = sprintf("%d", n + 0)
    for (i = length(s) - 3; i > 0; i -= 3) s = substr(s, 1, i) "," substr(s, i + 1)
    return s
}
function fmt_time(n) {
    if (n >= 1e9) return sprintf("%.2f s",  n / 1e9)
    if (n >= 1e6) return sprintf("%.2f ms", n / 1e6)
    if (n >= 1e3) return sprintf("%.2f us", n / 1e3)
    return sprintf("%.1f ns", n + 0)
}
function fmt_bytes(n) {
    if (n >= 2^30) return sprintf("%.2f GB", n / 2^30)
    if (n >= 2^20) return sprintf("%.2f MB", n / 2^20)
    if (n >= 2^10) return sprintf("%.2f KB", n / 2^10)
    return sprintf("%d B", n + 0)
}
/^Benchmark/ {
    sub(/-[0-9]+$/, "", $1); sub(/^Benchmark/, "", $1)
    tm = thr = mem = alloc = ""
    for (i = 3; i <= NF; i++) {
        if      ($i == "ns/op")     tm    = fmt_time($(i-1))
        else if ($i == "MB/s")      thr   = sprintf("%.1f MB/s", $(i-1))
        else if ($i == "B/op")      mem   = fmt_bytes($(i-1)) "/op"
        else if ($i == "allocs/op") alloc = commify($(i-1)) " allocs/op"
    }
    printf "  %-34s %12s  %11s  %12s  %14s  %20s\n", $1, commify($2), tm, thr, mem, alloc
}'
}

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
    local expected_version="2.11.2"
    
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
    gen)
        echo ">>> Generating code"
        run_project_cmd "go generate" "$@"
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
    bench)
        echo ">>> Running benchmarks"
        if [ $# -gt 0 ]; then
            (cd "$1" && go test ./... -run '^$' -bench . -benchmem -count 1) | format_bench
        else
            out="$root/benchmarks.txt"
            : > "$out"
            for project in $projects; do
                [ -d "$project" ] || continue
                (cd "$project" && go test -list '^Benchmark' ./... 2>/dev/null | grep -qE '^Benchmark') || continue
                {
                    printf '\n## %s\n\n' "$project"
                    (cd "$project" && go test ./... -run '^$' -bench . -benchmem -count 1) | format_bench
                } | tee -a "$out"
            done
            echo
            echo "Results written to $out"
        fi
        ;;
    fuzz)
        fuzz_dur="${2:-30s}"
        if [ $# -gt 0 ]; then
            proj_list="$1"
        else
            proj_list="$projects"
        fi
        echo ">>> Running fuzz tests (${fuzz_dur} per target)"
        for project in $proj_list; do
            [ -d "$project" ] || continue
            targets=$(cd "$project" && go test -list '^Fuzz' ./... 2>/dev/null | grep -E '^Fuzz' || true)
            [ -n "$targets" ] || continue
            echo "$project"
            for target in $targets; do
                echo "  >>> $target"
                (cd "$project" && go test ./... -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzz_dur") || true
            done
        done
        ;;
    precommit)
        bash $0 gen "$@"
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
