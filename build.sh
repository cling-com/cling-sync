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
    echo "  release check|tag|build|upload|all"
    echo "      Tag, build, and publish a new patch release. Run on darwin."
    echo "        check  - verify HEAD has a green CI build on GitHub"
    echo "        tag    - tag HEAD with the next patch version (latest + 1)"
    echo "        build  - build the CLI for darwin and linux (arm64 and amd64) and a"
    echo "                 source archive for the current tag into ./dist"
    echo "        upload - push the current tag and publish ./dist as a GitHub release"
    echo "        all    - run check, tag, build, and upload in order"
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
    echo "  test [--race] [project|integration-bash]"
    echo "      Run tests. If no project is specified, run all tests including integration tests."
    echo "      Pass \`--race\` to run the Go tests under the race detector."
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

# Per-platform tool cache (<os>-<arch>, matching Go's GOOS-GOARCH so the protoc
# test helpers resolve it).
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64|Darwin-aarch64) tools_platform="darwin-arm64" ;;
    Darwin-x86_64)               tools_platform="darwin-amd64" ;;
    Linux-arm64|Linux-aarch64)   tools_platform="linux-arm64" ;;
    Linux-x86_64)                tools_platform="linux-amd64" ;;
    *) echo "Unsupported platform: $(uname -s)-$(uname -m)"; exit 1 ;;
esac
tools_dir="$root/tools/$tools_platform"
export CLING_GOLANGCI_LINT="$tools_dir/golangci-lint"

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
                echo ">>> Building CLI ($(go env GOOS)/$(go env GOARCH))"
                go build "$@" -o cling-sync ./cli
                if [ -n "${CS_DARWIN_CODESIGN:-}" ] && [ "$(uname -s)" = "Darwin" ] && [ "$(go env GOOS)" = "darwin" ]; then
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

# Latest vMAJOR.MINOR.PATCH release tag, or empty if there are none.
latest_version() {
    git for-each-ref --sort=-v:refname --format='%(refname:short)' 'refs/tags/*' \
        | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -n1
}

# Verify the current HEAD has a green CI build on GitHub.
run_check_release() {
    cd "$root"
    command -v gh >/dev/null 2>&1 || { echo "gh (GitHub CLI) is not installed"; exit 1; }
    local sha state
    sha=$(git rev-parse HEAD)
    echo ">>> Checking CI status for $sha"
    state=$(gh api "repos/{owner}/{repo}/commits/$sha/check-runs" --jq '
        if (.check_runs | length) == 0 then "none"
        elif any(.check_runs[]; .status != "completed") then "pending"
        elif all(.check_runs[]; .conclusion == "success" or .conclusion == "skipped") then "success"
        else "failure"
        end') || { echo "Could not query CI status for HEAD. Is it pushed to GitHub?"; exit 1; }
    case "$state" in
        success) echo "    CI is green" ;;
        none)    echo "No CI build found for HEAD. Push it and wait for CI."; exit 1 ;;
        pending) echo "CI is still running for HEAD. Wait for it to finish."; exit 1 ;;
        *)       echo "CI is not green for HEAD ($state)."; exit 1 ;;
    esac
}

# Tag HEAD with the next patch version: the latest release tag with its patch
# bumped.
run_tag_release() {
    cd "$root"
    if [ -n "$(git status --porcelain)" ]; then
        echo "Working tree is not clean. Commit or stash your changes before releasing."
        exit 1
    fi
    local latest ver major minor patch new
    latest=$(latest_version)
    [ -n "$latest" ] || { echo "No release tag found. Create the first release tag by hand."; exit 1; }
    ver="${latest#v}"
    major="${ver%%.*}"
    minor="${ver#*.}"; minor="${minor%%.*}"
    patch="${ver##*.}"
    new="v$major.$minor.$((patch + 1))"
    echo ">>> Tagging $new (previous: $latest)"
    git tag "$new"
}

# Build the CLI for darwin and linux (arm64 and amd64) and a source archive for
# the current release tag into ./dist.
run_build_release() {
    cd "$root"
    command -v podman >/dev/null 2>&1 || { echo "podman is required to run the linux binaries"; exit 1; }
    local version
    version=$(latest_version)
    [ -n "$version" ] || { echo "No release tag found. Run \`release tag\` first."; exit 1; }
    echo ">>> Building release artifacts for $version"
    rm -rf dist
    mkdir -p dist
    local platform os arch cgo bin help
    for platform in darwin/arm64 darwin/amd64 linux/arm64 linux/amd64; do
        os="${platform%/*}"
        arch="${platform#*/}"
        # The darwin keychain backend needs cgo; the linux one does not, and cgo
        # cannot cross-compile to linux from darwin.
        cgo=0
        [ "$os" = "darwin" ] && cgo=1
        GOOS="$os" GOARCH="$arch" CGO_ENABLED="$cgo" \
            bash "$root/build.sh" build cli -ldflags "-X main.version=$version"
        bin="cling-sync-$os-$arch"
        mv cling-sync "dist/$bin"
        # Run each binary and confirm it reports the release version.
        case "$os" in
            darwin) help=$("$root/dist/$bin" --help 2>&1 || true) ;;
            linux)  help=$(podman run --rm --platform "linux/$arch" -v "$root/dist:/dist:ro" \
                        docker.io/library/alpine "/dist/$bin" --help 2>&1 || true) ;;
        esac
        echo "$help" | grep -qF "$version" || { echo "FAIL: $bin did not report $version"; exit 1; }
        echo "    $bin runs and reports $version"
    done
    git archive --format=tar.gz --prefix="cling-sync-$version/" -o "dist/cling-sync-$version-src.tgz" "$version"
}

# Push the current release tag and publish ./dist as a GitHub release.
run_upload_release() {
    cd "$root"
    command -v gh >/dev/null 2>&1 || { echo "gh (GitHub CLI) is not installed"; exit 1; }
    local version prev
    version=$(latest_version)
    [ -n "$version" ] || { echo "No release tag found. Run \`release tag\` first."; exit 1; }
    echo ">>> Pushing git tag $version"
    git push origin "$version"
    echo ">>> Uploading release $version"
    # Release notes are the commit log since the previous release tag.
    prev=$(git for-each-ref --sort=-v:refname --format='%(refname:short)' 'refs/tags/*' \
        | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sed -n '2p')
    git log --format='- %s (%h) by %an' "$prev..$version" \
        | gh release create "$version" dist/* --title "$version" --notes-file -
    echo
    echo "Released $version"
}

run_release() {
    case "${1:-}" in
        check)  run_check_release ;;
        tag)    run_tag_release ;;
        build)  run_build_release ;;
        upload) run_upload_release ;;
        all)
            run_check_release
            run_tag_release
            run_build_release
            run_upload_release
            ;;
        *)
            echo "Usage: $0 release check|tag|build|upload|all"
            exit 1
            ;;
    esac
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
    # Install golangci-lint.
    local lint_version="2.11.2"
    local lint_current=""
    if [ -f "$tools_dir/golangci-lint" ]; then
        lint_current=$("$tools_dir/golangci-lint" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
    fi
    if [ "$lint_current" != "$lint_version" ]; then
        echo ">>> Downloading golangci-lint v$lint_version"
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
        local dirname="golangci-lint-${lint_version}-${os}-${arch}"
        local url="https://github.com/golangci/golangci-lint/releases/download/v${lint_version}/${dirname}.tar.gz"
        mkdir -p "$tools_dir"
        curl -SsL "$url" | tar xzO "${dirname}/golangci-lint" > "$tools_dir/golangci-lint"
        chmod +x "$tools_dir/golangci-lint"
    fi

    # Install protoc.
    local protoc_version="29.3"
    local protoc_current=""
    if [ -x "$tools_dir/protoc/bin/protoc" ]; then
        protoc_current=$("$tools_dir/protoc/bin/protoc" --version 2>/dev/null | awk '{print $2}')
    fi
    if [ "$protoc_current" != "$protoc_version" ]; then
        echo ">>> Downloading protoc v$protoc_version"
        local os arch
        case "$(uname -s)" in
            Darwin) os="osx" ;;
            Linux) os="linux" ;;
            *) echo "Unsupported OS: $(uname -s)"; exit 1 ;;
        esac
        case "$(uname -m)" in
            x86_64) arch="x86_64" ;;
            arm64|aarch64) arch="aarch_64" ;;
            *) echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
        esac
        local filename="protoc-${protoc_version}-${os}-${arch}.zip"
        local url="https://github.com/protocolbuffers/protobuf/releases/download/v${protoc_version}/${filename}"
        rm -rf "$tools_dir/protoc"
        mkdir -p "$tools_dir/protoc"
        local zip=$(mktemp)
        curl -SsL "$url" -o "$zip"
        unzip -q "$zip" -d "$tools_dir/protoc"
        rm -f "$zip"
    fi
}

cmd=$1
shift
case "$cmd" in
    build)
        run_build "$@"
        ;;
    release)
        run_release "$@"
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
            run_project_cmd "$tools_dir/golangci-lint fmt" "$@"
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
            run_project_cmd "$tools_dir/golangci-lint run" "$@"
            bash wasm/build.sh lint
        fi
        ;;
    test)
        race=""
        if [ "${1:-}" = "--race" ]; then
            race="-race -timeout 30m"
            shift
        fi
        if [ $# -gt 0 ] && [ "$1" = "integration-bash" ]; then
            echo ">>> Running tests"
            bash test/test.sh
            exit 0
        fi
        build_tools
        echo ">>> Running tests${race:+ (race)}"
        run_project_cmd "go test ./... -count 1 $race" "$@"
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
