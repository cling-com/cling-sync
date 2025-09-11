#!/usr/bin/env bash
# Quick integration tests using the CLI.
#
# We keep this script around to facilitate quick test experiments.
# But because this is a Bash script and thus not really portable (ehem, Windows),
# we don't want to rely on it and have the "real" integration tests in Go.

set -eu

cd "$(dirname "$0")"
cwd="$(pwd)"
PATH="$(pwd)/../:$PATH"

green="\033[32m"
red="\033[31m"
yellow="\033[33m"
grey="\033[90m"
cyan="\033[36m"
quiet=0
passphrase="testpassphrase"

# >>> Helper functions

log() {
    if [ $quiet -eq 0 ]; then
        echo -e "$@""\033[0m"
    fi
}

error() {
    echo -e "$@""\033[0m" >&2
}

setup() {
    cd "$cwd"

    log $cyan">>> Creating repository"
        cd ..
        local build_args=""
        if [ -z "${CS_TEST_NO_MOCK:-}" ]; then
            echo "Building with mock support"
            build_args="-tags mock"
        fi
        ./build.sh build cli $build_args >/dev/null
        cd test
        rm -rf work

        mkdir work
        cd work
        mkdir workspace repository
        cd workspace
        echo -n "$passphrase" | cling-sync --passphrase-from-stdin init ../repository

    log $cyan">>> Save the repository keys"
        cmd "echo -n '$passphrase' | cling-sync --passphrase-from-stdin security save-keys"
}

cmd() {
    log $grey"$@"
    eval "$@"
}

assert() {
    set +e
    local diff=$(
        diff -u \
            --label actual <(quiet=0 eval "$1") \
            --label expected <(quiet=0 eval "$2") \
        | grep -v "@@")
    set -e
    if [ "$diff" != "" ]; then
        error $red"assertion failed: \"$1\" != \"$2\""
        error "$diff"
        exit 1
    fi
    log $grey"assert \`$1\` == \`$2\`"
}

# Format the output so that it can be compared with `repository_ls`.
workspace_ls() {
    # The `awk` is used to set the directory size to zero and add a trailing slash to directories.
    find . -mindepth 1 -not -path './.cling*' -printf '%M %12s %T@  %P\n' \
        | awk '/^d/ {$2=0;$4=$4"/"} {printf "%s %12s %s  %s\n", $1, $2, $3, $4}' \
        | sort -k 4
}

repository_head() {
    cling-sync log --short | head -n 1 | awk '{print $1}'
}

repository_head_date() {
    cling-sync log --short | head -n 1 | awk '{print $2}'
}

# Format the output so that it can be compared with `workspace_ls`.
repository_ls() {
    cling-sync ls --short-file-mode --timestamp-format unix-fraction "$@" | sort -k 4
}

# >>> Tests

test_basic_scenario() {
    setup

    log $cyan">>> Merge empty repository and workspace"
        cmd cling-sync merge --no-progress

        log $green">>> There should be no revision"
        assert "cling-sync log --short" "echo 'No revisions'"

    log $cyan"\n>>> Add some files and merge"
        cmd "echo 'a' > a.txt"
        cmd "echo 'b' > b.txt"
        cmd "mkdir dir1"
        cmd "echo 'c' > dir1/c.txt"
        cling-sync merge --no-progress --message "first commit"

        log $green">>> A revision should have been created"
        assert "cling-sync log --short | wc -l" "echo 1"

        log $green">>> Files of head should match the workspace"
        assert "workspace_ls" "repository_ls"

        log $green">>> There should be no local changes"
        assert "cling-sync status" "echo 'No changes'"

        # Remember the revision id and the ls output for later.
        rev1_id=$(repository_head)
        rev1_date=$(repository_head_date)
        rev1_ls=$(repository_ls)

    log $cyan"\n>>> Remove a local file, change mtime of another, create a new file and merge"
        cmd "rm a.txt"
        cmd "echo bb > b.txt"
        cmd "touch -d '2025-06-28T13:33:50+02:00' b.txt"
        cmd "echo 'd' > dir1/d.txt"
        cling-sync merge --no-progress --message "second commit"

        log $green">>> A new revision should have been created"
        assert "cling-sync log --short | wc -l" "echo 2"

        log $green">>> Files of head should match the workspace"
        assert "workspace_ls" "repository_ls"

        rev2_id=$(repository_head)
        rev2_date=$(repository_head_date)

    log $cyan"\n>>> List an older revision"
        log $green">>> Listing the first revision should match"
        assert "echo -e \"$rev1_ls\"" "repository_ls --revision $rev1_id"

    log $cyan"\n>>> Log revision history"
        log $green">>> Log should contain the two revisions"
        expected=$(cat <<-EOF
$rev2_id $rev2_date second commit

    D a.txt
    M b.txt
    M dir1/
    A dir1/d.txt

$rev1_id $rev1_date first commit

    A a.txt
    A b.txt
    A dir1/
    A dir1/c.txt
EOF
)
        assert "cling-sync log --short --status" "echo -e \"$expected\""

    log $cyan"\n>>> Copy a file from an older revision"
        log $green">>> \`b.txt\` should contain the current content"
        assert "cat b.txt" "echo bb"

        rm b.txt # Remove the file first, because `cling-sync cp` will not overwrite it.
        cling-sync cp --no-progress --revision $rev1_id b.txt .

        log $green">>> \`b.txt\` should contain the old content"
        assert "cat b.txt" "echo b"

        log $green">>> \`b.txt\` should be marked as modified"
        assert "cling-sync status --no-progress --no-summary" "echo 'M b.txt'"
}

test_load_many_small_revisions() {
    local revisions=5
    log $cyan"\n>>> Load test: Add $revisions revisions"
        load_test $revisions 10 10 20
}

# Add many revisions with many files.
# Input:
#   $1 - number of revisions
#   $2 - number of files per revision
#   $3 - minimum file size in bytes
#   $4 - maximum file size in bytes
load_test() {
    setup

    local revisions=$1
    local files_per_revision=$2
    local filesize_min=$3
    local filesize_max=$4
    local files=0

    for ((i=1; i<=$revisions; i++)); do
        for ((j=1; j<=$files_per_revision; j++)); do
            files=$((files + 1))
            filesize=$(((RANDOM * 10000) % (filesize_max - filesize_min) + filesize_min))
            dd if=/dev/urandom of=load$files.txt bs=$filesize count=1 status=none
        done
        cmd "cling-sync merge --no-progress --message 'load test commit $i'"
    done
    assert "cling-sync log --short | wc -l" "echo $revisions"
}

# >>> Main entry point.

all_test_functions=$(declare -F | awk '{print $3}' | grep "^test_")

match="test_"
if [ $# -gt 0 ]; then
    if [ "$1" == "--help" ]; then
        echo "Usage: $0 [test-function-name] [--list]"
        echo "Run all tests if no test-function-name is given."
        exit 0
    fi
    if [ "$1" == "--list" ]; then
        echo "$all_test_functions"
        exit 0
    fi
    match="$1"
fi

for test_function in $all_test_functions; do
    if [[ $test_function =~ $match ]]; then
    log $yellow"\n>>> Running $test_function"
    $test_function
    fi
done
