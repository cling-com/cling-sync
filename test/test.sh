#!/usr/bin/env bash
# Integration tests using the CLI.

set -eu

cd "$(dirname "$0")"
PATH="$(pwd)/../:$PATH"

green="\033[32m"
red="\033[31m"
yellow="\033[33m"
grey="\033[90m"
cyan="\033[36m"
quiet=0

# >>> Helper functions

log() {
    if [ $quiet -eq 0 ]; then
        echo -e "$@""\033[0m"
    fi
}

error() {
    echo -e "$@""\033[0m" >&2
}

cling_sync() {
    passphrase="testpassphrase"
    command=$1
    shift
    if [ "$command" == "status" ] || [ "$command" == "merge" ]; then
        command=$command" --no-progress"
    fi
    echo -n "$passphrase" | cling-sync --passphrase-from-stdin $command "$@"
}

setup() {
    cd ..
    ./build.sh cli
    cd test
    rm -rf work

    mkdir work
    cd work
    mkdir workspace repository
    cd workspace
    cling_sync init ../repository 
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

ls_workspace() {
    # The `awk` is used to set the directory size to zero and add a trailing slash to directories.
    find . -mindepth 1 -not -path './.cling*' -printf '%M %12s %T@  %P\n' \
        | awk '/^d/ {$2=0;$4=$4"/"} {printf "%s %12s %s  %s\n", $1, $2, $3, $4}' \
        | sort -k 4
}

ls_repository() {
    cling_sync ls --short-file-mode --timestamp-format unix-fraction "$@" | sort -k 4
}

# >>> Tests

log $cyan">>> Creating repository"
    setup

log $cyan">>> Merge empty repository and workspace"
    cling_sync merge

    log $green">>> There should be no revision"
    assert "cling_sync log --short" "echo 'Empty repository'"

log $cyan"\n>>> Add some files and merge"
    cmd "echo 'a' > a.txt"
    cmd "echo 'b' > b.txt"
    cmd "mkdir dir1"
    cmd "echo 'c' > dir1/c.txt"
    cling_sync merge

    log $green">>> A revision should have been created"
    assert "cling_sync log --short | wc -l" "echo 1"

    log $green">>> Files of head should match the workspace"
    assert "ls_workspace" "ls_repository"

    log $green">>> There should be no local changes"
    assert "cling_sync status" "echo 'No changes'"

    # Remember the revision id and the ls output for later.
    rev1_ls=$(ls_repository)
    rev1_id=$(cling_sync log --short | head -n 1 | awk '{print $1}')

log $cyan"\n>>> Remove a local file, change mtime of another, create a new file and merge"
    cmd "rm a.txt"
    cmd "echo bb > b.txt"
    cmd "touch -d '2025-06-28T13:33:50+02:00' b.txt"
    cmd "echo 'd' > dir1/d.txt"
    cling_sync merge

    log $green">>> A new revision should have been created"
    assert "cling_sync log --short | wc -l" "echo 2"

    log $green">>> Files of head should match the workspace"
    assert "ls_workspace" "ls_repository"

log $cyan"\n>>> List an older revision"
    log $green">>> Listing the first revision should match"
    assert "echo -e \"$rev1_ls\"" "ls_repository --revision $rev1_id"

log $cyan"\n>>> Copy a file from an older revision"
    log $green">>> \`b.txt\` should contain the current content"
    assert "cat b.txt" "echo bb"

    rm b.txt # Remove the file first, because `cling_sync cp` will not overwrite it.
    cling_sync cp --revision $rev1_id b.txt .

    log $green">>> \`b.txt\` should contain the old content"
    assert "cat b.txt" "echo b"

    log $green">>> \`b.txt\` should be marked as modified"
    assert "cling_sync status --no-summary" "echo 'M b.txt'"
