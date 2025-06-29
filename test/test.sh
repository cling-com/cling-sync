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
    cd ..
    ./build.sh build cli >/dev/null
    cd test
    rm -rf work

    mkdir work
    cd work
    mkdir workspace repository
    cd workspace
    echo -n "$passphrase" | cling-sync --passphrase-from-stdin init ../repository
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

log $cyan">>> Creating repository"
    setup

log $cyan">>> Save the repository keys"
    cmd "echo -n '$passphrase' | cling-sync --passphrase-from-stdin security save-keys"

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
