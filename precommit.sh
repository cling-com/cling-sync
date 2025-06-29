#!/bin/sh
# Execute everything to make sure we are ready to commit.

set -eu

run() {
    echo "$1: go mod tidy"
    (cd "$1" && go mod tidy)
    echo "$1: fmt"
    sh ./fmt.sh "$1"
    echo "$1: lint"
    sh ./lint.sh "$1"
    echo "$1: test"
    sh ./test.sh "$1"
}

if [ $# -eq 0 ]; then
    for mod in lib workspace cli; do
        run "$mod"
    done
    echo "integration tests"
    bash test/test.sh > /dev/null
else
    run "$1"
fi

echo "\nLooks perfect, go ahead and commit this beauty."
