# Integration Tests

Integration tests run against a real repository using the CLI.

The main tests are written in Go so they are portable.

The Bash integration tests (`test.sh`) are mainly for quick experiments.

## Running the tests

Run the Go tests from the repository root:

    ./build.sh test test

Run the Bash tests from this directory:

    ./test.sh

Run the tests (Go and Bash) in a Docker container:

    ./docker.sh
