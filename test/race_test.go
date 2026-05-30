//go:build race

package main

// The integration tests run cling-sync out of process, so `go test -race` only
// instruments the harness. Building the binary with -race (and halting on the
// first race) makes races inside cling-sync itself fail these tests too.
func raceBuildArgs() []string { return []string{"-race"} }

func raceEnv() []string { return []string{"GORACE=halt_on_error=1"} }
