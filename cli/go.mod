module github.com/flunderpero/cling-sync/cli

go 1.26.5

require (
	github.com/flunderpero/cling-sync/http v0.0.0
	github.com/flunderpero/cling-sync/lib v0.0.0
	github.com/flunderpero/cling-sync/workspace v0.0.0
	golang.org/x/term v0.45.0
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/flunderpero/cling-sync/lib v0.0.0 => ../lib

replace github.com/flunderpero/cling-sync/workspace v0.0.0 => ../workspace

replace github.com/flunderpero/cling-sync/http v0.0.0 => ../http
