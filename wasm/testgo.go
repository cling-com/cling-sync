// A simple test runner for Wasm that builds a Wasm test binary and runs it in Node.js.
//
// If Node.js v22 or higher is not installed, tests are skipped.
//
// A Wasm test consists of two parts:
//   - a Go test file (my_test.go) that calls `RunWasmTests`
//   - a Wasm test file (my_check.go) that registers tests to
//     be run (see `RegisterTest` in `testwasm.go`)
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Build and run the Wasm tests.
// `srcFiles` is a list of files to compile and is passed to `go build`.
func RunWasmTests(tb testing.TB, srcFiles ...string) {
	tb.Helper()
	if err := skipIfNodeJSNotInstalled(); err != nil {
		tb.Skip(err.Error())
	}
	wasmPath := compile(tb, srcFiles)
	runNodeJS(tb, wasmPath)
}

func runNodeJS(tb testing.TB, wasmPath string) { //nolint:funlen
	tb.Helper()
	nodeJSScript := `
		(async () => {
			const fs = require('fs');
			const path = require('path');
			const process = require('process');

			// Load the Go Wasm support file.
			const wasmExecPath = path.join(process.env.GOROOT, 'lib/wasm/wasm_exec.js');
			require(wasmExecPath);

			// Load the Wasm binary.
			const go = new Go();
			const wasm = await WebAssembly.instantiate(fs.readFileSync(process.env.WASM_BINARY), go.importObject)
			go.run(wasm.instance);

			// Run the test.
			wasm.instance.exports.registerTests();
			try {
				await runTests();
			} catch (err) {
				console.log(err);
				process.exit(1);
			}
			process.exit(0);
		})()`

	// Run the Node.js script from stdin and stream its output.
	cmd := exec.Command("node", "-")
	cmd.Env = []string{"GOROOT=" + runtime.GOROOT(), "WASM_BINARY=" + wasmPath} //nolint:staticcheck
	stdin, err := cmd.StdinPipe()
	if err != nil {
		tb.Fatalf("Failed to create stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		tb.Fatalf("Failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		tb.Fatalf("Failed to create stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		tb.Fatalf("Failed to start node: %v", err)
	}
	if _, err := stdin.Write([]byte(nodeJSScript)); err != nil {
		tb.Fatalf("Failed to write script to stdin: %v", err)
	}
	_ = stdin.Close()

	// Stream stdout.
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			tb.Log(scanner.Text())
		}
	}()

	// Stream stderr.
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			tb.Log("stderr:", scanner.Text())
		}
	}()

	// Wait for completion.
	if err := cmd.Wait(); err != nil {
		tb.Fatalf("Node.js test failed: %v", err)
	}
}

func compile(tb testing.TB, wasmScripts []string) string {
	tb.Helper()
	wasmPath := filepath.Join(tb.TempDir(), "main_test.wasm")
	args := []string{"build", "-o", wasmPath, "./testwasm.go"}
	args = append(args, wasmScripts...)
	buildCmd := exec.Command("go", args...)
	buildCmd.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm") //nolint:forbidigo
	if output, err := buildCmd.CombinedOutput(); err != nil {
		tb.Fatalf("Failed to build Wasm: %v\n%s", err, output)
	}
	return wasmPath
}

func skipIfNodeJSNotInstalled() error {
	// Test if Node.js v22 is installed.
	cmd := exec.Command("node", "--version")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Node.js not installed: %s", output) //nolint:staticcheck
	}
	majorVersion := strings.Split(string(output), ".")[0]
	if majorVersion < "v22" {
		return fmt.Errorf("Node.js version 22 or higher is required, got %s", majorVersion) //nolint:staticcheck
	}
	return nil
}
