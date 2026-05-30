//go:build !wasm

// Native driver for the wasm tests: builds the wasm test binary once per
// available compiler (Go always, TinyGo when installed) and runs it in Node.js.
//
// Build tags pick what compiles, since TinyGo builds a whole package, not a
// file list:
//
//	wasm && test && <check>  test build: testwasm.go harness + one *_check.go,
//	                         selected by <check> (checkrepo or checkhttp)
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// RunWasmTests builds and runs the checks selected by `checkTag` (e.g.
// "checkrepo") under each available compiler as a subtest. `extraEnv` is passed
// to Node.js and read there via `process.env.<NAME>`. See the file header for
// the overall design.
func RunWasmTests(t *testing.T, checkTag string, extraEnv ...string) {
	t.Helper()
	if err := skipIfNodeJSNotInstalled(t.Context()); err != nil {
		t.Skip(err.Error())
	}
	for _, c := range wasmCompilers() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if reason := c.skip(); reason != "" {
				t.Skip(reason)
			}
			wasmPath := filepath.Join(t.TempDir(), "main_test.wasm")
			c.build(t, wasmPath, "test "+checkTag)
			runNodeJS(t, c.wasmExecJS(t), wasmPath, extraEnv)
		})
	}
}

type wasmCompiler struct {
	name       string
	skip       func() string
	build      func(t *testing.T, outPath, tags string)
	wasmExecJS func(t *testing.T) string
}

func wasmCompilers() []wasmCompiler {
	return []wasmCompiler{
		{
			name: "go",
			skip: func() string { return "" },
			build: func(t *testing.T, outPath, tags string) {
				t.Helper()
				runBuild(t, "go", "build", "-tags", tags, "-o", outPath, ".")
			},
			wasmExecJS: func(t *testing.T) string {
				t.Helper()
				return filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js") //nolint:staticcheck
			},
		},
		{
			name: "tinygo",
			skip: func() string {
				if _, err := exec.LookPath("tinygo"); err != nil {
					return "TinyGo not installed"
				}
				return ""
			},
			build: func(t *testing.T, outPath, tags string) {
				t.Helper()
				runBuild(t, "tinygo", "build", "-no-debug", "-tags", tags, "-o", outPath, ".")
			},
			wasmExecJS: func(t *testing.T) string {
				t.Helper()
				out, err := exec.CommandContext(t.Context(), "tinygo", "env", "TINYGOROOT").Output()
				if err != nil {
					t.Fatalf("Failed to find TINYGOROOT: %v", err)
				}
				return filepath.Join(strings.TrimSpace(string(out)), "targets", "wasm_exec.js")
			},
		},
	}
}

func runBuild(t *testing.T, tool string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), tool, args...)
	cmd.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm") //nolint:forbidigo
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build Wasm with %s: %v\n%s", tool, err, output)
	}
}

func runNodeJS(t *testing.T, wasmExecJS, wasmPath string, extraEnv []string) { //nolint:funlen
	t.Helper()
	nodeJSScript := `
		(async () => {
			const fs = require('fs');
			const process = require('process');

			// Load the Wasm support file (Go or TinyGo flavour).
			require(process.env.WASM_EXEC_JS);

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
	cmd := exec.CommandContext(t.Context(), "node", "-")
	cmd.Env = append([]string{
		"WASM_EXEC_JS=" + wasmExecJS,
		"WASM_BINARY=" + wasmPath,
	}, extraEnv...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to create stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("Failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("Failed to create stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}
	if _, err := stdin.Write([]byte(nodeJSScript)); err != nil {
		t.Fatalf("Failed to write script to stdin: %v", err)
	}
	_ = stdin.Close()

	// Stream stdout.
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			t.Log(scanner.Text())
		}
	}()

	// Stream stderr.
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Log("stderr:", scanner.Text())
		}
	}()

	// Wait for completion.
	if err := cmd.Wait(); err != nil {
		t.Fatalf("Node.js test failed: %v", err)
	}
}

func skipIfNodeJSNotInstalled(ctx context.Context) error {
	// Test if Node.js v22 is installed.
	cmd := exec.CommandContext(ctx, "node", "--version")
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
