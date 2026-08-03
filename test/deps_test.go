package main

import (
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// What each package pulls in transitively. A failure means a dependency edge
// changed, so decide whether the new edge is wanted before updating the table.
var wantDeps = map[string]packageDeps{ //nolint:gochecknoglobals
	"./lib": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync", "golang.org/x/sys"},
		internal: []string{},
		env:      nil,
	},
	"./http": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync", "golang.org/x/sys"},
		internal: []string{"lib"},
		env:      nil,
	},
	"./workspace": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync", "golang.org/x/sys"},
		internal: []string{"http", "lib"},
		env:      nil,
	},
	"./keychain": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync", "golang.org/x/sys"},
		internal: []string{"lib"},
		env:      nil,
	},
	"./cmd/cling-sync": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync", "golang.org/x/sys", "golang.org/x/term"},
		internal: []string{"http", "keychain", "lib", "workspace"},
		env:      nil,
	},
	"./wasm": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync"},
		internal: []string{"http", "lib", "workspace"},
		env:      []string{"GOOS=js", "GOARCH=wasm"},
	},
	"./wasm/cmd": {
		modules:  []string{"golang.org/x/crypto", "golang.org/x/sync"},
		internal: []string{"http", "lib", "wasm", "workspace"},
		env:      []string{"GOOS=js", "GOARCH=wasm"},
	},
}

const modulePath = "github.com/cling-com/cling-sync"

type packageDeps struct {
	// External modules, excluding the standard library and cling-sync itself.
	modules []string
	// cling-sync packages, without the module path prefix.
	internal []string
	// Extra environment for `go list`, for packages that only build elsewhere.
	env []string
}

func TestPackageDependencies(t *testing.T) {
	t.Parallel()
	for pkg, want := range wantDeps {
		t.Run(pkg, func(t *testing.T) {
			t.Parallel()
			modules, internal := listDeps(t, pkg, want.env)
			if !reflect.DeepEqual(modules, want.modules) {
				t.Errorf("%s external modules:\n  want %v\n  got  %v", pkg, want.modules, modules)
			}
			if !reflect.DeepEqual(internal, want.internal) {
				t.Errorf("%s cling-sync packages:\n  want %v\n  got  %v", pkg, want.internal, internal)
			}
		})
	}
}

// listDeps returns the sorted transitive dependencies of `pkg`, split into
// external modules and cling-sync packages. The package itself is excluded.
func listDeps(t *testing.T, pkg string, env []string) ([]string, []string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "go", "list", "-deps",
		"-f", "{{with .Module}}{{.Path}}{{end}}|{{.ImportPath}}", pkg)
	cmd.Dir = ".."
	cmd.Env = append(os.Environ(), env...) //nolint:forbidigo
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pkg, err, out)
	}
	modules := []string{}
	internal := []string{}
	self := modulePath + strings.TrimPrefix(pkg, ".")
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		module, importPath, _ := strings.Cut(line, "|")
		switch {
		case importPath == self:
		case strings.HasPrefix(importPath, modulePath+"/"):
			internal = append(internal, strings.TrimPrefix(importPath, modulePath+"/"))
		case module != "" && module != modulePath:
			modules = append(modules, module)
		}
	}
	slices.Sort(modules)
	slices.Sort(internal)
	return slices.Compact(modules), slices.Compact(internal)
}
