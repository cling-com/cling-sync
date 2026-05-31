//go:build !wasm

package workspace

import (
	"bytes"
	"context"
	"regexp"
	"sort"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

const (
	syncTargetsControlFile  = "sync-targets"
	syncTargetSectionPrefix = "repository."
	syncTargetURIKey        = "repository"
)

const syncTargetsHeaderComment = `List of sync-repo targets for this workspace.
Managed by ` + "`cling-sync sync-repo`" + ` (init / add / delete).`

// SyncTarget is one registered sync destination.
type SyncTarget struct {
	Name string
	URI  string
}

var syncTargetNameRegexp = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// ValidateSyncTargetName rejects names that aren't ASCII alphanumeric or '-'.
func ValidateSyncTargetName(name string) error {
	if name == "" {
		return lib.Errorf("sync target name must not be empty")
	}
	if !syncTargetNameRegexp.MatchString(name) {
		return lib.Errorf("sync target name %q must be ASCII alphanumeric or '-'", name)
	}
	return nil
}

// LoadSyncTargets returns the workspace's registered sync targets sorted by name.
func LoadSyncTargets(ctx context.Context, w *Workspace) ([]SyncTarget, error) {
	has, err := w.Storage.HasControlFile(ctx, lib.ControlFileSectionConf, syncTargetsControlFile)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to check for sync targets")
	}
	if !has {
		return nil, nil
	}
	data, err := w.Storage.ReadControlFile(ctx, lib.ControlFileSectionConf, syncTargetsControlFile)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read sync targets")
	}
	toml, err := lib.ReadToml(bytes.NewReader(data))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to parse sync targets")
	}
	targets := make([]SyncTarget, 0, len(toml))
	for section, kvs := range toml {
		name, ok := strings.CutPrefix(section, syncTargetSectionPrefix)
		if !ok {
			return nil, lib.Errorf("unexpected section %q in sync targets", section)
		}
		if err := ValidateSyncTargetName(name); err != nil {
			return nil, lib.WrapErrorf(err, "invalid sync target name in stored config")
		}
		uri, ok := kvs[syncTargetURIKey]
		if !ok {
			return nil, lib.Errorf("sync target %q is missing %q key", name, syncTargetURIKey)
		}
		targets = append(targets, SyncTarget{Name: name, URI: uri})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Name < targets[j].Name })
	return targets, nil
}

// GetSyncTarget looks up a registered target by name. `found` is false if
// no target with that name is registered.
func GetSyncTarget(ctx context.Context, w *Workspace, name string) (uri string, found bool, err error) {
	targets, err := LoadSyncTargets(ctx, w)
	if err != nil {
		return "", false, err
	}
	for _, t := range targets {
		if t.Name == name {
			return t.URI, true, nil
		}
	}
	return "", false, nil
}

// AddSyncTarget registers a new target. Returns an error if `name` is
// invalid, already present, or if the target's repository config does not
// match the workspace's source repository (the sync precondition).
// `passphrase` is forwarded to `OpenStorage` so it can decrypt S3 URIs (both
// `w`'s source URI and the target `uri`). Non-S3 URIs ignore it.
func AddSyncTarget(ctx context.Context, w *Workspace, name, uri string, passphrase []byte) error {
	if err := ValidateSyncTargetName(name); err != nil {
		return err
	}
	if _, found, err := GetSyncTarget(ctx, w, name); err != nil {
		return err
	} else if found {
		return lib.Errorf("sync target %q already exists", name)
	}
	src, err := OpenStorage(string(w.RemoteRepository), passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to open source storage")
	}
	srcToml, err := src.Open(ctx)
	if err != nil {
		return lib.WrapErrorf(err, "failed to read source repository config")
	}
	dst, err := OpenStorage(uri, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to open target storage at %s", uri)
	}
	dstToml, err := dst.Open(ctx)
	if err != nil {
		return lib.WrapErrorf(err, "failed to read target repository config at %s", uri)
	}
	if !srcToml.Eq(dstToml) {
		return lib.Errorf("target repository at %s does not share the same configuration as the source", uri)
	}
	targets, err := LoadSyncTargets(ctx, w)
	if err != nil {
		return err
	}
	targets = append(targets, SyncTarget{Name: name, URI: uri})
	return writeSyncTargets(ctx, w, targets)
}

// DeleteSyncTarget removes the named target. Returns an error if it isn't
// registered.
func DeleteSyncTarget(ctx context.Context, w *Workspace, name string) error {
	targets, err := LoadSyncTargets(ctx, w)
	if err != nil {
		return err
	}
	out := make([]SyncTarget, 0, len(targets))
	found := false
	for _, t := range targets {
		if t.Name == name {
			found = true
			continue
		}
		out = append(out, t)
	}
	if !found {
		return lib.Errorf("sync target %q does not exist", name)
	}
	return writeSyncTargets(ctx, w, out)
}

func writeSyncTargets(ctx context.Context, w *Workspace, targets []SyncTarget) error {
	toml := lib.Toml{}
	for _, t := range targets {
		toml[syncTargetSectionPrefix+t.Name] = map[string]string{syncTargetURIKey: t.URI}
	}
	var buf bytes.Buffer
	if err := lib.WriteToml(&buf, syncTargetsHeaderComment, toml); err != nil {
		return lib.WrapErrorf(err, "failed to encode sync targets")
	}
	if err := w.Storage.WriteControlFile(
		ctx, lib.ControlFileSectionConf, syncTargetsControlFile, buf.Bytes(),
	); err != nil {
		return lib.WrapErrorf(err, "failed to write sync targets")
	}
	return nil
}

// RunSync syncs the workspace's repository to the registered target named
// `name`. The caller drives multi-target iteration and aggregation.
func RunSync(
	ctx context.Context,
	w *Workspace,
	name string,
	monitor lib.RepositorySyncMonitor,
	passphrase []byte,
	workers int,
) error {
	uri, found, err := GetSyncTarget(ctx, w, name)
	if err != nil {
		return lib.WrapErrorf(err, "failed to look up sync target %q", name)
	}
	if !found {
		return lib.Errorf("no sync target named %q", name)
	}
	src, err := OpenStorage(string(w.RemoteRepository), passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to open source storage")
	}
	dst, err := OpenStorage(uri, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to open target storage")
	}
	tempFS, err := w.TempFS.MkSub("sync-repo-" + name)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create temp directory for sync")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	opts := lib.RepositorySyncOptions{Monitor: monitor, Workers: workers}
	if err := lib.SyncRepository(ctx, src, dst, tempFS, opts); err != nil {
		return lib.WrapErrorf(err, "sync to %q failed", name)
	}
	return nil
}
