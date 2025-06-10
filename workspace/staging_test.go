package workspace

import (
	"os"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestStaging(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Setup the workspace.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b/c.txt", "cc")
		rt.AddLocal("b/e/f.txt", "fff")
		rt.UpdateLocalMode("b/c.txt", 0o400)

		// Create a remote commit with a modified file, missing files, and a new file.
		commit, err := lib.NewCommit(rt.Repository, t.TempDir())
		assert.NoError(err)
		assert.NoError(commit.Add(fakeRevisionEntryExt("a.txt", lib.RevisionEntryAdd, 0o600, "a")))
		bDirEntry := fakeRevisionEntry("b", lib.RevisionEntryAdd)
		bDirEntry.Metadata = rt.LocalFileMetadata("b")
		assert.NoError(commit.Add(bDirEntry))
		assert.NoError(commit.Add(fakeRevisionEntryExt("b/remote.txt", lib.RevisionEntryAdd, 0o123, "rrr")))
		remoteRev1, err := commit.Commit(fakeCommitInfo())
		assert.NoError(err)
		assert.Equal(remoteRev1, rt.RemoteHead())
		assert.Equal(false, remoteRev1.IsRoot())
		rt.VerifyRevision(remoteRev1, []RevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, "a"},
			{"b", lib.RevisionEntryAdd, 0o700 | os.ModeDir, ""},
			{"b/remote.txt", lib.RevisionEntryAdd, 0o123, "rrr"},
		})

		// Create a staging.
		staging, err := NewStaging(
			rt.Workspace.WorkspacePath, nil, t.TempDir(), NewTestStagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		VerifyRevisionTemp(t, finalized, []RevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, "a"},
			{"b", lib.RevisionEntryAdd, 0o700 | os.ModeDir, ""},
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, "cc"},
			{"b/e", lib.RevisionEntryAdd, 0o700 | os.ModeDir, ""},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, "fff"},
		})

		// Merge the staging with a snapshot of the remote revision.
		snapshot, err := lib.NewRevisionSnapshot(rt.Repository, remoteRev1, t.TempDir())
		assert.NoError(err)
		merged, err := staging.MergeWithSnapshot(snapshot)
		assert.NoError(err)
		VerifyRevisionTemp(t, merged, []RevisionEntryInfo{
			{"a.txt", lib.RevisionEntryUpdate, 0o600, "a"},
			// Note that `b/` did not change (and is hence omitted).
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, "cc"},
			// Metadata of `b/remote.txt` should match the repository version.
			{"b/remote.txt", lib.RevisionEntryDelete, 0o123, "rrr"},
			{"b/e", lib.RevisionEntryAdd, 0o700 | os.ModeDir, ""},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, "fff"},
		})
	})
}

// func TestStagingMergeSnapshotFuzz(t *testing.T) {
// 	t.Parallel()
// 	// Uncomment to test more random seeds.
// 	// seeds := []uint64{}
// 	// for range 50 {
// 	// 	seeds = append(seeds, rand.Uint64()) //nolint:gosec
// 	// }
// 	seeds := []uint64{42, rand.Uint64(), rand.Uint64()} //nolint:gosec
// 	for _, seed := range seeds {
// 		t.Run(fmt.Sprintf("Seed %d", seed), func(t *testing.T) {
// 			t.Parallel()
// 			fuzzStagingMergeSnapshot(t, seed, false)
// 		})
// 	}
// }
//
// // Test with random data.
// // This will create multiple commits with random adds, updates, and deletes.
// // From time to time an empty commit will be added.
// func fuzzStagingMergeSnapshot(t *testing.T, randSeed uint64, debug bool) { //nolint:thelper
// 	const (
// 		steps             = 20
// 		maxFilesPerCommit = 50
// 		numUniqueFiles    = 100
// 		numIgnored        = 10
// 	)
// 	// Create a reproducible RNG.
// 	r := rand.New(rand.NewPCG(randSeed, 0)) //nolint:gosec
// 	rt := NewRepositoryTest(t)
// 	assert := lib.NewAssert(t)
//
// 	revId := rt.RemoteHead()
//
// 	// Our model of the repository.
// 	repoState := map[string]*lib.FileMetadata{}
//
// 	for step := range steps {
// 		if debug {
// 			t.Logf("Step %d", step)
// 		}
//
// 		staged := map[string]*lib.FileMetadata{}
// 		seen := map[string]bool{}
//
// 		// Create random staging entries.
// 		numFiles := r.IntN(maxFilesPerCommit)
// 		for len(staged) != numFiles {
// 			path := fmt.Sprintf("a/%03d.txt", r.IntN(numUniqueFiles))
// 			if seen[path] {
// 				continue // avoid duplicate staging entries
// 			}
// 			seen[path] = true
// 			if repoMd, ok := repoState[path]; ok {
// 				if r.IntN(2) == 1 {
// 					// Update the entry.
// 					md := *repoMd
// 					md.ModeAndPerm += 1
// 					staged[path] = &md
// 				}
// 				// Otherwise omit the entry, so it will be deleted.
// 			} else {
// 				// Add the entry.
// 				staged[path] = fakeFileMetadata(0)
// 			}
// 		}
// 		pathFilter := &lib.PathExclusionFilter{nil, nil} //nolint:composites
// 		if debug && numIgnored > 0 {
// 			t.Log("Excluding")
// 		}
// 		for i := range numIgnored {
// 			pattern, err := lib.NewPathPattern(fmt.Sprintf("a/%03d.txt", i))
// 			assert.NoError(err)
// 			pathFilter.Excludes = append(pathFilter.Excludes, pattern)
// 			if debug {
// 				t.Logf("%3d: %s\n", i, pattern)
// 			}
// 		}
// 		files := make([]RevisionEntryInfo, 0, len(staged))
// 		for path, md := range staged {
// 			files = append(files, RevisionEntryInfo{Path: path, Mode: md.ModeAndPerm.AsFileMode(), Type: lib.RevisionEntryAdd, Content: ""})
// 		}
// 		if debug {
// 			t.Logf("Staging")
// 			slices.SortFunc(files, func(a, b RevisionEntryInfo) int { return strings.Compare(a.Path, b.Path) })
// 			for i, file := range files {
// 				t.Logf("%3d: %s %s\n", i, file.Path, file.Mode)
// 			}
// 		}
// 		nextRevId, err := commitStaging(t, repo, revId, pathFilter, files)
// 		if errors.Is(err, ErrEmptyCommit) {
// 			assert.Equal(0, len(repoState), "repository should be empty")
// 			continue
// 		}
// 		assert.NoError(err)
//
// 		// Filter out the ignored paths from staged.
// 		filteredStaged := map[string]*FileMetadata{}
// 		for path, md := range staged {
// 			if pathFilter.Include(path) {
// 				filteredStaged[path] = md
// 			}
// 		}
// 		staged = filteredStaged
//
// 		// Compare our current model with the real world.
// 		entries := readRevisionSnapshot(t, repo, nextRevId, nil)
// 		if debug {
// 			t.Logf("Revision")
// 			for i, entry := range entries {
// 				t.Logf("%3d: %s %s\n", i, entry.Path, entry.Metadata.ModeAndPerm)
// 			}
// 		}
// 		for _, entry := range entries {
// 			md, ok := staged[string(entry.Path)]
// 			assert.Equal(true, ok, "missing path in repo %q", entry.Path)
// 			assert.Equal(md, entry.Metadata)
// 		}
// 		assert.Equal(len(staged), len(entries), "Staged:\n%s\nRevision:\n%s", staged, entries)
// 		repoState = staged
// 		revId = nextRevId
// 	}
// }
