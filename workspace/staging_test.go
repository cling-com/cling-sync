package workspace

import (
	"io/fs"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestStaging(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Setup the workspace.
		w.Write("a.txt", "a")
		w.Write("b/c.txt", "cc")
		w.Write("b/e/f.txt", "fff")
		w.Chmod("b/c.txt", 0o400)

		// Create a remote commit with a modified file, missing files, and a new file.
		commit, err := lib.NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntryExt("a.txt", lib.RevisionEntryAdd, 0o600, "a")))
		bDirEntry := td.RevisionEntry("b", lib.RevisionEntryAdd)
		bDirEntry.Metadata = w.FileMetadata("b")
		assert.NoError(commit.Add(bDirEntry))
		assert.NoError(commit.Add(td.RevisionEntryExt("b/remote.txt", lib.RevisionEntryAdd, 0o123, "rrr")))
		remoteRev1, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)
		assert.Equal(remoteRev1, r.Head())
		assert.Equal(false, remoteRev1.IsRoot())
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"b", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/remote.txt", lib.RevisionEntryAdd, 0o123, td.SHA256("rrr")},
		}, r.RevisionInfos(remoteRev1))

		// Create a staging.
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"b", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, td.SHA256("cc")},
			{"b/e", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("fff")},
		}, r.RevisionTempInfos(finalized))

		// Merge the staging with a snapshot of the remote revision.
		snapshot, err := lib.NewRevisionSnapshot(r.Repository, remoteRev1, td.NewFS(t))
		assert.NoError(err)
		merged, err := staging.MergeWithSnapshot(snapshot)
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryUpdate, 0o600, td.SHA256("a")},
			// Note that `b/` did not change (and is hence omitted).
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, td.SHA256("cc")},
			// Metadata of `b/remote.txt` should match the repository version.
			{"b/remote.txt", lib.RevisionEntryDelete, 0o123, td.SHA256("rrr")},
			{"b/e", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("fff")},
		}, r.RevisionTempInfos(merged))
	})

	t.Run("With .clingignore and .gitignore files", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Setup the workspace.
		w.Write(".clingignore", "*.png")
		w.Write("a.txt", "a")
		w.Write("b.png", "b")
		w.Write("dir1/.gitignore", "dir2\n*.txt")
		w.Write("dir1/a.txt", "a")
		w.Write("dir1/b.md", "b")
		w.Write("dir1/dir2/a.md", "c")
		w.Write("dir1/dir3/a.txt", "a")
		w.Write("dir1/dir3/b.png", "b")
		w.Write("dir1/dir3/c.md", "c")

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{".clingignore", lib.RevisionEntryAdd, 0o600, td.SHA256("*.png")},
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"dir1", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/.gitignore", lib.RevisionEntryAdd, 0o600, td.SHA256("dir2\n*.txt")},
			{"dir1/b.md", lib.RevisionEntryAdd, 0o600, td.SHA256("b")},
			{"dir1/dir3", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/dir3/c.md", lib.RevisionEntryAdd, 0o600, td.SHA256("c")},
		}, r.RevisionTempInfos(finalized))
	})
}
