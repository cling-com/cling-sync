//nolint:forbidigo
package lib

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestStagingCommit(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		// First commit
		rev1, err := commitStaging(t, repo, head, nil, []testFile{
			// Add paths in random order to test that they are sorted.
			{"a/3.txt", 0, RevisionEntryAdd},
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
		assert.NoError(err)
		checkRevision(t, repo, rev1, head, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
			{"a/3.txt", 0, RevisionEntryAdd},
		})

		// Second commit
		rev2, err := commitStaging(t, repo, rev1, nil, []testFile{
			{"a/4.txt", 0, RevisionEntryAdd},     // new
			{"a/1.txt", 42, RevisionEntryUpdate}, // updated
			{"a/2.txt", 0, RevisionEntryUpdate},  // same (but staged again)
		})
		assert.NoError(err)
		checkRevision(t, repo, rev2, rev1, []testFile{
			{"a/1.txt", 42, RevisionEntryUpdate},
			{"a/3.txt", 0, RevisionEntryDelete},
			{"a/4.txt", 0, RevisionEntryAdd},
		})
	})

	t.Run("Empty staging deletes all files in the repository", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		// First commit
		rev1, err := commitStaging(t, repo, head, nil, []testFile{
			{"a/3.txt", 0, RevisionEntryAdd},
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
		assert.NoError(err)
		checkRevision(t, repo, rev1, head, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
			{"a/3.txt", 0, RevisionEntryAdd},
		})

		// Second commit
		rev2, err := commitStaging(t, repo, rev1, nil, []testFile{})
		assert.NoError(err)
		checkRevision(t, repo, rev2, rev1, []testFile{
			{"a/1.txt", 0, RevisionEntryDelete},
			{"a/2.txt", 0, RevisionEntryDelete},
			{"a/3.txt", 0, RevisionEntryDelete},
		})
	})

	t.Run("More revision entries than staging entries", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		// First commit
		rev1, err := commitStaging(t, repo, head, nil, []testFile{
			{"a/3.txt", 0, RevisionEntryAdd},
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
		assert.NoError(err)
		checkRevision(t, repo, rev1, head, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
			{"a/3.txt", 0, RevisionEntryAdd},
		})

		// Second commit
		rev2, err := commitStaging(t, repo, rev1, nil, []testFile{
			{"a/1.txt", 42, RevisionEntryAdd}, // updated
		})
		assert.NoError(err)
		checkRevision(t, repo, rev2, rev1, []testFile{
			{"a/1.txt", 42, RevisionEntryUpdate},
			{"a/2.txt", 0, RevisionEntryDelete},
			{"a/3.txt", 0, RevisionEntryDelete},
		})
	})

	t.Run("No changes", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		// First commit
		rev1, err := commitStaging(t, repo, head, nil, []testFile{
			{"a/3.txt", 0, RevisionEntryAdd},
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
		assert.NoError(err)
		checkRevision(t, repo, rev1, head, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
			{"a/3.txt", 0, RevisionEntryAdd},
		})

		// Second commit
		_, err = commitStaging(t, repo, rev1, nil, []testFile{
			{"a/3.txt", 0, RevisionEntryAdd},
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
		assert.Error(err, "empty commit")
	})

	t.Run("Duplicate paths", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		_, err = commitStaging(t, repo, head, nil, []testFile{
			{"a.txt", 0, RevisionEntryAdd},
			{"a.txt", 0, RevisionEntryAdd},
		})
		assert.Error(err, "duplicate revision entry path: a.txt")
	})

	t.Run("Ignored paths", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)
		head, err := repo.Head()
		assert.NoError(err)

		filter, err := NewPathExclusionFilter([]string{"a/b"}, []string{})
		assert.NoError(err)
		rev1, err := commitStaging(t, repo, head, filter, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
			{"a/b/3.txt", 0, RevisionEntryAdd},
			{"a/b/4.txt", 0, RevisionEntryAdd},
		})
		assert.NoError(err)
		checkRevision(t, repo, rev1, head, []testFile{
			{"a/1.txt", 0, RevisionEntryAdd},
			{"a/2.txt", 0, RevisionEntryAdd},
		})
	})
}

func TestStagingCommitFuzz(t *testing.T) {
	t.Parallel()
	// Uncomment to test more random seeds.
	// seeds := []uint64{}
	// for range 50 {
	// 	seeds = append(seeds, rand.Uint64()) //nolint:gosec
	// }
	seeds := []uint64{42, rand.Uint64(), rand.Uint64()} //nolint:gosec
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("Seed %d", seed), func(t *testing.T) {
			t.Parallel()
			fuzzTesting(t, seed, false)
		})
	}
}

// Test with random data.
// This will create multiple commits with random adds, updates, and deletes.
// From time to time an empty commit will be added.
func fuzzTesting(t *testing.T, randSeed uint64, debug bool) {
	t.Helper()
	const (
		steps             = 20
		maxFilesPerCommit = 50
		numUniqueFiles    = 100
		numIgnored        = 10
	)
	// Create a reproducible RNG.
	r := rand.New(rand.NewPCG(randSeed, 0)) //nolint:gosec
	repo, _ := testRepository(t)
	assert := NewAssert(t)

	revId, err := repo.Head()
	assert.NoError(err)

	// Our model of the repository.
	repoState := map[string]*FileMetadata{}

	for step := range steps {
		if debug {
			fmt.Println("Step", step)
		}

		staged := map[string]*FileMetadata{}
		seen := map[string]bool{}

		// Create random staging entries.
		numFiles := r.IntN(maxFilesPerCommit)
		for len(staged) != numFiles {
			path := fmt.Sprintf("a/%03d.txt", r.IntN(numUniqueFiles))
			if seen[path] {
				continue // avoid duplicate staging entries
			}
			seen[path] = true
			if repoMd, ok := repoState[path]; ok {
				if r.IntN(2) == 1 {
					// Update the entry.
					md := *repoMd
					md.ModeAndPerm += 1
					staged[path] = &md
				}
				// Otherwise omit the entry, so it will be deleted.
			} else {
				// Add the entry.
				staged[path] = fakeFileMetadata(0)
			}
		}
		pathFilter := &PathExclusionFilter{nil, nil}
		for i := range numIgnored {
			pattern, err := NewPathPattern(fmt.Sprintf("a/%03d.txt", i))
			assert.NoError(err)
			pathFilter.Excludes = append(pathFilter.Excludes, pattern)
		}
		files := make([]testFile, 0, len(staged))
		for path, md := range staged {
			files = append(files, testFile{path: path, mode: uint32(md.ModeAndPerm), typ: RevisionEntryAdd})
		}
		if debug {
			fmt.Println("Staging")
			slices.SortFunc(files, func(a, b testFile) int { return strings.Compare(a.path, b.path) })
			for i, file := range files {
				fmt.Printf("%3d: %s %s\n", i, file.path, ModeAndPerm(file.mode))
			}
		}
		nextRevId, err := commitStaging(t, repo, revId, pathFilter, files)
		if errors.Is(err, ErrEmptyCommit) {
			assert.Equal(0, len(repoState), "repository should be empty")
			assert.Equal(0, len(staged), "no files should have been staged")
			continue
		}

		// Filter out the ignored paths from staged.
		filteredStaged := map[string]*FileMetadata{}
		for path, md := range staged {
			if pathFilter.Include(path) {
				filteredStaged[path] = md
			}
		}
		staged = filteredStaged

		// Compare our current model with the real world.
		entries := readRevisionSnapshot(t, repo, nextRevId, nil)
		if debug {
			fmt.Println("Revision")
			for i, entry := range entries {
				fmt.Printf("%3d: %s %s\n", i, entry.Path, entry.Metadata.ModeAndPerm)
			}
		}
		for _, entry := range entries {
			md, ok := staged[string(entry.Path)]
			assert.Equal(true, ok, "missing path in repo %q", entry.Path)
			assert.Equal(md, entry.Metadata)
		}
		assert.Equal(len(staged), len(entries), "Staged:\n%s\nRevision:\n%s", staged, entries)
		repoState = staged
		revId = nextRevId
	}
}

type testFile struct {
	path string
	mode uint32
	typ  RevisionEntryType
}

func (tf testFile) String() string {
	return fmt.Sprintf("testFile{%s, %s, %s}", tf.path, ModeAndPerm(tf.mode), tf.typ)
}

func addFiles(t *testing.T, st *Staging, files []testFile) {
	t.Helper()
	for _, f := range files {
		_, err := st.Add(NewPath(strings.Split(f.path, "/")...), fakeFileMetadata(ModeAndPerm(f.mode)))
		if err != nil {
			t.Fatalf("Add failed for %s: %v", f.path, err)
		}
	}
}

func commitStaging(
	t *testing.T,
	repo *Repository,
	base RevisionId,
	pathFilter PathFilter,
	files []testFile,
) (RevisionId, error) {
	t.Helper()
	assert := NewAssert(t)
	st, err := NewStaging(base, pathFilter, t.TempDir())
	assert.NoError(err)
	addFiles(t, st, files)

	revId, err := st.Commit(
		repo,
		&CommitInfo{Author: fmt.Sprintf("author: %s", base), Message: fmt.Sprintf("message: %s", base)},
	)
	return revId, err
}

func checkRevision(t *testing.T, repo *Repository, revId, parent RevisionId, expected []testFile) {
	t.Helper()
	assert := NewAssert(t)

	rev, entries, err := readRevision(repo, revId)
	assert.NoError(err)
	assert.Equal(parent, rev.Parent)
	assert.Equal(fmt.Sprintf("author: %s", parent), rev.Author)
	assert.Equal(fmt.Sprintf("message: %s", parent), rev.Message)

	expectedEntries := make([]*RevisionEntry, len(expected))
	for i, f := range expected {
		entry := fakeRevisionEntry(f.path, f.typ)
		if entry.Metadata != nil {
			entry.Metadata.ModeAndPerm = ModeAndPerm(f.mode)
		}
		expectedEntries[i] = entry
	}
	assert.Equal(expectedEntries, entries)
}
