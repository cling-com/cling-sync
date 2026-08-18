package lib

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

func TestRevisionEntry(t *testing.T) {
	t.Parallel()
	t.Run("RevisionPathCompare", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dirEntry := func(path string) *RevisionEntry {
			t.Helper()
			entry := td.RevisionEntry(path, RevisionEntryKindAdd)
			entry.Metadata = *td.PathMetadata(FileModeDir)
			return entry
		}
		fileEntry := func(path string) *RevisionEntry {
			t.Helper()
			return td.RevisionEntry(path, RevisionEntryKindAdd)
		}
		entries := []*RevisionEntry{
			fileEntry("a.zip"),
			fileEntry("abcd.txt"),
			dirEntry("a"),
			fileEntry("a/1.md"),
			fileEntry("a/2.md"),
			dirEntry("abc"),
			fileEntry("abc/1.md"),
		}
		// Randomize the order of the entries.
		rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
		actual := make([]*RevisionEntry, len(entries))
		copy(actual, entries)
		slices.SortFunc(actual, (*RevisionEntry).PathCompare)
		actualPaths := make([]string, len(actual))
		for i, entry := range actual {
			actualPaths[i] = entry.Path.String()
		}
		assert.Equal([]string{
			"a",
			"a.zip",
			"a/1.md",
			"a/2.md",
			"abc",
			"abc/1.md",
			"abcd.txt",
		}, actualPaths)
	})

	t.Run("RevisionEntry.PathCompare with different kinds", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := td.RevisionEntryExt("a", RevisionEntryKindDelete, FileModeDir, "")
		assert.Equal(
			0,
			sut.PathCompare(td.RevisionEntryExt("a", RevisionEntryKindDelete, FileModeDir, "")),
		)
		assert.Equal(0, sut.PathCompare(td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, "")))
		assert.Equal(
			0,
			sut.PathCompare(td.RevisionEntryExt("a", RevisionEntryKindUpdate, FileModeDir, "")),
		)

		// Files are greater than directories.
		assert.Equal(1, sut.PathCompare(td.RevisionEntryExt("a", RevisionEntryKindUpdate, 0, "")))
	})

	t.Run("Unmarshalling does not alias the buffer", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// `ProtobufReader` decodes without copying, so this does not hold for
		// every message. It has to hold for `RevisionEntry`, because the n-way
		// merge, `TempCache`, and `DisplayOrderReader` all keep entries while
		// the block they came from is read over.
		//
		// Every field is a chance to alias, so `want` below has to fill all of
		// them. If the shape changes, the test data must change with it.
		assert.Fields([]string{
			"Kind lib.RevisionEntryKind",
			"Path lib.Path",
			"Metadata lib.PathMetadata",
		}, reflect.TypeFor[RevisionEntry]())
		assert.Fields([]string{
			"FileMode lib.FileMode",
			"Mtime lib.Timestamp",
			"Size int64",
			"FileHash lib.Sha256",
			"BlockIds []lib.BlockId",
			"SymLinkTarget *lib.Path",
			"Uid *uint32",
			"Gid *uint32",
			"Birthtime *lib.Timestamp",
		}, reflect.TypeFor[PathMetadata]())

		// A symlink is the only kind that fills every field, and `Update`
		// because `Add` is 0.
		want := td.RevisionEntryExt("sub/a.txt", RevisionEntryKindUpdate, FileModeSymlink|0o777, "content")
		assert.AllFieldsSet(want)

		// Marshall and unmarshall.
		buf := make([]byte, want.MarshallSize())
		w := NewProtobufWriter(buf)
		assert.NoError(want.Marshall(w))
		got, err := UnmarshallRevisionEntry(NewProtobufReader(w.Bytes()))
		assert.NoError(err)

		// Write garbage to the BlockBuf so if anything would alias it would
		// change the `want`.
		for i := range buf {
			buf[i] = 0xff
		}
		assert.Equal(want, got)
	})
}

func TestRevisionEntryTemp(t *testing.T) {
	t.Parallel()

	t.Run("Sort order", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntry.PathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		// Use a small chunk size to force rotation.
		sut := NewRevisionEntryTempWriter(fs, 700)

		add := func(path string, mode FileMode) {
			err := sut.Add(
				&RevisionEntry{Kind: RevisionEntryKindAdd, Path: Path{path}, Metadata: *td.PathMetadata(mode)},
			)
			assert.NoError(err)
		}

		add("sub", FileModeDir)
		add("sub/sub", FileModeDir)
		add(".a.txt", 0)
		add("a.txt", 0)
		add("z.txt", 0)
		add("sub.txt", 0)
		add("sub/.a.txt", 0)
		add("sub/a.txt", 0)
		add("sub/z.txt", 0)
		add("sub/sub.txt", 0)
		add("sub/sub/.a.txt", 0)
		add("sub/sub/a.txt", 0)
		add("sub/sub/z.txt", 0)

		temp, err := sut.CloseAndSort()
		assert.NoError(err)
		assert.Equal(true, sut.chunks > 1, "should be multiple chunks")
		merged := readAllRevsisionTemp(t, temp, nil)
		actualPaths := make([]string, len(merged))
		for i, entry := range merged {
			actualPaths[i] = entry.Path.String()
		}
		assert.Equal([]string{
			".a.txt",
			"a.txt",
			"sub",
			// `.` sorts below `/`, so a sibling file separates a directory from
			// its contents.
			"sub.txt",
			"sub/.a.txt",
			"sub/a.txt",
			"sub/sub",
			"sub/sub.txt",
			"sub/sub/.a.txt",
			"sub/sub/a.txt",
			"sub/sub/z.txt",
			"sub/z.txt",
			"z.txt",
		}, actualPaths)
	})
}
