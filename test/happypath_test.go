//nolint:forbidigo
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

const passphrase = "testpassphrase"

var clingSyncBin string //nolint:gochecknoglobals

// Just test a simple scenario that covers most of the common CLI commands.
func TestHappyPath(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Merge empty repository and workspace (merge)")
	{
		sut.ClingSync("merge", "--no-progress")
		assert.Equal("No revisions", head(sut.ClingSync("log", "--short")), "There should be no revision")
	}

	t.Log("Add some files and merge (log, ls, merge, status)")
	{
		sut.Write("a.txt", "a")
		sut.Write("b.txt", "b")
		sut.Mkdir("dir1")
		sut.Write("dir1/d.txt", "d")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")

		log := sut.ClingSync("log", "--short")
		assert.NotEqual("No revisions", log)
		assert.Equal(1, td.Wc("-l", log), "A revision should have been created")
		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
		assert.Equal("No changes", sut.ClingSync("status"), "There should be no local changes")
	}
	rev1Id := sut.RepositoryHead()
	rev1Date := sut.RepositoryHeadDate()
	rev1Ls := sut.Ls()

	t.Log("Remove a local file, change mtime of another, create a new file and merge (status, merge, log, ls)")
	{
		sut.Rm("a.txt")
		sut.Write("b.txt", "bb")
		sut.Touch("dir1/d.txt", time.Now().Add(time.Second))
		sut.Write("c.txt", "C")
		assert.Equal(td.Dedent(`
			D a.txt
			M b.txt
			A c.txt
			M dir1/d.txt
			1 added, 2 updated, 1 deleted
		`), sut.ClingSync("status", "--chtime"), "There should be local changes")

		sut.ClingSync("merge", "--no-progress", "--chtime", "--message", "second commit")

		log := sut.ClingSync("log", "--short")
		assert.Equal(2, td.Wc("-l", log), "Two revisions should have been created")
		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}
	rev2Id := sut.RepositoryHead()
	rev2Date := sut.RepositoryHeadDate()
	rev2Ls := sut.Ls()

	t.Log("List an older revision (ls)")
	{
		assert.Equal(
			rev1Ls,
			sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction", "--revision", rev1Id),
			"Listing the first revision should match",
		)
	}

	t.Log("Log revision history (log)")
	{
		assert.Equal(td.Dedent(fmt.Sprintf(`
            %s %s second commit

                D a.txt
                M b.txt
                A c.txt
                M dir1/d.txt

            %s %s first commit

                A a.txt
                A b.txt
                A dir1/
                A dir1/d.txt
			`, rev2Id, rev2Date, rev1Id, rev1Date)),
			sut.ClingSync("log", "--short", "--status"),
			"Log should contain the two revisions")
	}

	t.Log("Copy a file from an older revision (cp, status)")
	{
		assert.Equal("bb", sut.Cat("b.txt"), "`b.txt` should contain the current content")

		// First, try without `--overwrite` - it should fail.
		stderr := sut.ClingSyncError("cp", "--no-progress", "--revision", rev1Id, "b.txt", ".")
		assert.Contains(stderr, "failed to copy b.txt")
		assert.Contains(stderr, "file exists")
		assert.Equal("bb", sut.Cat("b.txt"), "`b.txt` should contain still the current content")

		// Now try with `--overwrite` - it should succeed.
		sut.ClingSync("cp", "--no-progress", "--overwrite", "--revision", rev1Id, "b.txt", ".")
		assert.Equal("b", sut.Cat("b.txt"), "`b.txt` should contain the previous content")
		assert.Equal(
			"M b.txt",
			sut.ClingSync("status", "--no-progress", "--no-summary"),
			"`b.txt` should be marked as modified",
		)

		// Merge the change, so the workspace is up to date.
		sut.ClingSync("merge", "--no-progress", "--message", "revert b.txt")
	}

	t.Log("Attach the repository to a second workspace")
	{
		workspace1Ls := sut.Ls()
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "attach", "../repository", "../workspace2")
		sut.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")
		workspace2Ls := sut.Ls()
		assert.Equal(workspace1Ls, workspace2Ls)
	}

	t.Log("Create and resolve conflicts with --accept-local (merge)")
	{
		sut.Chdir("../workspace2")
		sut.Write("b.txt", "b from workspace2")
		sut.Mkdir("dir2")
		sut.Write("dir2/e.txt", "e")
		sut.ClingSync("merge", "--no-progress", "--message", "conflict")

		// Back to workspace1 and add conflicting changes.
		sut.Chdir("../workspace")
		sut.Write("b.txt", "b from workspace")
		sut.Mkdir("dir2")
		sut.Write("dir2/e.txt", "e from workspace")

		// Merge should fail.
		stderr := sut.ClingSyncError("merge", "--no-progress")
		assert.Contains(stderr, td.Dedent(`
			
			  b.txt (remote: update, local: update)
			  dir2/e.txt (remote: add, local: add)
			
			No files were changed, you need to resolve the conflicts manually.
			`))

		// Accept the local changes.
		sut.ClingSync("merge", "--no-progress", "--chtime", "--accept-local")
		status := sut.ClingSync("status", "--no-progress")
		assert.Equal("No changes", status)

		// The workspace changes should have been committed.
		assert.Equal("b from workspace", sut.Cat("b.txt"))
		assert.Equal("e from workspace", sut.Cat("dir2/e.txt"))
		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}

	t.Log("Reset to the first commit (reset)")
	{
		// We use `--chtime` so that `ls` will return exactly the same output as when we did first
		// merge.
		sut.ClingSync("reset", "--no-progress", "--chtime", rev1Id)
		assert.Equal(rev1Ls, sut.Ls())
	}

	t.Log("Make some changes and reset to second commit (reset)")
	{
		sut.Write("a.txt", "achange")
		sut.Write("new.txt", "new")
		ls := sut.Ls()

		// Reset w/o `--force` should fail.
		err := sut.ClingSyncError("reset", "--no-progress", "--chtime", rev2Id)
		assert.Contains(err, "Reset aborted due to local changes")
		assert.Equal(ls, sut.Ls())

		// Reset with `--force` should succeed.
		sut.ClingSync("reset", "--no-progress", "--chtime", "--force", rev2Id)
		assert.Equal(rev2Ls, sut.Ls())
	}

	t.Log("Check health (check)")
	{
		check := sut.ClingSync("check", "--no-progress")
		assert.Contains(check, "Repository is healthy")
		assert.Contains(check, "5 revisions")
	}

	t.Log("Attach to a non-empty directory (attach --allow-non-empty)")
	{
		nonEmptyDir := sut.Path("../workspace_nonempty")
		err := os.MkdirAll(nonEmptyDir, 0o700)
		assert.NoError(err, "failed to create non-empty dir")
		// Three kinds of pre-existing files:
		// 1. `local-only.txt` has no remote counterpart.
		// 2. `b.txt` matches the current repository content (silent adopt).
		// 3. `c.txt` has different content from the repository (commit as update).
		err = os.WriteFile(filepath.Join(nonEmptyDir, "local-only.txt"), []byte("local"), 0o600)
		assert.NoError(err, "failed to write local-only.txt")
		err = os.WriteFile(filepath.Join(nonEmptyDir, "b.txt"), []byte("b from workspace"), 0o600)
		assert.NoError(err, "failed to write matching b.txt")
		err = os.WriteFile(filepath.Join(nonEmptyDir, "c.txt"), []byte("locally edited C"), 0o600)
		assert.NoError(err, "failed to write modified c.txt")

		// Attach should refuse a non-empty target by default.
		stderr := sut.ClingSyncError("attach", "../repository", "../workspace_nonempty")
		assert.Contains(stderr, "is not empty")
		assert.Contains(stderr, "--allow-non-empty")

		// With --allow-non-empty, attach succeeds. The merge should adopt
		// matching files, commit the modified file as an update, add the
		// local-only file, and fetch every other remote file.
		sut.ClingSyncStdin(
			passphrase,
			"--passphrase-from-stdin",
			"attach",
			"--allow-non-empty",
			"../repository",
			"../workspace_nonempty",
		)
		sut.Chdir("../workspace_nonempty")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress", "--chtime", "--message", "attach non-empty workspace")

		assert.Equal("local", sut.Cat("local-only.txt"), "local-only file should be preserved")
		assert.Equal("b from workspace", sut.Cat("b.txt"), "matching file should be adopted untouched")
		assert.Equal("locally edited C", sut.Cat("c.txt"), "modified file should be committed as update")
		// Remote-only files come down.
		assert.Equal("d", sut.Cat("dir1/d.txt"), "remote-only dir file should be fetched")
		assert.Equal("e from workspace", sut.Cat("dir2/e.txt"), "remote-only dir file should be fetched")
		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
		assert.Equal("No changes", sut.ClingSync("status", "--chtime"), "After merge, no local changes should remain")
	}
}

func TestSyncRepoHappyPath(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Create repository history")
	{
		sut.Write("a.txt", "a")
		sut.Write("dir/b.txt", "b")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")

		sut.Write("a.txt", "aa")
		sut.Write("c.txt", "c")
		sut.Rm("dir/b.txt")
		sut.ClingSync("merge", "--no-progress", "--message", "second commit")
	}

	t.Log("Initialize sync target repository")
	sut.ClingSync("sync-repo", "init", "../sync-target")

	t.Log("Run repository sync")
	sut.ClingSync("sync-repo", "run", "../sync-target")

	srcStorage, err := lib.NewFileStorage(lib.NewRealFS(sut.Path("../repository")), lib.StoragePurposeRepository)
	assert.NoError(err)
	dstStorage, err := lib.NewFileStorage(lib.NewRealFS(sut.Path("../sync-target")), lib.StoragePurposeRepository)
	assert.NoError(err)
	srcRepo, err := lib.OpenRepository(srcStorage, []byte(passphrase))
	assert.NoError(err)
	dstRepo, err := lib.OpenRepository(dstStorage, []byte(passphrase))
	assert.NoError(err)

	assert.Equal(sut.RepositoryHead(), headFromRepository(t, dstRepo))
	assertSameRepositoryHistory(t, srcRepo, dstRepo)
	assertSameRepositoryFS(t, sut.Path("../repository"), sut.Path("../sync-target"))

	t.Log("Add another commit")
	{
		sut.Write("a.txt", "aaa")
		sut.Write("dir/d.txt", "d")
		sut.ClingSync("merge", "--no-progress", "--message", "third commit")
	}
	t.Log("Run repository sync")
	sut.ClingSync("sync-repo", "run", "../sync-target")
	assert.Equal(sut.RepositoryHead(), headFromRepository(t, dstRepo))
	assertSameRepositoryHistory(t, srcRepo, dstRepo)
	assertSameRepositoryFS(t, sut.Path("../repository"), sut.Path("../sync-target"))
}

func TestChmodChtimeChown(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	user, err := user.Current()
	assert.NoError(err)
	groups, err := user.GroupIds()
	assert.NoError(err)
	assert.Greater(len(groups), 1, "There should be at least two groups for the current user")
	uid, err := strconv.Atoi(user.Uid)
	assert.NoError(err)
	grp1, err := strconv.Atoi(groups[0])
	assert.NoError(err)
	grp2, err := strconv.Atoi(groups[1])
	assert.NoError(err)

	t.Log("Add and merge some files")
	{
		sut.Write("mode.txt", "mode")
		sut.Chmod("mode.txt", 0o777)
		sut.Write("own.txt", "own")
		sut.Chown("own.txt", uid, grp1)
		sut.Write("time.txt", "time")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")

		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}

	t.Log("Attach the repository to a second and third workspace")
	{
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "attach", "../repository", "../workspace2")
		sut.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")

		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "attach", "../repository", "../workspace3")
		sut.Chdir("../workspace3")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")
		sut.Chdir("../workspace")
	}

	t.Log("Change file mode, mtime, and ownership")
	{
		sut.Chmod("mode.txt", 0o700)
		sut.Touch("time.txt", time.Now())
		sut.Chown("own.txt", uid, grp2)
	}

	t.Log("Status with different flags")
	{
		assert.Equal(
			"No changes",
			sut.ClingSync("status", "--no-progress"),
			"There should be no local changes by default",
		)

		assert.Equal(td.Dedent(`
			M mode.txt
			0 added, 1 updated, 0 deleted
		`), sut.ClingSync("status", "--no-progress", "--chmod"), "Local changes with --chmod")

		assert.Equal(td.Dedent(`
			M time.txt
			0 added, 1 updated, 0 deleted
		`), sut.ClingSync("status", "--no-progress", "--chtime"), "Local changes with --chtime")

		assert.Equal(td.Dedent(`
			M own.txt
			0 added, 1 updated, 0 deleted
		`), sut.ClingSync("status", "--no-progress", "--chown"), "Local changes with --chown")

		assert.Equal(td.Dedent(`
			M mode.txt
			M own.txt
			M time.txt
			0 added, 3 updated, 0 deleted
		`), sut.ClingSync("status", "--no-progress", "--chown", "--chtime", "--chmod"), "Local changes with all flags")
	}

	t.Log("Merge without --chtime, --chmod, and --chown")
	{
		assert.Equal("No changes", sut.ClingSync("merge", "--no-progress", "--message", "first commit"))
		assert.Equal(
			td.Dedent(`
				M mode.txt
				M own.txt
				M time.txt
				0 added, 3 updated, 0 deleted
			`),
			sut.ClingSync(
				"status",
				"--chtime",
				"--chmod",
				"--chown",
				"--no-progress",
			),
			"There should be no local changes after merge",
		)
	}

	t.Log("Merge with --chtime, --chmod, and --chown")
	{
		sut.ClingSync("merge", "--no-progress", "--chtime", "--message", "with --chtime")
		assert.Equal(2, td.Wc("-l", sut.ClingSync("log", "--short")), "A new revision should have been created")
		assert.Equal(
			td.Dedent(`
				M mode.txt
				M own.txt
				0 added, 2 updated, 0 deleted
			`),
			sut.ClingSync(
				"status",
				"--chtime",
				"--chmod",
				"--chown",
				"--no-progress",
			),
			"time.txt should have been committed",
		)

		sut.ClingSync("merge", "--no-progress", "--chmod", "--message", "with --chmod")
		assert.Equal(3, td.Wc("-l", sut.ClingSync("log", "--short")), "A new revision should have been created")
		assert.Equal(
			td.Dedent(`
				M own.txt
				0 added, 1 updated, 0 deleted
			`),
			sut.ClingSync(
				"status",
				"--chtime",
				"--chmod",
				"--chown",
				"--no-progress",
			),
			"mode.txt should have been committed",
		)

		sut.ClingSync("merge", "--no-progress", "--chown", "--message", "with --chown")
		assert.Equal(4, td.Wc("-l", sut.ClingSync("log", "--short")), "A new revision should have been created")
		assert.Equal("No changes", sut.ClingSync("status", "--chtime", "--chmod", "--chown", "--no-progress"))
	}

	t.Log("Merge into second workspace without --chtime, --chmod, and --chown")
	{
		sut.Chdir("../workspace2")
		assert.Contains(sut.ClingSync("merge", "--no-progress"), "No local changes")
		assert.Equal(4, td.Wc("-l", sut.ClingSync("log", "--short")), "No new revision should have been created")

		assert.NotEqual(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")

		// Merging again will see the local mode, mtime, and ownership changes as the new changes.
		sut.ClingSync("merge", "--no-progress", "--chtime", "--chmod", "--chown")
		assert.Equal(5, td.Wc("-l", sut.ClingSync("log", "--short")), "The old flags should have been committed")

		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}

	t.Log("Merge into third workspace with --chtime, --chmod, and --chown")
	{
		sut.Chdir("../workspace3")
		assert.Contains(sut.ClingSync("merge", "--no-progress", "--chtime", "--chmod", "--chown"), "No local changes")
		assert.Equal(5, td.Wc("-l", sut.ClingSync("log", "--short")), "No new revision should have been created")

		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}
}

func TestPathPrefix(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Commit some files outside of the path prefix")
	{
		sut.Write("a.txt", "a")
		sut.Mkdir("dir1")
		sut.Write("dir1/b.txt", "b")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")
	}

	t.Log("Attach the repository to a new workspace with a path prefix - we should not see any files")
	{
		sut.ClingSyncStdin(
			passphrase,
			"--passphrase-from-stdin",
			"attach",
			"--path-prefix",
			"look/here/",
			"../repository",
			"../workspace2",
		)
		sut.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		ls := sut.ClingSync("ls")
		assert.Equal("", ls)
	}

	t.Log("Merge files from the workspace with a path prefix")
	{
		sut.Write("c.txt", "c")
		sut.Mkdir("dir2")
		sut.Write("dir2/d.txt", "d")
		sut.ClingSync("merge", "--no-progress", "--message", "from prefix")
		assert.Equal(td.Dedent(`
			c.txt
			dir2/
			dir2/d.txt
		`), td.Column(sut.Ls(), 4))
		log := sut.ClingSync("log", "--short")
		// Merging again should not do anything.
		sut.ClingSync("merge", "--no-progress")
		assert.Equal(log, sut.ClingSync("log", "--short"))
	}

	t.Log("Files have been merged to the right directory")
	{
		sut.Chdir("../workspace")
		sut.ClingSync("merge", "--no-progress")
		ls := sut.ClingSync("ls")
		assert.Equal(td.Dedent(`
			a.txt
			dir1/
			dir1/b.txt
			look/
			look/here/
			look/here/c.txt
			look/here/dir2/
			look/here/dir2/d.txt
		`), td.Column(ls, 4))
	}

	t.Log("Run `ls` in workspace with path prefix")
	{
		sut.Chdir("../workspace2")
		ls := sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction")
		assert.Equal(td.Dedent(`
			c.txt
			dir2/
			dir2/d.txt
		`), td.Column(ls, 4))
	}

	t.Log("Run `status` in workspace with path prefix")
	{
		sut.Write("new.txt", "new")
		status := sut.ClingSync("status", "--no-progress", "--no-summary")
		assert.Equal(td.Dedent(`
			A new.txt
		`), status)
	}

	t.Log("Attach a non-empty directory with path-prefix (attach --allow-non-empty --path-prefix)")
	{
		// Build a fresh directory whose layout mirrors the prefix-relative
		// view of the repository under `look/here/`. The merge should
		// adopt the matching file, commit the modified file as an update,
		// add the local-only file, and not flag any of them as conflicts.
		nonEmptyDir := sut.Path("../workspace_prefix_nonempty")
		err := os.MkdirAll(filepath.Join(nonEmptyDir, "dir2"), 0o700)
		assert.NoError(err, "failed to create non-empty dir")
		// 1. Local-only file (committed as ADD).
		err = os.WriteFile(filepath.Join(nonEmptyDir, "local.txt"), []byte("local"), 0o600)
		assert.NoError(err)
		// 2. Matches repo content (adopted silently).
		err = os.WriteFile(filepath.Join(nonEmptyDir, "c.txt"), []byte("c"), 0o600)
		assert.NoError(err)
		// 3. Different content from repo (committed as UPDATE).
		err = os.WriteFile(filepath.Join(nonEmptyDir, "dir2/d.txt"), []byte("locally edited d"), 0o600)
		assert.NoError(err)

		sut.ClingSyncStdin(
			passphrase,
			"--passphrase-from-stdin",
			"attach",
			"--allow-non-empty",
			"--path-prefix",
			"look/here/",
			"../repository",
			"../workspace_prefix_nonempty",
		)
		sut.Chdir("../workspace_prefix_nonempty")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")

		sut.ClingSync("merge", "--no-progress", "--message", "prefix attach non-empty workspace")

		assert.Equal("local", sut.Cat("local.txt"))
		assert.Equal("c", sut.Cat("c.txt"))
		assert.Equal("locally edited d", sut.Cat("dir2/d.txt"))
		assert.Equal("No changes", sut.ClingSync("status"))
	}
}

func TestClingIgnoreAndGitIgnore(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Merge empty repository and workspace (merge)")
	{
		sut.ClingSync("merge", "--no-progress")
		assert.Equal("No revisions", head(sut.ClingSync("log", "--short")), "There should be no revision")
	}

	t.Log("Add some files including .clingignore and .gitignore files and merge (log, merge, status)")
	{
		sut.Write(".clingignore", "*.png")
		sut.Write("a.txt", "a")
		sut.Write("b.png", "b")
		sut.Mkdir("dir1")
		sut.Write("dir1/.gitignore", "dir2\n*.txt")
		sut.Write("dir1/a.txt", "a")
		sut.Write("dir1/b.png", "b")
		sut.Write("dir1/c.md", "c")
		sut.Mkdir("dir1/dir2")
		sut.Write("dir1/dir2/a.md", "a")
		sut.Mkdir("dir1/dir3")
		sut.Write("dir1/dir3/a.txt", "a")
		sut.Write("dir1/dir3/b.png", "b")
		sut.Write("dir1/dir3/c.md", "c")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")

		log := sut.ClingSync("log", "--short")
		assert.NotEqual("No revisions", log)
		assert.Equal(1, td.Wc("-l", log), "A revision should have been created")
		assert.Equal("No changes", sut.ClingSync("status"), "There should be no local changes")
	}

	t.Log("List files in repository (ls)")
	{
		ls := sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction")
		assert.Equal(td.Dedent(`
			.clingignore
			a.txt
			dir1/
			dir1/.gitignore
			dir1/c.md
			dir1/dir3/
			dir1/dir3/c.md
		`), td.Column(ls, 4))
	}

	t.Log("Ignoring `dir3` should remove `dir3` from the new revision")
	{
		sut.Write(".clingignore", "*png\ndir1/dir3")
		sut.Write("dir1/dir3/e.md", "e")
		sut.ClingSync("merge", "--no-progress", "--message", "ignore dir3")
		log := sut.ClingSync("log", "--short")
		assert.Equal(2, td.Wc("-l", log), "A revision should have been created")

		ls := sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction")
		assert.Equal(td.Dedent(`
			.clingignore
			a.txt
			dir1/
			dir1/.gitignore
			dir1/c.md
		`), td.Column(ls, 4))
	}

	t.Log("All ignored files should still be present in the workspace")
	{
		sut.Ls()
		assert.Equal(td.Dedent(`
			.clingignore
			a.txt
			b.png
			dir1/
			dir1/.gitignore
			dir1/a.txt
			dir1/b.png
			dir1/c.md
			dir1/dir2/
			dir1/dir2/a.md
			dir1/dir3/
			dir1/dir3/a.txt
			dir1/dir3/b.png
			dir1/dir3/c.md
			dir1/dir3/e.md
		`), td.Column(sut.Ls(), 4))
	}
}

func TestRepositoryOverHTTP(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Serve repository over HTTP")
	{
		t.Log(gray("    > cling-sync serve --log-requests --address 127.0.0.1:9123 ../repository"))
		cmd := sut.cmd("serve", "--log-requests", "--address", "127.0.0.1:9123", "../repository")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		assert.NoError(err, "failed to start cling-sync serve")
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()
		t0 := time.Now()
		for time.Since(t0) < 10*time.Second {
			conn, err := net.DialTimeout("tcp", "127.0.0.1:9123", 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	t.Log("Attach repository over HTTP and merge (merge, ls)")
	{
		workspace1Ls := sut.Ls()
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "attach", "http://localhost:9123", "../workspace2")
		sut.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")
		workspace2Ls := sut.Ls()
		assert.Equal(workspace1Ls, workspace2Ls)
	}

	t.Log("Commit local changes (merge)")
	{
		sut.Write("new.txt", "new")
		sut.ClingSync("merge", "--no-progress", "--message", "commit local changes")
		log := sut.ClingSync("log", "--short", "--status")
		assert.Equal(td.Dedent(fmt.Sprintf(`
            %s %s commit local changes

                A new.txt
			`, sut.RepositoryHead(), sut.RepositoryHeadDate())),
			log)
	}
}

func TestSymlinks(t *testing.T) {
	t.Parallel()
	sut := NewSut(t)
	assert := sut.assert

	t.Log("Commit a symlink and verify it is restored in another workspace")
	{
		sut.Write("a.txt", "a")
		sut.Symlink("a.txt", "link")
		sut.ClingSync("merge", "--no-progress", "--message", "first commit")
		ls := sut.ClingSync("ls", "--short-file-mode")
		assert.Contains(ls, "link")
		assert.Contains(ls, "a.txt")
	}

	t.Log("Attach a second workspace and verify the symlink is materialised")
	{
		sut.ClingSyncStdin(
			passphrase,
			"--passphrase-from-stdin",
			"attach",
			"../repository",
			"../workspace2",
		)
		sut.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")
		linkPath := filepath.Join(sut.workDir, "link")
		target, err := os.Readlink(linkPath)
		assert.NoError(err, "readlink %s", linkPath)
		assert.Equal("a.txt", target)
	}

	t.Log("Repoint the symlink and verify the new target propagates")
	{
		sut.Chdir("../workspace")
		sut.Write("b.txt", "b")
		sut.Rm("link")
		sut.Symlink("b.txt", "link")
		sut.ClingSync("merge", "--no-progress", "--message", "repoint link")
		sut.Chdir("../workspace2")
		sut.ClingSync("merge", "--no-progress")
		target, err := os.Readlink(filepath.Join(sut.workDir, "link"))
		assert.NoError(err)
		assert.Equal("b.txt", target)
	}

	t.Log("Symlink pointing outside the workspace is rejected at commit time")
	{
		sut.Chdir("../workspace")
		sut.Symlink("../escape", "bad-link")
		stderr := sut.ClingSyncError("merge", "--no-progress", "--message", "bad link")
		assert.Contains(stderr, "symlink target escapes path root")
		sut.Rm("bad-link")
	}

	t.Log("File, directory, and symlink can transition into each other within a single revision")
	{
		sut.Chdir("../workspace")
		sut.Write("transitions/target.txt", "T")
		sut.Write("transitions/f_to_d", "fd")
		sut.Write("transitions/d_to_f/inner.txt", "df")
		sut.Write("transitions/f_to_l", "fl")
		sut.Symlink("target.txt", "transitions/l_to_f")
		sut.Write("transitions/d_to_l/inner.txt", "dl")
		sut.Symlink("target.txt", "transitions/l_to_d")
		sut.ClingSync("merge", "--no-progress", "--message", "set up transitions")

		sut.Rm("transitions/f_to_d")
		sut.Write("transitions/f_to_d/inside.txt", "now a dir")
		sut.Rm("transitions/d_to_f")
		sut.Write("transitions/d_to_f", "now a file")
		sut.Rm("transitions/f_to_l")
		sut.Symlink("target.txt", "transitions/f_to_l")
		sut.Rm("transitions/l_to_f")
		sut.Write("transitions/l_to_f", "now a file")
		sut.Rm("transitions/d_to_l")
		sut.Symlink("target.txt", "transitions/d_to_l")
		sut.Rm("transitions/l_to_d")
		sut.Write("transitions/l_to_d/inside.txt", "now a dir")
		sut.ClingSync("merge", "--no-progress", "--message", "transition all kinds")

		sut.Chdir("../workspace2")
		sut.ClingSync("merge", "--no-progress")
		assert.Equal(
			td.Sort(td.Column(sut.Ls(), 4), 1),
			td.Sort(td.Column(sut.ClingSync("ls"), 4), 1),
			"Workspace and repository contents must match after type transitions",
		)
		fToL, err := os.Readlink(filepath.Join(sut.workDir, "transitions/f_to_l"))
		assert.NoError(err)
		assert.Equal("target.txt", fToL)
		dToL, err := os.Readlink(filepath.Join(sut.workDir, "transitions/d_to_l"))
		assert.NoError(err)
		assert.Equal("target.txt", dToL)
		assert.Equal("now a file", sut.Cat("transitions/l_to_f"))
		assert.Equal("now a file", sut.Cat("transitions/d_to_f"))
		assert.Equal("now a dir", sut.Cat("transitions/f_to_d/inside.txt"))
		assert.Equal("now a dir", sut.Cat("transitions/l_to_d/inside.txt"))
	}

	t.Log("In a prefix workspace, symlinks whose target falls outside the prefix are silently skipped")
	{
		sut.Write("outside.txt", "x")
		sut.Mkdir("look")
		sut.Mkdir("look/here")
		sut.Write("look/here/a.txt", "a")
		sut.Symlink("../../outside.txt", "look/here/outlink")
		sut.Symlink("a.txt", "look/here/inlink")
		sut.ClingSync("merge", "--no-progress", "--message", "prefix-aware symlinks")

		sut.ClingSyncStdin(
			passphrase,
			"--passphrase-from-stdin",
			"attach",
			"--path-prefix",
			"look/here/",
			"../repository",
			"../workspace3",
		)
		sut.Chdir("../workspace3")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")
		sut.ClingSync("merge", "--no-progress")

		inTarget, err := os.Readlink(filepath.Join(sut.workDir, "inlink"))
		assert.NoError(err)
		assert.Equal("a.txt", inTarget)

		_, statErr := os.Lstat(filepath.Join(sut.workDir, "outlink"))
		assert.Equal(true, os.IsNotExist(statErr), "outlink should be absent in the prefix workspace")
	}
}

type Sut struct {
	*lib.TestFS
	t            *testing.T
	assert       lib.Assert
	workDir      string // absolute path the Sut's helpers and `cling-sync` invocations run in
	keychainFile string // per-test mock-keychain file; isolates `save-passphrase` from sibling tests
}

func NewSut(t *testing.T) *Sut {
	t.Helper()
	assert := lib.NewAssert(t)
	// `t.TempDir` would auto-clean the directory at test end; we deliberately
	// keep it around so failures can be inspected.
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling_sync_integration_*") //nolint:usetesting
	assert.NoError(err, "failed to create temporary directory")
	t.Logf("Using temporary directory: %s", tmpDir)

	workspaceDir := filepath.Join(tmpDir, "workspace")
	err = os.MkdirAll(workspaceDir, 0o700)
	assert.NoError(err, "failed to create workspace directory")

	fs := lib.NewRealFS(workspaceDir)
	sut := &Sut{td.NewTestFS(t, fs), t, assert, workspaceDir, filepath.Join(tmpDir, "keychain.json")}
	sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "init", "../repository")
	sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-passphrase")

	return sut
}

func (s *Sut) Path(rel string) string {
	return filepath.Clean(filepath.Join(s.workDir, rel))
}

func (s *Sut) Chdir(rel string) {
	s.t.Helper()
	s.workDir = s.Path(rel)
	s.TestFS = td.NewTestFS(s.t, lib.NewRealFS(s.workDir))
}

func (s *Sut) ClingSync(args ...string) string {
	s.t.Helper()
	return s.ClingSyncStdin("", args...)
}

// Same as `Run`, but pass the given string to stdin.
func (s *Sut) ClingSyncStdin(stdin string, args ...string) string {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > cling-sync %s", strings.Join(args, " "))))
	cmd := s.cmd(args...)
	cmd.Stdin = strings.NewReader(stdin)
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	s.assert.NoError(err, "failed to run `cling-sync %s`: %s", strings.Join(args, " "), stderr.String())
	res := strings.TrimSpace(stdout.String())
	return res
}

// Same as `Run`, but is expected to fail.
// Return stderr.
func (s *Sut) ClingSyncError(args ...string) string {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > cling-sync %s", strings.Join(args, " "))))
	cmd := s.cmd(args...)
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	s.assert.Error(err, "", "`cling-sync %s` should fail, but got %s", strings.Join(args, " "), stdout.String())
	res := strings.TrimSpace(stderr.String())
	return res
}

func (s *Sut) RepositoryHead() string {
	s.t.Helper()
	log := head(s.ClingSync("log", "--short"))
	return td.Column(log, 1)
}

func (s *Sut) RepositoryHeadDate() string {
	s.t.Helper()
	log := head(s.ClingSync("log", "--short"))
	return td.Column(log, 2)
}

func (s *Sut) Mkdir(path string) {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > mkdir(%s)", path)))
	s.TestFS.Mkdir(path)
}

func (s *Sut) Ls() string {
	s.t.Helper()
	s.t.Log(gray("    > ls"))
	lines := []string{}
	err := s.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}
		if path == ".cling" {
			return filepath.SkipDir
		}
		info, err := d.Info()
		s.assert.NoError(err, "failed to get info for %s", path)
		size := info.Size()
		if d.IsDir() {
			size = 0
			path += "/"
		}
		lines = append(
			lines,
			fmt.Sprintf(
				"%s %12d %d.%09d0  %s",
				info.Mode().String(),
				size,
				info.ModTime().Unix(),
				info.ModTime().Nanosecond(),
				path,
			),
		)
		return nil
	})
	s.assert.NoError(err, "failed to walk directory")
	return strings.Join(lines, "\n")
}

func (s *Sut) Write(path string, content string) {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > add(%s, %q)", path, content)))
	s.TestFS.Write(path, content)
}

func (s *Sut) Rm(path string) {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > rm(%s)", path)))
	s.TestFS.Rm(path)
}

func (s *Sut) Touch(path string, mtime time.Time) {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > touch(%s, %s)", path, mtime.Format(time.RFC3339))))
	s.TestFS.Touch(path, mtime)
}

func (s *Sut) cmd(args ...string) *exec.Cmd {
	cmd := exec.Command(clingSyncBin, args...)
	cmd.Dir = s.workDir
	// The mock keychain backs every entry with a single JSON file. Without
	// per-test isolation, parallel `save-passphrase` calls would race on
	// the read-modify-write of that file.
	cmd.Env = append(os.Environ(), "CLING_SYNC_MOCK_KEYCHAIN_FILE="+s.keychainFile)
	return cmd
}

func gray(s string) string {
	// todo: Ignore color codes in CI for example.
	return "\033[90m" + s + "\033[0m"
}

func headFromRepository(t *testing.T, repo *lib.Repository) string {
	t.Helper()
	assert := lib.NewAssert(t)
	head, err := repo.Head()
	assert.NoError(err)
	return head.String()
}

func assertSameRepositoryHistory(t *testing.T, src, dst *lib.Repository) {
	t.Helper()
	assert := lib.NewAssert(t)
	srcRevisionId, err := src.Head()
	assert.NoError(err)
	dstRevisionId, err := dst.Head()
	assert.NoError(err)
	assert.Equal(srcRevisionId, dstRevisionId)
	buf := lib.NewBlockBuf()
	for !srcRevisionId.IsRoot() {
		srcRevision, err := src.ReadRevision(srcRevisionId, buf)
		assert.NoError(err)
		dstRevision, err := dst.ReadRevision(dstRevisionId, buf)
		assert.NoError(err)
		assert.Equal(srcRevision, dstRevision)
		srcRevisionId = srcRevision.ParentRevisionId
		dstRevisionId = dstRevision.ParentRevisionId
	}
	assert.Equal(true, dstRevisionId.IsRoot())
}

func assertSameRepositoryFS(t *testing.T, srcRoot, dstRoot string) {
	t.Helper()
	assert := lib.NewAssert(t)
	srcPaths := []string{}
	dstPaths := []string{}
	srcModes := map[string]os.FileMode{}
	dstModes := map[string]os.FileMode{}
	err := filepath.WalkDir(srcRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcRoot, path)
		if err != nil {
			return err
		}
		if rel == ".cling/repository/locks" {
			return filepath.SkipDir
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		srcPaths = append(srcPaths, rel)
		srcModes[rel] = info.Mode()
		return nil
	})
	assert.NoError(err)
	err = filepath.WalkDir(dstRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dstRoot, path)
		if err != nil {
			return err
		}
		if rel == ".cling/repository/locks" {
			return filepath.SkipDir
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		dstPaths = append(dstPaths, rel)
		dstModes[rel] = info.Mode()
		return nil
	})
	assert.NoError(err)
	sort.Strings(srcPaths)
	sort.Strings(dstPaths)
	assert.Equal(srcPaths, dstPaths)
	for _, rel := range srcPaths {
		assert.Equal(srcModes[rel], dstModes[rel], rel)
		if srcModes[rel].IsDir() {
			continue
		}
		srcData, err := os.ReadFile(filepath.Join(srcRoot, rel))
		assert.NoError(err)
		dstData, err := os.ReadFile(filepath.Join(dstRoot, rel))
		assert.NoError(err)
		assert.Equal(srcData, dstData, rel)
	}
}

func head(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) == 0 {
		return ""
	}
	return lines[0]
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cling_sync_integration_bin_*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create build tmpdir: %v\n", err)
		os.Exit(1)
	}
	clingSyncBin = filepath.Join(dir, "cling-sync")
	buildArgs := []string{"build"}
	if os.Getenv("CS_TEST_NO_MOCK") == "" {
		buildArgs = append(buildArgs, "-tags", "mock")
	}
	buildArgs = append(buildArgs, "-o", clingSyncBin, "../cli")
	cmd := exec.Command("go", buildArgs...)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build cling-sync: %v\n%s\n", err, string(out))
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
