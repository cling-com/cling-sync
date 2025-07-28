//nolint:paralleltest,forbidigo
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals
const passphrase = "testpassphrase"

// Just test a simple scenario that covers most of the common CLI commands.
func TestHappyPath(t *testing.T) {
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
		`), sut.ClingSync("status"), "There should be local changes")

		sut.ClingSync("merge", "--no-progress", "--message", "second commit")

		log := sut.ClingSync("log", "--short")
		assert.Equal(2, td.Wc("-l", log), "Two revisions should have been created")
		assert.Equal(
			td.Sort(sut.Ls(), 4),
			td.Sort(sut.ClingSync("ls", "--short-file-mode", "--timestamp-format", "unix-fraction"), 4),
			"Files of head should match the workspace")
	}
	rev2Id := sut.RepositoryHead()
	rev2Date := sut.RepositoryHeadDate()

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
		t.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-keys")
		sut.ClingSync("merge", "--no-progress")
		workspace2Ls := sut.Ls()
		assert.Equal(workspace1Ls, workspace2Ls)
	}

	t.Log("Create and resolve conflicts with --accept-local (merge)")
	{
		t.Chdir("../workspace2")
		sut.Write("b.txt", "b from workspace2")
		sut.Mkdir("dir2")
		sut.Write("dir2/e.txt", "e")
		sut.ClingSync("merge", "--no-progress", "--message", "conflict")

		// Back to workspace1 and add conflicting changes.
		t.Chdir("../workspace")
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
		sut.ClingSync("merge", "--no-progress", "--accept-local")
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
}

func TestPathPrefix(t *testing.T) {
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
		t.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-keys")
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
	}

	t.Log("Files have been merged to the right directory")
	{
		t.Chdir("../workspace")
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
		t.Chdir("../workspace2")
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
}

func TestRepositoryOverHTTP(t *testing.T) {
	sut := NewSut(t)
	assert := sut.assert

	serveStdout := bytes.NewBuffer(nil)
	t.Log("Serve repository over HTTP")
	{
		t.Log(gray("    > cling-sync serve --log-requests --address 127.0.0.1:9123 ../repository"))
		cmd := exec.Command("../cling-sync", "serve", "--log-requests", "--address", "127.0.0.1:9123", "../repository")
		stderr := bytes.NewBuffer(nil)
		cmd.Stdout = serveStdout
		cmd.Stderr = stderr
		err := cmd.Start()
		assert.NoError(
			err,
			"failed to serve repository over HTTP: stderr: %s, stdout: %s",
			stderr.String(),
			serveStdout.String(),
		)
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()
		t0 := time.Now()
		for time.Since(t0) < 10*time.Second {
			if strings.Contains(serveStdout.String(), "Serving") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	t.Log("Attach repository over HTTP and merge (merge, ls)")
	{
		workspace1Ls := sut.Ls()
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "attach", "http://localhost:9123", "../workspace2")
		t.Chdir("../workspace2")
		sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-keys")
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

	t.Log("Make sure we actually talked to the HTTP server")
	{
		logs := serveStdout.String()
		assert.Contains(logs, "method=GET path=/storage/open")              // Open
		assert.Contains(logs, "method=GET path=/storage/control/refs/head") // ReadControlFile
		assert.Contains(logs, "method=PUT path=/storage/control/refs/head") // WriteControlFile
		assert.Contains(logs, "method=HEAD path=/storage/block")            // HasBlock
		assert.Contains(logs, "method=GET path=/storage/block")             // ReadBlock
		assert.Contains(logs, "method=PUT path=/storage/block")             // WriteBlock
	}
}

type Sut struct {
	*lib.TestFS
	t      *testing.T
	assert lib.Assert
}

//  1. Create a new repository and change into the workspace directory, so all
//     commands are executed in the workspace.
//     The temporary directory is _not_ cleaned up after the test, so it can be
//     inspected.
//  2. Build the `cling-sync` binary into the temporary directory.
//  3. Create a new repository and change into the workspace directory.
//  4. Save the repository keys so subsequent calls to `cling-sync` don't need
//     the passphrase.
func NewSut(t *testing.T) *Sut {
	t.Helper()
	assert := lib.NewAssert(t)
	tmpDir := filepath.Join(os.TempDir(), "cling_sync_integration")
	t.Logf("Using temporary directory: %s", tmpDir)

	// Make sure the temporary directory can be removed.
	err := filepath.WalkDir(tmpDir, func(path string, d os.DirEntry, err error) error {
		_ = os.Chmod(path, 0o777) //nolint:gosec
		return nil
	})
	assert.NoError(err, "failed to make temporary directory writable")
	err = os.RemoveAll(tmpDir)
	assert.NoError(err, "failed to remove temporary directory")
	err = os.MkdirAll(tmpDir, 0o700)
	assert.NoError(err, "failed to create temporary directory")

	// Build the `cling-sync` binary.
	t.Log("Building `cling-sync` binary")
	buildArgs := []string{"build", "-o", fmt.Sprintf("%s/cling-sync", tmpDir), "../cli"}
	t.Log(gray("    go " + strings.Join(buildArgs, " ")))
	cmd := exec.Command("go", buildArgs...)
	out, err := cmd.CombinedOutput()
	assert.NoError(err, "failed to build `cling-sync` binary: %s", string(out))

	// Create a workspace directory and change into it.
	t.Log("Creating repository")
	workspaceDir := filepath.Join(tmpDir, "workspace")
	err = os.MkdirAll(workspaceDir, 0o700)
	assert.NoError(err, "failed to create workspace directory")
	t.Chdir(workspaceDir)
	assert.NoError(err, "failed to change into temporary directory")

	fs := lib.NewRealFS(".")
	sut := &Sut{td.NewTestFS(t, fs), t, assert}
	sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "init", "../repository")
	sut.ClingSyncStdin(passphrase, "--passphrase-from-stdin", "security", "save-keys")

	return sut
}

// ClingSync the `cling-sync` command with the given arguments.
// The test will fail if the command fails.
// Return the stdout of the command.
func (s *Sut) ClingSync(args ...string) string {
	s.t.Helper()
	return s.ClingSyncStdin("", args...)
}

// Same as `Run`, but pass the given string to stdin.
func (s *Sut) ClingSyncStdin(stdin string, args ...string) string {
	s.t.Helper()
	s.t.Log(gray(fmt.Sprintf("    > cling-sync %s", strings.Join(args, " "))))
	cmd := exec.Command("../cling-sync", args...)
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
	cmd := exec.Command("../cling-sync", args...)
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

func gray(s string) string {
	// todo: Ignore color codes in CI for example.
	return "\033[90m" + s + "\033[0m"
}

func head(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) == 0 {
		return ""
	}
	return lines[0]
}
