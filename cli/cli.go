//nolint:forbidigo
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
	ws "github.com/flunderpero/cling-sync/workspace"
	"golang.org/x/term"
)

const (
	appName                 = "cling-sync"
	fastScanFlagDescription = "Speed up scanning by skipping file hash comparisons.\nFile changes are detected by trusting file metadata (size, ctime, inode).\nWARNING: May miss some changes, especially on network or FUSE file-systems.\nWhen in doubt, run without this flag for thorough verification."
)

func AttachCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Help       bool
		PathPrefix string
	}{}
	flags := flag.NewFlagSet("attach", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.PathPrefix, "path-prefix", "", "Only attach to this path inside the repository")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s attach <repository-uri> <directory>\n\n", appName)
		fmt.Fprint(os.Stderr, "Attach a local directory to a repository.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  repository-uri\n")
		fmt.Fprint(os.Stderr, "        Either a path to a local repository or an URL to\n")
		fmt.Fprintf(os.Stderr, "        a remote repository (see `%s serve`).\n", appName)
		fmt.Fprint(os.Stderr, "  directory\n")
		fmt.Fprint(os.Stderr, "        Path to the local workspace directory the repository\n")
		fmt.Fprint(os.Stderr, "        should be attached to.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 2 {
		return lib.Errorf("two positional arguments are required: <repository-path> <directory>")
	}
	localPath, err := filepath.Abs(flags.Arg(1))
	if err != nil {
		return lib.WrapErrorf(err, "failed to get absolute path for %s", flags.Arg(1))
	}
	// Make sure the local directory either does not exist or is empty.
	_, err = os.Stat(localPath)
	if errors.Is(err, os.ErrNotExist) { //nolint:gocritic
		if err := os.MkdirAll(localPath, 0o700); err != nil {
			return lib.WrapErrorf(err, "failed to create directory %s", localPath)
		}
	} else if err != nil {
		return lib.Errorf("cannot stat local directory %s", localPath)
	} else {
		files, err := os.ReadDir(localPath)
		if err != nil {
			return lib.Errorf("failed to read local directory %s", localPath)
		}
		if len(files) > 0 {
			return lib.Errorf("local directory %s is not empty", localPath)
		}
	}
	repositoryURI := flags.Arg(0)
	var storage lib.Storage
	if clingHTTP.IsHTTPStorageUIR(repositoryURI) {
		storage = clingHTTP.NewHTTPStorageClient(
			repositoryURI,
			clingHTTP.NewDefaultHTTPClient(http.DefaultClient),
		)
	} else {
		repositoryURI, err = filepath.Abs(repositoryURI)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get absolute path for %s", repositoryURI)
		}
		storage, err = lib.NewFileStorage(lib.NewRealFS(repositoryURI), lib.StoragePurposeRepository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to connect to repository storage")
		}
	}
	_, err = openRepositoryWithPassphrase(storage, passphraseFromStdin)
	if err != nil {
		return err
	}
	// We know the repository exists, so let's create the workspace.
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return lib.WrapErrorf(err, "failed to create temporary directory")
	}
	pathPrefix, err := ws.ValidatePathPrefix(args.PathPrefix)
	if err != nil {
		return lib.WrapErrorf(err, "invalid path prefix %q", args.PathPrefix)
	}
	workspace, err := ws.NewWorkspace(
		lib.NewRealFS(localPath),
		lib.NewRealFS(tmpDir),
		ws.RemoteRepository(repositoryURI),
		pathPrefix,
	)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create workspace")
	}
	workspace.Close() //nolint:errcheck,gosec
	fmt.Printf("Attached %s to %s\n", localPath, repositoryURI)
	return nil
}

func InitCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Help                bool
		AllowWeakPassphrase bool
	}{}
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.AllowWeakPassphrase, "allow-weak-passphrase", false, "Allow weak passphrase (not recommended)")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s init <repository-path>\n\n", appName)
		fmt.Fprint(os.Stderr, "Create and initialize a new local repository.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  repository-path\n")
		fmt.Fprint(os.Stderr, "        The repository will be created at this path.\n")
		fmt.Fprint(os.Stderr, "        The directory must not exist or must be empty.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 1 {
		return lib.Errorf("one positional argument is required: <repository-path>")
	}
	if !IsTerm(os.Stdin) && !passphraseFromStdin {
		return lib.Errorf(
			"a new repository can only be created in an interactive terminal session or --passphrase-from-stdin must be used",
		)
	}
	var passphrase []byte
	if passphraseFromStdin {
		var err error
		passphrase, err = io.ReadAll(os.Stdin)
		if err != nil {
			return lib.WrapErrorf(err, "failed to read passphrase from stdin")
		}
	} else {
		_, err := fmt.Fprint(os.Stderr, "Enter passphrase: ")
		if err != nil {
			return err //nolint:wrapcheck
		}
		passphrase, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			return lib.WrapErrorf(err, "failed to read passphrase")
		}
		_, _ = fmt.Fprintln(os.Stdout)
	}
	if err := lib.CheckPassphraseStrength(passphrase); err != nil {
		if args.AllowWeakPassphrase {
			fmt.Fprintf(os.Stderr, "\nWarning: %s\n", err.Error())
		} else {
			return err //nolint:wrapcheck
		}
	}
	if !passphraseFromStdin {
		_, err := fmt.Fprint(os.Stdout, "Repeat passphrase: ")
		if err != nil {
			return err //nolint:wrapcheck
		}
		passphraseRepeat, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			return lib.WrapErrorf(err, "failed to read passphrase")
		}
		if string(passphrase) != string(passphraseRepeat) {
			return lib.Errorf("passphrases do not match")
		}
	}
	repositoryPath, err := filepath.Abs(flags.Arg(0))
	if err != nil {
		return lib.WrapErrorf(err, "failed to get absolute path for %s", flags.Arg(0))
	}
	stat, err := os.Stat(repositoryPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(repositoryPath, 0o700); err != nil {
				return lib.WrapErrorf(err, "failed to create directory %s", repositoryPath)
			}
		} else {
			return lib.WrapErrorf(err, "failed to stat %s", repositoryPath)
		}
	} else if !stat.IsDir() {
		return lib.Errorf("%s is not a directory", repositoryPath)
	}
	files, err := os.ReadDir(repositoryPath)
	if err != nil {
		return lib.WrapErrorf(err, "failed to read directory %s", repositoryPath)
	}
	if len(files) > 0 {
		return lib.Errorf("directory %s is not empty", repositoryPath)
	}
	storage, err := lib.NewFileStorage(lib.NewRealFS(repositoryPath), lib.StoragePurposeRepository)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create storage")
	}
	_, err = lib.InitNewRepository(storage, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to initialize repository")
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return lib.WrapErrorf(err, "failed to create temporary directory")
	}
	workspace, err := ws.NewWorkspace(
		lib.NewRealFS("."),
		lib.NewRealFS(tmpDir),
		ws.RemoteRepository(repositoryPath),
		lib.Path{},
	)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create workspace")
	}
	workspace.Close() //nolint:errcheck,gosec
	return nil
}

func CpCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help         bool
		Revision     string
		IgnoreErrors bool
		Verbose      bool
		NoProgress   bool
		Overwrite    bool
		Chown        bool
		Exclude      lib.ExtendedGlobPatterns
		Include      lib.ExtendedGlobPatterns
	}{}
	flags := flag.NewFlagSet("cp", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to copy from")
	flags.BoolVar(&args.IgnoreErrors, "ignore-errors", false, "Ignore errors")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Chown, "chown", false, "Restore file ownership from the repository.")
	flags.BoolVar(&args.Overwrite, "overwrite", false, "Overwrite existing files")
	globPatternFlag(
		flags,
		"exclude",
		"Exclude paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as for the <pattern> argument.",
		&args.Exclude,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s cp <pattern> <target>\n\n", appName)
		fmt.Fprint(os.Stderr, "Copy files from the repository to a local directory.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  pattern\n")
		fmt.Fprint(
			os.Stderr,
			"        Repository paths matching the given pattern are copied.\n"+globPatternDescription("        "),
		)
		fmt.Fprint(os.Stderr, "\n  target\n")
		fmt.Fprint(os.Stderr, "        The target directory where files are copied to.\n")
		fmt.Fprint(os.Stderr, "        Example: `cp path/to/file /tmp` creates `/tmp/path/to/file`\n")
		fmt.Fprint(os.Stderr, "\n\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 2 {
		return lib.Errorf("two positional arguments are required: <pattern> <target>")
	}
	if len(args.Exclude) == 0 && len(args.Include) > 0 {
		return lib.Errorf("include patterns can only be used with exclude patterns")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	pathFilter := &lib.AllPathFilter{Filters: []lib.PathFilter{
		lib.NewPathInclusionFilter([]string{flags.Arg(0)}),
		&lib.PathExclusionFilter{args.Exclude},
	}}
	cpOnExists := ws.CpOnExistsAbort
	if args.Overwrite {
		cpOnExists = ws.CpOnExistsOverwrite
	}
	mon := NewCpMonitor(cpOnExists, args.Verbose, args.IgnoreErrors, args.NoProgress)
	revisionId, err := revisionId(repository, args.Revision)
	if err != nil {
		return err
	}
	opts := &ws.CpOptions{
		PathFilter:             pathFilter,
		Monitor:                mon,
		RevisionId:             revisionId,
		RestorableMetadataFlag: lib.RestorableMetadataAll,
	}
	if !args.Chown {
		opts.RestorableMetadataFlag ^= lib.RestorableMetadataOwnership
	}
	tmpFS, err := workspace.TempFS.MkSub("cp")
	if err != nil {
		return err //nolint:wrapcheck
	}
	err = ws.Cp(repository, lib.NewRealFS(flags.Arg(1)), opts, tmpFS)
	mon.Close()
	if args.IgnoreErrors && mon.errors > 0 {
		fmt.Printf("%d errors ignored\n", mon.errors)
	}
	if err != nil {
		return err //nolint:wrapcheck
	}
	mbs := (float64(mon.bytesWritten) / float64(time.Since(mon.startTime).Seconds()))
	fmt.Printf(
		"%d files copied (%s at %s/s)\n",
		mon.paths,
		ws.FormatBytes(mon.bytesWritten),
		ws.FormatBytes(int64(mbs)),
	)
	return nil
}

func ResetCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help       bool
		Chown      bool
		Chtime     bool
		Chmod      bool
		Verbose    bool
		NoProgress bool
		FastScan   bool
		Force      bool
	}{}
	flags := flag.NewFlagSet("reset", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Chown, "chown", false, "Include file ownership changes")
	flags.BoolVar(&args.Chmod, "chmod", false, "Include file mode changes")
	flags.BoolVar(&args.Chtime, "chtime", false, "Include file time changes")
	flags.BoolVar(&args.FastScan, "fast-scan", false, fastScanFlagDescription)
	flags.BoolVar(&args.Force, "force", false, "Ignore local changes. All local changes will be lost.")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s reset <revision-id>\n\n", appName)
		fmt.Fprint(os.Stderr, "Reset the workspace to a specific revision.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 1 {
		return lib.Errorf("one positional argument is required: <revision-id>")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	revisionId, err := revisionId(repository, flags.Arg(0))
	if err != nil {
		return err
	}
	stagingMonitor := NewStagingMonitor(args.Verbose, args.NoProgress)
	cpMonitor := NewCpMonitor(ws.CpOnExistsAbort, args.Verbose, false, args.NoProgress)
	restorableMetadataFlag := lib.RestorableMetadataAll
	if !args.Chown {
		restorableMetadataFlag ^= lib.RestorableMetadataOwnership
	}
	if !args.Chtime {
		restorableMetadataFlag ^= lib.RestorableMetadataMTime
	}
	if !args.Chmod {
		restorableMetadataFlag ^= lib.RestorableMetadataMode
	}
	opts := &ws.ResetOptions{
		RevisionId:             revisionId,
		Force:                  args.Force,
		StagingMonitor:         stagingMonitor,
		CpMonitor:              cpMonitor,
		RestorableMetadataFlag: restorableMetadataFlag,
		UseStagingCache:        args.FastScan,
	}
	if err := ws.Reset(workspace, repository, opts); err != nil {
		return err //nolint:wrapcheck
	}
	stagingMonitor.Close()
	wsHead, err := workspace.Head()
	if err != nil {
		return err //nolint:wrapcheck
	}
	fmt.Printf("Reset to revision %s\n", wsHead)
	return nil
}

func MergeCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help        bool
		Message     string
		Author      string
		Chown       bool
		Chtime      bool
		Chmod       bool
		Verbose     bool
		AcceptLocal bool
		NoProgress  bool
		FastScan    bool
	}{}
	defaultAuthor := "<anonymous>"
	whoami, err := user.Current()
	if err == nil {
		defaultAuthor = whoami.Username
	}
	defaultMessage := "Synced with cling-sync"
	flags := flag.NewFlagSet("merge", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.AcceptLocal, "accept-local", false, "Ignore all conflicts and commit all local changes")
	flags.BoolVar(&args.Chown, "chown", false, "Include file ownership changes")
	flags.BoolVar(&args.Chmod, "chmod", false, "Include file mode changes")
	flags.BoolVar(&args.Chtime, "chtime", false, "Include file time changes")
	flags.BoolVar(&args.FastScan, "fast-scan", false, fastScanFlagDescription)
	flags.StringVar(&args.Author, "author", defaultAuthor, "Author name")
	flags.StringVar(&args.Message, "message", defaultMessage, "Commit message")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s merge\n\n", appName)
		fmt.Fprint(os.Stderr, "Commit all local changes to the repository\n")
		fmt.Fprint(os.Stderr, "and merge all changes from the repository into the workspace.\n")
		fmt.Fprint(os.Stderr, "As a result, the workspace will be identical to the repository.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 0 {
		return lib.Errorf("no positional arguments allowed")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	stagingMonitor := NewStagingMonitor(args.Verbose, args.NoProgress)
	cpMonitor := NewCpMonitor(ws.CpOnExistsAbort, args.Verbose, false, args.NoProgress)
	commitMonitor := NewCommitMonitor(args.Verbose, args.NoProgress)
	restorableMetadataFlag := lib.RestorableMetadataAll
	if !args.Chown {
		restorableMetadataFlag ^= lib.RestorableMetadataOwnership
	}
	if !args.Chtime {
		restorableMetadataFlag ^= lib.RestorableMetadataMTime
	}
	if !args.Chmod {
		restorableMetadataFlag ^= lib.RestorableMetadataMode
	}
	opts := &ws.MergeOptions{
		Author:                 args.Author,
		Message:                args.Message,
		StagingMonitor:         stagingMonitor,
		CpMonitor:              cpMonitor,
		CommitMonitor:          commitMonitor,
		RestorableMetadataFlag: restorableMetadataFlag,
		UseStagingCache:        args.FastScan,
	}
	var revisionId lib.RevisionId
	if args.AcceptLocal {
		revisionId, err = ws.ForceCommit(workspace, repository, &ws.ForceCommitOptions{MergeOptions: *opts})
	} else {
		revisionId, err = ws.Merge(workspace, repository, opts)
	}
	stagingMonitor.Close()
	if errors.Is(err, ws.ErrUpToDate) {
		fmt.Println("No changes")
		return nil
	}
	conflicts := ws.MergeConflictsError{}
	if errors.As(err, &conflicts) {
		var sb strings.Builder
		sb.WriteString("merge aborted due to conflicts:\n\n")
		for _, conflict := range conflicts {
			sb.WriteString(fmt.Sprintf("  %s (remote: %s, local: %s)\n",
				conflict.WorkspaceEntry.Path,
				conflict.RepositoryEntry.Type,
				conflict.WorkspaceEntry.Type,
			))
		}
		sb.WriteString(fmt.Sprintf(`
No files were changed, you need to resolve the conflicts manually.

To accept all local changes, run `+"`"+`%s merge --accept-local`+"`"+`
To select remote changes, run `+"`"+`%s cp --overwrite <remote-path> .`+"`"+`
`, appName, appName))
		return lib.Errorf("%s", sb.String())
	}
	if err != nil {
		return err //nolint:wrapcheck
	}
	if commitMonitor.paths == 0 {
		fmt.Println("No local changes, workspace is up to date now")
		return nil
	}
	compressionRatio := "n/a"
	if commitMonitor.rawBytesAdded > 0 {
		compressionRatio = fmt.Sprintf(
			"%.2f",
			float64(commitMonitor.compressedBytesAdded)/float64(commitMonitor.rawBytesAdded),
		)
	}
	fmt.Printf(
		"Revision %s (%s added, compressed: %s)\n",
		revisionId,
		ws.FormatBytes(commitMonitor.rawBytesAdded),
		compressionRatio,
	)
	return nil
}

func StatusCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help       bool
		Short      bool
		Verbose    bool
		NoProgress bool
		Exclude    lib.ExtendedGlobPatterns
		NoSummary  bool
		Chown      bool
		Chmod      bool
		Chtime     bool
		FastScan   bool
	}{}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Only show the number of added, updated, and deleted files")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Chown, "chown", false, "Include file ownership changes")
	flags.BoolVar(&args.Chmod, "chmod", false, "Include file mode changes")
	flags.BoolVar(&args.Chtime, "chtime", false, "Include file time changes")
	flags.BoolVar(&args.FastScan, "fast-scan", false, fastScanFlagDescription)
	flags.BoolVar(&args.NoSummary, "no-summary", false, "Do not show a summary at the end")
	globPatternFlag(
		flags,
		"exclude",
		"Exclude paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as the [pattern] argument.",
		&args.Exclude,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s status [pattern]\n\n", appName)
		fmt.Fprint(os.Stderr, "Show the difference between the working directory and the repository.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  pattern (optional)\n")
		fmt.Fprint(
			os.Stderr,
			"        Show status only for paths matching the given pattern.\n"+globPatternDescription("        "),
		)
		fmt.Fprint(os.Stderr, "\n\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	var pathFilter lib.PathFilter
	if len(flags.Args()) == 1 {
		pathFilter = lib.NewPathInclusionFilter([]string{flags.Arg(0)})
	}
	if len(flags.Args()) > 1 {
		return lib.Errorf("too many positional arguments")
	}
	if len(args.Exclude) > 0 {
		exclusionFilter := &lib.PathExclusionFilter{args.Exclude}
		if pathFilter != nil {
			pathFilter = &lib.AllPathFilter{[]lib.PathFilter{pathFilter, exclusionFilter}}
		} else {
			pathFilter = exclusionFilter
		}
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	tmpFS, err := workspace.TempFS.MkSub("status")
	if err != nil {
		return err //nolint:wrapcheck
	}
	mon := NewStagingMonitor(args.Verbose, args.NoProgress)
	restorableMetadataFlag := lib.RestorableMetadataAll
	if !args.Chown {
		restorableMetadataFlag ^= lib.RestorableMetadataOwnership
	}
	if !args.Chtime {
		restorableMetadataFlag ^= lib.RestorableMetadataMTime
	}
	if !args.Chmod {
		restorableMetadataFlag ^= lib.RestorableMetadataMode
	}
	opts := &ws.StatusOptions{
		PathFilter:             pathFilter,
		Monitor:                mon,
		RestorableMetadataFlag: restorableMetadataFlag,
		UseStagingCache:        args.FastScan,
	}
	result, err := ws.Status(workspace, repository, opts, tmpFS)
	mon.Close()
	if err != nil {
		return err //nolint:wrapcheck
	}
	if args.Short {
		fmt.Println(result.Summary())
		return nil
	}
	for _, file := range result {
		fmt.Println(file.Format())
	}
	if !args.NoSummary {
		fmt.Println(result.Summary())
	}
	return nil
}

func LsCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help            bool
		Revision        string
		Short           bool
		Human           bool
		TimestampFormat string
		ShortFileMode   bool
		FileHash        bool
	}{
		TimestampFormat: time.RFC3339,
	}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to show")
	flags.BoolVar(&args.Short, "short", false, "Show short listing (same as --timestamp-format=relative)")
	flags.BoolVar(&args.FileHash, "file-hash", false, "Show file hash")
	flags.BoolVar(
		&args.Human,
		"human",
		false,
		"Show human readable file sizes (same as --timestamp-format=rfc3339 --full-file-mode)",
	)
	flags.Func(
		"timestamp-format",
		"Timestamp format: relative, rfc3339, unix, unix-fraction (default rfc3339)",
		func(value string) error {
			if value != "relative" && value != "rfc3339" && value != "unix" && value != "unix-fraction" {
				return lib.Errorf("invalid timestamp-format: %s", value)
			}
			if value == "rfc3339" {
				value = time.RFC3339
			}
			args.TimestampFormat = value
			return nil
		},
	)
	flags.BoolVar(
		&args.ShortFileMode,
		"short-file-mode",
		false,
		"Show short file mode (only permissions and file type)",
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s ls [pattern]\n\n", appName)
		fmt.Fprint(os.Stderr, "List files in the repository.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  pattern\n")
		fmt.Fprint(os.Stderr, "        The pattern syntax is the same as for the `commit --ignore` option.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	var pathFilter lib.PathFilter
	if len(flags.Args()) == 1 {
		pathFilter = lib.NewPathInclusionFilter([]string{flags.Arg(0)})
	}
	if len(flags.Args()) > 1 {
		return lib.Errorf("too many positional arguments")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	revisionId, err := revisionId(repository, args.Revision)
	if err != nil {
		return err
	}
	opts := &ws.LsOptions{RevisionId: revisionId, PathFilter: pathFilter, PathPrefix: workspace.PathPrefix}
	tmpFS, err := workspace.TempFS.MkSub("ls")
	if err != nil {
		return err //nolint:wrapcheck
	}
	files, err := ws.Ls(repository, tmpFS, opts)
	if err != nil {
		return err //nolint:wrapcheck
	}
	if args.Short {
		args.TimestampFormat = "relative"
		args.ShortFileMode = true
	}
	if args.Human {
		args.TimestampFormat = "rfc3339"
		args.ShortFileMode = false
	}
	format := &ws.LsFormat{
		FullPath:          true,
		FullMode:          !args.ShortFileMode,
		FileHash:          args.FileHash,
		TimestampFormat:   args.TimestampFormat,
		HumanReadableSize: args.Human,
	}
	dirFormat := *format
	dirFormat.FullPath = true
	for i, file := range files {
		if args.Short {
			if file.Metadata.ModeAndPerm.IsDir() {
				if i > 0 {
					fmt.Println()
				}
				fmt.Println(file.Format(&dirFormat))
				continue
			}
			fmt.Println(file.Format(format))
		} else {
			fmt.Println(file.Format(format))
		}
	}
	return nil
}

func LogCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help   bool
		Short  bool
		Status bool
	}{}
	flags := flag.NewFlagSet("log", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Show short log")
	flags.BoolVar(&args.Status, "status", false, "Show status of paths affected in a revision")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s log [pattern]\n\n", appName)
		fmt.Fprint(os.Stderr, "Show revision log.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  pattern (optional)\n")
		fmt.Fprint(os.Stderr, "        Show log only for paths matching the given pattern.\n")
		fmt.Fprint(os.Stderr, globPatternDescription("        "))
		fmt.Fprint(os.Stderr, "\n\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) > 1 {
		return lib.Errorf("too many positional arguments")
	}
	var pathFilter lib.PathFilter
	if len(flags.Args()) == 1 {
		pathFilter = lib.NewPathInclusionFilter([]string{flags.Arg(0)})
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	opts := &ws.LogOptions{PathFilter: pathFilter, Status: args.Status}
	logs, err := ws.Log(repository, opts)
	if err != nil {
		return err //nolint:wrapcheck
	}
	if len(logs) == 0 {
		fmt.Println("No revisions")
	}
	for i, log := range logs {
		if args.Short {
			fmt.Println(log.Short())
		} else {
			if i > 0 {
				fmt.Println()
			}
			fmt.Println(log.Long())
		}
		if !args.Status {
			continue
		}
		fmt.Println()
		for _, file := range log.Files {
			fmt.Printf("    %s\n", file.Format())
		}
		if args.Short && i < len(logs)-1 {
			fmt.Println()
		}
	}
	return nil
}

func CheckCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help       bool
		Verbose    bool
		NoProgress bool
		Data       bool
	}{}
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Data, "data", false, "Check all file data blocks of all paths in all revisions")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s check\n\n", appName)
		fmt.Fprint(os.Stderr, "Check the health of the repository.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 0 {
		return lib.Errorf("too many positional arguments")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	monitor := NewHeathCheckMonitor(args.Verbose, args.NoProgress)
	err = lib.CheckHealth(repository, lib.HealthCheckOptions{Monitor: monitor, DataBlocks: args.Data})
	monitor.Close()
	if err != nil {
		return err //nolint:wrapcheck
	}
	fmt.Printf("Repository is healthy\n")
	fmt.Printf("  [ok] revision chain is intact\n")
	fmt.Printf("  [ok] metadata blocks are valid\n")
	fmt.Printf("  [ok] paths in each revision are sorted\n")
	dataChecked := "--"
	if args.Data {
		dataChecked = "ok"
	}
	fmt.Printf("  [%s] actual file size matches path metadata\n", dataChecked)
	fmt.Printf("  [%s] actual file hash matches path metadata\n", dataChecked)
	fmt.Printf("  [%s] data blocks are valid\n", dataChecked)
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("  %d revisions\n", monitor.Revisions)
	fmt.Printf("  %d paths entries in all revisions\n", monitor.Paths)
	dataBlocks := "data blocks not checked"
	dataSize := "data blocks not checked"
	if args.Data {
		dataBlocks = fmt.Sprintf("%d data", monitor.DataBlocks)
		dataSize = fmt.Sprintf("%s (%dB) data", ws.FormatBytes(monitor.DataBytes), monitor.DataBytes)
	}
	fmt.Printf(
		"  %d unique blocks, %d metadata, %s\n",
		monitor.DataBlocks+monitor.MetadataBlocks,
		monitor.MetadataBlocks,
		dataBlocks,
	)
	totalBytes := monitor.MetadataBytes + monitor.DataBytes
	fmt.Printf("  %s (%dB) total, %s (%dB) metadata, %s\n",
		ws.FormatBytes(totalBytes),
		totalBytes,
		ws.FormatBytes(monitor.MetadataBytes),
		monitor.MetadataBytes,
		dataSize,
	)
	return nil
}

func SecurityCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help bool
	}{}
	flags := flag.NewFlagSet("security", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s security [command]\n\n", appName)
		fmt.Fprint(os.Stderr, "Configure security settings.\n\n")
		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprint(os.Stderr, "  save-keys\n")
		fmt.Fprint(os.Stderr, "        Save the keys of the repository so that this client stays authenticated.\n")
		fmt.Fprint(os.Stderr, "        The keys are encrypted with a random key that is securely stored in the\n")
		fmt.Fprint(os.Stderr, "        system's keyring.\n")
		fmt.Fprint(os.Stderr, "        If you change the keys of the repository, all saved keys become invalid.\n")
		fmt.Fprint(os.Stderr, "  delete-keys\n")
		fmt.Fprintf(os.Stderr, "        Delete the keys previously saved using `%s security save-keys`.\n", appName)
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) == 0 {
		return lib.Errorf("missing command")
	}
	switch flags.Arg(0) {
	case "save-keys":
		if len(flags.Args()) != 1 {
			return lib.Errorf("too many positional arguments")
		}
		repositoryStorage, err := openRepositoryStorage(workspace)
		if err != nil {
			return err
		}
		passphrase, err := readPassphrase(passphraseFromStdin)
		if err != nil {
			return err
		}
		keys, err := lib.DecryptRepositoryKeys(repositoryStorage, passphrase)
		if err != nil {
			return lib.WrapErrorf(err, "failed to decrypt repository keys")
		}
		encKey, err := lib.NewRawKey()
		if err != nil {
			return lib.WrapErrorf(err, "failed to generate local encryption key")
		}
		encKeyStr := hex.EncodeToString(encKey[:])
		encKeyCipher, err := lib.NewCipher(encKey)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create cipher")
		}
		err = AddKeychainEntry("com.cling.sync", string(workspace.RemoteRepository), encKeyStr)
		if errors.Is(err, ErrKeychainEntryAlreadyExists) {
			// Use the existing key to encrypt the RepositoryKeys.
			encKeyStr, err = GetKeychainEntry("com.cling.sync", string(workspace.RemoteRepository))
			if err != nil {
				return lib.WrapErrorf(err, "failed to get encryption key from keychain")
			}
			encKeyBytes, err := hex.DecodeString(encKeyStr)
			if err != nil {
				return lib.WrapErrorf(err, "failed to decode encryption key from keychain")
			}
			encKeyCipher, err = lib.NewCipher(lib.RawKey(encKeyBytes))
			if err != nil {
				return lib.WrapErrorf(err, "failed to create cipher")
			}
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to add repository keys to keychain")
		}
		if err := workspace.WriteRepositoryKeys(keys, encKeyCipher); err != nil {
			return lib.WrapErrorf(err, "failed to write repository keys")
		}
	case "delete-keys":
		if len(flags.Args()) != 1 {
			return lib.Errorf("too many positional arguments")
		}
		if err := workspace.DeleteRepositoryKeys(); err != nil {
			return lib.WrapErrorf(err, "failed to delete repository keys")
		}
		fmt.Println("Keys deleted")
	default:
		return lib.Errorf("unknown command: %s", flags.Arg(0))
	}
	return nil
}

func ServeCmd(argv []string) error {
	args := struct { //nolint:exhaustruct
		Address      string
		LogRequests  bool
		CORSAllowAll bool
		ReadTimeout  time.Duration
		WriteTimeout time.Duration
		Help         bool
	}{}
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.LogRequests, "log-requests", false, "Log all requests")
	flags.BoolVar(&args.CORSAllowAll, "cors-allow-all", false, "Allow all origins to access the repository (dangerous)")
	flags.StringVar(&args.Address, "address", "0.0.0.0:4242", "Address to listen on")
	flags.DurationVar(&args.ReadTimeout, "read-timeout", 10*time.Second, "Timeout for reading a response")
	flags.DurationVar(&args.WriteTimeout, "write-timeout", 10*time.Second, "Timeout for writing a response")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s serve <repository-path>\n\n", appName)
		fmt.Fprint(os.Stderr, "Serve a repository over HTTP.\n")
		fmt.Fprint(os.Stderr, "\nArguments:\n")
		fmt.Fprint(os.Stderr, "  repository-path\n")
		fmt.Fprint(os.Stderr, "        Path to the local repository to serve\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) != 1 {
		return lib.Errorf("one positional argument is required: <path-to-repository>")
	}
	repositoryPath, err := filepath.Abs(flags.Arg(0))
	if err != nil {
		return lib.WrapErrorf(err, "failed to get absolute path for %s", flags.Arg(0))
	}
	storage, err := lib.NewFileStorage(lib.NewRealFS(repositoryPath), lib.StoragePurposeRepository)
	if err != nil {
		return lib.WrapErrorf(err, "failed to open storage")
	}
	storageServer := clingHTTP.NewHTTPStorageServer(storage, args.Address)
	mux := http.NewServeMux()
	var handler http.Handler = mux
	storageServer.RegisterRoutes(mux)
	if args.LogRequests {
		handler = clingHTTP.RequestLogMiddleware(handler)
	}
	if args.CORSAllowAll {
		handler = clingHTTP.CORSMiddleware(handler)
	}
	server := &http.Server{ //nolint:exhaustruct
		Addr:         args.Address,
		Handler:      handler,
		ReadTimeout:  args.ReadTimeout,
		WriteTimeout: args.WriteTimeout,
	}
	fmt.Printf("Serving %s at http://%s\n", repositoryPath, args.Address)
	if err := server.ListenAndServe(); err != nil {
		return lib.WrapErrorf(err, "failed to serve repository")
	}
	return nil
}

func revisionId(repository *lib.Repository, revision string) (lib.RevisionId, error) {
	var revisionId lib.RevisionId
	if strings.ToLower(revision) == "head" {
		revisionId, err := repository.Head()
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read head revision")
		}
		return revisionId, nil
	}
	b, err := hex.DecodeString(revision)
	if err != nil {
		return lib.RevisionId{}, lib.Errorf("invalid revision id: %s", revision)
	}
	revisionId = lib.RevisionId(b)
	return revisionId, nil
}

func openWorkspace() (*ws.Workspace, error) {
	path, err := filepath.Abs(".")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get absolute path for %s", path)
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary directory")
	}
	return ws.OpenWorkspace(lib.NewRealFS(path), lib.NewRealFS(tmpDir)) //nolint:wrapcheck
}

func openRepository(workspace *ws.Workspace, passphraseFromStdin bool) (*lib.Repository, error) {
	storage, err := openRepositoryStorage(workspace)
	if err != nil {
		return nil, err
	}
	if workspace.HasRepositoryKeys() {
		encKeyStr, err := GetKeychainEntry("com.cling.sync", string(workspace.RemoteRepository))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to get encryption key from keychain")
		}
		encKey, err := hex.DecodeString(encKeyStr)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to decode encryption key from keychain")
		}
		encKeyCipher, err := lib.NewCipher(lib.RawKey(encKey))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to create cipher")
		}
		keys, err := workspace.ReadRepositoryKeys(encKeyCipher)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read repository keys from local storage")
		}
		repository, err := lib.OpenRepositoryWithKeys(storage, keys)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to open repository with saved keys")
		}
		return repository, nil
	}
	return openRepositoryWithPassphrase(storage, passphraseFromStdin)
}

func openRepositoryWithPassphrase(storage lib.Storage, passphraseFromStdin bool) (*lib.Repository, error) {
	passphrase, err := readPassphrase(passphraseFromStdin)
	if err != nil {
		return nil, err
	}
	repository, err := lib.OpenRepository(storage, passphrase)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open repository")
	}
	return repository, nil
}

func readPassphrase(passphraseFromStdin bool) ([]byte, error) {
	if !IsTerm(os.Stdin) && !passphraseFromStdin {
		return nil, lib.Errorf(
			"this command can only be run in an interactive terminal session or --passphrase-from-stdin must be used",
		)
	}
	var passphrase []byte
	if passphraseFromStdin {
		var err error
		passphrase, err = io.ReadAll(os.Stdin)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read passphrase from stdin")
		}
	} else {
		_, err := fmt.Fprint(os.Stderr, "Enter passphrase: ")
		if err != nil {
			return nil, err //nolint:wrapcheck
		}
		passphrase, err = term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read passphrase")
		}
		fmt.Fprint(os.Stderr, "\r                          \r")
	}
	return passphrase, nil
}

func openRepositoryStorage(workspace *ws.Workspace) (lib.Storage, error) { //nolint:ireturn
	if clingHTTP.IsHTTPStorageUIR(string(workspace.RemoteRepository)) {
		return clingHTTP.NewHTTPStorageClient(
			string(workspace.RemoteRepository),
			clingHTTP.NewDefaultHTTPClient(http.DefaultClient),
		), nil
	}
	storage, err := lib.NewFileStorage(lib.NewRealFS(string(workspace.RemoteRepository)), lib.StoragePurposeRepository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	return storage, nil
}

func globPatternDescription(indent string) string {
	// todo: Explain more and add examples
	return indent + strings.ReplaceAll(strings.TrimSpace(`
Patterns follow the same syntax as the git-ignore files.
Pattern syntax:
    **      matches any number of directories
    *       matches any number of characters in a single directory
    ?       matches a single character
	`), "\n", "\n"+indent)
}

func globPatternFlag(flags *flag.FlagSet, name string, usage string, value *lib.ExtendedGlobPatterns) {
	flags.Func(
		name,
		usage,
		func(pattern string) error {
			p := lib.NewExtendedGlobPattern(pattern, "")
			*value = append(*value, p)
			return nil
		},
	)
}

func main() { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Help                bool
		PassphraseFromStdin bool
	}{}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [command arguments]\n\n", appName)
		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprint(os.Stderr, "  attach       Attach a local directory to a repository\n")
		fmt.Fprint(os.Stderr, "  check        Check the health of the repository\n")
		fmt.Fprint(os.Stderr, "  cp           Copy files from the repository to a local directory\n")
		fmt.Fprint(os.Stderr, "  init         Initialize a new repository\n")
		fmt.Fprint(os.Stderr, "  ls           List files in the repository\n")
		fmt.Fprint(os.Stderr, "  log          Show revision log\n")
		fmt.Fprint(os.Stderr, "  merge        Merge changes from the repository and the workspace\n")
		fmt.Fprint(os.Stderr, "  reset        Reset the workspace to a specific revision\n")
		fmt.Fprint(os.Stderr, "  security	  See and configure security settings\n")
		fmt.Fprint(os.Stderr, "  serve        Serve a repository over HTTP\n")
		fmt.Fprint(os.Stderr, "  status       Show repository status\n")
		fmt.Fprint(os.Stderr, "\nGlobal flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nRun '%s <command> --help' for more information on a command.\n", appName)
	}
	flag.BoolVar(&args.Help, "help", false, "Show help message")
	flag.BoolVar(
		&args.PassphraseFromStdin,
		"passphrase-from-stdin",
		false,
		"Read passphrase from stdin - useful for scripting, but use with caution as it might expose the passphrase",
	)
	flag.Parse()
	if args.Help {
		flag.Usage()
		os.Exit(0)
	}
	if flag.NArg() < 1 {
		PrintErr("Missing command\n")
		flag.Usage()
		os.Exit(1)
	}
	argv := flag.Args()[1:]
	cmd := flag.Arg(0)
	var err error
	switch cmd {
	case "attach":
		err = AttachCmd(argv, args.PassphraseFromStdin)
	case "check":
		err = CheckCmd(argv, args.PassphraseFromStdin)
	case "cp":
		err = CpCmd(argv, args.PassphraseFromStdin)
	case "init":
		err = InitCmd(argv, args.PassphraseFromStdin)
	case "ls":
		err = LsCmd(argv, args.PassphraseFromStdin)
	case "log":
		err = LogCmd(argv, args.PassphraseFromStdin)
	case "merge":
		err = MergeCmd(argv, args.PassphraseFromStdin)
	case "reset":
		err = ResetCmd(argv, args.PassphraseFromStdin)
	case "security":
		err = SecurityCmd(argv, args.PassphraseFromStdin)
	case "serve":
		err = ServeCmd(argv)
	case "status":
		err = StatusCmd(argv, args.PassphraseFromStdin)
	case "":
		flag.Usage()
		os.Exit(0)
	default:
		PrintErr("%s is not a valid command. See '%s --help'.", cmd, appName)
		os.Exit(1)
	}
	if err != nil {
		PrintErr(err.Error())
		os.Exit(1)
	}
}
