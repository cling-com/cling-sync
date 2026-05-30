//nolint:forbidigo
package main

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/flunderpero/cling-sync/cli/keychain"
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
		Help          bool
		PathPrefix    string
		AllowNonEmpty bool
	}{}
	flags := flag.NewFlagSet("attach", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.PathPrefix, "path-prefix", "", "Only attach to this path inside the repository")
	flags.BoolVar(
		&args.AllowNonEmpty,
		"allow-non-empty",
		false,
		"Allow attaching to a directory that already contains files.\nExisting files matching the repository by content are adopted as-is;\nfiles at the same path with different content become merge conflicts;\nfiles not present in the repository are committed as new additions\non the next merge.",
	)
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
	// Make sure the local directory either does not exist or is empty
	// (unless --allow-non-empty was passed).
	_, err = os.Stat(localPath)
	if errors.Is(err, os.ErrNotExist) { //nolint:gocritic
		if err := os.MkdirAll(localPath, 0o700); err != nil {
			return lib.WrapErrorf(err, "failed to create directory %s", localPath)
		}
	} else if err != nil {
		return lib.Errorf("cannot stat local directory %s", localPath)
	} else if !args.AllowNonEmpty {
		files, err := os.ReadDir(localPath)
		if err != nil {
			return lib.Errorf("failed to read local directory %s", localPath)
		}
		if len(files) > 0 {
			return lib.Errorf(
				"local directory %s is not empty (use --allow-non-empty to attach anyway)",
				localPath,
			)
		}
	}
	repositoryURI := flags.Arg(0)
	if err := clingHTTP.RejectBareHTTPURI(repositoryURI); err != nil {
		return err //nolint:wrapcheck
	}
	var storage lib.Storage
	switch {
	case clingHTTP.IsS3StorageURI(repositoryURI):
		passphrase, err := readPassphrase(passphraseFromStdin)
		if err != nil {
			return err
		}
		encryptedURI, err := resolveS3URI(repositoryURI, passphrase, passphraseFromStdin)
		if err != nil {
			return err
		}
		cfg, _, err := clingHTTP.DecodeS3URI(encryptedURI, passphrase)
		if err != nil {
			return lib.WrapErrorf(err, "failed to decode S3 URI")
		}
		storage = clingHTTP.NewS3StorageClient(cfg, nil)
		if _, err := lib.OpenRepository(storage, passphrase); err != nil {
			return lib.WrapErrorf(err, "failed to open repository")
		}
		repositoryURI = encryptedURI
	default:
		repositoryURI, err = filepath.Abs(repositoryURI)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get absolute path for %s", repositoryURI)
		}
		storage, err = lib.NewFileStorage(lib.NewRealFS(repositoryURI), lib.StoragePurposeRepository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to connect to repository storage")
		}
		if _, err := openRepositoryWithPassphrase(storage, passphraseFromStdin); err != nil {
			return err
		}
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
		passphrase, err = term.ReadPassword(int(os.Stdin.Fd())) //nolint:gosec
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
		passphraseRepeat, err := term.ReadPassword(int(os.Stdin.Fd())) //nolint:gosec
		if err != nil {
			return lib.WrapErrorf(err, "failed to read passphrase")
		}
		if string(passphrase) != string(passphraseRepeat) {
			return lib.Errorf("passphrases do not match")
		}
	}
	rawTarget := flags.Arg(0)
	if err := clingHTTP.RejectBareHTTPURI(rawTarget); err != nil {
		return err //nolint:wrapcheck
	}
	var (
		repositoryURI string
		storage       lib.Storage
	)
	if clingHTTP.IsS3StorageURI(rawTarget) {
		encryptedURI, err := resolveS3URI(rawTarget, passphrase, passphraseFromStdin)
		if err != nil {
			return err
		}
		cfg, _, err := clingHTTP.DecodeS3URI(encryptedURI, passphrase)
		if err != nil {
			return lib.WrapErrorf(err, "failed to decode S3 URI")
		}
		storage = clingHTTP.NewS3StorageClient(cfg, nil)
		repositoryURI = encryptedURI
	} else {
		repositoryPath, err := filepath.Abs(rawTarget)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get absolute path for %s", rawTarget)
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
		storage, err = lib.NewFileStorage(lib.NewRealFS(repositoryPath), lib.StoragePurposeRepository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create storage")
		}
		repositoryURI = repositoryPath
	}
	if _, err := lib.InitNewRepository(storage, passphrase); err != nil {
		return lib.WrapErrorf(err, "failed to initialize repository")
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return lib.WrapErrorf(err, "failed to create temporary directory")
	}
	workspace, err := ws.NewWorkspace(
		lib.NewRealFS("."),
		lib.NewRealFS(tmpDir),
		ws.RemoteRepository(repositoryURI),
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
	mon := NewCpMonitor(CLIMonitorMode(args.Verbose, args.NoProgress), cpOnExists, args.IgnoreErrors)
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
	mon.close()
	if args.IgnoreErrors && mon.Errors > 0 {
		fmt.Printf("%d errors ignored\n", mon.Errors)
	}
	if err != nil {
		return err //nolint:wrapcheck
	}
	mbs := float64(mon.BytesWritten) / float64(time.Since(mon.StartTime).Seconds())
	fmt.Printf(
		"%d files copied (%s at %s/s)\n",
		mon.Paths,
		ws.FormatBytes(mon.BytesWritten),
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
	stagingMonitor, cpMonitor := NewResetMonitors(CLIMonitorMode(args.Verbose, args.NoProgress))
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
		stagingMonitor.close()
		cpMonitor.close()
		return err //nolint:wrapcheck
	}
	stagingMonitor.close()
	cpMonitor.close()
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
	stagingMonitor, cpMonitor, commitMonitor := NewMergeMonitors(
		CLIMonitorMode(args.Verbose, args.NoProgress),
	)
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
	stagingMonitor.close()
	cpMonitor.close()
	commitMonitor.close()
	if errors.Is(err, ws.ErrUpToDate) {
		fmt.Println("No changes")
		return nil
	}
	conflicts := ws.MergeConflictsError{}
	if errors.As(err, &conflicts) {
		var sb strings.Builder
		sb.WriteString("merge aborted due to conflicts:\n\n")
		for _, conflict := range conflicts {
			fmt.Fprintf(&sb, "  %s (remote: %s, local: %s)\n",
				conflict.WorkspaceEntry.Path,
				conflict.RepositoryEntry.Kind,
				conflict.WorkspaceEntry.Kind)
		}
		fmt.Fprintf(&sb, `
No files were changed, you need to resolve the conflicts manually.

To accept all local changes, run `+"`"+`%s merge --accept-local`+"`"+`
To select remote changes, run `+"`"+`%s cp --overwrite <remote-path> .`+"`"+`
`, appName, appName)
		return lib.Errorf("%s", sb.String())
	}
	if err != nil {
		return err //nolint:wrapcheck
	}
	if commitMonitor.Paths == 0 {
		fmt.Println("No local changes, workspace is up to date now")
		return nil
	}
	compressionRatio := "n/a"
	if commitMonitor.RawBytesAdded > 0 {
		compressionRatio = fmt.Sprintf(
			"%.2f",
			float64(commitMonitor.CompressedBytesAdded)/float64(commitMonitor.RawBytesAdded),
		)
	}
	fmt.Printf(
		"Revision %s (%s added, compressed: %s)\n",
		revisionId,
		ws.FormatBytes(commitMonitor.RawBytesAdded),
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
	flags := flag.NewFlagSet("status", flag.ExitOnError)
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
	mon := NewStatusMonitor(CLIMonitorMode(args.Verbose, args.NoProgress))
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
	mon.close()
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
	for i, file := range files {
		if args.Short && file.Metadata.FileMode.IsDir() && i > 0 {
			fmt.Println()
		}
		fmt.Println(file.Format(format))
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

const (
	healthCheckReportDir          = ".cling/report"
	healthCheckReportFile         = "health-check.txt"
	healthCheckOrphanedBlocksFile = "health-check-orphaned-blocks.txt"
)

func CheckCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen,gocognit
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help           bool
		Verbose        bool
		NoProgress     bool
		Data           bool
		OrphanedBlocks bool
		Full           bool
		Repository     string
	}{}
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Data, "data", false, "Check all file data blocks of all paths in all revisions")
	flags.BoolVar(&args.OrphanedBlocks, "orphaned-blocks", false,
		"Detect blocks in storage that are not referenced by any revision")
	flags.BoolVar(&args.Full, "full", false, "Run all checks (implies --data and --orphaned-blocks)")
	flags.StringVar(&args.Repository, "repository", "", "Check this repository instead of the workspace repository")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s check\n\n", appName)
		fmt.Fprint(os.Stderr, "Check the health of a repository.\n")
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
	if args.Full {
		args.Data = true
		args.OrphanedBlocks = true
	}
	var repository *lib.Repository
	if args.Repository != "" {
		if err := clingHTTP.RejectBareHTTPURI(args.Repository); err != nil {
			return err //nolint:wrapcheck
		}
		var passphrase []byte
		if clingHTTP.IsS3StorageURI(args.Repository) {
			passphrase, err = readPassphrase(passphraseFromStdin)
			if err != nil {
				return err
			}
		}
		storage, err := ws.OpenStorage(args.Repository, passphrase)
		if err != nil {
			return lib.WrapErrorf(err, "failed to open repository storage")
		}
		if passphrase != nil {
			repository, err = lib.OpenRepository(storage, passphrase)
			if err != nil {
				return lib.WrapErrorf(err, "failed to open repository")
			}
		} else {
			repository, err = openRepositoryWithPassphrase(storage, passphraseFromStdin)
			if err != nil {
				return err
			}
		}
	} else {
		repository, err = openRepository(workspace, passphraseFromStdin)
		if err != nil {
			return err
		}
	}
	tempFS, err := workspace.TempFS.MkSub("check")
	if err != nil {
		return lib.WrapErrorf(err, "failed to create temp directory for health check")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	monitor := NewHeathCheckMonitor(CLIMonitorMode(args.Verbose, args.NoProgress))
	err = lib.CheckHealth(repository, tempFS, lib.HealthCheckOptions{
		Monitor:             monitor,
		CheckBlocks:         args.Data,
		CheckOrphanedBlocks: args.OrphanedBlocks,
	})
	monitor.Finish()
	monitor.close()
	if err != nil {
		return err //nolint:wrapcheck
	}
	if err := os.MkdirAll(healthCheckReportDir, 0o700); err != nil {
		return lib.WrapErrorf(err, "failed to create %s", healthCheckReportDir)
	}
	reportPath := filepath.Join(healthCheckReportDir, healthCheckReportFile)
	orphansPath := filepath.Join(healthCheckReportDir, healthCheckOrphanedBlocksFile)
	report, err := monitor.Report(args.Data, args.OrphanedBlocks, orphansPath)
	if err != nil {
		return err //nolint:wrapcheck
	}
	fmt.Print(report)
	if err := os.WriteFile(reportPath, []byte(report), 0o600); err != nil {
		return lib.WrapErrorf(err, "failed to write %s", reportPath)
	}
	fmt.Printf("Report saved to: %s\n", reportPath)
	return nil
}

func SyncRepoCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen,gocognit
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	var help bool
	flags := flag.NewFlagSet("sync-repo", flag.ExitOnError)
	flags.BoolVar(&help, "help", false, "Show help message")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s sync-repo <command> [args]\n\n", appName)
		fmt.Fprint(os.Stderr, "Manage and run mirror copies of this repository.\n\n")
		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprint(os.Stderr, "  init <name> <dir>\n")
		fmt.Fprint(os.Stderr, "        Create a new local repository at `dir` and register it as `name`.\n")
		fmt.Fprint(os.Stderr, "  add <name> <uri>\n")
		fmt.Fprint(os.Stderr, "        Register an existing repository (local path or URL) as `name`.\n")
		fmt.Fprint(os.Stderr, "  list\n")
		fmt.Fprint(os.Stderr, "        List all registered sync targets.\n")
		fmt.Fprint(os.Stderr, "  delete <name>\n")
		fmt.Fprint(os.Stderr, "        Unregister a sync target. The target storage is not removed.\n")
		fmt.Fprint(os.Stderr, "  run [flags] [name]\n")
		fmt.Fprint(os.Stderr, "        Sync to every registered target, or to a single named target.\n")
		fmt.Fprint(os.Stderr, "        Failures are reported but do not stop subsequent targets.\n")
		fmt.Fprint(os.Stderr, "        Run `sync-repo run --help` for its flags.\n")
		fmt.Fprint(os.Stderr, "\nNames must be ASCII alphanumeric including '-'.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if help {
		flags.Usage()
		return nil
	}
	if len(flags.Args()) == 0 {
		flags.Usage()
		return lib.Errorf("missing command")
	}
	posArgs := flags.Args()[1:]
	switch flags.Arg(0) {
	case "init":
		if len(posArgs) != 2 {
			return lib.Errorf("usage: sync-repo init <name> <dir-or-s3-uri>")
		}
		name := posArgs[0]
		if err := ws.ValidateSyncTargetName(name); err != nil {
			return lib.WrapErrorf(err, "invalid sync target name")
		}
		if _, found, err := ws.GetSyncTarget(workspace, name); err != nil {
			return lib.WrapErrorf(err, "failed to check sync targets")
		} else if found {
			return lib.Errorf("sync target %q already exists", name)
		}
		passphrase, err := readWorkspaceRepositoryPassphrase(workspace, passphraseFromStdin)
		if err != nil {
			return err
		}
		src, err := openRepositoryStorage(workspace, passphrase)
		if err != nil {
			return err
		}
		toml, err := src.Open()
		if err != nil {
			return lib.WrapErrorf(err, "failed to open source storage")
		}
		rawTarget := posArgs[1]
		if err := clingHTTP.RejectBareHTTPURI(rawTarget); err != nil {
			return err //nolint:wrapcheck
		}
		if clingHTTP.IsS3StorageURI(rawTarget) {
			encryptedURI, err := resolveS3URI(rawTarget, passphrase, passphraseFromStdin)
			if err != nil {
				return err
			}
			cfg, _, err := clingHTTP.DecodeS3URI(encryptedURI, passphrase)
			if err != nil {
				return lib.WrapErrorf(err, "failed to decode S3 target URI")
			}
			storage := clingHTTP.NewS3StorageClient(cfg, nil)
			if err := storage.Init(toml, lib.RepositoryConfigHeaderComment); err != nil {
				return lib.WrapErrorf(err, "failed to initialize S3 target repository")
			}
			if err := lib.WriteRef(storage, "head", lib.RevisionId{}); err != nil {
				return lib.WrapErrorf(err, "failed to write head reference")
			}
			if err := ws.AddSyncTarget(workspace, name, encryptedURI, passphrase); err != nil {
				return lib.WrapErrorf(err, "target was initialized but could not be registered")
			}
			fmt.Printf("Initialized and registered sync target %q at %s\n", name, encryptedURI)
			return nil
		}
		targetRepositoryPath, err := filepath.Abs(rawTarget)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get absolute path for %s", rawTarget)
		}
		if _, err := os.Stat(targetRepositoryPath); err == nil {
			return lib.Errorf("target directory already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return lib.WrapErrorf(err, "failed to stat %s", targetRepositoryPath)
		}
		if err := os.MkdirAll(targetRepositoryPath, 0o700); err != nil {
			return lib.WrapErrorf(err, "failed to create target directory %s", targetRepositoryPath)
		}
		storage, err := lib.NewFileStorage(lib.NewRealFS(targetRepositoryPath), lib.StoragePurposeRepository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create storage")
		}
		if err := storage.Init(toml, lib.RepositoryConfigHeaderComment); err != nil {
			return lib.WrapErrorf(err, "failed to initialize target repository")
		}
		if err := lib.WriteRef(storage, "head", lib.RevisionId{}); err != nil {
			return lib.WrapErrorf(err, "failed to write head reference")
		}
		if err := ws.AddSyncTarget(workspace, name, targetRepositoryPath, passphrase); err != nil {
			return lib.WrapErrorf(err, "target was initialized at %s but could not be registered", targetRepositoryPath)
		}
		fmt.Printf("Initialized and registered sync target %q at %s\n", name, targetRepositoryPath)
		return nil
	case "add":
		if len(posArgs) != 2 {
			return lib.Errorf("usage: sync-repo add <name> <uri>")
		}
		name := posArgs[0]
		uri := posArgs[1]
		if err := clingHTTP.RejectBareHTTPURI(uri); err != nil {
			return err //nolint:wrapcheck
		}
		// The workspace's own URI may be S3 (encrypted), in which case we need
		// its passphrase to decrypt and read the source config. We also need
		// it to encrypt a raw s3+ target URI before storage.
		needPassphrase := clingHTTP.IsS3StorageURI(string(workspace.RemoteRepository)) ||
			clingHTTP.IsS3StorageURI(uri)
		var passphrase []byte
		if needPassphrase {
			var err error
			passphrase, err = readWorkspaceRepositoryPassphrase(workspace, passphraseFromStdin)
			if err != nil {
				return err
			}
		}
		switch {
		case clingHTTP.IsS3StorageURI(uri):
			uri, err = resolveS3URI(uri, passphrase, passphraseFromStdin)
			if err != nil {
				return err
			}
		default:
			abs, err := filepath.Abs(uri)
			if err != nil {
				return lib.WrapErrorf(err, "failed to get absolute path for %s", uri)
			}
			uri = abs
		}
		if err := ws.AddSyncTarget(workspace, name, uri, passphrase); err != nil {
			return lib.WrapErrorf(err, "failed to add sync target")
		}
		fmt.Printf("Registered sync target %q -> %s\n", name, uri)
		return nil
	case "list":
		if len(posArgs) != 0 {
			return lib.Errorf("usage: sync-repo list")
		}
		targets, err := ws.LoadSyncTargets(workspace)
		if err != nil {
			return lib.WrapErrorf(err, "failed to load sync targets")
		}
		if len(targets) == 0 {
			fmt.Println("No sync targets registered.")
			return nil
		}
		nameWidth := 0
		for _, t := range targets {
			if len(t.Name) > nameWidth {
				nameWidth = len(t.Name)
			}
		}
		for _, t := range targets {
			fmt.Printf("%-*s  %s\n", nameWidth, t.Name, t.URI)
		}
		return nil
	case "delete":
		if len(posArgs) != 1 {
			return lib.Errorf("usage: sync-repo delete <name>")
		}
		if err := ws.DeleteSyncTarget(workspace, posArgs[0]); err != nil {
			return lib.WrapErrorf(err, "failed to delete sync target")
		}
		fmt.Printf("Unregistered sync target %q\n", posArgs[0])
		return nil
	case "run":
		runArgs := struct { //nolint:exhaustruct
			Help       bool
			Verbose    bool
			NoProgress bool
			Workers    int
		}{}
		runFlags := flag.NewFlagSet("sync-repo run", flag.ExitOnError)
		runFlags.BoolVar(&runArgs.Help, "help", false, "Show help message")
		runFlags.BoolVar(&runArgs.Verbose, "verbose", false, "Show detailed progress")
		runFlags.BoolVar(&runArgs.NoProgress, "no-progress", false, "Do not show progress")
		runFlags.IntVar(&runArgs.Workers, "workers", 2, "Number of blocks to copy in parallel")
		runFlags.Usage = func() {
			fmt.Fprintf(os.Stderr, "Usage: %s sync-repo run [flags] [name]\n\n", appName)
			fmt.Fprint(os.Stderr, "Sync to every registered target, or to a single named target.\n")
			fmt.Fprint(os.Stderr, "Failures are reported but do not stop subsequent targets.\n")
			fmt.Fprint(os.Stderr, "\nFlags:\n")
			runFlags.PrintDefaults()
		}
		if err := runFlags.Parse(posArgs); err != nil {
			return err //nolint:wrapcheck
		}
		if runArgs.Help {
			runFlags.Usage()
			return nil
		}
		var names []string
		switch len(runFlags.Args()) {
		case 0:
			targets, err := ws.LoadSyncTargets(workspace)
			if err != nil {
				return lib.WrapErrorf(err, "failed to load sync targets")
			}
			if len(targets) == 0 {
				return lib.Errorf("no sync targets registered; use `sync-repo init` or `sync-repo add` first")
			}
			names = make([]string, len(targets))
			for i, t := range targets {
				names[i] = t.Name
			}
		case 1:
			names = []string{runFlags.Arg(0)}
		default:
			return lib.Errorf("usage: sync-repo run [flags] [name]")
		}
		// Determine if any source or target URI is S3, in which case we need
		// the workspace passphrase to decrypt it.
		needPassphrase := clingHTTP.IsS3StorageURI(string(workspace.RemoteRepository))
		if !needPassphrase {
			targets, err := ws.LoadSyncTargets(workspace)
			if err != nil {
				return lib.WrapErrorf(err, "failed to load sync targets")
			}
			for _, t := range targets {
				if clingHTTP.IsS3StorageURI(t.URI) {
					needPassphrase = true
					break
				}
			}
		}
		var passphrase []byte
		if needPassphrase {
			var err error
			passphrase, err = readWorkspaceRepositoryPassphrase(workspace, passphraseFromStdin)
			if err != nil {
				return err
			}
		}
		mode := CLIMonitorMode(runArgs.Verbose, runArgs.NoProgress)
		for _, name := range names {
			mon := NewSyncRepoMonitor(name, mode)
			err := ws.RunSync(context.Background(), workspace, name, mon, passphrase, runArgs.Workers)
			mon.done(err)
			if err != nil {
				return err //nolint:wrapcheck
			}
		}
		return nil
	default:
		return lib.Errorf("unknown command: %s", flags.Arg(0))
	}
}

func resolveS3URI(rawTarget string, passphrase []byte, passphraseFromStdin bool) (string, error) {
	if clingHTTP.S3URIHasEmbeddedCredentials(rawTarget) {
		return rawTarget, nil
	}
	creds, err := readS3Credentials(passphraseFromStdin)
	if err != nil {
		return "", err
	}
	encryptedURI, err := clingHTTP.EncodeS3URI(rawTarget, creds, passphrase)
	if err != nil {
		return "", lib.WrapErrorf(err, "failed to encode S3 URI")
	}
	return encryptedURI, nil
}

func SecurityCmd(argv []string, passphraseFromStdin bool) error {
	args := struct { //nolint:exhaustruct
		Help bool
	}{}
	flags := flag.NewFlagSet("security", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s security [command]\n\n", appName)
		fmt.Fprint(os.Stderr, "Configure security settings.\n\n")
		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprint(os.Stderr, "  save-passphrase\n")
		fmt.Fprint(os.Stderr, "        Save the repository passphrase so that this client stays authenticated.\n")
		fmt.Fprint(os.Stderr, "        The passphrase is AEAD-encrypted with a random local key. The local key\n")
		fmt.Fprint(os.Stderr, "        is stored in the system keychain. Neither alone unlocks the repository.\n")
		fmt.Fprint(os.Stderr, "        If the repository passphrase is changed, the saved copy becomes invalid.\n")
		fmt.Fprint(os.Stderr, "  delete-passphrase\n")
		fmt.Fprintf(
			os.Stderr,
			"        Delete the passphrase previously saved using `%s security save-passphrase`.\n",
			appName,
		)
		fmt.Fprint(os.Stderr, "  encrypt-s3-url [--credentials-file <path>] <endpoint>\n")
		fmt.Fprint(os.Stderr, "        Print a self-contained cling-sync S3 URI for <endpoint> with the S3\n")
		fmt.Fprint(os.Stderr, "        access credentials encrypted under the repository passphrase. Useful\n")
		fmt.Fprint(os.Stderr, "        for attaching the same repository from another of your machines\n")
		fmt.Fprint(os.Stderr, "        without re-entering the S3 credentials. Opens the repository at\n")
		fmt.Fprint(os.Stderr, "        <endpoint> with the given credentials and passphrase first.\n")
		fmt.Fprint(os.Stderr, "\n")
		fmt.Fprint(os.Stderr, "        Credentials come from --credentials-file (lines\n")
		fmt.Fprint(os.Stderr, "        `CLING_S3_KEY_ID=...` and `CLING_S3_ACCESS_KEY=...`) or from the\n")
		fmt.Fprint(os.Stderr, "        CLING_S3_* / AWS_* env vars.\n")
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
	if flags.Arg(0) == "encrypt-s3-url" {
		return securityEncryptS3URLCmd(flags.Args()[1:], passphraseFromStdin)
	}

	op := flags.Arg(0)
	if op != "save-passphrase" && op != "delete-passphrase" {
		return lib.Errorf("unknown command: %s", op)
	}
	return securityPassphraseCmd(op, flags.Args()[1:], passphraseFromStdin)
}

//nolint:funlen
func securityPassphraseCmd(op string, positional []string, passphraseFromStdin bool) error {
	if len(positional) != 0 {
		return lib.Errorf("too many positional arguments")
	}
	workspace, err := openWorkspace()
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	if op == "delete-passphrase" {
		if err := workspace.DeleteSavedPassphrase(); err != nil {
			return lib.WrapErrorf(err, "failed to delete saved passphrase")
		}
		if err := keychain.DeleteKeychainEntry(
			context.Background(),
			"com.cling.sync",
			string(workspace.RemoteRepository),
		); err != nil && !errors.Is(err, keychain.ErrKeychainEntryNotFound) {
			return lib.WrapErrorf(err, "failed to delete local encryption key from keychain")
		}
		fmt.Println("Saved passphrase deleted")
		return nil
	}
	passphrase, err := readPassphrase(passphraseFromStdin)
	if err != nil {
		return err
	}
	repositoryStorage, err := openRepositoryStorage(workspace, passphrase)
	if err != nil {
		return err
	}
	if _, err := lib.OpenRepository(repositoryStorage, passphrase); err != nil {
		return lib.WrapErrorf(err, "failed to validate passphrase against repository")
	}
	// Two layers: keychain holds a random local key, workspace holds the
	// AEAD-encrypted passphrase. Neither alone unlocks the repo.
	var encKey lib.RawKey
	existing, err := keychain.GetKeychainEntry(
		context.Background(),
		"com.cling.sync",
		string(workspace.RemoteRepository),
	)
	switch {
	case err == nil:
		decoded, err := hex.DecodeString(existing)
		if err != nil {
			return lib.WrapErrorf(err, "failed to decode existing keychain entry")
		}
		encKey = lib.RawKey(decoded)
	case errors.Is(err, keychain.ErrKeychainEntryNotFound):
		encKey, err = lib.NewRawKey()
		if err != nil {
			return lib.WrapErrorf(err, "failed to generate local encryption key")
		}
		if err := keychain.AddKeychainEntry(
			context.Background(),
			"com.cling.sync",
			string(workspace.RemoteRepository),
			hex.EncodeToString(encKey[:]),
		); err != nil {
			return lib.WrapErrorf(err, "failed to save local encryption key to keychain")
		}
	default:
		return lib.WrapErrorf(err, "failed to read local encryption key from keychain")
	}
	encKeyCipher, err := lib.NewCipher(encKey)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create cipher")
	}
	if err := workspace.WriteSavedPassphrase(passphrase, encKeyCipher); err != nil {
		return lib.WrapErrorf(err, "failed to write saved passphrase")
	}
	return nil
}

func securityEncryptS3URLCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		CredentialsFile string
	}{}
	flags := flag.NewFlagSet("security encrypt-s3-url", flag.ExitOnError)
	flags.StringVar(&args.CredentialsFile, "credentials-file", "",
		"File with `CLING_S3_KEY_ID=...` and `CLING_S3_ACCESS_KEY=...` lines (TOML or .env style).")
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if flags.NArg() != 1 {
		return lib.Errorf("encrypt-s3-url requires exactly one positional argument: <endpoint>")
	}
	endpoint := flags.Arg(0)
	if !clingHTTP.IsS3StorageURI(endpoint) {
		return lib.Errorf("endpoint must start with `s3+http://` or `s3+https://`, got %q", endpoint)
	}
	var creds clingHTTP.S3Credentials
	if args.CredentialsFile != "" {
		data, err := os.ReadFile(args.CredentialsFile)
		if err != nil {
			return lib.WrapErrorf(err, "failed to read --credentials-file")
		}
		var id, secret string
		for line := range strings.SplitSeq(string(data), "\n") {
			line = strings.TrimSpace(line)
			var dst *string
			switch {
			case strings.HasPrefix(line, "CLING_S3_KEY_ID"):
				dst = &id
			case strings.HasPrefix(line, "CLING_S3_ACCESS_KEY"):
				dst = &secret
			default:
				continue
			}
			_, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			*dst = strings.Trim(strings.TrimSpace(v), `"`)
		}
		if id == "" || secret == "" {
			return lib.Errorf("--credentials-file is missing CLING_S3_KEY_ID or CLING_S3_ACCESS_KEY")
		}
		creds = clingHTTP.S3Credentials{AccessKeyID: id, SecretAccessKey: []byte(secret)}
	} else {
		envCreds, ok, err := readEnvS3Credentials()
		if err != nil {
			return err
		}
		if !ok {
			return lib.Errorf(
				"set --credentials-file, or CLING_S3_KEY_ID + CLING_S3_ACCESS_KEY (or AWS_*)",
			)
		}
		creds = envCreds
	}
	passphrase, err := readPassphrase(passphraseFromStdin)
	if err != nil {
		return err
	}
	cfg, err := clingHTTP.ParseS3Endpoint(endpoint, creds)
	if err != nil {
		return lib.WrapErrorf(err, "failed to parse endpoint")
	}
	if _, err := lib.OpenRepository(clingHTTP.NewS3StorageClient(cfg, nil), passphrase); err != nil {
		return lib.WrapErrorf(err, "failed to open repository at %s", endpoint)
	}
	uri, err := clingHTTP.EncodeS3URI(endpoint, creds, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to encode S3 URI")
	}
	fmt.Println(uri)
	return nil
}

func ServeCmd(argv []string) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Address      string
		LogRequests  bool
		CORSAllowAll bool
		ReadTimeout  time.Duration
		WriteTimeout time.Duration
		Region       string
		Help         bool
	}{}
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.LogRequests, "log-requests", false, "Log all requests")
	flags.BoolVar(&args.CORSAllowAll, "cors-allow-all", false, "Allow all origins")
	flags.StringVar(&args.Address, "address", "0.0.0.0:4242", "Address to listen on")
	flags.DurationVar(&args.ReadTimeout, "read-timeout", 10*time.Second, "Timeout for reading a response")
	flags.DurationVar(&args.WriteTimeout, "write-timeout", 10*time.Second, "Timeout for writing a response")
	flags.StringVar(&args.Region, "region", "us-east-1", "Region for SigV4 verification")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s serve <repository-path>\n\n", appName)
		fmt.Fprint(os.Stderr, "Serve an existing local repository as an S3-compatible bucket.\n")
		fmt.Fprint(os.Stderr, "Credentials live in the repository's `conf/serve` control file and\n")
		fmt.Fprint(os.Stderr, "are auto-generated on first run.\n")
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
	if _, err := storage.Open(); err != nil {
		return lib.WrapErrorf(err, "failed to open repository")
	}
	confPath := filepath.Join(repositoryPath, ".cling", "repository", "conf", "serve")
	var ak, sk string
	created := false
	data, err := storage.ReadControlFile(lib.ControlFileSectionConf, "serve")
	switch {
	case err == nil:
		toml, err := lib.ReadToml(bytes.NewReader(data))
		if err != nil {
			return lib.WrapErrorf(err, "failed to parse conf/serve")
		}
		ak = toml["serve"]["CLING_S3_KEY_ID"]
		sk = toml["serve"]["CLING_S3_ACCESS_KEY"]
		if ak == "" || sk == "" {
			return lib.Errorf("conf/serve is missing CLING_S3_KEY_ID or CLING_S3_ACCESS_KEY under [serve]")
		}
	case errors.Is(err, lib.ErrControlFileNotFound):
		const keyIDAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		idRand, err := lib.Rand(16)
		if err != nil {
			return lib.WrapErrorf(err, "failed to generate key id")
		}
		id := []byte("CLIA")
		for _, b := range idRand {
			id = append(id, keyIDAlphabet[int(b)%len(keyIDAlphabet)])
		}
		ak = string(id)
		secretBytes, err := lib.Rand(30)
		if err != nil {
			return lib.WrapErrorf(err, "failed to generate access key")
		}
		sk = base64.RawStdEncoding.EncodeToString(secretBytes)
		toml := lib.Toml{"serve": {"CLING_S3_KEY_ID": ak, "CLING_S3_ACCESS_KEY": sk}}
		var buf bytes.Buffer
		if err := lib.WriteToml(&buf, "", toml); err != nil {
			return lib.WrapErrorf(err, "failed to encode conf/serve")
		}
		if err := storage.WriteControlFile(lib.ControlFileSectionConf, "serve", buf.Bytes()); err != nil {
			return lib.WrapErrorf(err, "failed to write conf/serve")
		}
		created = true
	default:
		return lib.WrapErrorf(err, "failed to read conf/serve")
	}
	mux := http.NewServeMux()
	clingHTTP.NewS3StorageServer(storage, args.Region, ak, sk).RegisterRoutes(mux)
	var handler http.Handler = mux
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
	if created {
		fmt.Printf("First run - new credentials created at %s\n", confPath)
	} else {
		fmt.Printf("Read credentials from %s\n", confPath)
	}
	fmt.Printf(
		"Get an authenticated URL with:\n  %s security encrypt-s3-url --credentials-file %s s3+http://%s\n",
		appName, confPath, args.Address,
	)
	fmt.Printf("Serving %s at s3+http://%s\n", repositoryPath, args.Address)
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

// readWorkspaceRepositoryPassphrase returns the repository passphrase for the
// given workspace. If a saved-passphrase keychain entry exists, that path is
// used; otherwise the user is prompted (or stdin is consumed). Either way the
// returned passphrase is what the repository, and the S3 URI userinfo, were
// encrypted with.
func readWorkspaceRepositoryPassphrase(workspace *ws.Workspace, passphraseFromStdin bool) ([]byte, error) {
	if workspace.HasSavedPassphrase() {
		encKeyStr, err := keychain.GetKeychainEntry(
			context.Background(),
			"com.cling.sync",
			string(workspace.RemoteRepository),
		)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read local encryption key from keychain")
		}
		encKey, err := hex.DecodeString(encKeyStr)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to decode local encryption key from keychain")
		}
		encKeyCipher, err := lib.NewCipher(lib.RawKey(encKey))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to create cipher")
		}
		passphrase, err := workspace.ReadSavedPassphrase(encKeyCipher)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read saved passphrase")
		}
		return passphrase, nil
	}
	return readPassphrase(passphraseFromStdin)
}

func openRepository(workspace *ws.Workspace, passphraseFromStdin bool) (*lib.Repository, error) {
	passphrase, err := readWorkspaceRepositoryPassphrase(workspace, passphraseFromStdin)
	if err != nil {
		return nil, err
	}
	storage, err := openRepositoryStorage(workspace, passphrase)
	if err != nil {
		return nil, err
	}
	repository, err := lib.OpenRepository(storage, passphrase)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open repository")
	}
	return repository, nil
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

const s3KeyMinLen = 16

// readEnvS3Credentials returns the env-resolved S3 credentials. `ok` is true
// iff one of the two env pairs is fully set. Mixing across pairs or setting
// only one var of a pair is rejected.
func readEnvS3Credentials() (clingHTTP.S3Credentials, bool, error) {
	clingID, clingSecret := os.Getenv("CLING_S3_KEY_ID"), os.Getenv("CLING_S3_ACCESS_KEY")
	awsID, awsSecret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if (clingID != "") != (clingSecret != "") {
		return clingHTTP.S3Credentials{}, false, lib.Errorf(
			"CLING_S3_KEY_ID and CLING_S3_ACCESS_KEY must both be set",
		)
	}
	if clingID != "" {
		if len(clingID) < s3KeyMinLen || len(clingSecret) < s3KeyMinLen {
			return clingHTTP.S3Credentials{}, false, lib.Errorf(
				"CLING_S3_KEY_ID and CLING_S3_ACCESS_KEY must each be at least %d bytes", s3KeyMinLen,
			)
		}
		return clingHTTP.S3Credentials{AccessKeyID: clingID, SecretAccessKey: []byte(clingSecret)}, true, nil
	}
	if (awsID != "") != (awsSecret != "") {
		return clingHTTP.S3Credentials{}, false, lib.Errorf(
			"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must both be set",
		)
	}
	if awsID != "" {
		return clingHTTP.S3Credentials{AccessKeyID: awsID, SecretAccessKey: []byte(awsSecret)}, true, nil
	}
	return clingHTTP.S3Credentials{AccessKeyID: "", SecretAccessKey: nil}, false, nil
}

// readS3Credentials returns the access-key + secret-key for an S3 endpoint.
// Resolution order:
//
//  1. `CLING_S3_KEY_ID` + `CLING_S3_ACCESS_KEY` (cling-sync `serve` peer).
//  2. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (real S3 provider).
//  3. TTY prompt (only if stdin is a terminal and `passphraseFromStdin` is
//     false. When stdin is already consumed by the passphrase, we won't try
//     to read creds from it).
//
// Otherwise returns an error telling the user to set the env vars.
func readS3Credentials(passphraseFromStdin bool) (clingHTTP.S3Credentials, error) {
	if creds, ok, err := readEnvS3Credentials(); err != nil || ok {
		return creds, err
	}
	if passphraseFromStdin {
		return clingHTTP.S3Credentials{}, lib.Errorf(
			"with --passphrase-from-stdin set CLING_S3_KEY_ID and CLING_S3_ACCESS_KEY " +
				"(or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)",
		)
	}
	if !IsTerm(os.Stdin) {
		return clingHTTP.S3Credentials{}, lib.Errorf(
			"set CLING_S3_KEY_ID and CLING_S3_ACCESS_KEY " +
				"(or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY), or run interactively",
		)
	}
	if _, err := fmt.Fprint(os.Stderr, "S3 access key id: "); err != nil {
		return clingHTTP.S3Credentials{}, err //nolint:wrapcheck
	}
	var akInput string
	if _, err := fmt.Fscanln(os.Stdin, &akInput); err != nil {
		return clingHTTP.S3Credentials{}, lib.WrapErrorf(err, "failed to read access key id")
	}
	if _, err := fmt.Fprint(os.Stderr, "S3 secret access key: "); err != nil {
		return clingHTTP.S3Credentials{}, err //nolint:wrapcheck
	}
	skBytes, err := term.ReadPassword(int(os.Stdin.Fd())) //nolint:gosec
	if err != nil {
		return clingHTTP.S3Credentials{}, lib.WrapErrorf(err, "failed to read secret access key")
	}
	fmt.Fprintln(os.Stderr)
	return clingHTTP.S3Credentials{
		AccessKeyID:     strings.TrimSpace(akInput),
		SecretAccessKey: skBytes,
	}, nil
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
		passphrase, err = term.ReadPassword(int(os.Stdin.Fd())) //nolint:gosec
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read passphrase")
		}
		fmt.Fprint(os.Stderr, "\r                          \r")
	}
	return passphrase, nil
}

func openRepositoryStorage(workspace *ws.Workspace, passphrase []byte) (lib.Storage, error) { //nolint:ireturn
	storage, err := ws.OpenStorage(string(workspace.RemoteRepository), passphrase)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open repository storage")
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
		fmt.Fprint(os.Stderr, "Remote repositories: only S3-compatible backends are currently supported.\n")
		fmt.Fprintf(
			os.Stderr,
			"Use `s3+<http-url>` URIs to point at one, and `%s serve` to host one.\n\n",
			appName,
		)
		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprint(os.Stderr, "  attach       Attach a local directory to a repository\n")
		fmt.Fprint(os.Stderr, "  check        Check the health of the repository\n")
		fmt.Fprint(os.Stderr, "  cp           Copy files from the repository to a local directory\n")
		fmt.Fprint(os.Stderr, "  init         Initialize a new repository\n")
		fmt.Fprint(os.Stderr, "  ls           List files in the repository\n")
		fmt.Fprint(os.Stderr, "  log          Show revision log\n")
		fmt.Fprint(os.Stderr, "  merge        Merge changes from the repository and the workspace\n")
		fmt.Fprint(os.Stderr, "  reset        Reset the workspace to a specific revision\n")
		fmt.Fprint(os.Stderr, "  security     Configure security settings (saved passphrase, encrypted S3 URIs)\n")
		fmt.Fprint(os.Stderr, "  serve        Serve a local repository as an S3-compatible bucket\n")
		fmt.Fprint(os.Stderr, "  status       Show repository status\n")
		fmt.Fprint(os.Stderr, "  sync-repo    Sync repository to another repository")
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
	case "sync-repo":
		err = SyncRepoCmd(argv, args.PassphraseFromStdin)
	case "":
		flag.Usage()
		os.Exit(0)
	default:
		PrintErr("%s is not a valid command. See '%s --help'.", cmd, appName)
		os.Exit(1)
	}
	if err != nil {
		PrintErr("%s", err.Error())
		os.Exit(1)
	}
}
