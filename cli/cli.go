//nolint:forbidigo
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
	ws "github.com/flunderpero/cling-sync/workspace"
	"golang.org/x/term"
)

const appName = "cling-sync"

func InitCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Help                bool
		AllowWeakPassphrase bool
	}{}
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.AllowWeakPassphrase, "allow-weak-passphrase", false, "Allow weak passphrase")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s init <dst>\n\n", appName)
		fmt.Fprint(os.Stderr, "Initialize a new repository at <dst>.\n")
		fmt.Fprint(os.Stderr, "The destination directory <dst> must not exist or must be empty.\n")
		fmt.Fprint(os.Stderr, "\nFlags:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(argv); err != nil {
		return err //nolint:wrapcheck
	}
	if args.Help {
		flags.Usage()
	}
	if len(flags.Args()) != 1 {
		return lib.Errorf("one positional argument is required: <dst>")
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
	storage, err := lib.NewFileStorage(repositoryPath, lib.StoragePurposeRepository)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create storage")
	}
	_, err = lib.InitNewRepository(storage, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to initialize repository")
	}
	workspace, err := ws.NewWorkspace(".", ws.RemoteRepository(repositoryPath))
	if err != nil {
		return lib.WrapErrorf(err, "failed to create workspace")
	}
	workspace.Close() //nolint:errcheck,gosec
	return nil
}

func CpCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := ws.OpenWorkspace(".")
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
		Exclude      []lib.PathPattern
		Include      []lib.PathPattern
	}{}
	flags := flag.NewFlagSet("cp", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to copy from")
	flags.BoolVar(&args.IgnoreErrors, "ignore-errors", false, "Ignore errors")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.Verbose, "v", false, "Short for --verbose")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.Overwrite, "overwrite", false, "Overwrite existing files")
	pathPatternFlag(
		flags,
		"exclude",
		"Exclude paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as for the <pattern> argument.",
		&args.Exclude,
	)
	pathPatternFlag(
		flags,
		"include",
		"Override --exclude patterns and include paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as for the <pattern> argument.",
		&args.Include,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s cp <pattern> <target>\n\n", appName)
		fmt.Fprint(os.Stderr, "Copy files from the repository to a local directory.\n\n")
		fmt.Fprint(os.Stderr, "  <pattern>\n")
		fmt.Fprint(
			os.Stderr,
			"        Repository paths matching the given pattern are copied.\n"+pathPatternDescription("        "),
		)
		fmt.Fprint(os.Stderr, "\n  <target>\n")
		fmt.Fprint(os.Stderr, "        The target directory where the files are copied to.")
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
	pattern, err := lib.NewPathPattern(flags.Arg(0))
	if err != nil {
		return lib.WrapErrorf(err, "invalid pattern: %s", flags.Arg(0))
	}
	pathFilter := &lib.AllPathFilter{Filters: []lib.PathFilter{
		&lib.PathInclusionFilter{Includes: []lib.PathPattern{pattern}},
		&lib.PathExclusionFilter{Excludes: args.Exclude, Includes: args.Include},
	}}
	tmpDir, err := workspace.NewTmpDir("cp")
	if err != nil {
		return err //nolint:wrapcheck
	}
	cpOnExists := ws.CpOnExistsAbort
	if args.Overwrite {
		cpOnExists = ws.CpOnExistsOverwrite
	}
	mon := NewCpMonitor(workspace.WorkspacePath, cpOnExists, args.Verbose, args.IgnoreErrors, args.NoProgress)
	revisionId, err := revisionId(repository, args.Revision)
	if err != nil {
		return err
	}
	opts := &ws.CpOptions{
		PathFilter: pathFilter,
		Monitor:    mon,
		RevisionId: revisionId,
	}
	err = ws.Cp(repository, flags.Arg(1), opts, tmpDir)
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

func MergeCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := ws.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help       bool
		Message    string
		Author     string
		Verbose    bool
		NoProgress bool
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
	flags.BoolVar(&args.Verbose, "v", false, "Short for --verbose")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
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
		return lib.Errorf("no positional arguments are required")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	stagingMonitor := NewStagingMonitor(workspace.WorkspacePath, args.Verbose, args.NoProgress)
	cpMonitor := NewCpMonitor(workspace.WorkspacePath, ws.CpOnExistsAbort, args.Verbose, false, args.NoProgress)
	commitMonitor := NewCommitMonitor(args.Verbose, args.NoProgress)
	opts := &ws.MergeOptions{
		Author:         args.Author,
		Message:        args.Message,
		StagingMonitor: stagingMonitor,
		CpMonitor:      cpMonitor,
		CommitMonitor:  commitMonitor,
	}
	revisionId, err := ws.Merge(workspace, repository, opts)
	stagingMonitor.Close()
	if errors.Is(err, ws.ErrUpToDate) {
		fmt.Println("No changes")
		return nil
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
	workspace, err := ws.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help       bool
		Short      bool
		Verbose    bool
		NoProgress bool
		Exclude    []lib.PathPattern
		Include    []lib.PathPattern
		NoSummary  bool
	}{}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Only show the number of added, updated, and deleted files")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	flags.BoolVar(&args.NoSummary, "no-summary", false, "Do not show a summary at the end")
	pathPatternFlag(
		flags,
		"exclude",
		"Exclude paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as the [pattern] argument.",
		&args.Exclude,
	)
	pathPatternFlag(
		flags,
		"include",
		"Override --exclude patterns and include paths matching the given pattern (can be used multiple times).\nThe pattern syntax is the same as the [pattern] argument.",
		&args.Include,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s status [pattern]\n\n", appName)
		fmt.Fprint(os.Stderr, "Show the difference between the working directory and the repository.\n\n")
		fmt.Fprint(os.Stderr, "  [pattern] (optional)\n")
		fmt.Fprint(
			os.Stderr,
			"        Show status only for paths matching the given pattern.\n"+pathPatternDescription("        "),
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
		p, err := lib.NewPathPattern(flags.Arg(0))
		if err != nil {
			return lib.WrapErrorf(err, "invalid pattern: %s", flags.Arg(0))
		}
		pathFilter = &lib.PathInclusionFilter{Includes: []lib.PathPattern{p}}
	}
	if len(flags.Args()) > 1 {
		return lib.Errorf("too many positional arguments")
	}
	if len(args.Exclude) == 0 && len(args.Include) > 0 {
		return lib.Errorf("include patterns can only be used with exclude patterns")
	}
	if len(args.Exclude) > 0 {
		exclusionFilter := &lib.PathExclusionFilter{Excludes: args.Exclude, Includes: args.Include}
		if pathFilter != nil {
			pathFilter = &lib.AllPathFilter{Filters: []lib.PathFilter{pathFilter, exclusionFilter}}
		} else {
			pathFilter = exclusionFilter
		}
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	tmpDir, err := workspace.NewTmpDir("status")
	if err != nil {
		return err //nolint:wrapcheck
	}
	mon := NewStagingMonitor(workspace.WorkspacePath, args.Verbose, args.NoProgress)
	opts := &ws.StatusOptions{PathFilter: pathFilter, Monitor: mon}
	result, err := ws.Status(workspace, repository, opts, tmpDir)
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
	workspace, err := ws.OpenWorkspace(".")
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
	}{
		TimestampFormat: time.RFC3339,
	}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to show")
	flags.BoolVar(&args.Short, "short", false, "Show short listing (same as `--timestamp-format=relative`)")
	flags.BoolVar(
		&args.Human,
		"human",
		false,
		"Show human readable file sizes (same as `--timestamp-format=rfc3339 --full-file-mode`)",
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
		fmt.Fprint(os.Stderr, "List files in the repository.\n\n")
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
	var pattern *lib.PathPattern
	if len(flags.Args()) == 1 {
		p, err := lib.NewPathPattern(flags.Arg(0))
		if err != nil {
			return lib.WrapErrorf(err, "invalid pattern: %s", flags.Arg(0))
		}
		pattern = &p
	}
	if len(flags.Args()) > 1 {
		return lib.Errorf("too many positional arguments")
	}
	repository, err := openRepository(workspace, passphraseFromStdin)
	if err != nil {
		return err
	}
	var pathFilter lib.PathFilter
	if pattern != nil {
		pathFilter = &lib.PathInclusionFilter{Includes: []lib.PathPattern{*pattern}}
	}
	revisionId, err := revisionId(repository, args.Revision)
	if err != nil {
		return err
	}
	opts := &ws.LsOptions{RevisionId: revisionId, PathFilter: pathFilter}
	files, err := ws.Ls(repository, opts)
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
	workspace, err := ws.OpenWorkspace(".")
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
		fmt.Fprint(os.Stderr, "Show revision log.\n\n")
		fmt.Fprint(os.Stderr, "  [pattern] (optional)\n")
		fmt.Fprint(os.Stderr, "        Show log only for paths matching the given pattern.\n")
		fmt.Fprint(os.Stderr, pathPatternDescription("        "))
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
		p, err := lib.NewPathPattern(flags.Arg(0))
		if err != nil {
			return lib.WrapErrorf(err, "invalid pattern: %s", flags.Arg(0))
		}
		pathFilter = &lib.PathInclusionFilter{Includes: []lib.PathPattern{p}}
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

func SecurityCmd(argv []string, passphraseFromStdin bool) error { //nolint:funlen
	workspace, err := ws.OpenWorkspace(".")
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
		fmt.Fprint(os.Stderr, "        Save the keys to the repository so that you don't need\n")
		fmt.Fprint(os.Stderr, "        the passphrase to execute a command.\n")
		fmt.Fprint(os.Stderr, "        Warning: Everyone with access to the `keys.txt` file can\n")
		fmt.Fprint(os.Stderr, "        decrypt the whole repository.\n")
		fmt.Fprint(os.Stderr, "        If you change the keys in the repository, all saved keys become invalid.\n")
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
		if err := workspace.WriteRepositoryKeys(keys); err != nil {
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

func openRepository(workspace *ws.Workspace, passphraseFromStdin bool) (*lib.Repository, error) {
	storage, err := openRepositoryStorage(workspace)
	if err != nil {
		return nil, err
	}
	keys, ok, err := workspace.ReadRepositoryKeys()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read repository keys from local storage")
	}
	if ok {
		repository, err := lib.OpenRepositoryWithKeys(storage, keys)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to open repository with saved keys")
		}
		return repository, nil
	}
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

func openRepositoryStorage(workspace *ws.Workspace) (*lib.FileStorage, error) {
	storage, err := lib.NewFileStorage(string(workspace.RemoteRepository), lib.StoragePurposeRepository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	return storage, nil
}

func pathPatternDescription(indent string) string {
	// todo: Add examples.
	return indent + strings.ReplaceAll(strings.TrimSpace(`
A pattern must match the full path or a directory within the path.
Pattern syntax:
    **      matches any number of directories
    *       matches any number of characters in a single directory
    ?       matches a single character
	`), "\n", "\n"+indent)
}

func pathPatternFlag(flags *flag.FlagSet, name string, usage string, value *[]lib.PathPattern) {
	flags.Func(
		name,
		usage,
		func(pattern string) error {
			p, err := lib.NewPathPattern(pattern)
			if err != nil {
				return lib.WrapErrorf(err, "invalid pattern: %s", pattern)
			}
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
		fmt.Fprint(os.Stderr, "  cp           Copy files from the repository to a local directory\n")
		fmt.Fprint(os.Stderr, "  init         Initialize a new repository\n")
		fmt.Fprint(os.Stderr, "  ls           List files in the repository\n")
		fmt.Fprint(os.Stderr, "  log          Show revision log\n")
		fmt.Fprint(os.Stderr, "  merge        Merge changes from the repository and the workspace\n")
		fmt.Fprint(os.Stderr, "  security	  See and configure security settings\n")
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
	case "init":
		err = InitCmd(argv, args.PassphraseFromStdin)
	case "cp":
		err = CpCmd(argv, args.PassphraseFromStdin)
	case "merge":
		err = MergeCmd(argv, args.PassphraseFromStdin)
	case "log":
		err = LogCmd(argv, args.PassphraseFromStdin)
	case "ls":
		err = LsCmd(argv, args.PassphraseFromStdin)
	case "security":
		err = SecurityCmd(argv, args.PassphraseFromStdin)
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
