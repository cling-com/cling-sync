//nolint:forbidigo
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
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

func InitCmd(argv []string) error { //nolint:funlen
	if !IsTerm(os.Stdin) {
		return lib.Errorf("a new repository can only be created in an interactive terminal session")
	}
	args := struct { //nolint:exhaustruct
		Help              bool
		AllowWeakPassword bool
	}{}
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.AllowWeakPassword, "allow-weak-password", false, "Allow weak password")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s init <dst>\n\n", appName)
		fmt.Fprintf(os.Stderr, "Initialize a new repository at <dst>.\n")
		fmt.Fprintf(os.Stderr, "The destination directory <dst> must not exist or must be empty.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	_, err := fmt.Fprint(os.Stderr, "Enter passphrase: ")
	if err != nil {
		return err //nolint:wrapcheck
	}
	passphrase, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return lib.WrapErrorf(err, "failed to read passphrase")
	}
	_, _ = fmt.Fprintln(os.Stdout)
	if err := lib.CheckPassphraseStrength(passphrase); err != nil {
		if args.AllowWeakPassword {
			fmt.Fprintf(os.Stderr, "\nWarning: %s\n", err.Error())
		} else {
			return err //nolint:wrapcheck
		}
	}
	_, err = fmt.Fprint(os.Stdout, "Repeat passphrase: ")
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

func CpCmd(argv []string) error { //nolint:funlen
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
		"Exclude paths matching the given pattern (can be used multiple times).",
		&args.Exclude,
	)
	pathPatternFlag(
		flags,
		"include",
		"Include paths matching the given pattern (can be used multiple times).",
		&args.Include,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s cp <pattern> <target>\n\n", appName)
		fmt.Fprintf(os.Stderr, "Copy files from the repository to a local directory.\n")
		fmt.Fprintf(os.Stderr, "The pattern syntax is the same as for the `--exclude` option.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	repository, err := openRepository(workspace)
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

func MergeCmd(argv []string) error { //nolint:funlen
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
		fmt.Fprintf(os.Stderr, "Commit all local changes to the repository\n")
		fmt.Fprintf(os.Stderr, "and merge all changes from the repository into the workspace.\n")
		fmt.Fprintf(os.Stderr, "As a result, the workspace will be identical to the repository.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	repository, err := openRepository(workspace)
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

func StatusCmd(argv []string) error { //nolint:funlen
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
	}{}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Only show the number of added, updated, and deleted files")
	flags.BoolVar(&args.Verbose, "verbose", false, "Show progress")
	flags.BoolVar(&args.NoProgress, "no-progress", false, "Do not show progress")
	pathPatternFlag(
		flags,
		"exclude",
		"Exclude paths matching the given pattern (can be used multiple times).",
		&args.Exclude,
	)
	pathPatternFlag(
		flags,
		"include",
		"Include paths matching the given pattern (can be used multiple times).",
		&args.Include,
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s status [pattern]\n\n", appName)
		fmt.Fprintf(os.Stderr, "Show the difference between the working directory and the repository.\n\n")
		fmt.Fprintf(os.Stderr, "  pattern\n")
		fmt.Fprintf(os.Stderr, "        The pattern syntax is the same as for the `--exclude` option.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	repository, err := openRepository(workspace)
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
	fmt.Println()
	fmt.Println(result.Summary())
	return nil
}

func LsCmd(argv []string) error { //nolint:funlen
	workspace, err := ws.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help     bool
		Revision string
		Short    bool
		Human    bool
	}{}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to show")
	flags.BoolVar(&args.Short, "short", false, "Show short listing")
	flags.BoolVar(&args.Human, "human", false, "Show human readable file sizes")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s ls [pattern]\n\n", appName)
		fmt.Fprintf(os.Stderr, "List files in the repository.\n\n")
		fmt.Fprintf(os.Stderr, "  pattern\n")
		fmt.Fprintf(os.Stderr, "        The pattern syntax is the same as for the `commit --ignore` option.\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	repository, err := openRepository(workspace)
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
	timestampFormat := time.RFC3339
	if args.Short {
		timestampFormat = "relative"
	}
	format := &ws.LsFormat{
		FullPath:          !args.Short,
		FullMode:          !args.Short,
		TimestampFormat:   timestampFormat,
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

func LogCmd(argv []string) error {
	workspace, err := ws.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	defer workspace.Close() //nolint:errcheck
	args := struct {        //nolint:exhaustruct
		Help  bool
		Short bool
	}{}
	flags := flag.NewFlagSet("log", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Show short log")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s log\n\n", appName)
		fmt.Fprintf(os.Stderr, "Show revision log.n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
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
	repository, err := openRepository(workspace)
	if err != nil {
		return err
	}
	logs, err := ws.Log(repository)
	if err != nil {
		return err //nolint:wrapcheck
	}
	if len(logs) == 0 {
		fmt.Println("Empty repository")
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

func openRepository(workspace *ws.Workspace) (*lib.Repository, error) {
	if !IsTerm(os.Stdin) {
		return nil, lib.Errorf("this command can only be run in an interactive terminal session")
	}
	_, err := fmt.Fprint(os.Stderr, "Enter passphrase: ")
	if err != nil {
		return nil, err //nolint:wrapcheck
	}
	passphrase, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read passphrase")
	}
	fmt.Fprint(os.Stderr, "\r                          \r")
	storage, err := lib.NewFileStorage(string(workspace.RemoteRepository), lib.StoragePurposeRepository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	repository, err := lib.OpenRepository(storage, passphrase)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open repository")
	}
	return repository, nil
}

func pathPatternDescription() string {
	// todo: Add examples.
	return strings.TrimSpace(`
A pattern must match the full path or a directory within the path.
Include patterns (see --include) can be used to override exclude patterns.
Pattern syntax:
    **      matches any number of directories
    *       matches any number of characters in a single directory
    ?       matches a single character
	`)
}

func pathPatternFlag(flags *flag.FlagSet, name string, usage string, value *[]lib.PathPattern) {
	flags.Func(
		name,
		usage+"\n"+pathPatternDescription(),
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

func main() {
	args := struct { //nolint:exhaustruct
		Help bool
	}{}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [command arguments]\n\n", appName)
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  cp           Copy files from the repository to a local directory\n")
		fmt.Fprintf(os.Stderr, "  merge        Merge changes from the repository and the workspace\n")
		fmt.Fprintf(os.Stderr, "  init         Initialize a new repository\n")
		fmt.Fprintf(os.Stderr, "  ls           List files in the repository\n")
		fmt.Fprintf(os.Stderr, "  log          Show revision log\n")
		fmt.Fprintf(os.Stderr, "  status       Show repository status\n")
		fmt.Fprintf(os.Stderr, "\nRun '%s <command> --help' for more information on a command.\n", appName)
	}
	flag.BoolVar(&args.Help, "help", false, "Show help message")
	flag.Parse()
	if args.Help {
		flag.Usage()
		os.Exit(0)
	}
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Missing command\n\n")
		flag.Usage()
		os.Exit(1)
	}
	argv := flag.Args()[1:]
	cmd := flag.Arg(0)
	var err error
	switch cmd {
	case "init":
		err = InitCmd(argv)
	case "cp":
		err = CpCmd(argv)
	case "merge":
		err = MergeCmd(argv)
	case "log":
		err = LogCmd(argv)
	case "ls":
		err = LsCmd(argv)
	case "status":
		err = StatusCmd(argv)
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
