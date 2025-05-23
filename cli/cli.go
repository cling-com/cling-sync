//nolint:forbidigo
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
	"github.com/flunderpero/cling-sync/workspace"
	"golang.org/x/term"
)

const appName = "cling-sync"

func isTerm(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}

func printErr(msg string, args ...any) {
	s := "\nError: "
	if isTerm(os.Stdout) {
		s = fmt.Sprintf("\x1b[31m%s\x1b[0m", s)
	}
	fmt.Fprintf(os.Stderr, s+msg+"\n", args...)
}

func initCmd(argv []string) error { //nolint:funlen
	if !isTerm(os.Stdin) {
		return lib.Errorf("a new repository can only be created in an interactive terminal session")
	}
	args := struct { //nolint:exhaustruct
		Help                bool
		AllowUnsafePassword bool
	}{}
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.AllowUnsafePassword, "allow-unsafe-password", false, "Allow unsafe password")
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
	_, err := fmt.Fprint(os.Stdout, "Enter passphrase: ")
	if err != nil {
		return err //nolint:wrapcheck
	}
	passphrase, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return lib.WrapErrorf(err, "failed to read passphrase")
	}
	_, _ = fmt.Fprintln(os.Stdout)
	if err := lib.CheckPassphraseStrength(passphrase); err != nil {
		if args.AllowUnsafePassword {
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
	storage, err := lib.NewFileStorage(repositoryPath)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create storage")
	}
	_, err = lib.InitNewRepository(storage, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to initialize repository")
	}
	_, err = workspace.NewWorkspace(".", workspace.RemoteRepository(repositoryPath))
	if err != nil {
		return lib.WrapErrorf(err, "failed to create workspace")
	}
	return nil
}

func commitCmd(argv []string) error { //nolint:funlen
	ws, err := workspace.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	args := struct { //nolint:exhaustruct
		Help    bool
		Message string
		Author  string
		Exclude []lib.PathPattern
		Include []lib.PathPattern
	}{}
	defaultAuthor := "<anonymous>"
	whoami, err := user.Current()
	if err == nil {
		defaultAuthor = whoami.Username
	}
	defaultMessage := "Synced with cling-sync"
	flags := flag.NewFlagSet("commit", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Author, "author", defaultAuthor, "Author name")
	flags.StringVar(&args.Message, "message", defaultMessage, "Commit message")
	// todo: Ignore patterns must be respected by the revision snapshot too or
	//       otherwise you will delete files that match the ignore patters and
	//		 are in the repository.
	//		 Put differently, we want ignore to ignore paths from being processed
	//		 during the commit at all.
	flags.Func(
		"exclude",
		// todo: Centralize this description or add it everywhere patterns are used.
		// todo: Add examples.
		strings.TrimSpace(`
Exclude paths matching the given pattern (can be used multiple times).
A pattern must match the full path or a directory within the path.
Include patterns (see --include) can be used to override exclude patterns.
Pattern syntax:
    **      matches any number of directories
    *       matches any number of characters in a single directory
    ?       matches a single character
		`),
		func(pattern string) error {
			p, err := lib.NewPathPattern(pattern)
			if err != nil {
				return lib.WrapErrorf(err, "invalid pattern: %s", pattern)
			}
			args.Exclude = append(args.Exclude, p)
			return nil
		},
	)
	flags.Func(
		"include",
		// todo: Centralize this description or add it everywhere patterns are used.
		// todo: Add examples.
		strings.TrimSpace(`
Include patterns are used to override exclude patterns. They are not evaluated
when determining whether a path should be included in the commit.
		`),
		func(pattern string) error {
			p, err := lib.NewPathPattern(pattern)
			if err != nil {
				return lib.WrapErrorf(err, "invalid pattern: %s", pattern)
			}
			args.Include = append(args.Include, p)
			return nil
		},
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s commit\n\n", appName)
		fmt.Fprintf(os.Stderr, "Synchronize all local changes to the repository.\n")
		fmt.Fprintf(os.Stderr, "All files not present will be removed in the revision.\n")
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
	if len(args.Exclude) == 0 && len(args.Include) > 0 {
		return lib.Errorf("include patterns can only be used with exclude patterns")
	}
	repository, err := openRepository(ws)
	if err != nil {
		return err
	}
	pathFilter := &lib.PathExclusionFilter{Excludes: args.Exclude, Includes: args.Include}
	revisionId, err := workspace.Commit(
		ws.WorkspacePath,
		repository,
		&workspace.CommitConfig{PathFilter: pathFilter, Author: args.Author, Message: args.Message},
	)
	if err != nil {
		return err //nolint:wrapcheck
	}
	fmt.Printf("Revision %s\n", revisionId)
	return nil
}

func lsCmd(argv []string) error {
	ws, err := workspace.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	args := struct { //nolint:exhaustruct
		Help     bool
		Revision string
	}{}
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.StringVar(&args.Revision, "revision", "HEAD", "Revision to show")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s ls [pattern]\n\n", appName)
		fmt.Fprintf(os.Stderr, "List files in the repository.\n\n")
		fmt.Fprintf(os.Stderr, "  pattern\n")
		fmt.Fprintf(os.Stderr, "        The pattern syntax is the same as for the `commit --ignore` option.\n")
		fmt.Fprintf(os.Stderr, "        A pattern must match the full path of the file.\n")
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
	repository, err := openRepository(ws)
	if err != nil {
		return err
	}
	var revisionId lib.RevisionId
	if strings.ToLower(args.Revision) == "head" {
		revisionId, err = repository.Head()
		if err != nil {
			return lib.WrapErrorf(err, "failed to read head revision")
		}
	} else {
		b, err := hex.DecodeString(args.Revision)
		if err != nil {
			return lib.Errorf("invalid revision id: %s", args.Revision)
		}
		revisionId = lib.RevisionId(b)
	}
	files, err := workspace.Ls(repository, revisionId, pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}
	for _, file := range files {
		fmt.Println(file.String())
	}
	return nil
}

func logCmd(argv []string) error {
	ws, err := workspace.OpenWorkspace(".")
	if err != nil {
		return lib.WrapErrorf(err, "failed to open workspace")
	}
	args := struct { //nolint:exhaustruct
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
	repository, err := openRepository(ws)
	if err != nil {
		return err
	}
	logs, err := workspace.Log(repository)
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

func openRepository(workspace *workspace.Workspace) (*lib.Repository, error) {
	if !isTerm(os.Stdin) {
		return nil, lib.Errorf("this command can only be run in an interactive terminal session")
	}
	_, err := fmt.Fprint(os.Stdout, "Enter passphrase: ")
	if err != nil {
		return nil, err //nolint:wrapcheck
	}
	passphrase, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read passphrase")
	}
	_, _ = fmt.Fprintln(os.Stdout)
	storage, err := lib.NewFileStorage(string(workspace.RemoteRepository))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	repository, err := lib.OpenRepository(storage, passphrase)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open repository")
	}
	return repository, nil
}

func main() {
	args := struct { //nolint:exhaustruct
		Help bool
	}{}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [command arguments]\n\n", appName)
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  commit       Synchronize local changes to the repository\n")
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
	cmd := flag.Arg(0)
	var err error
	switch cmd {
	case "init":
		err = initCmd(flag.Args()[1:])
	case "commit":
		err = commitCmd(flag.Args()[1:])
	case "log":
		err = logCmd(flag.Args()[1:])
	case "ls":
		err = lsCmd(flag.Args()[1:])
	case "":
		flag.Usage()
		os.Exit(0)
	default:
		printErr("%s is not a valid command. See '%s --help'.", cmd, appName)
		os.Exit(1)
	}
	if err != nil {
		printErr(err.Error())
		os.Exit(1)
	}
}
