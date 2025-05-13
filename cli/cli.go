//nolint:forbidigo
package main

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
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

func initCmd(argv []string) error {
	if !isTerm(os.Stdin) {
		return lib.Errorf("a new repository can only be created in an interactive terminal session")
	}
	args := struct { //nolint:exhaustruct
		Help bool
	}{}
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s init <dst>\n\n", appName)
		fmt.Fprintf(os.Stderr, "Initialize a new repository at <dst>.\n")
		fmt.Fprintf(os.Stderr, "The destination directory <dst> must not exist or must be empty.\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
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
		return err //nolint:wrapcheck
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
	storage, err := lib.NewFileStorage(flags.Arg(0))
	if err != nil {
		return lib.WrapErrorf(err, "failed to create storage")
	}
	repository, err := lib.InitNewRepository(storage, passphrase)
	if err != nil {
		return lib.WrapErrorf(err, "failed to initialize repository")
	}
	_ = repository
	return nil
}

func syncCmd(argv []string) error { //nolint:funlen
	args := struct { //nolint:exhaustruct
		Help    bool
		Add     bool
		Message string
		Author  string
		Ignore  []lib.PathPattern
	}{}
	defaultAuthor := "<anonymous>"
	whoami, err := user.Current()
	if err == nil {
		defaultAuthor = whoami.Username
	}
	defaultMessage := "Synced with cling-sync"
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Add, "add", false, "Add new files only")
	flags.StringVar(&args.Author, "author", defaultAuthor, "Author name")
	flags.StringVar(&args.Message, "message", defaultMessage, "Commit message")
	flags.Func(
		"ignore",
		strings.TrimSpace(`
Ignore paths matching the given pattern (can be used multiple times)
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
			args.Ignore = append(args.Ignore, p)
			return nil
		},
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s sync <src> <dst>\n\n", appName)
		fmt.Fprintf(os.Stderr, "Synchronize files from the source directory <src> to the repository at <dst>.\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
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
		return lib.Errorf("two positional arguments are required: <src> <dst>")
	}
	repository, err := openRepository(flags.Arg(1))
	if err != nil {
		return err
	}
	revisionId, err := Sync(
		flags.Arg(0),
		repository,
		&SyncConfg{Ignore: args.Ignore, Author: args.Author, Message: args.Message},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Revision %s\n", revisionId)
	return nil
}

func logCmd(argv []string) error {
	args := struct { //nolint:exhaustruct
		Help  bool
		Short bool
	}{}
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.BoolVar(&args.Help, "help", false, "Show help message")
	flags.BoolVar(&args.Short, "short", false, "Show short log")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s log <dst>\n\n", appName)
		fmt.Fprintf(os.Stderr, "Show revision log for the repository at <dst>.\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
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
		return lib.Errorf("one positional argument is required: <dst>")
	}
	repository, err := openRepository(flags.Arg(0))
	if err != nil {
		return err
	}
	logs, err := Log(repository)
	if err != nil {
		return err
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

func openRepository(path string) (*lib.Repository, error) {
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
	storage, err := lib.NewFileStorage(path)
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
		fmt.Fprintf(os.Stderr, "  init         Initialize a new repository\n")
		fmt.Fprintf(os.Stderr, "  sync         Synchronize files\n")
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
	switch cmd {
	case "init":
		if err := initCmd(flag.Args()[1:]); err != nil {
			printErr(err.Error())
			os.Exit(1)
		}
	case "sync":
		if err := syncCmd(flag.Args()[1:]); err != nil {
			printErr(err.Error())
			os.Exit(1)
		}
	case "log":
		if err := logCmd(flag.Args()[1:]); err != nil {
			printErr(err.Error())
			os.Exit(1)
		}
	case "":
		flag.Usage()
		os.Exit(0)
	default:
		printErr("%s is not a valid command. See '%s --help'.", cmd, appName)
		os.Exit(1)
	}
}
