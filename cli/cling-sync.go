package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
	"golang.org/x/term"
)

const appName = "cling-sync"

func usage() string {
	return strings.Trim(fmt.Sprintf(`
Usage: %s <command> [arguments]

Available commands:

	init - initialize a new repository
	sync - Sync changes from the source director to the repository

init src dst

	Initialize a new repository for 'src' at 'dst'.

sync

	Sync changes from the source directory to the repository.

	`, appName), "\n ")
}

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

func initRepo() error {
	if !isTerm(os.Stdin) {
		return lib.Errorf("a new repository can only be created in a terminal.")
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
	storage, err := lib.NewFileStorage("/tmp/tt")
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

func main() {
	var help bool
	flag.BoolVar(&help, "help", false, "Show help message")
	flag.Parse()
	if help {
		if _, err := fmt.Fprintln(os.Stdout, usage()); err != nil {
			printErr(err.Error())
			os.Exit(1)
		}
	}
	cmd := flag.Arg(0)
	switch cmd {
	case "init":
		if err := initRepo(); err != nil {
			printErr(err.Error())
			os.Exit(1)
		}
	case "":
		printErr("no command given. See %s --help for usage.", appName)
		os.Exit(1)
	default:
		printErr("%s is not a valid command. See '%s --help'.", cmd, appName)
		os.Exit(1)
	}
}
