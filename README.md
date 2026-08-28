# cling-sync

cling-sync is a client-side encrypted, revisional, content-addressed
archival store. You put data in. You can retrieve any version of it
later. The server, if any, never sees plaintext and never sees file
names. Every block on disk is indistinguishable from random.

> [!WARNING]
> This project is still in development.

## Contents

1. [Concepts](#concepts)
2. [Quick start](#quick-start)
3. [Command reference](#command-reference)
4. [Remote repositories](#remote-repositories)
5. [Ignore files](#ignore-files)
6. [Pattern reference](#pattern-reference)
7. [Symlinks](#symlinks)
8. [How it works](#how-it-works)
9. [Threat model](#threat-model)
10. [Development](#development)

## Concepts

- **Repository.** The encrypted store. A directory containing a config
  file, a set of immutable encrypted blocks, and a small set of named
  references.
- **Block.** An AEAD-encrypted byte object of up to 8 MiB. Identified
  by an HMAC over its plaintext under a per-repository secret key.
  Blocks are written once and never modified.
- **Revision.** An immutable snapshot of the repository contents at a
  point in time. A revision is itself stored in one or more blocks. Each
  revision points at its parent. The chain ends at the first revision.
- **Head.** A named reference that records the current revision id.
- **Workspace.** A local working copy attached to a repository. It holds
  files in their normal form, a small config file, and optional cached
  state.
- **Merge.** The two-way reconciliation between a workspace and its
  repository. Pulls new revisions, then commits local changes as a new
  revision.

## Quick start

Build the CLI:

    ./build.sh build cli

Create a workspace and its backing repository in one step. `init`
creates the repository at the given path and attaches the current
directory as a workspace pointing at it.

    mkdir myproject && cd myproject
    cling-sync init /path/to/repo

Back up `/path/to/repo/.cling/repository.txt` somewhere safe. The file
holds the encrypted key material and the Argon2id parameters for your
passphrase. Without it the repository cannot be opened even with the
correct passphrase. Print it, store it offline, or keep a copy on a
second machine. The file is not secret on its own. The passphrase is
still required to derive the user key.

To attach to an existing repository, use `attach`. The repository is
either a local path or an S3 URI (any URI starting with `s3+http://`
or `s3+https://`). See [Remote repositories](#remote-repositories)
for the S3 setup.

    cling-sync attach /path/to/repo /path/to/local/directory
    cling-sync attach s3+https://my-bucket.s3.region.example.com /path/to/local/directory

Edit files. Commit them as a new revision and pull anything new from the
repository:

    cling-sync merge

List revisions:

    cling-sync log

Restore an earlier revision into the workspace. Use `cling-sync log` to
find the revision id, then:

    cling-sync reset 9f3a...c104
    cling-sync reset HEAD

## Command reference

All commands operate on the current directory's workspace unless noted.
Run `cling-sync <command> --help` for the full flag list.

The commands that do not need a workspace (`cat`, `check`, `cp`,
`import`, `ls`, `log`, `serve`) accept `--repository <path-or-uri>` to
operate on a repository directly, bypassing the workspace. The argument
is a local path or an `s3+...` URI, opened the same way as `attach`. 

### Paths and filters

Every path and pattern argument is relative to the workspace's path
prefix, and every command reports paths in that same relative space. A
workspace attached with `--path-prefix look/here/` sees the repository
path `look/here/dir/f.txt` as `dir/f.txt`, and that is what `cat`, `cp`,
`log`, `ls`, and `status` accept and print. Without a prefix, or under
`--repository`, the relative space is the repository root.

The commands that name repository paths (`cat`, `cp`, `import`, `log`,
`ls`) accept `--path-prefix <dir>/` to use a different subtree for one
invocation, and `--path-prefix /` to use the whole repository from its
root. `merge`, `reset`, and `status` are pinned to the workspace's own
prefix, because their other side is the workspace tree itself.

    cling-sync ls                            # the subtree
    cling-sync ls --path-prefix dir1/        # another subtree
    cling-sync ls --path-prefix /            # the whole repository

Patterns use the git-ignore syntax, documented in full under
[Pattern reference](#pattern-reference), matched against the relative
path. A pattern naming a directory also matches everything below it.
Quote every pattern, because the shell must not expand it: it is matched
against the repository, not against your working directory.

There are two kinds of pattern, and they differ in one way: whether they
are anchored. An anchored pattern has to match from the start of the
path, so `notes.md` matches `notes.md` but not `docs/notes.md`. An
unanchored one may match at any depth, so `notes.md` matches both.

**A pattern argument names a path, so it is anchored.** `cp`, `ls`, and
`status` take a pattern argument. A bare pattern matches only at the top of 
the relative space, and `**/` opts back in to matching at any depth. 
A lone `/` is the start of the path, so it means everything.

A leading `!` is an ordinary character here, not a negation.

    cling-sync ls 'notes.md'      # only at the top
    cling-sync ls '**/notes.md'   # at any depth
    cling-sync ls 'docs'          # docs and everything below it
    cling-sync ls '/'             # everything
    cling-sync ls '!odd'          # a file actually named !odd

**`--exclude` is a filter, so it is not anchored.** A bare pattern
matches at any depth, which is what you want for `build` or `*.tmp`. It
can be repeated, and `cp`, `ls`, `status`, and `log` all take it.

    cling-sync ls --exclude 'build' --exclude '**/*.tmp'

**`--include`** exists on `log` and `import`, and is not anchored either.
Neither command takes a pattern argument: `log` works mainly on
revisions, and `import` already spends both of its arguments on the
source and the destination.

#### How filters combine

A path has to survive every filter, so `--include` narrows and
`--exclude` removes. Which of the two you write first makes no
difference.

**`--include` cannot bring back what `--exclude` dropped.** That differs
from tools where `--include` overrides `--exclude`. Here the two are
combined rather than ranked, so this matches nothing at all:

    cling-sync log --exclude '**/c.txt' --include '**/c.txt'

To carve an exception out of an exclusion, negate inside `--exclude` with
a leading `!`. Each `--exclude` takes one pattern, so repeat the flag to
give several. They are applied left to right and the last one to match a
path wins, so a negation has to come after whatever it is re-including.

    cling-sync ls --exclude 'build' --exclude '!build/keep.txt'  # keeps it
    cling-sync ls --exclude '!build/keep.txt' --exclude 'build'  # drops it

### `init <repository-path>`

Create a new repository at the given path and attach the current
directory as a workspace pointing at it. Prompts for a passphrase.
Stores the public-but-not-secret repository config at
`<repository-path>/.cling/repository.txt` and writes the workspace
config to `./.cling/workspace.txt`.

If you only want to create the repository without binding the current
directory, run `init` from an unrelated directory.

To create the repository on an S3 bucket, pass an `s3+https://` URI.
See [Remote repositories](#remote-repositories).

    cling-sync init s3+https://my-bucket.s3.region.example.com

`--argon2id` sets how expensive it is to derive the key from your
passphrase (see [Cryptography](#cryptography)). `m` is memory in KiB,
`t` is iterations, `p` is threads. It defaults to `m=131072,t=4,p=2`
and must stay within `m=12288..1048576`, `t=3..64`, `p=1..64`. Stick
with the default unless you have a reason not to. The cost is paid on
every command that opens the repository, and it is recorded in
`repository.txt`, so it can only be chosen here.

### `attach <repository> <directory>`

Attach to an existing repository. Binds the workspace at `<directory>`
to the given repository. The `<repository>` argument is either a local
filesystem path or an `s3+https://` (or `s3+http://`) URI. See
[Remote repositories](#remote-repositories) for the full S3 setup.
Writes the workspace config to `<directory>/.cling/workspace.txt`.

    cling-sync attach s3+https://my-bucket.s3.region.example.com /path/to/workspace

The `--path-prefix <p>` flag attaches to a subtree of the repository.
All operations are then scoped to that subtree, and paths are shown
relative to it.

By default, the local directory must be empty or not yet exist. This
guards against accidentally attaching to the wrong directory. Pass
`--allow-non-empty` to attach to a directory that already contains
files.

### `merge`

The main operation. Pulls all new revisions from the repository into the
workspace, then commits local changes as a new revision. Conflicts must
be resolved manually.

Ownership, mode, and mtime are recorded on every entry, but they are
not treated as changes and they are not reapplied on restore. Handling
these across systems is error-prone (uid and gid differ between
machines, umask interacts with mode, mtime precision varies on some
filesystems), so the default is to leave them alone. The `--chown`,
`--chmod`, and `--chtime` flags opt each field back in for the current
invocation. The same flags govern both directions: detection of local
changes during commit, and restoration of metadata onto files written
back from the repository.

### `import <source> <destination>/`

Add a local directory to the repository as a new revision. The directory
does not have to be a workspace and is not attached as one. Use it to
put a tree into a repository once, where `merge` would mean binding that
tree to it forever.

Everything in `<source>` lands in `<destination>`, which must end with
`/` and is relative to the path prefix like every other path argument.
With `~/Photos/2026/img.jpg` on disk:

    cling-sync import ~/Photos backup/         # backup/2026/img.jpg
    cling-sync import ~/Photos backup/Photos/  # backup/Photos/2026/img.jpg
    cling-sync import ~/Photos /               # 2026/img.jpg

The changes are shown first, in the same form and the same path space as
`status`, together with the author and message, and nothing is written
until you confirm. `--yes` skips the prompt, `--dry-run` only prints.
Without a terminal to confirm on, `--yes` is required.

An import never removes anything from the repository. Paths in
`<destination>` that the source does not have are left as they are, so
importing is always safe to repeat. Removing a path from a repository
needs a workspace and `merge`.

Only new paths are committed. If the import would overwrite a path that
is already in `<destination>`, the changes are shown and the import
stops, telling you to re-run with `--overwrite`.

`--chown`, `--chmod`, and `--chtime` mean what they do for `merge`, but
only for paths that are already there: a new path always records all
three. They decide whether a file whose contents did not change but
whose metadata did counts as an overwrite, so they require
`--overwrite`.

`--include` and `--exclude` are filters, so they are not anchored and
match against paths relative to `<source>`. See
[Pattern reference](#pattern-reference). `.gitignore` and `.clingignore`
are respected as everywhere else. Symlinks are refused: their target is
only meaningful relative to a workspace, and an imported directory is
not one.

    cling-sync import --include '**/*.jpg' --exclude 'raw' ~/Photos backup/

### `status`

Show which workspace paths differ from the head revision. An optional
glob pattern limits the output, and `--exclude` drops paths it matched.

    cling-sync status
    cling-sync status 'src/**'
    cling-sync status --exclude 'build'

### `log [--include <pattern>] [--exclude <pattern>] [--revision <id>[..<id>]] [--status]`

Show the revision chain. `--include` and `--exclude` take the same
patterns as `ls` and `cp` and restrict the log to revisions that still
have a matching path. Both can be repeated. `--revision <id>` shows only that revision,
the same as it does for `cat`, `cp`, and `ls`. A range `<old>..<new>`
shows every revision after `<old>` up to `<new>`, so it excludes
`<old>`, like git. The default is `..head`, the whole chain.
`--status` shows added, updated, and deleted paths per revision.

    cling-sync log --short
    cling-sync log --status --include 'src/**'
    cling-sync log --exclude '**/*.lock'
    cling-sync log --revision HEAD~1
    cling-sync log --revision HEAD~3..HEAD

> [!NOTE]
> This differs from git, where `git log <commit>` also shows every
> ancestor. Here a bare revision means that one revision, so
> `--revision` reads the same on every command that takes it.

A path prefix does not hide history. Every revision is listed, because
revision ids, `~<n>`, and ranges all address the whole chain, and a log
that showed fewer revisions than those can reach would contradict them.
The prefix only scopes the paths `--status` prints. `--include` and
`--exclude` are different: they are explicit filters, so they do
restrict which revisions are listed.

Whenever `--status` leaves paths out, it says so, so a short or empty
list is never mistaken for a revision that changed nothing.

    (0 of 3 paths shown, --path-prefix / shows the whole repository)

### `ls [<pattern>]`

List paths in a revision, the head by default, optionally filtered by a
glob pattern and `--exclude`. `--revision <id>` lists a non-head
revision.

`--depth <n>` lists at most `n` levels below the listing root, which is
the path prefix in effect, `1` being its direct children. `0`, the
default, means unlimited.

    cling-sync ls
    cling-sync ls '*.md'
    cling-sync ls --exclude 'build'
    cling-sync ls --depth 1
    cling-sync ls --revision 9f3a...c104 'src/**'

### `cat <path>`

Print a single file from a revision. `--revision <id>` reads a non-head
revision. When stdout is a terminal the file is shown in a pager,
otherwise it is written to stdout.

    cling-sync cat notes.md
    cling-sync cat --revision HEAD~1 notes.md

### `cp <pattern> <target>`

Copy paths matching `<pattern>` from a revision into `<target>`,
recreating their directory structure under it. `--revision <id>`
selects a non-head revision and `--exclude` drops paths the pattern
matched.

    cling-sync cp '*' /tmp/restore
    cling-sync cp 'docs/**' .
    cling-sync cp --exclude '**/*.tmp' '*' /tmp/restore
    cling-sync cp --revision 9f3a...c104 report.pdf .

The restored layout mirrors the relative space the pattern matches in,
so `--path-prefix /` recreates full repository paths under `<target>`.

### `reset <revision>`

Reset the workspace to the given revision, discarding local changes.

A revision is addressed by its hex id or by `HEAD` for the current
head, optionally with a git-style `~<n>` suffix to walk `n` revisions
back toward the root (`HEAD~1` is the parent of the head). This form is
accepted everywhere a revision is taken: `reset`, `--revision`, and the
bounds of a `log` range.

    cling-sync reset HEAD~1
    cling-sync reset 9f3a...c104

### `check [--data]`

Verify repository and workspace integrity. The report is written to the
current directory or `--report-dir <dir>` redirects it.

**Repository.** Walks the revision chain and confirms every referenced
block decrypts. With `--data`, additionally reads and decrypts the file
data inside each revision.

**Workspace.** Verifies the [block cache](#workspace-block-cache): every
cached block must exist in the repository (`--data` compares the bytes),
the cache must respect its size limit, and its accounting must be
roughly right. Any issue is resolved by `check --clear-workspace-cache`,
which empties the cache and is always safe. With `--repository` the
workspace check is skipped, the command bypasses the workspace entirely.

### `security save-passphrase`

Store the passphrase in the workspace at
`.cling/workspace/security/encrypted-passphrase`. The file is
AEAD-encrypted with a random local key held in the OS keychain.
Convenience only. See [Threat model](#threat-model) for what this
scheme does and does not protect against.

On macOS, you may need:

    security unlock-keychain ~/Library/Keychains/login.keychain-db

On Linux, the key is stored via `secret-tool`. Unlock the Gnome keyring
with:

    printf '\n' | gnome-keyring-daemon --unlock

### `security delete-passphrase`

Remove the saved passphrase and the matching keychain entry.

### `security encrypt-s3-url [--credentials-file <path>] <endpoint>`

Print a self-contained cling-sync S3 URI for `<endpoint>` with the S3
access credentials encrypted under the repository passphrase. Useful
for attaching the same repository from another of your machines
without re-entering the S3 credentials. Opens the repository at
`<endpoint>` with the given credentials and passphrase first, so a
typo in any of them fails fast instead of producing a dead URI. See
[Encrypted S3 URIs](#encrypted-s3-uris) for the format.

### `sync-repo <init|add|list|delete|run>`

Manage and run mirror copies of this workspace's repository. The list
of targets is stored in the workspace under
`.cling/workspace/conf/sync-targets`. Names must be ASCII alphanumeric
or `-`.

- `sync-repo init <name> <dir-or-uri>`: create a new repository with
  this workspace's repository config and register it as `name`. The
  argument is either a local path or an `s3+https://` URI.
- `sync-repo add <name> <uri>`: register an existing repository as
  `name`. The URI is either a local path or an `s3+https://` URI. The
  target is opened and its configuration is required to match the
  source, so mismatched or unreachable URIs are rejected at
  registration time.
- `sync-repo list`: list registered targets.
- `sync-repo delete <name>`: unregister a target. The target storage
  is not removed.
- `sync-repo run [name]`: sync to every registered target, or to a
  single named target. Failures are reported per target and
  re-summarised at the end. One bad target does not stop subsequent
  ones. No passphrase needed because the operation works purely at
  the storage layer.

### `serve --address <addr>`

Expose the workspace repository as an S3 endpoint. Pass
`--repository <path-or-uri>` to serve a repository directly instead. See
[Running your own S3 server](#running-your-own-s3-server).

## Remote repositories

cling-sync only supports S3 as a remote. There is no native protocol.

The reason is reach. S3 with AWS SigV4 is the de facto interface for
blob storage. Every major provider speaks it: AWS, Cloudflare R2,
Backblaze B2, Scaleway, Wasabi, MinIO, Garage, SeaweedFS, and many
more. SigV4 signs every request, and the server signs its responses,
so the client detects any tampering on the wire. cling-sync already
encrypts data client-side. The S3 layer adds authentication of the
transport. Anything S3 can hold, cling-sync can use.

We test against [Scaleway Object Storage](https://www.scaleway.com/en/object-storage/)
and the built-in [`cling-sync serve`](#running-your-own-s3-server).
AWS S3 is expected to work out of the box. Anything supporting SigV4
and conditional `PUT` with `If-None-Match: *` should work.

The IAM policy on the bucket must grant `s3:ListBucket`,
`s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject`.

### Attaching to a bucket

The bucket URL must use the `s3+https://` scheme (or `s3+http://` for
local servers without TLS). The `s3+` prefix is what flags the URL as
an S3 endpoint. A bare `http://` or `https://` URL is rejected.

    cling-sync init   s3+https://my-bucket.s3.region.example.com
    cling-sync attach s3+https://my-bucket.s3.region.example.com /path/to/workspace

The path part of the URL is an optional key prefix:

    cling-sync attach s3+https://my-bucket.s3.region.example.com/some/prefix /path/to/workspace

The first time the CLI sees the bucket, it needs the S3 access key
id and secret. It takes them from the first source that is set:

1. `CLING_S3_KEY_ID` and `CLING_S3_ACCESS_KEY`.
2. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
3. A TTY prompt, unless `--passphrase-from-stdin` is also set.

The CLI encrypts the credentials with the repository passphrase and
stores them in the workspace. Plaintext credentials never touch disk.

### Encrypted S3 URIs

The form stored in the workspace embeds the encrypted credentials in
the userinfo of the URL:

    s3+https://<argon2id-phc>:<ciphertext>@my-bucket.s3.region.example.com/prefix

The `argon2id-phc` field is the Argon2id parameters and the random
salt in [PHC string format](https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md)
(`$argon2id$v=19$m=...,t=...,p=...$<salt>`), base64url-encoded so it
fits the userinfo. The `ciphertext` is the access key id and secret,
AEAD-encrypted. The encryption key is what Argon2id produces from the
repository passphrase plus the salt and parameters in `argon2id-phc`.
The decoder reproduces the same key from the same passphrase. AEAD
additional data binds the ciphertext to `scheme://host/path`, so the
URI cannot be cut and pasted onto a different endpoint.

You can produce one of these URIs explicitly:

    cling-sync security encrypt-s3-url s3+https://my-bucket.s3.region.example.com/prefix

The output is a single self-contained string. Paste it into the
`attach` command on another of your machines to bind a new workspace
to the same bucket without re-entering the S3 credentials. The
repository passphrase is still required to decrypt it. See the
[threat model](#encrypted-s3-uri) for what the URI does and does not
protect.

### Running your own S3 server

`cling-sync serve` exposes the workspace repository as an S3 endpoint.
Pass `--repository` to serve a repository directly, without a workspace.

    cling-sync init /path/to/repo
    cling-sync serve --address 127.0.0.1:9000 --repository /path/to/repo

On first run, `serve` generates random S3 credentials and writes them
into the repository at `.cling/repository/conf/serve`. The same file
is read on later runs, so the credentials persist. The startup output
prints the ready-to-run `cling-sync security encrypt-s3-url
--credentials-file ...` command for turning those credentials into
an [encrypted S3 URI](#encrypted-s3-uris).

The server speaks pure S3. SigV4, virtual-hosted-style addressing,
XML errors. It serves exactly one repository. Put a TLS-terminating
reverse proxy in front for anything beyond localhost.

The server only ever sees AEAD-encrypted blocks. It cannot read their
contents, cannot tamper with them undetected, and cannot forge new
ones.

## Ignore files

cling-sync respects `.gitignore` and `.clingignore`, using the same
patterns as everything else. See [Pattern reference](#pattern-reference).
A file applies to its own directory and everything below it, and a
pattern in a nested file can negate one from a parent.

During `merge`, a repository entry whose path matches a workspace ignore
pattern is not written into the workspace. The entry stays in the
repository, so another workspace without that pattern still receives it.

> [!NOTE]
> One difference from Git. Adding a pattern that matches existing
> tracked files and then running `merge` marks those paths as deleted
> in the next revision. Nothing is actually removed: the files in the
> workspace are untouched, and earlier revisions still contain them.

Once a directory is ignored, cling-sync never looks inside it, so a `!`
pattern cannot bring anything below it back. Git behaves the same way.
This ignores all of `build/`, and the second line does nothing:

    build
    !build/keep.txt

Ignore the contents instead of the directory, and the negation is reached:

    build/*
    !build/keep.txt

## Pattern reference

Patterns follow the [git-ignore syntax](https://git-scm.com/docs/gitignore),
which is the best introduction to them. Git's documentation does not
cover every corner it implements, so this is what cling-sync actually
does.

**Wildcards**

- `*` matches any run of characters inside one path component.
- `?` matches exactly one byte and never a `/`.
- `**` matches any number of directories.
- A pattern matching a directory also matches everything below it, so
  `build` covers `build/out/x.o`.

**Character classes**

- `[abc]` matches one listed character, `[a-z]` a range, `[a-ce-g]`
  several ranges.
- `[!abc]` negates. `[^abc]` negates too, which Git implements but does
  not document, so prefer `!`.
- A reversed range like `[z-a]` matches nothing, as does an unclosed `[`.
- POSIX classes are written `[[:digit:]]`, and `alnum`, `alpha`, `blank`,
  `cntrl`, `digit`, `graph`, `lower`, `print`, `punct`, `space`, `upper`
  and `xdigit` are supported. An unknown one makes the whole pattern
  match nothing.

**Escaping**

- `\` escapes the next character, and escaping an ordinary character is
  harmless: `\R` is just `R`.
- A pattern ending in a lone `\` matches nothing.

**Whole-pattern rules**

- A leading `/` anchors the pattern. A pattern that contains a `/`
  anywhere but at the end is anchored already.
- A leading `!` negates the pattern in ignore files and in `--exclude`
  and `--include`. In a pattern argument it is an ordinary character.
- A trailing `/` requires a directory.
- A leading `#` makes the whole line a comment that matches nothing, so
  `--exclude '#tmp'` quietly does nothing. Write `\#tmp` for a literal
  hash.
- Trailing spaces are stripped unless escaped as `\ `. Leading and
  interior spaces are matched as they are.

**Matching is byte-based**

- Matching is case sensitive: `readme.md` does not match `README.md`.
- Bytes are matched rather than runes, so `?` covers one byte:
  `?ber.txt` misses `über.txt` while `??ber.txt` matches it. Literals and
  `*` are unaffected.
- Hidden files are not special: `*` and `?` match a leading dot.

## Symlinks

Symlinks are tracked if their target is inside the workspace. The
target is stored as a path relative to the workspace root.

Constraints:

- A symlink with an absolute target, or a target outside the
  workspace, is rejected at commit time
  (`symlink target escapes path root`).
- The link and its `mtime` are restored. Mode and ownership of the
  link are not.
- With `--path-prefix`, a symlink whose target is outside the prefix
  is invisible: `merge` does not create it, `ls`, `status`, and `log`
  do not show it. If you create a file or directory at that path, it
  replaces the link in the repository. Until then the link stays, and
  a workspace without the prefix still sees it. `import` treats such a
  path as new, so it replaces the link without `--overwrite`.
- `import` refuses symlinks outright. A stored target is only meaningful
  relative to a workspace, and an imported directory is not one.

## How it works

### Cryptography

All secrets are derived from one user passphrase.

Algorithms used:

- [**Argon2id**](https://www.rfc-editor.org/rfc/rfc9106) for key
  derivation. Defaults: time = 4, memory = 128 MiB, lanes = 2, and
  they can only be changed at [`init`](#init-repository-path).
- [**XChaCha20-Poly1305**](https://en.wikipedia.org/wiki/ChaCha20-Poly1305)
  (AEAD) for every encryption. 24 byte random nonce, 16 byte tag.
- [**HMAC-SHA256**](https://www.rfc-editor.org/rfc/rfc6234) for block
  ids.

An **AEAD** (authenticated encryption with associated data) takes a
key, a nonce, a plaintext, and an optional extra input called the
**additional authenticated data (AAD)**. It produces a ciphertext
plus an authentication tag. Decryption requires the exact same key,
nonce, ciphertext, and AAD. If any of them differs, decryption fails
and no plaintext is returned. The AAD itself is not encrypted and not
stored in the ciphertext, but it is bound to the ciphertext by the
tag, so it cannot be altered without detection. cling-sync uses this
binding to glue values that must travel as a unit, for example by
passing a block id as AAD when encrypting that block (see
[Blocks](#blocks)).

On-disk key material lives in `.cling/repository.txt`, each entry
AEAD-encrypted under a key derived from the passphrase via Argon2id:

- <a id="kek"></a>**Repository master key (KEK).** A 32 byte secret
  used to encrypt the header of every block.
- <a id="blockid-hmac-key"></a>**BlockId HMAC key.** A 32 byte secret
  used as the HMAC-SHA256 key that turns block plaintext into a block
  id.
- <a id="gearcdc-seed"></a>**[GearCDC](https://joshleeb.com/posts/gear-hashing.html) seed.**
  A 32 byte value used to randomise the chunk boundaries when
  splitting large files.

One more piece of key material lives encrypted inside every block:

- <a id="dek"></a>**Data encryption key (DEK).** A fresh 32 byte random
  secret per block, used to encrypt that block's data. Stored inside
  the block header, which is itself encrypted under the KEK.

Public material in `.cling/repository.txt`:

- **Argon2id parameters.** The 32 byte salt plus the time, memory, and
  parallelism cost factors that drive the KDF.

To open a repository:

1. Read `.cling/repository.txt`.
2. Derive `userKey = Argon2id(passphrase, salt, time, memory, lanes)`.
3. Decrypt the KEK, the BlockId HMAC key, and the GearCDC seed under
   the user key. If any AEAD fails, the passphrase is wrong or the
   file was tampered with.

A block is then decrypted in two steps: the KEK decrypts the block
header, and the DEK recovered from the header decrypts the block
data.

### Storage layout

The repository directory looks like this.

    <repo>/.cling/repository.txt          public config (Argon2id params, encrypted keys)
    <repo>/.cling/repository/refs/head    current revision id (hex)
    <repo>/.cling/repository/conf/<name>  optional settings, e.g. the serve credentials
    <repo>/.cling/repository/objects/<aa>/<bb>/<hex-rest>   blocks

Each block lives at a path derived from its id. The `objects/aa/bb/`
two-level fan-out keeps directory sizes manageable.

The workspace directory looks like this.

    <ws>/.cling/workspace.txt             workspace config (remote URI, path prefix)
    <ws>/.cling/workspace/refs/head       last revision merged into this workspace
    <ws>/.cling/workspace/conf/<name>     optional settings, e.g. sync targets and the cache cap
    <ws>/.cling/workspace/objects/<aa>/<bb>/<hex-rest>    cached revision blocks, see below
    <ws>/.cling/workspace/security/encrypted-passphrase   optional, see save-passphrase

Files outside `.cling` are the user's files in their normal, unencrypted
form.

### Blocks

A block is a bounded byte object that cling-sync writes once and never
mutates. The on-disk size of a block is at most 8 MiB.

A block id is the HMAC-SHA256 of the block's plaintext under the
[BlockId HMAC key](#blockid-hmac-key). Two consequences:

- Identical plaintext always produces the same id, so duplicate content
  is stored once.
- The id reveals nothing about the content to anyone without that key.

A block on disk holds two AEAD ciphertexts. The block header is
encrypted with the [repository master key (KEK)](#kek). The block data
is encrypted with a single-use [data encryption key (DEK)](#dek) that
lives inside the encrypted header. Both ciphertexts use the block id as
AEAD associated data, so a block stored under the wrong id fails to
decrypt.

The header carries a format version, a compression flag, the DEK, and
the unpadded data length.

To read a block:

1. Decrypt the header with the KEK and the block id as AAD.
2. Check the header's format version.
3. Decrypt the data with the DEK from the header and the same block id
   as AAD.
4. Trim trailing padding using the unpadded data length from the header.
5. If the compression flag is set, decompress.

Three pieces of processing happen on the writer side before encryption:
content-defined chunking, compression, and padding.

#### Content-defined chunking

Large files are split into chunks by the
[GearCDC](https://joshleeb.com/posts/gear-hashing.html) algorithm,
seeded with the [GearCDC seed](#gearcdc-seed). GearCDC rolls a hash
over the file and tries to pick a "good" boundary at content-defined
positions, so the same positions are chosen across versions of a file
that share those bytes. The benefit is that an edit in the middle of
a file re-chunks only the region around the edit plus a small amount
of collateral on either side. The surrounding chunks keep their
boundaries and their block ids, so only the changed chunks are
written as new blocks. Chunks average around 2 to 4 MiB.

#### Compression

If the block is at least 1 KiB and a 1 KiB sample looks compressible by
an entropy estimate, the block is compressed with
[Deflate](https://www.rfc-editor.org/rfc/rfc1951) level 6. If
compression saves less than 5 percent, the original bytes are kept.

#### Padding

The block data is padded up to the next
[Padmé](https://arxiv.org/abs/1806.03160) boundary. Padding is added
before encryption, so it is covered by the AEAD. The unpadded length
sits inside the encrypted header, so the on-disk block size is one of a
small quantised set rather than the exact plaintext length. This makes
it harder for an attacker with repository access to fingerprint known
files by their on-disk size (see [Fingerprinting](#fingerprinting)).

### Revisions

A revision is an atomic snapshot of repository contents at a point in
time. The revision record itself is a single block. Its
**revision id** is just the [block id](#blocks) of that block.

A revision record contains:

- a magic prefix, so a recovery tool can identify a revision block
  without an external index,
- a timestamp,
- the parent revision id (zero for the first revision),
- an optional commit message and author,
- the ordered list of block ids that hold the revision's entries.

Each entry block holds a batch of `RevisionEntry` records. Every entry
records, for a single path, whether it was added, updated, or deleted
in this revision, together with the path's full metadata: file mode,
modification time, size, content hash, the ordered list of block ids
that hold the file data, an optional symlink target, optional uid, gid,
and birthtime. Paths that did not change in a revision do not appear
in it. They are inherited from the parent.

The current revision is named in `.cling/repository/refs/head`. To
follow the history, a client reads `head`, fetches the named revision
block, decrypts it, then walks parent links.

Paths in revisions are repository-relative. The following are rejected:

- absolute paths (leading `/`)
- `.` or `..` segments
- a trailing `/`
- Windows volume prefixes
- length greater than 4096 bytes

### Workspace block cache

Reading history means reading revision blocks, and with a remote
repository every read is a round trip. The workspace keeps a copy of
every revision block it reads or writes under
`.cling/workspace/objects`. Data blocks are never cached. The cached
blocks are byte-identical to the repository's, so they are just as
encrypted.

The cache is transient. A missing or corrupt entry is fetched from the
repository again, and deleting the directory is always safe. The head
reference is never cached, so a warm cache cannot serve stale history.

The cache is capped at 1 GB by default. Reaching the cap evicts random
cached blocks. To change the cap, create
`.cling/workspace/conf/object-cache`:

    [cache]
    max-bytes = "500000000"

`0` removes the cap. The cache size is tracked in
`.cling/workspace/cache/object-stats` and rebuilt when that file is
missing. `check` verifies the cache against the repository, and
`check --clear-workspace-cache` empties it.

### On-disk wire format

All on-disk and in-block structures are defined in
[`lib/format.proto`](lib/format.proto), using a strict subset of
[proto3](https://protobuf.dev/programming-guides/proto3/). The wire
encoding follows the standard
[protobuf encoding](https://protobuf.dev/programming-guides/encoding/).
Only two wire types appear.

| Wire type | Name             | Used by                          |
| --------- | ---------------- | -------------------------------- |
| 0         | varint           | integers and enums               |
| 2         | length-delimited | bytes, strings, nested messages  |

A varint is a base-128 integer. Each byte carries seven payload bits.
A set high bit means more bytes follow. Varints are capped at ten
bytes.

Each field starts with a tag varint. The tag encodes
`(field_number << 3) | wire_type`.

A length-delimited field is a tag, then a varint length, then that many
bytes.

Repeated fields appear as one tagged entry per element. Packed encoding
is not used. Optional fields are omitted when not set. Required fields
are always written. Unknown tags are skipped on read, which makes
backwards-compatible additions possible.

## Threat model

cling-sync is designed against a **storage-only adversary**: someone
who can read, write, swap, or delete any byte of the repository on
disk or in flight, but does not have the passphrase, the KEK, or the
BlockId HMAC key. Think of a malicious remote host, a compromised
HTTP server, or a hostile file share.

A **local adversary** runs code on the user's machine with at least
the user's privileges. They can read process memory, attach a
debugger, capture coredumps, read the OS keychain, and log keystrokes
including the passphrase as it is typed. cling-sync's cryptographic
guarantees do not extend to this adversary. See
[Saved passphrase](#saved-passphrase) and
[Process memory](#process-memory) for the specifics.

Running [`sync-repo`](#sync-repo-initaddlistdeleterun) to a host you
don't control places that host in the storage-only-adversary set. The
target receives a full copy of `repository.txt`, so anyone with
access to it can mount an offline Argon2id passphrase crack at their
leisure. See [Choosing a passphrase](#choosing-a-passphrase).

### What a storage-only adversary cannot do

cling-sync's design protects against the following. 

- **Decrypt block contents.** Every block is AEAD-encrypted under a
  unique per-block DEK.
- **Tamper with a block.** Any byte flip inside a block fails AEAD
  authentication on read.
- **Substitute one block for another.** The block id is HMAC-SHA256 over
  the plaintext under a secret key, and the id is bound as AEAD
  associated data on both the header and the data. A block stored under
  the wrong id will not decrypt.
- **Forge a block.** Without the BlockId HMAC key, the adversary cannot
  compute a valid id for chosen content.
- **Weaken the legitimate user's KDF.** Rewriting the Argon2id
  parameters in `repository.txt` makes the legitimate user derive a
  different key. The encrypted KEK then refuses to decrypt and the
  repository fails to open. No data is exposed.
- **Speed up an offline passphrase crack by editing the on-disk
  parameters.** The salt is bound to each encrypted master key as
  AEAD associated data, so it cannot be changed undetected. The time,
  memory, and lanes parameters are not in the AAD, but changing them
  produces a different derived key, so the AEAD on the master keys
  fails anyway. Either way, a brute-forcer has to run Argon2id at the
  original cost per guess.
- **Swap the three encrypted master keys in `repository.txt`.** Each
  blob's AAD is the salt plus a per-key label, so an attempt to
  reassign the KEK ciphertext to the BlockId HMAC slot (or any other
  permutation) fails authentication.

### What a storage-only adversary can still do

- **Delete data.** Removing blocks, the head reference, or the config
  file is always available. cling-sync cannot restore what is not
  there.
- **Roll back the head reference.** Replacing the head with an older
  revision id silently moves the repository back to that revision.
  Every older revision is internally valid, so the rollback is
  indistinguishable from a legitimate state. 
- **Force denial of service via Argon2id parameters.** The parameters
  are not bounded from above. Setting memory or time to absurd values
  makes the next legitimate open allocate to exhaustion or hang before
  the passphrase is processed. This is a known gap.
- **Observe size and access patterns.** See
  [Fingerprinting](#fingerprinting).

### Fingerprinting

Two defenses make it harder to fingerprint known file contents from
the outside.

- **Chunk boundaries are unpredictable.** GearCDC is seeded with a
  random 32 byte value per repository, stored encrypted under the user
  key.
- **Block sizes are quantised.** Padmé padding lifts each block to one
  of a small set of sizes. The unpadded length is inside the
  AEAD-protected header, not on disk in the clear.

What remains visible or exploitable:

- Total repository size, number of blocks, and access patterns are not
  hidden.
- If the GearCDC seed leaks (memory dump, coredump, attached
  debugger), chunk boundaries become predictable. An attacker with
  prior knowledge of a candidate file's contents can then test whether
  it is present. The contents stay protected by AEAD. This is an
  accepted limitation of any content-defined chunking system.
- None of these defenses help once the KEK is compromised.

### Choosing a passphrase

Both `repository.txt` and every encrypted S3 URI carry the Argon2id
parameters and salt. Anyone who obtains either file can mount an
offline guessing attack at their own pace. Argon2id makes each
guess costly, but only a strong passphrase keeps the cost
prohibitive.

The current authoritative guidance is
[NIST SP 800-63B-4](https://csrc.nist.gov/pubs/sp/800/63/b/4/final)
§3.1.1 "Passwords" (September 2024): prefer length over composition.
A long passphrase drawn from a wordlist (Diceware-style, six or more
words) is stronger and more memorable than a short string of mixed
character classes. Composition rules of the form "at least one
uppercase, one digit, one special character" are explicitly removed.

### Encrypted S3 URI

A URI produced by `cling-sync security encrypt-s3-url`, or written by
the CLI when a workspace is first attached to an S3 bucket, holds the
S3 access key id and secret encrypted under the repository passphrase.
The passphrase is required to decode it. The URI itself is not a
secret in the strong sense.

What an attacker who intercepts the URI cannot do without the
repository passphrase:

- **Recover the S3 access key id or secret.** The credentials are
  AEAD-protected. Tampering fails the AEAD check on decode.
- **Replay the URI against a different endpoint.** The AEAD additional
  data is `scheme://host/path`. Changing the host or the path before
  the userinfo invalidates the AEAD on decode.

What that same attacker can still do:

- **See the server's address.** The `host[:port]` is in plaintext in
  the URI. So is the path prefix. Anything that leaks the URI reveals
  which bucket and prefix you back up to.
- **Mount an offline attack on the repository passphrase.** Same
  exposure as for a leaked `repository.txt`. See
  [Choosing a passphrase](#choosing-a-passphrase) for what makes that
  attack expensive.

To revoke a leaked URI, invalidate either factor it carries:

- **Rotate the S3 access key on the bucket.** Every URI that wrapped
  the old credentials stops working at once. Every workspace that
  held one must then be re-attached.
- **Change the repository passphrase.** A new passphrase derives a
  different key, so every URI encrypted under the old one becomes
  unreadable.

### Saved passphrase

`cling-sync security save-passphrase` writes the passphrase into the
workspace, encrypted with a random local key held in the OS keychain.
This is for convenience on a trusted workstation.

It does not protect against:

- code running as the same user (anything that can read the keychain
  entry can decrypt the saved passphrase),
- memory forensics, coredumps, hibernation images (the passphrase is
  in process memory while cling-sync runs),
- a compromised OS keychain backend.

If your threat model includes a hostile local machine, do not use
`save-passphrase`.

### Process memory

While cling-sync is running, the following plaintext key material
lives in process memory:

- the passphrase, until the user key has been derived,
- the user key derived from it,
- the KEK, the BlockId HMAC key, and the GearCDC seed,
- the DEK of each block currently being encrypted or decrypted.

cling-sync does not actively wipe this memory. Anything that exposes
the process address space exposes these secrets: coredumps, swap,
hibernation images, an attached debugger, another process running as
the same user with the right privileges. If any of those are in your
threat model, terminate cling-sync as soon as you finish using it,
and prefer machines without swap or hibernation.

## Development

cling-sync targets MacOS and Linux. Windows is best-effort and not
tested.

The code is plain Go (no CGO). The only external dependencies are a
small selection of `golang.org/x` modules: `crypto` (Argon2id,
XChaCha20-Poly1305), `term` (passphrase prompt), `sys`. The Wasm build
optionally uses [TinyGo](https://tinygo.org/) for size reduction.

`./build.sh --help` lists the available subcommands. The common ones:

    ./build.sh build cli       # produce ./cling-sync
    ./build.sh gen             # regenerate protobuf-derived Go code
    ./build.sh fmt             # format
    ./build.sh lint            # lint
    ./build.sh test            # run all Go tests
    ./build.sh precommit       # gen, fmt, lint, test, integration

Mobile and desktop clients live at
https://github.com/cling-com/cling-sync-clients.

### Using cling-sync as a library

The repository is a single Go module, `github.com/cling-com/cling-sync`.

    go get github.com/cling-com/cling-sync@latest

    lib        repository format, blocks, revisions, encryption
    workspace  workspaces, staging, merge, and the read commands
    http       S3 storage client and server, SigV4, s3+http:// URIs
    keychain   passphrase storage in the OS keychain
    wasm       the browser API, built with GOOS=js GOARCH=wasm

Install the command:

    go install github.com/cling-com/cling-sync/cmd/cling-sync@latest

### Wasm

cling-sync compiles to WebAssembly. A sample page lives in `wasm/`.

Serve a repository, build the Wasm example, then open it:

    cling-sync serve --cors-allow-all --address 127.0.0.1:4242 --repository /path/to/repo
    ./wasm/build.sh dev
    open http://127.0.0.1:8000/example.html

The default Go compiler produces a Wasm binary of about 5 MiB. Building
with `--tinygo` uses [TinyGo](https://tinygo.org/) and reduces it to
about 750 KiB.
