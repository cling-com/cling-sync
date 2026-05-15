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
4. [Hosting a repository over HTTP](#hosting-a-repository-over-http)
5. [Ignore files](#ignore-files)
6. [How it works](#how-it-works)
7. [Threat model](#threat-model)
8. [Development](#development)

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

To attach to an existing repository, use `attach`. The repository can
be a local path or a remote URL served by
[`cling-sync serve`](#hosting-a-repository-over-http):

    cling-sync attach /path/to/repo /path/to/local/directory
    cling-sync attach http://repo.example:4242 /path/to/local/directory

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

### `init <repository-path>`

Create a new repository at the given path and attach the current
directory as a workspace pointing at it. Prompts for a passphrase.
Stores the public-but-not-secret repository config at
`<repository-path>/.cling/repository.txt` and writes the workspace
config to `./.cling/workspace.txt`.

If you only want to create the repository without binding the current
directory, run `init` from an unrelated directory.

### `attach <repository> <directory>`

Attach to an existing repository. Binds the workspace at `<directory>`
to the given repository. The `<repository>` argument is either a local
filesystem path or a remote URL served by
[`cling-sync serve`](#hosting-a-repository-over-http). Writes the
workspace config to `<directory>/.cling/workspace.txt`.

The `--path-prefix <p>` flag attaches to a subtree of the repository.
All operations except `cp` are then limited to that subtree.

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

### `status`

Show which workspace paths differ from the head revision.

### `log [<pattern>] [--status]`

Show the revision chain. With a pattern, restrict to revisions that
touched a matching path. With `--status`, show added/updated/deleted
paths per revision.

### `ls [<pattern>]`

List paths in the current revision. Accepts a glob pattern.

### `cp <pattern> <target>`

Copy paths from the repository into a local directory, without going
through a workspace. Useful for partial extraction. `--revision <id>`
selects a non-head revision.

### `reset <revision>`

Reset the workspace to the given revision, discarding local changes.
A revision is addressed by its hex id, or by the literal `HEAD` for
the current head revision.

### `check [--data]`

Verify repository integrity. Walks the revision chain and confirms every
referenced block decrypts. With `--data`, additionally reads and
decrypts the file data inside each revision.

### `security save-passphrase`

Store the passphrase in the workspace at
`.cling/workspace/security/passphrase.enc`. The file is AEAD-encrypted
with a random local key held in the OS keychain. Convenience only. See
[Threat model](#threat-model) for what this scheme does and does not
protect against.

On macOS, you may need:

    security unlock-keychain ~/Library/Keychains/login.keychain-db

On Linux, the key is stored via `secret-tool`. Unlock the Gnome keyring
with:

    printf '\n' | gnome-keyring-daemon --unlock

### `security delete-passphrase`

Remove the saved passphrase and the matching keychain entry.

### `sync-repo init <dir>` / `sync-repo run <repo>`

Initialise a second repository as a sync target, then copy new blocks
and revisions from this workspace's repository to the target. Used to
keep mirror copies.

### `serve --address <addr> <repository-path>`

Serve a repository over HTTP. See
[Hosting a repository over HTTP](#hosting-a-repository-over-http).

## Hosting a repository over HTTP

    cling-sync serve --address 127.0.0.1:4242 /path/to/repo

Then attach from elsewhere:

    cling-sync attach http://127.0.0.1:4242 /path/to/workspace

The HTTP layer is intentionally minimal. There is no authentication, no
TLS, and no authorisation. Put a reverse proxy with TLS and access
control in front. [Caddy](https://caddyserver.com/) is one option.

The server only ever sees AEAD-encrypted blocks. It cannot read their
contents, cannot tamper with them undetected, and cannot forge new
ones. It can refuse to serve, delete, or roll back. See
[Threat model](#threat-model) for the full list of what a malicious
server can and cannot do.

## Ignore files

cling-sync respects `.gitignore` and `.clingignore`. The syntax is the
[Git syntax](https://git-scm.com/docs/gitignore).

> [!NOTE]
> One difference from Git. Adding a pattern that matches existing
> tracked files and then running `merge` marks those paths as deleted
> in the next revision. Nothing is actually removed: the files in the
> workspace are untouched, and earlier revisions still contain them.

## How it works

### Cryptography

All secrets are derived from one user passphrase.

Algorithms used:

- [**Argon2id**](https://www.rfc-editor.org/rfc/rfc9106) for key
  derivation. Defaults: time = 4, memory = 128 MiB, lanes = 2.
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
    <repo>/.cling/repository/objects/<aa>/<bb>/<hex-rest>   blocks

Each block lives at a path derived from its id. The `objects/aa/bb/`
two-level fan-out keeps directory sizes manageable.

The workspace directory looks like this.

    <ws>/.cling/workspace.txt             workspace config (remote URI, path prefix)
    <ws>/.cling/workspace/refs/head       last revision merged into this workspace
    <ws>/.cling/workspace/security/passphrase.enc   optional, see save-passphrase

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
in it; they are inherited from the parent.

The current revision is named in `.cling/repository/refs/head`. To
follow the history, a client reads `head`, fetches the named revision
block, decrypts it, then walks parent links.

Paths in revisions are repository-relative. The following are rejected:

- absolute paths (leading `/`)
- `.` or `..` segments
- a trailing `/`
- Windows volume prefixes
- length greater than 4096 bytes

Symlinks are not supported yet.

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

### Wasm

cling-sync compiles to WebAssembly. A sample page lives in `wasm/`.

Serve a repository, build the Wasm example, then open it:

    cling-sync serve --cors-allow-all --address 127.0.0.1:4242 /path/to/repo
    ./build.sh wasm dev
    open http://127.0.0.1:8000/example.html

The default Go compiler produces a Wasm binary of about 5 MiB. Building
with `--optimize` uses [TinyGo](https://tinygo.org/) and reduces it to
about 600 KiB.
