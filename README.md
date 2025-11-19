# cling-sync: The Secure Forever Store

> [!WARNING]
> This project is still in development. The format on disk might change at any time and
> there might be no way to convert data from one version to another. Also, expect some bugs.

## Synopsis

cling-sync is a _client-side encrypted_, _revisional_ archival storage system.

The main goal is to provide a place where you can put all your data without worrying about
losing it. Everything you put in once stays there forever.

### Terminology

- **Repository**: The place where all data is stored after it has been encrypted.

- **Workspace**: The local working copy of the repository files.

- **Merge**: The process of applying all changes from the repository to the workspace and
  vice versa.

- **Revision**: A snapshot of the repository at a specific point in time, usually created by
  a `merge` operation.


### Modification Tracking

By default, only changes to the file contents are tracked. The file's metadata (ownership, mtime,
and mode) are stored in the repository, but modifications to these metadata are not detected in
consecutive merges unless the `--chown`, `--chmod`, or `--chtime` flags are used.

When merging new files from the repository, mtime and mode are restored from the repository but not
ownership. This is because ownership is highly dependent on the user's environment whereas mtime
and mode are not.

This way of tracking modifications is best suited for the main use case of cling-sync: making sure
user data is never lost without the complications of a "real" backup system.

## OS Support

Currently, the main focus is on supporting MacOS and Linux. It should work on Windows, but is not
tested at the moment.

The fact that everything but the CLI is written in plain Go (no CGO) and uses only the 
standard library with a few select `golang.org/x` dependencies should make it highly portable.

## Usage

**Build the Command Line Interface (CLI) tool:**

Install [Go](https://go.dev/doc/install) version 1.24.2 or later and run:

    ./build.sh build cli

**Run the CLI tool:**

    ./cling-sync <command>

See `./cling-sync --help` for more information.

### Example Workflow

#### Initialize a new repository attached to the current directory

    cling-sync init /path/to/repository

This will create a new repository at `/path/to/repository` where all encrypted data is stored.
Additionally, a `.cling` directory is created in the current directory that ties the repository
to this directory.

Examine `/path/to/repository/.cling/repository.txt` to learn how to backup the encryption keys.

#### Attach to an existing repository

    cling-sync attach /path/to/repository /path/to/local/directory

This will create a new workspace at `/path/to/local/directory` that is connected to the repository
at `/path/to/repository`.

#### Attach to a path inside an existing repository

    cling-sync attach --path-prefix my/path/prefix/ /path/to/repository /path/to/local/directory

This will create a new workspace at `/path/to/local/directory` that is connected to the repository
at `/path/to/repository` and will only show files that are inside the path prefix `my/path/prefix/`.

All operations with the exception of `cp` will be limited to the path prefix.

#### Save the encryption keys to the repository

    cling-sync security save-keys

Store the encryption keys of the repository in `.cling/workspace/security/keys.enc`.

The file is encrypted with a random key that is securely stored in the system's keyring.

**Notes an MacOS:**

You might need to unlock the keychain with 
`security unlock-keychain ~/Library/Keychains/login.keychain-db`

**Notes on Linux:**

This feature uses `secret-tool` to store the random key in the system's keyring.
You can unlock it (Gnome) with:
`echo '\n' | gnome-keyring-daemon --unlock`

#### Merge the local workspace with the repository

    cling-sync merge

This will copy all new or modified files from the repository and delete all files that are not in
the repository's latest revision. After this, changes from the local workspace are committed to the
repository. If there are conflicts, the user is asked to resolve them.

#### Reset the workspace to a specific revision

    cling-sync reset head

This will reset the workspace to the latest repository revision.

#### Show the status of the workspace

    cling-sync status

#### Show the log of revisions

    cling-sync log 'path/to/somewhere/**/*.txt' --status

Show all revisions that contain a path that matches the pattern and show all paths that were added,
updated, or deleted.

#### Serve the repository over HTTP

    cling-sync serve --address 127.0.0.1:4242 /path/to/repository

This will start a HTTP server on port `4242` that serves the repository at `/path/to/repository`.

    cling-sync attach http://127.0.0.1:4242 /path/to/workspace

This will attach the repository at `127.0.0.1:4242` to the workspace at `/path/to/workspace`.

### Ignore files

cling-sync respects `.gitignore` and `.clingignore` files. The syntax is the same as for
[git](https://git-scm.com/docs/gitignore).

> [!NOTE]
> There is one difference between how Git and cling-sync handle ignore files. If you add
> a pattern or path to a `.clingignore` or `.gitignore` file and merge it into the repository,
> all matching files will be removed from the current revision.
> **No files will be removed from the workspace.**
> And as always, older revisions will still contain the files.

## Wasm Support

Wasm support is a main focus of this project.

Play around with the Wasm example included in this repository. First, serve a repository:

    cling-sync serve --cors-allow-all --address 127.0.0.1:4242 /path/to/repository

Then, build the Wasm example:

    ./build.sh wasm dev

Finally, open the example in your browser:

    open http://127.0.0.1:8000/example.html

### Output Size

Using the standard Go compiler (default), the Wasm binary is quite huge (about 5MB).

To compile using [TinyGo](https://tinygo.org/), use the `--optimize` flag:

    ./build.sh wasm dev --optimize

This reduces the binary size to about 600KB, which is okay for now.

## Cryptography

The repository cryptography relies on these values you can find in `.cling/repository.txt`:

- An encrypted 32-byte **Key Encryption Key (KEK)** that is the root key used to derive all other
  _Data Encryption Keys (DEK)_.

- A 32 byte **Block ID HMAC Key** that is used to sign the block id based on the content.

- A 32 byte **User Key Salt** that is used in the Key Derivation Function (KDF) to derive
  an encryption key to encrypt/decrypt the KEK.

All of these values are not strictly secret - without the passphrase, data cannot be decrypted.

### Algorithms Used

| Purpose               | Algorithm                 | Notes                                      |
| --------------------- | ------------------------- | ------------------------------------------ |
| Key derivation        | Argon2id                  | 5 iterations, 64MB RAM, 1 thread           |
| Encryption (all data) | XChaCha20-Poly1305 (AEAD) | Nonce-misuse resistant; 24B nonce, 16B tag |
| Block ID generation   | HMAC-SHA256               | Uses per-repo secret HMAC key              |

### User Authentication / KEK Encryption

The flow to arrive at the KEK:

- The user provides their passphrase

- The **Argon2id KDF** is used to derive a key from the _passphrase_ and the _User Key Salt_.

- That key is then used to decrypt the encrypted KEK.

- The KEK is then used to decrypt the encrypted _Block ID HMAC Key_.

### Blocks

#### Block IDs

A block ID is calculated like this: `HMAC(SHA256(blockContent), BlockIDHMACKey)` where `BlockIDHMACKey`
is the _Block ID HMAC Key_ stored in `.cling/repository.txt`.

This makes blocks content addressable, but you cannot make any assumptions about the content of a
block based on its block id.

#### Data Encryption

File contents and all metadata are stored in blocks of up to _8MB_ in size. Each block is encrypted
with a unique, random 32 byte _Data Encryption Key (DEK)_. That _DEK_ is encrypted with the _KEK_
and stored alongside the random nonce used in the block header (see below).

#### Data Deduplication (Content-Defined Chunking)

If only a part of a file is modified, only that part (more or less) is stored in the repository.
Block boundaries are not fixed, but are calculated using the [GearCDC](https://joshleeb.com/posts/gear-hashing.html)
algorithm.
Basically, the algorithm keeps a rolling hash of the content to detect a "good boundary" so that a
block is at best around 2-4MB in size. Because this is based on the actual content, even changes in
the middle of a file are detected and at some point, the algorithm will detect the boundaries of
blocks that were not changed.
This also means that for files smaller than the average block size, deduplication is not effective.

#### Compression

A block may be compressed if it is at least 1KB in size and the first 1KB "looks" compressable,
i.e. the entropy of the data is low enough. If the compression ratio of the whole block is below 5%,
the block is stored uncompressed.

The compression algorithm is [Deflate](https://en.wikipedia.org/wiki/DEFLATE) with level 6. 

## File Formats

All integer types are written as little-endian, and all strings are UTF-8 encoded.

### Metadata

`FileMetadata` is serialized to:

| Size (bytes) | Type       | Field             | Description                                    |
| ------------ | ---------- | ----------------- | ---------------------------------------------- |
| 2            | uint16     | _format version_  | Serialization format version (`0x01`)          |
| 4            | uint64     | ModeAndPerm       | File mode and permission flags (see below)     |
| _(12)_       | _timespec_ | **MTime**         | File modification time                         |
| 8            | int64      | - MTimeSec        | File modification time (seconds since epoch)   |
| 4            | int32      | - MTimeNsec       | File modification time (nanoseconds)           |
| 8            | int64      | Size              | File size                                      |
| 32           | SHA256     | FileHash          | Hash of the file contents                      |
|              | _array_    | **BlockIds**      | Block IDs of the file contents                 |
| 2            | uint16     | - Length          | Number of block IDs (N)                        |
| 32 \* N      | BlockId    | - BlockIds        | Block IDs (N)                                  |
|              | _string_   | **SymlinkTarget** | The symlink target path or empty               |
| 2            | uint16     | - Length          | Length of target file name (M)                 |
| M            | uint8      | - Bytes           | utf-8 encoded string                           |
| 4            | uint32     | UID               | Optional: Owner of the file (2^31 if missing)  |
| 4            | uint32     | GID               | Optional: Group of the file (2^31 if missing)  |
| _(12)_       | _timespec_ | **Birthtime**     | Optional: File creation time                   |
| 8            | int64      | - BirthtimeSec    | File creation time (seconds since epoch) or -1 |
| 4            | int32      | - BirthtimeNsec   | File creation time (nanoseconds) or -1         |

### Block

`Block` is serialized to:

| Size (bytes) | Type   | Field            | Description                                 |
| ------------ | ------ | ---------------- | ------------------------------------------- |
| _(96)_       |        | **Header**       | Header of the block                         |
| 2            | uint16 | _format version_ | Serialization format version (`0x01`)       |
| 8            | uint64 | Flags            | Flags for the block (see below)             |
| 72           | EncKey | EncryptedDEK     | Block's encryption key (encrypted with KEK) |
| 4            | uint32 | DataSize         | Size of the following data (N)              |
| 10           |        | _padding_        | Header padding to 96 bytes                  |
| N            | uint8  | **Data**         | Encrypted data of the block                 |

## Development

This repository is self-contained and does not depend on any external tools or libraries.
