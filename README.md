# cling-sync: The Secure Forever Store

## Synopsis

cling-sync is a _client-side encrypted_, _revisional_ archival storage system.

## Cryptography Overview

Let's look at a birds view of how cryptography is used.
The repository cryptography relies on these values you can find in `.cling/repository.txt`:

- An encrypted 32-byte **Key Encryption Key (KEK)** that is the root key used to derive all other
  _Data Encryption Keys (DEK)_.

- A 32 byte **User Key Salt** that is used in the Key Derivation Function (KDF) to derive
  an encryption key to encrypt/decrypt the KEK.

Both of these values are not strictly secret - the passphrase is still needed to decrypt the KEK.

### Algorithms Used

| Purpose                  | Algorithm                   | Notes                                              |
|--------------------------|-----------------------------|-----------------------------------------------------|
| Key derivation           | Argon2id                    | 5 iterations, 64MB RAM, 1 thread                   |
| Encryption (all data)    | XChaCha20-Poly1305 (AEAD)   | Nonce-misuse resistant; 24B nonce, 16B tag         |
| Block ID generation      | HMAC-SHA256                 | Uses per-repo secret HMAC key                      |

### User Authentication / KEK Encryption

The flow to arrive at the KEK:

- The user provides their passphrase

- The **Argon2id KDF** _(5 iterations, 64MB RAM, 1 thread)_ is used to derive a key from the
  _passphrase_ and the _User Key Salt_.

- That key is then used to decrypt the encrypted KEK.

### Blocks

#### Block IDs

A repository wide HMAC key is derived from the KEK like this: `SHA256(kek + "-blockId")`.

That HMAC key is then used to calculate the block id like this: `HMAC(SHA256(blockContent), HMACKey)`

#### Data Encryption

The content of files are stored in blocks of up to _4MB_ in size. Each block is encrypted with
a unique, random 32 byte _Data Encryption Key (DEK)_. That DEK is then stored alongside the random
nonce used in the block header (see below).

## File Formats

All integer types are written as little-endian, and all strings are UTF-8 encoded.

### Metadata

`FileRevision` is serialized to:

| Size (bytes)  | Type      | Field             | Description                                   |
|----------------|-----------|--------------------|-----------------------------------------------|
| 2             | uint16    | _format version_  | Serialization format version (`0x01`)         |
| _(12)_        | _timespec_| **SyncTime**      | Time of sync                                  |
| 8             | int64     | - SyncTimeSec     | Time of sync (seconds since epoch)            |
| 4             | int32     | - SyncTimeNsec    | Time of sync (nanoseconds)                    |
| 8             | uint64    | ModeAndPerm       | File mode and permission flags (see below)    |
| _(12)_        | _timespec_| **MTime**         | File modification time                        |
| 8             | int64     | - MTimeSec        | File modification time (seconds since epoch)  | 
| 4             | int32     | - MTimeNsec       | File modification time (nanoseconds)          |
| 8             | int64     | Size              | File size                                     |
| 32            | SHA256    | FileHash          | Hash of the file contents                     |
|               | _array_   | **BlockIds**      | Block IDs of the file contents                |
| 2             | uint16    | - Length          | Number of block IDs (N)                       |
| 32 * N        | BlockId   | - BlockIds        | Block IDs (N)                                 |
|               | _string_  | **Target**        | Either the symlink or move target path        |
| 2             | uint16    | - Length          | Length of target file name (M)                |
| M             | uint8     | - Bytes           | utf-8 encoded string                          |
| 4             | uint32    | UID               | Optional: Owner of the file (2^31 if missing) |
| 4             | uint32    | GID               | Optional: Group of the file (2^31 if missing) |
| _(12)_        | _timespec_| **Birthtime**     | Optional: File creation time                  |
| 8             | int64     | - BirthtimeSec    | File creation time (seconds since epoch) or -1|
| 4             | int32     | - BirthtimeNsec   | File creation time (nanoseconds) or -1        |


### Block

`Block` is serialized to:

| Size (bytes)  | Type      | Field             | Description                                   |
|----------------|-----------|--------------------|-----------------------------------------------|
| _(96)_        |           | **Header**        | Header of the block                           |
| 2             | uint16    | _format version_  | Serialization format version (`0x01`)         |
| 8             | uint64    | Flags             | Flags for the block (see below)               |
| 72            | EncKey    | EncryptedDEK      | Block's encryption key (encrypted with KEK)   |
| 4             | uint32    | DataSize          | Size of the following data (N)                |
| 10            |           | _padding_         | Header padding to 96 bytes                    |
| N             | uint8     | **Data**          | Encrypted data of the block                   |

## Documentation To-do's

- How deflate is used: https://www.rfc-editor.org/rfc/rfc1951.txt
