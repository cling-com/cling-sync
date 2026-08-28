// Commit a set of changes, reading the file data they need from a local directory.
package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"

	"github.com/cling-com/cling-sync/lib"
)

var (
	ErrSourceVanished = lib.Errorf("file vanished while committing")
	ErrSourceModified = lib.Errorf("file was modified while committing")
)

type CommitMonitor interface {
	OnStart(entry *lib.RevisionEntry) error
	// bytesWritten: if nil, the block already existed; otherwise, the total block size (including
	// header) written.
	OnAddBlock(entry *lib.RevisionEntry, blockId lib.BlockId, dataSize int, bytesWritten *int) error
	OnEnd(entry *lib.RevisionEntry) error
	OnBeforeCommit() error
}

// The files to commit and where their data is read from.
type CommitFilesSrc struct {
	Src   lib.FS
	Files *lib.Temp[*lib.RevisionEntry]
}

// The repository the files are committed to, and the revision they are
// committed onto.
type CommitFilesDest struct {
	View *lib.RepositoryView
	// The revision `Files` were computed against, and the parent of the new one.
	// Blocks it already holds are reused instead of read and uploaded again.
	Snapshot *lib.ViewSnapshot
}

type CommitFilesOptions struct {
	Author  string
	Message string
	Monitor CommitMonitor
	// Which metadata differences count as a change. An entry that differs in no
	// other way is left out of the revision.
	RestorableMetadataFlag lib.RestorableMetadataFlag
}

// Commit `src.Files` as a new revision, uploading the file data they need.
// Return `lib.ErrEmptyCommit` if nothing was left to commit, `lib.ErrHeadChanged`
// if the repository moved on, and `ErrSourceVanished` or `ErrSourceModified` if
// the source changed underneath.
func CommitFiles( //nolint:funlen
	ctx context.Context,
	src *CommitFilesSrc,
	dest *CommitFilesDest,
	opts *CommitFilesOptions,
	tmpFS lib.FS,
) (lib.RevisionId, error) {
	commit, err := dest.View.NewCommit(ctx, tmpFS, dest.Snapshot)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit")
	}
	baseline, err := dest.Snapshot.Cache()
	if err != nil {
		return lib.RevisionId{}, err //nolint:wrapcheck
	}
	mon := opts.Monitor
	if err := mon.OnBeforeCommit(); err != nil {
		return lib.RevisionId{}, err //nolint:wrapcheck
	}
	blockBuf := lib.NewBlockBuf()
	reader := src.Files.Reader(nil)
	for {
		entry, err := reader.Read(blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read changes")
		}
		if err := mon.OnStart(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor start failed for %s", entry.Path)
		}
		if entry.Kind != lib.RevisionEntryKindDelete {
			srcPath := entry.Path
			stat, err := src.Src.Stat(srcPath.String())
			if errors.Is(err, fs.ErrNotExist) {
				return lib.RevisionId{}, lib.WrapErrorf(ErrSourceVanished, "%s", srcPath)
			}
			if err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to stat %s", srcPath)
			}
			baselineEntry, inBaseline, err := baseline.Get(entry.PathKey())
			if err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get %s from the baseline", entry.Path)
			}
			md := entry.Metadata
			sameContent := inBaseline && entry.Metadata.FileHash == baselineEntry.Metadata.FileHash
			switch {
			case sameContent &&
				entry.Metadata.IsEqualRestorableAttributes(baselineEntry.Metadata, opts.RestorableMetadataFlag):
				// Nothing changed at all, so it does not belong in the revision.
				// Happens when the changes were computed against an older
				// revision than the one being committed onto.
				if err := mon.OnEnd(entry); err != nil {
					return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor end failed for %s", entry.Path)
				}
				continue
			case sameContent:
				// Only the metadata changed, the blocks in the repository still hold the content.
				md.BlockIds = baselineEntry.Metadata.BlockIds
			default:
				md, err = AddFileToRepository(ctx, src.Src, srcPath, stat, dest.View.Repository, entry, mon)
				if err != nil {
					return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add %s to the repository", srcPath)
				}
			}
			if md.FileHash != entry.Metadata.FileHash {
				return lib.RevisionId{}, lib.WrapErrorf(
					ErrSourceModified,
					"%s (hash: %s vs %s)",
					srcPath,
					md.FileHash,
					entry.Metadata.FileHash,
				)
			}
			entry.Metadata = md
		}
		if err := commit.Add(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add %s to commit", entry.Path)
		}
		if err := mon.OnEnd(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor end failed for %s", entry.Path)
		}
	}
	info := &lib.CommitInfo{Author: opts.Author, Message: opts.Message}
	revisionId, err := commit.Commit(ctx, info)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit")
	}
	return revisionId, nil
}

// Add the file contents to the repository and return the file metadata.
func AddFileToRepository(
	ctx context.Context,
	srcFS lib.FS,
	path lib.Path,
	fileInfo fs.FileInfo,
	repository *lib.Repository,
	entry *lib.RevisionEntry,
	mon CommitMonitor,
) (lib.PathMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	if fileInfo.Mode()&fs.ModeSymlink != 0 {
		md := lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil)
		md.SymLinkTarget = entry.Metadata.SymLinkTarget
		return md, nil
	}
	// Fast path: If the entry already has BlockIds and the size of the file did
	// not change, only calculate the hash.
	// If the hash is the same, we can skip the whole block calculation.
	if entry != nil && len(entry.Metadata.BlockIds) > 0 &&
		entry.Metadata.Size == fileInfo.Size() {
		md, err := computeFileHash(srcFS, path, fileInfo)
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to create file metadata")
		}
		if bytes.Equal(md.FileHash[:], entry.Metadata.FileHash[:]) {
			md.BlockIds = entry.Metadata.BlockIds
			return md, nil
		}
	}
	blockIds := []lib.BlockId{}
	fileHash := sha256.New()
	f, err := srcFS.OpenRead(path.String())
	if err != nil {
		return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	// Read blocks and add them to the repository.
	cdc := lib.NewGearCDCWithDefaults(f, repository.GearCDCTable())
	writeBuf := lib.NewBlockBuf()
	for {
		data, err := cdc.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
		}
		if _, err := fileHash.Write(data); err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
		}
		blockId, bytesWritten, err := repository.WriteBlock(ctx, data, writeBuf, lib.WriteBlockOpts{})
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to write block")
		}
		if err := mon.OnAddBlock(entry, blockId, len(data), bytesWritten); err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "commit monitor add block failed for %s", path)
		}
		blockIds = append(blockIds, blockId)
	}
	return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), blockIds), nil
}
