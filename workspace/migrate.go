package workspace

import (
	"context"

	"github.com/cling-com/cling-sync/lib"
)

// Point a workspace at the revision that replaced the one it names, after
// `check --fix-repo` rewrote every revision of the repository.
//
// The rewrite keeps the same revisions in the same order and leaves the old
// ones in storage, so the replacement is simply the revision sitting the same
// distance from the root. That holds whether or not the workspace was up to
// date, and running it again does nothing, because the second pass counts along
// the new chain and lands on the same revision.
//
// Temporary, and goes away with the rest of the migration.
func FixHeadAfterRewrite(
	ctx context.Context,
	ws *Workspace,
	repository *lib.Repository,
) (from lib.RevisionId, to lib.RevisionId, err error) {
	from, err = ws.Head(ctx)
	if err != nil {
		return from, to, lib.WrapErrorf(err, "failed to read workspace head")
	}
	if from.IsRoot() {
		// Never merged, so there is nothing to translate.
		return from, from, nil
	}
	// Reading a revision does not read its entries, so the old chain can still
	// be walked even though its entries are in the order that is now rejected.
	depth := 0
	buf := lib.NewBlockBuf()
	for id := from; !id.IsRoot(); {
		revision, err := repository.ReadRevision(ctx, id, buf)
		if err != nil {
			return from, to, lib.WrapErrorf(err, "failed to walk the workspace head %s", id)
		}
		depth++
		id = revision.ParentRevisionId
	}
	chain, err := lib.ReadRevisionChain(ctx, repository)
	if err != nil {
		return from, to, lib.WrapErrorf(err, "failed to read the revision chain")
	}
	if depth > len(chain) {
		return from, to, lib.Errorf(
			"the workspace head is %d revisions from the root, but the repository has only %d",
			depth, len(chain))
	}
	to = chain[len(chain)-depth]
	if to == from {
		return from, to, nil
	}
	if err := lib.WriteRef(ctx, ws.Storage, "head", to); err != nil {
		return from, to, lib.WrapErrorf(err, "failed to write workspace head")
	}
	return from, to, nil
}
