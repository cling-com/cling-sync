// Generated from format.proto - DO NOT EDIT

//nolint:gocritic,exhaustruct,funlen,wrapcheck,nolintlint
package workspace

import "github.com/flunderpero/cling-sync/lib"

type StagingEntry struct {
	RepoPath lib.Path
	Metadata lib.PathMetadata
	Ctime    lib.Timestamp
	Size     int64
	Inode    uint64
}

func (o *StagingEntry) Validate() error {
	return nil
}

func (o *StagingEntry) Marshall(w lib.ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteBytes(1, []byte(o.RepoPath.String())); err != nil {
		return err
	}
	if err := w.WriteMessage(2, o.Metadata.Marshall); err != nil {
		return err
	}
	if err := w.WriteMessage(3, o.Ctime.Marshall); err != nil {
		return err
	}
	if err := w.WriteTag(4, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(o.Size); err != nil {
		return err
	}
	if err := w.WriteUint64(5, o.Inode); err != nil {
		return err
	}
	return nil
}

func (o *StagingEntry) MarshallSize() int {
	sw := lib.NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallStagingEntry(r *lib.ProtobufReader) (StagingEntry, error) {
	o := StagingEntry{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return StagingEntry{}, err
		}
		switch tag {
		case 1:
			b, err := r.ReadBytes()
			if err != nil {
				return StagingEntry{}, err
			}
			pv, err := lib.NewPath(string(b))
			if err != nil {
				return StagingEntry{}, err
			}
			o.RepoPath = pv
		case 2:
			b, err := r.ReadBytes()
			if err != nil {
				return StagingEntry{}, err
			}
			v, err := lib.UnmarshallPathMetadata(lib.NewProtobufReader(b))
			if err != nil {
				return StagingEntry{}, err
			}
			o.Metadata = v
		case 3:
			b, err := r.ReadBytes()
			if err != nil {
				return StagingEntry{}, err
			}
			v, err := lib.UnmarshallTimestamp(lib.NewProtobufReader(b))
			if err != nil {
				return StagingEntry{}, err
			}
			o.Ctime = v
		case 4:
			i, err := r.ReadVarint()
			if err != nil {
				return StagingEntry{}, err
			}
			o.Size = i
		case 5:
			u, err := r.ReadUint64()
			if err != nil {
				return StagingEntry{}, err
			}
			o.Inode = u
		default:
			if err := r.Skip(wireType); err != nil {
				return StagingEntry{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return StagingEntry{}, err
	}
	return o, nil
}
