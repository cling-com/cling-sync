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

func UnmarshallStagingEntry(r *lib.ProtobufReader) (*StagingEntry, error) {
	o := &StagingEntry{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 2 {
				return nil, lib.Errorf("StagingEntry.RepoPath: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			pv, err := lib.NewPath(string(b))
			if err != nil {
				return nil, err
			}
			o.RepoPath = pv
		case 2:
			if wireType != 2 {
				return nil, lib.Errorf("StagingEntry.Metadata: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := lib.UnmarshallPathMetadata(lib.NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Metadata = *v
		case 3:
			if wireType != 2 {
				return nil, lib.Errorf("StagingEntry.Ctime: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := lib.UnmarshallTimestamp(lib.NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Ctime = *v
		case 4:
			if wireType != 0 {
				return nil, lib.Errorf("StagingEntry.Size: unexpected wire type %d, want 0", wireType)
			}
			i, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			o.Size = i
		case 5:
			if wireType != 0 {
				return nil, lib.Errorf("StagingEntry.Inode: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint64()
			if err != nil {
				return nil, err
			}
			o.Inode = u
		default:
			if err := r.Skip(wireType); err != nil {
				return nil, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return o, nil
}

type StagingEntryChunk struct {
	Entries []*StagingEntry
}

func (o *StagingEntryChunk) Validate() error {
	if len(o.Entries) > 16777215 {
		return lib.Errorf("StagingEntryChunk.Entries must not be longer than 16777215")
	}
	return nil
}

func (o *StagingEntryChunk) Marshall(w lib.ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	for _, v := range o.Entries {
		if err := w.WriteMessage(1, v.Marshall); err != nil {
			return err
		}
	}
	return nil
}

func (o *StagingEntryChunk) MarshallSize() int {
	sw := lib.NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallStagingEntryChunk(r *lib.ProtobufReader) (*StagingEntryChunk, error) {
	o := &StagingEntryChunk{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 2 {
				return nil, lib.Errorf("StagingEntryChunk.Entries: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallStagingEntry(lib.NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Entries = append(o.Entries, v)
		default:
			if err := r.Skip(wireType); err != nil {
				return nil, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return o, nil
}
