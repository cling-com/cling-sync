// Generated from format.proto - DO NOT EDIT

//nolint:gocritic,exhaustruct,funlen,wrapcheck,nolintlint
package lib

type Compression uint32

const (
	CompressionNone    Compression = 0
	CompressionDeflate Compression = 1
)

type BlockHeader struct {
	Version           uint32
	Compression       Compression
	Dek               RawKey
	EncryptedDataSize uint32
}

func (o *BlockHeader) Validate() error {
	switch o.Compression {
	case CompressionNone, CompressionDeflate:
	default:
		return Errorf("BlockHeader.Compression has invalid value %d", o.Compression)
	}
	return nil
}

func (o *BlockHeader) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteTag(1, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.Version)); err != nil {
		return err
	}
	if err := w.WriteTag(2, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.Compression)); err != nil {
		return err
	}
	if err := w.WriteBytes(3, o.Dek[:]); err != nil {
		return err
	}
	if err := w.WriteTag(4, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.EncryptedDataSize)); err != nil {
		return err
	}
	return nil
}

func (o *BlockHeader) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallBlockHeader(r *ProtobufReader) (*BlockHeader, error) {
	o := &BlockHeader{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 0 {
				return nil, Errorf("BlockHeader.Version: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.Version = u
		case 2:
			if wireType != 0 {
				return nil, Errorf("BlockHeader.Compression: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.Compression = Compression(u)
		case 3:
			if wireType != 2 {
				return nil, Errorf("BlockHeader.Dek: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			if len(b) != 32 {
				return nil, Errorf("BlockHeader.Dek must have length 32")
			}
			o.Dek = RawKey(b)
		case 4:
			if wireType != 0 {
				return nil, Errorf("BlockHeader.EncryptedDataSize: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.EncryptedDataSize = u
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

type Block struct {
	EncryptedHeader []byte
	EncryptedData   []byte
}

func (o *Block) Validate() error {
	if len(o.EncryptedHeader) > 512 {
		return Errorf("Block.EncryptedHeader must not be longer than 512")
	}
	if len(o.EncryptedData) > 8257576 {
		return Errorf("Block.EncryptedData must not be longer than 8257576")
	}
	return nil
}

func (o *Block) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteBytes(1, o.EncryptedHeader[:]); err != nil {
		return err
	}
	if err := w.WriteBytes(2, o.EncryptedData[:]); err != nil {
		return err
	}
	return nil
}

func (o *Block) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallBlock(r *ProtobufReader) (*Block, error) {
	o := &Block{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 2 {
				return nil, Errorf("Block.EncryptedHeader: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			o.EncryptedHeader = b
		case 2:
			if wireType != 2 {
				return nil, Errorf("Block.EncryptedData: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			o.EncryptedData = b
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

type Timestamp struct {
	Sec  int64
	Nsec uint32
}

func (o *Timestamp) Validate() error {
	return nil
}

func (o *Timestamp) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteTag(1, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(o.Sec); err != nil {
		return err
	}
	if err := w.WriteTag(2, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.Nsec)); err != nil {
		return err
	}
	return nil
}

func (o *Timestamp) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallTimestamp(r *ProtobufReader) (*Timestamp, error) {
	o := &Timestamp{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 0 {
				return nil, Errorf("Timestamp.Sec: unexpected wire type %d, want 0", wireType)
			}
			i, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			o.Sec = i
		case 2:
			if wireType != 0 {
				return nil, Errorf("Timestamp.Nsec: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.Nsec = u
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

type FileMode uint32

const (
	FileModeOtherExec  FileMode = 0x001
	FileModeOtherWrite FileMode = 0x002
	FileModeOtherRead  FileMode = 0x004
	FileModeGroupExec  FileMode = 0x008
	FileModeGroupWrite FileMode = 0x010
	FileModeGroupRead  FileMode = 0x020
	FileModeOwnerExec  FileMode = 0x040
	FileModeOwnerWrite FileMode = 0x080
	FileModeOwnerRead  FileMode = 0x100
	FileModePerm       FileMode = 0x1FF
	FileModeDir        FileMode = 0x400
	FileModeSymlink    FileMode = 0x800
	FileModeSetUid     FileMode = 0x1000
	FileModeSetGid     FileMode = 0x2000
	FileModeSticky     FileMode = 0x4000
)

type PathMetadata struct {
	FileMode      FileMode
	Mtime         Timestamp
	Size          int64
	FileHash      Sha256
	BlockIds      []BlockId
	SymLinkTarget *string
	Uid           *uint32
	Gid           *uint32
	Birthtime     *Timestamp
}

func (o *PathMetadata) Validate() error {
	if len(o.BlockIds) > 4294967295 {
		return Errorf("PathMetadata.BlockIds must not be longer than 4294967295")
	}
	if o.SymLinkTarget == nil && o.FileMode&FileModeSymlink != 0 {
		return Errorf("PathMetadata.SymLinkTarget must be set")
	}
	return nil
}

func (o *PathMetadata) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteTag(1, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.FileMode)); err != nil {
		return err
	}
	if err := w.WriteMessage(2, o.Mtime.Marshall); err != nil {
		return err
	}
	if err := w.WriteTag(3, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(o.Size); err != nil {
		return err
	}
	if err := w.WriteBytes(4, o.FileHash[:]); err != nil {
		return err
	}
	for _, v := range o.BlockIds {
		if err := w.WriteBytes(5, v[:]); err != nil {
			return err
		}
	}
	if o.SymLinkTarget != nil {
		if err := w.WriteBytes(6, []byte((*o.SymLinkTarget))); err != nil {
			return err
		}
	}
	if o.Uid != nil {
		if err := w.WriteTag(7, 0); err != nil {
			return err
		}
		if err := w.WriteVarint(int64((*o.Uid))); err != nil {
			return err
		}
	}
	if o.Gid != nil {
		if err := w.WriteTag(8, 0); err != nil {
			return err
		}
		if err := w.WriteVarint(int64((*o.Gid))); err != nil {
			return err
		}
	}
	if o.Birthtime != nil {
		if err := w.WriteMessage(9, (*o.Birthtime).Marshall); err != nil {
			return err
		}
	}
	return nil
}

func (o *PathMetadata) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallPathMetadata(r *ProtobufReader) (*PathMetadata, error) {
	o := &PathMetadata{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 0 {
				return nil, Errorf("PathMetadata.FileMode: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.FileMode = FileMode(u)
		case 2:
			if wireType != 2 {
				return nil, Errorf("PathMetadata.Mtime: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Mtime = *v
		case 3:
			if wireType != 0 {
				return nil, Errorf("PathMetadata.Size: unexpected wire type %d, want 0", wireType)
			}
			i, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			o.Size = i
		case 4:
			if wireType != 2 {
				return nil, Errorf("PathMetadata.FileHash: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			if len(b) != 32 {
				return nil, Errorf("PathMetadata.FileHash must have length 32")
			}
			o.FileHash = Sha256(b)
		case 5:
			if wireType != 2 {
				return nil, Errorf("PathMetadata.BlockIds: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			if len(b) != 32 {
				return nil, Errorf("every entry in PathMetadata.BlockIds must have length 32")
			}
			o.BlockIds = append(o.BlockIds, BlockId(b))
		case 6:
			if wireType != 2 {
				return nil, Errorf("PathMetadata.SymLinkTarget: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v := string(b)
			o.SymLinkTarget = &v
		case 7:
			if wireType != 0 {
				return nil, Errorf("PathMetadata.Uid: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			v := u
			o.Uid = &v
		case 8:
			if wireType != 0 {
				return nil, Errorf("PathMetadata.Gid: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			v := u
			o.Gid = &v
		case 9:
			if wireType != 2 {
				return nil, Errorf("PathMetadata.Birthtime: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Birthtime = v
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

type RevisionEntryKind uint32

const (
	RevisionEntryKindAdd    RevisionEntryKind = 0
	RevisionEntryKindUpdate RevisionEntryKind = 1
	RevisionEntryKindDelete RevisionEntryKind = 2
)

type RevisionEntry struct {
	Kind     RevisionEntryKind
	Path     Path
	Metadata PathMetadata
}

func (o *RevisionEntry) Validate() error {
	switch o.Kind {
	case RevisionEntryKindAdd, RevisionEntryKindUpdate, RevisionEntryKindDelete:
	default:
		return Errorf("RevisionEntry.Kind has invalid value %d", o.Kind)
	}
	return nil
}

func (o *RevisionEntry) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteTag(1, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.Kind)); err != nil {
		return err
	}
	if err := w.WriteBytes(2, []byte(o.Path.String())); err != nil {
		return err
	}
	if err := w.WriteMessage(3, o.Metadata.Marshall); err != nil {
		return err
	}
	return nil
}

func (o *RevisionEntry) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallRevisionEntry(r *ProtobufReader) (*RevisionEntry, error) {
	o := &RevisionEntry{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 0 {
				return nil, Errorf("RevisionEntry.Kind: unexpected wire type %d, want 0", wireType)
			}
			u, err := r.ReadUint32()
			if err != nil {
				return nil, err
			}
			o.Kind = RevisionEntryKind(u)
		case 2:
			if wireType != 2 {
				return nil, Errorf("RevisionEntry.Path: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			pv, err := NewPath(string(b))
			if err != nil {
				return nil, err
			}
			o.Path = pv
		case 3:
			if wireType != 2 {
				return nil, Errorf("RevisionEntry.Metadata: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallPathMetadata(NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Metadata = *v
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

type RevisionEntryChunk struct {
	Entries []*RevisionEntry
}

func (o *RevisionEntryChunk) Validate() error {
	if len(o.Entries) > 16777215 {
		return Errorf("RevisionEntryChunk.Entries must not be longer than 16777215")
	}
	return nil
}

func (o *RevisionEntryChunk) Marshall(w ProtobufWriter) error {
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

func (o *RevisionEntryChunk) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallRevisionEntryChunk(r *ProtobufReader) (*RevisionEntryChunk, error) {
	o := &RevisionEntryChunk{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 2 {
				return nil, Errorf("RevisionEntryChunk.Entries: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallRevisionEntry(NewProtobufReader(b))
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

type Revision struct {
	Magic            string
	Timestamp        Timestamp
	ParentRevisionId RevisionId
	Message          *string
	Author           *string
	BlockIds         []BlockId
}

func (o *Revision) Validate() error {
	if len(o.BlockIds) > 65535 {
		return Errorf("Revision.BlockIds must not be longer than 65535")
	}
	return nil
}

func (o *Revision) Marshall(w ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteBytes(1, []byte(o.Magic)); err != nil {
		return err
	}
	if err := w.WriteMessage(2, o.Timestamp.Marshall); err != nil {
		return err
	}
	if err := w.WriteBytes(3, o.ParentRevisionId[:]); err != nil {
		return err
	}
	if o.Message != nil {
		if err := w.WriteBytes(4, []byte((*o.Message))); err != nil {
			return err
		}
	}
	if o.Author != nil {
		if err := w.WriteBytes(5, []byte((*o.Author))); err != nil {
			return err
		}
	}
	for _, v := range o.BlockIds {
		if err := w.WriteBytes(6, v[:]); err != nil {
			return err
		}
	}
	return nil
}

func (o *Revision) MarshallSize() int {
	sw := NewProtobufSizeWriter()
	_ = o.Marshall(sw)
	return sw.Size()
}

func UnmarshallRevision(r *ProtobufReader) (*Revision, error) {
	o := &Revision{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, err
		}
		switch tag {
		case 1:
			if wireType != 2 {
				return nil, Errorf("Revision.Magic: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			o.Magic = string(b)
		case 2:
			if wireType != 2 {
				return nil, Errorf("Revision.Timestamp: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return nil, err
			}
			o.Timestamp = *v
		case 3:
			if wireType != 2 {
				return nil, Errorf("Revision.ParentRevisionId: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			if len(b) != 32 {
				return nil, Errorf("Revision.ParentRevisionId must have length 32")
			}
			o.ParentRevisionId = RevisionId(b)
		case 4:
			if wireType != 2 {
				return nil, Errorf("Revision.Message: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v := string(b)
			o.Message = &v
		case 5:
			if wireType != 2 {
				return nil, Errorf("Revision.Author: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			v := string(b)
			o.Author = &v
		case 6:
			if wireType != 2 {
				return nil, Errorf("Revision.BlockIds: unexpected wire type %d, want 2", wireType)
			}
			b, err := r.ReadBytes()
			if err != nil {
				return nil, err
			}
			if len(b) != 32 {
				return nil, Errorf("every entry in Revision.BlockIds must have length 32")
			}
			o.BlockIds = append(o.BlockIds, BlockId(b))
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
