// Generated from format.proto - DO NOT EDIT

//nolint:gocritic,exhaustruct,funlen
package lib

type Compression uint32

const (
	CompressionNone    Compression = 0
	CompressionDeflate Compression = 1
)

type BlockKind uint32

const (
	BlockKindDefault  BlockKind = 0
	BlockKindRevision BlockKind = 1
)

type BlockHeader1 struct {
	Version           uint32
	BlockKind         BlockKind
	Compression       Compression
	Dek               RawKey
	EncryptedDataSize uint32
}

func (o *BlockHeader1) Validate() error {
	switch o.BlockKind {
	case BlockKindDefault, BlockKindRevision:
	default:
		return Errorf("BlockHeader1.BlockKind has invalid value %d", o.BlockKind)
	}
	switch o.Compression {
	case CompressionNone, CompressionDeflate:
	default:
		return Errorf("BlockHeader1.Compression has invalid value %d", o.Compression)
	}
	return nil
}

func (o *BlockHeader1) Marshall(w *ProtobufWriter) error {
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
	if err := w.WriteVarint(int64(o.BlockKind)); err != nil {
		return err
	}
	if err := w.WriteTag(3, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.Compression)); err != nil {
		return err
	}
	if err := w.WriteBytes(4, o.Dek[:]); err != nil {
		return err
	}
	if err := w.WriteTag(5, 0); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(o.EncryptedDataSize)); err != nil {
		return err
	}
	return nil
}

func UnmarshallBlockHeader1(r *ProtobufReader) (BlockHeader1, error) {
	o := BlockHeader1{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return BlockHeader1{}, err
		}
		switch tag {
		case 1:
			u, err := r.ReadUint32()
			if err != nil {
				return BlockHeader1{}, err
			}
			o.Version = u
		case 2:
			u, err := r.ReadUint32()
			if err != nil {
				return BlockHeader1{}, err
			}
			o.BlockKind = BlockKind(u)
		case 3:
			u, err := r.ReadUint32()
			if err != nil {
				return BlockHeader1{}, err
			}
			o.Compression = Compression(u)
		case 4:
			b, err := r.ReadBytes()
			if err != nil {
				return BlockHeader1{}, err
			}
			if len(b) != 32 {
				return BlockHeader1{}, Errorf("BlockHeader1.Dek must have length 32")
			}
			o.Dek = RawKey(b)
		case 5:
			u, err := r.ReadUint32()
			if err != nil {
				return BlockHeader1{}, err
			}
			o.EncryptedDataSize = u
		default:
			if err := r.Skip(wireType); err != nil {
				return BlockHeader1{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return BlockHeader1{}, err
	}
	return o, nil
}

type Block1 struct {
	EncryptedHeader []byte
	EncryptedData   []byte
}

func (o *Block1) Validate() error {
	if len(o.EncryptedHeader) > 512 {
		return Errorf("Block1.EncryptedHeader must not be longer than 512")
	}
	if len(o.EncryptedData) > 8388080 {
		return Errorf("Block1.EncryptedData must not be longer than 8388080")
	}
	return nil
}

func (o *Block1) Marshall(w *ProtobufWriter) error {
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

func UnmarshallBlock1(r *ProtobufReader) (Block1, error) {
	o := Block1{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return Block1{}, err
		}
		switch tag {
		case 1:
			b, err := r.ReadBytes()
			if err != nil {
				return Block1{}, err
			}
			o.EncryptedHeader = b
		case 2:
			b, err := r.ReadBytes()
			if err != nil {
				return Block1{}, err
			}
			o.EncryptedData = b
		default:
			if err := r.Skip(wireType); err != nil {
				return Block1{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return Block1{}, err
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

func (o *Timestamp) Marshall(w *ProtobufWriter) error {
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

func UnmarshallTimestamp(r *ProtobufReader) (Timestamp, error) {
	o := Timestamp{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return Timestamp{}, err
		}
		switch tag {
		case 1:
			i, err := r.ReadVarint()
			if err != nil {
				return Timestamp{}, err
			}
			o.Sec = i
		case 2:
			u, err := r.ReadUint32()
			if err != nil {
				return Timestamp{}, err
			}
			o.Nsec = u
		default:
			if err := r.Skip(wireType); err != nil {
				return Timestamp{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return Timestamp{}, err
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

type File struct {
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

func (o *File) Validate() error {
	if len(o.BlockIds) > 4294967295 {
		return Errorf("File.BlockIds must not be longer than 4294967295")
	}
	if o.SymLinkTarget == nil && o.FileMode&FileModeSymlink != 0 {
		return Errorf("File.SymLinkTarget must be set")
	}
	return nil
}

func (o *File) Marshall(w *ProtobufWriter) error {
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

func UnmarshallFile(r *ProtobufReader) (File, error) {
	o := File{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return File{}, err
		}
		switch tag {
		case 1:
			u, err := r.ReadUint32()
			if err != nil {
				return File{}, err
			}
			o.FileMode = FileMode(u)
		case 2:
			b, err := r.ReadBytes()
			if err != nil {
				return File{}, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return File{}, err
			}
			o.Mtime = v
		case 3:
			i, err := r.ReadVarint()
			if err != nil {
				return File{}, err
			}
			o.Size = i
		case 4:
			b, err := r.ReadBytes()
			if err != nil {
				return File{}, err
			}
			if len(b) != 32 {
				return File{}, Errorf("File.FileHash must have length 32")
			}
			o.FileHash = Sha256(b)
		case 5:
			b, err := r.ReadBytes()
			if err != nil {
				return File{}, err
			}
			if len(b) != 32 {
				return File{}, Errorf("every entry in File.BlockIds must have length 32")
			}
			o.BlockIds = append(o.BlockIds, BlockId(b))
		case 6:
			b, err := r.ReadBytes()
			if err != nil {
				return File{}, err
			}
			v := string(b)
			o.SymLinkTarget = &v
		case 7:
			u, err := r.ReadUint32()
			if err != nil {
				return File{}, err
			}
			v := u
			o.Uid = &v
		case 8:
			u, err := r.ReadUint32()
			if err != nil {
				return File{}, err
			}
			v := u
			o.Gid = &v
		case 9:
			b, err := r.ReadBytes()
			if err != nil {
				return File{}, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return File{}, err
			}
			o.Birthtime = &v
		default:
			if err := r.Skip(wireType); err != nil {
				return File{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return File{}, err
	}
	return o, nil
}

type RevisionEntryKind uint32

const (
	RevisionEntryKindAdd    RevisionEntryKind = 0
	RevisionEntryKindUpdate RevisionEntryKind = 1
	RevisionEntryKindDelete RevisionEntryKind = 2
)

type RevisionEntry1 struct {
	Kind RevisionEntryKind
	Path Path
	File File
}

func (o *RevisionEntry1) Validate() error {
	switch o.Kind {
	case RevisionEntryKindAdd, RevisionEntryKindUpdate, RevisionEntryKindDelete:
	default:
		return Errorf("RevisionEntry1.Kind has invalid value %d", o.Kind)
	}
	return nil
}

func (o *RevisionEntry1) Marshall(w *ProtobufWriter) error {
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
	if err := w.WriteMessage(3, o.File.Marshall); err != nil {
		return err
	}
	return nil
}

func UnmarshallRevisionEntry1(r *ProtobufReader) (RevisionEntry1, error) {
	o := RevisionEntry1{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return RevisionEntry1{}, err
		}
		switch tag {
		case 1:
			u, err := r.ReadUint32()
			if err != nil {
				return RevisionEntry1{}, err
			}
			o.Kind = RevisionEntryKind(u)
		case 2:
			b, err := r.ReadBytes()
			if err != nil {
				return RevisionEntry1{}, err
			}
			pv, err := NewPath(string(b))
			if err != nil {
				return RevisionEntry1{}, err
			}
			o.Path = pv
		case 3:
			b, err := r.ReadBytes()
			if err != nil {
				return RevisionEntry1{}, err
			}
			v, err := UnmarshallFile(NewProtobufReader(b))
			if err != nil {
				return RevisionEntry1{}, err
			}
			o.File = v
		default:
			if err := r.Skip(wireType); err != nil {
				return RevisionEntry1{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return RevisionEntry1{}, err
	}
	return o, nil
}

type RevisionEntryChunk struct {
	Entries []RevisionEntry1
}

func (o *RevisionEntryChunk) Validate() error {
	if len(o.Entries) > 16777215 {
		return Errorf("RevisionEntryChunk.Entries must not be longer than 16777215")
	}
	return nil
}

func (o *RevisionEntryChunk) Marshall(w *ProtobufWriter) error {
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

func UnmarshallRevisionEntryChunk(r *ProtobufReader) (RevisionEntryChunk, error) {
	o := RevisionEntryChunk{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return RevisionEntryChunk{}, err
		}
		switch tag {
		case 1:
			b, err := r.ReadBytes()
			if err != nil {
				return RevisionEntryChunk{}, err
			}
			v, err := UnmarshallRevisionEntry1(NewProtobufReader(b))
			if err != nil {
				return RevisionEntryChunk{}, err
			}
			o.Entries = append(o.Entries, v)
		default:
			if err := r.Skip(wireType); err != nil {
				return RevisionEntryChunk{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return RevisionEntryChunk{}, err
	}
	return o, nil
}

type Revision1 struct {
	Timestamp        Timestamp
	ParentRevisionId RevisionId
	Message          *string
	Author           *string
	BlockIds         []BlockId
}

func (o *Revision1) Validate() error {
	if len(o.BlockIds) > 65535 {
		return Errorf("Revision1.BlockIds must not be longer than 65535")
	}
	return nil
}

func (o *Revision1) Marshall(w *ProtobufWriter) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := w.WriteMessage(1, o.Timestamp.Marshall); err != nil {
		return err
	}
	if err := w.WriteBytes(2, o.ParentRevisionId[:]); err != nil {
		return err
	}
	if o.Message != nil {
		if err := w.WriteBytes(3, []byte((*o.Message))); err != nil {
			return err
		}
	}
	if o.Author != nil {
		if err := w.WriteBytes(4, []byte((*o.Author))); err != nil {
			return err
		}
	}
	for _, v := range o.BlockIds {
		if err := w.WriteBytes(5, v[:]); err != nil {
			return err
		}
	}
	return nil
}

func UnmarshallRevision1(r *ProtobufReader) (Revision1, error) {
	o := Revision1{}
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return Revision1{}, err
		}
		switch tag {
		case 1:
			b, err := r.ReadBytes()
			if err != nil {
				return Revision1{}, err
			}
			v, err := UnmarshallTimestamp(NewProtobufReader(b))
			if err != nil {
				return Revision1{}, err
			}
			o.Timestamp = v
		case 2:
			b, err := r.ReadBytes()
			if err != nil {
				return Revision1{}, err
			}
			if len(b) != 32 {
				return Revision1{}, Errorf("Revision1.ParentRevisionId must have length 32")
			}
			o.ParentRevisionId = RevisionId(b)
		case 3:
			b, err := r.ReadBytes()
			if err != nil {
				return Revision1{}, err
			}
			v := string(b)
			o.Message = &v
		case 4:
			b, err := r.ReadBytes()
			if err != nil {
				return Revision1{}, err
			}
			v := string(b)
			o.Author = &v
		case 5:
			b, err := r.ReadBytes()
			if err != nil {
				return Revision1{}, err
			}
			if len(b) != 32 {
				return Revision1{}, Errorf("every entry in Revision1.BlockIds must have length 32")
			}
			o.BlockIds = append(o.BlockIds, BlockId(b))
		default:
			if err := r.Skip(wireType); err != nil {
				return Revision1{}, err
			}
		}
	}
	if err := o.Validate(); err != nil {
		return Revision1{}, err
	}
	return o, nil
}
