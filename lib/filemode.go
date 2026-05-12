package lib

import "io/fs"

// FileModeType is the set of bits that indicate a non-regular file (directory or symlink).
const FileModeType FileMode = FileModeDir | FileModeSymlink

func NewFileMode(fm fs.FileMode) FileMode {
	mode := FileMode(fm.Perm())
	if fm&fs.ModeDir != 0 {
		mode |= FileModeDir
	}
	if fm&fs.ModeSymlink != 0 {
		mode |= FileModeSymlink
	}
	if fm&fs.ModeSetuid != 0 {
		mode |= FileModeSetUid
	}
	if fm&fs.ModeSetgid != 0 {
		mode |= FileModeSetGid
	}
	if fm&fs.ModeSticky != 0 {
		mode |= FileModeSticky
	}
	return mode
}

func (m FileMode) AsFsFileMode() fs.FileMode {
	mode := fs.FileMode(m.Perm())
	if m&FileModeDir != 0 {
		mode |= fs.ModeDir
	}
	if m&FileModeSymlink != 0 {
		mode |= fs.ModeSymlink
	}
	if m&FileModeSetUid != 0 {
		mode |= fs.ModeSetuid
	}
	if m&FileModeSetGid != 0 {
		mode |= fs.ModeSetgid
	}
	if m&FileModeSticky != 0 {
		mode |= fs.ModeSticky
	}
	return mode
}

func (m FileMode) String() string {
	const str = "dLugtr"
	bits := []FileMode{FileModeDir, FileModeSymlink, FileModeSetUid, FileModeSetGid, FileModeSticky}
	var buf [14]byte
	for i, b := range bits {
		if m&b != 0 {
			buf[i] = str[i]
		} else {
			buf[i] = '-'
		}
	}
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&FileMode(1<<(8-i)) != 0 {
			buf[i+5] = byte(c) //nolint:gosec
		} else {
			buf[i+5] = '-'
		}
	}
	return string(buf[:])
}

// Return a string in the style of `ls -l`.
func (m FileMode) ShortString() string {
	var buf [10]byte
	buf[0] = '-'
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&FileMode(1<<(8-i)) != 0 {
			buf[i+1] = byte(c) //nolint:gosec
		} else {
			buf[i+1] = '-'
		}
	}
	if m&FileModeSymlink != 0 {
		buf[0] = 'l'
	} else if m&FileModeDir != 0 {
		buf[0] = 'd'
	}
	const ownerExecMask FileMode = 1 << 6
	const groupExecMask FileMode = 1 << 3
	const othersExecMask FileMode = 1 << 0
	// SetUID (modifies owner execute at index 3).
	if m&FileModeSetUid != 0 {
		if m&ownerExecMask != 0 {
			buf[3] = 's'
		} else {
			buf[3] = 'S'
		}
	}
	// SetGID (modifies group execute at index 6).
	if m&FileModeSetGid != 0 {
		if m&groupExecMask != 0 {
			buf[6] = 's'
		} else {
			buf[6] = 'S'
		}
	}
	// Sticky (modifies others execute at index 9).
	if m&FileModeSticky != 0 {
		if m&othersExecMask != 0 {
			buf[9] = 't'
		} else {
			buf[9] = 'T'
		}
	}
	return string(buf[:])
}

func (m FileMode) IsDir() bool {
	return m&FileModeDir != 0
}

func (m FileMode) IsSymlink() bool {
	return m&FileModeSymlink != 0
}

func (m FileMode) IsRegular() bool {
	return m&FileModeType == 0
}

func (m FileMode) IsSticky() bool {
	return m&FileModeSticky != 0
}

func (m FileMode) IsSetUID() bool {
	return m&FileModeSetUid != 0
}

func (m FileMode) IsSetGID() bool {
	return m&FileModeSetGid != 0
}

func (m FileMode) Perm() FileMode {
	return m & FileModePerm
}
