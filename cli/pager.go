//nolint:forbidigo
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/flunderpero/cling-sync/lib"
	"golang.org/x/term"
)

// Pager shows content in the terminal's alternate screen buffer with simple
// scrolling, restoring the original screen on exit.
type Pager struct {
	in       *os.File
	out      *os.File
	tabWidth int
}

func NewPager(in, out *os.File) *Pager {
	return &Pager{in: in, out: out, tabWidth: 8}
}

// ErrNotATerminal is returned by Show when `in` cannot be put into raw mode,
// which usually means it is not a terminal.
var ErrNotATerminal = lib.Errorf("cannot page: input is not a terminal")

// Show pages through content in the terminal's alternate screen buffer.
// It returns ErrNotATerminal if `in` cannot be put into raw mode.
func (p *Pager) Show(content []byte) error {
	fd := int(p.in.Fd()) //nolint:gosec
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return ErrNotATerminal
	}
	defer term.Restore(fd, oldState) //nolint:errcheck
	// Enter alt screen and hide cursor.
	if _, err := fmt.Fprint(p.out, "\x1b[?1049h\x1b[?25l"); err != nil {
		return lib.WrapErrorf(err, "failed to enter alt screen")
	}
	// On exit: show cursor again and exit alt screen.
	defer fmt.Fprint(p.out, "\x1b[?25h\x1b[?1049l") //nolint:errcheck
	var rows []string
	wrapWidth := -1
	top := 0
	in := make([]byte, 16)
	for {
		// Re-measured every pass, so a resize is picked up on the next keypress (no SIGWINCH).
		width, height := p.size()
		if width != wrapWidth {
			rows = p.wrap(content, width)
			wrapWidth = width
		}
		pageRows := height - 1
		maxTop := max(0, len(rows)-pageRows)
		top = clamp(top, 0, maxTop)
		p.render(rows, top, pageRows)
		n, err := p.in.Read(in)
		if err != nil || n == 0 {
			return nil
		}
		// `in[:n]` is the bytes of one keypress: a single byte for a plain key,
		// or a multi-byte CSI escape sequence (`ESC [ ...`) for arrows etc.
		switch key := in[:n]; {
		case key[0] == 'q', key[0] == 3, n == 1 && key[0] == 27: // q, Ctrl-C (0x03), Esc (0x1b)
			return nil
		case key[0] == 'j', isSeq(key, 'B'): // j, down arrow
			top++
		case key[0] == 'k', isSeq(key, 'A'): // k, up arrow
			top--
		case key[0] == ' ', key[0] == 'f', isSeq(key, '6'): // space, f, Page Down
			top += pageRows
		case key[0] == 'b', isSeq(key, '5'): // b, Page Up
			top -= pageRows
		case key[0] == 'g', isSeq(key, 'H'), isSeq(key, '1'): // g, Home
			top = 0
		case key[0] == 'G', isSeq(key, 'F'), isSeq(key, '4'): // G, End
			top = maxTop
		}
		top = clamp(top, 0, maxTop)
	}
}

func (p *Pager) size() (int, int) {
	width, height, err := term.GetSize(int(p.out.Fd())) //nolint:gosec
	if err != nil || width < 1 || height < 2 {
		return 80, 24
	}
	return width, height
}

func (p *Pager) render(rows []string, top, pageRows int) {
	var sb strings.Builder
	sb.WriteString("\x1b[H\x1b[2J")
	for i := range pageRows {
		if idx := top + i; idx < len(rows) {
			sb.WriteString(rows[idx])
		}
		sb.WriteString("\x1b[K\r\n")
	}
	end := min(top+pageRows, len(rows))
	start := top + 1
	if len(rows) == 0 {
		start = 0
	}
	fmt.Fprintf(&sb, "\x1b[7m %d-%d/%d  (q quit  j/k space/b g/G move)\x1b[0m", start, end, len(rows))
	fmt.Fprint(p.out, sb.String()) //nolint:errcheck
}

// wrap splits content into display rows no wider than width terminal columns.
func (p *Pager) wrap(content []byte, width int) []string {
	if width < 1 {
		width = 1
	}
	lines := bytes.Split(content, []byte("\n"))
	if n := len(lines); n > 0 && len(lines[n-1]) == 0 {
		lines = lines[:n-1] // Drop the trailing empty segment from a final newline.
	}
	rows := make([]string, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, p.wrapLine(string(bytes.TrimSuffix(line, []byte("\r"))), width)...)
	}
	return rows
}

// wrapLine breaks one logical line into display rows, expanding tabs to tab
// stops and never splitting a multi-column rune across rows.
func (p *Pager) wrapLine(line string, width int) []string {
	rows := []string{}
	var sb strings.Builder
	col := 0
	for _, r := range line {
		cells, w := p.cells(r, col)
		if w > 0 && col > 0 && col+w > width {
			rows = append(rows, sb.String())
			sb.Reset()
			col = 0
			cells, w = p.cells(r, col) // A tab's width depends on the column.
		}
		sb.WriteString(cells)
		col += w
	}
	rows = append(rows, sb.String())
	return rows
}

// cells renders a single rune at the given column, returning the printable
// string and the number of terminal columns it occupies.
func (p *Pager) cells(r rune, col int) (string, int) {
	switch {
	case r == '\t':
		w := p.tabWidth - col%p.tabWidth
		return strings.Repeat(" ", w), w
	case r == '\r':
		return "", 0
	case r < 0x20 || r == 0x7f:
		return fmt.Sprintf("^%c", r^0x40), 2 // Caret notation, like less.
	default:
		return string(r), runeWidth(r)
	}
}

// runeWidth returns the terminal column count of a printable rune: 0 for
// combining and zero-width marks, 2 for East Asian wide characters and emoji,
// 1 otherwise.
func runeWidth(r rune) int {
	switch {
	case unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf):
		return 0
	case unicode.Is(wideRanges, r):
		return 2
	default:
		return 1
	}
}

// isSeq reports whether key is the CSI escape sequence `ESC [ b` (e.g. b='A'
// is the up arrow, b='6' the start of Page Down's `ESC [ 6 ~`).
func isSeq(key []byte, b byte) bool {
	return len(key) >= 3 && key[0] == 27 && key[1] == '[' && key[2] == b
}

func clamp(v, lo, hi int) int {
	return min(max(v, lo), hi)
}

// wideRanges holds the East Asian Wide and Fullwidth blocks plus the common
// emoji blocks, all of which occupy two terminal columns.
var wideRanges = &unicode.RangeTable{ //nolint:gochecknoglobals
	R16: []unicode.Range16{
		{0x1100, 0x115F, 1}, // Hangul Jamo
		{0x2329, 0x232A, 1}, // angle brackets
		{0x2E80, 0x303E, 1}, // CJK radicals, Kangxi, symbols (incl. ideographic space)
		{0x3041, 0x33FF, 1}, // Hiragana, Katakana, CJK symbols, enclosed
		{0x3400, 0x4DBF, 1}, // CJK Unified Ideographs Extension A
		{0x4E00, 0x9FFF, 1}, // CJK Unified Ideographs
		{0xA000, 0xA4CF, 1}, // Yi
		{0xAC00, 0xD7A3, 1}, // Hangul Syllables
		{0xF900, 0xFAFF, 1}, // CJK Compatibility Ideographs
		{0xFE10, 0xFE19, 1}, // Vertical forms
		{0xFE30, 0xFE6F, 1}, // CJK Compatibility Forms, Small Form Variants
		{0xFF00, 0xFF60, 1}, // Fullwidth Forms
		{0xFFE0, 0xFFE6, 1}, // Fullwidth signs
	},
	R32: []unicode.Range32{
		{0x1F300, 0x1F64F, 1}, // Misc Symbols and Pictographs, Emoticons
		{0x1F900, 0x1F9FF, 1}, // Supplemental Symbols and Pictographs
		{0x1FA70, 0x1FAFF, 1}, // Symbols and Pictographs Extended-A
		{0x20000, 0x2FFFD, 1}, // CJK Unified Ideographs Extension B-F
		{0x30000, 0x3FFFD, 1}, // CJK Unified Ideographs Extension G
	},
	LatinOffset: 0, // No R16 entry is in the Latin-1 range.
}
