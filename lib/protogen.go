//go:build ignore

package main

import (
	"fmt"
	"go/format"
	"log"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

type token struct {
	typ string
	val string
}

func tokenize(src string) []token {
	result := []token{}
	for !strings.HasPrefix(src, "option ") {
		switch {
		case slices.Contains([]byte{'[', ']', '{', '}', '(', ')', ';', ':', ',', '='}, src[0]):
			result = append(result, token{src[0:1], ""})
			src = src[1:]
		case slices.Contains([]byte{' ', '\n', '\r', '\t'}, src[0]):
			src = src[1:]
		case src[0] == '"':
			end := strings.IndexByte(src[1:], '"')
			result = append(result, token{"string", src[1 : end+1]})
			src = src[end+2:]
		case src[0:2] == "//":
			end := strings.IndexByte(src, '\n')
			src = src[end+1:]
		case unicode.IsLetter(rune(src[0])):
			var value strings.Builder
			for unicode.IsLetter(rune(src[0])) || unicode.IsDigit(rune(src[0])) || src[0] == '_' || src[0] == '.' {
				value.WriteString(string(src[0]))
				src = src[1:]
			}
			result = append(result, token{"word", value.String()})
		case unicode.IsDigit(rune(src[0])):
			var value strings.Builder
			for unicode.IsDigit(rune(src[0])) || src[0] == 'x' || src[0] >= 'A' && src[0] <= 'F' {
				value.WriteString(string(src[0]))
				src = src[1:]
			}
			result = append(result, token{"number", value.String()})
		default:
			panic(fmt.Errorf("failed to parse at: %s", src[0:20]))
		}
	}
	return result
}

type generator struct {
	tokens       []token
	sb           strings.Builder
	messages     []string
	enums        []string
	enumVariants map[string][]string
	pkg          string
	libPrefix    string
}

func newGenerator(tokens []token, pkg string) *generator {
	libPrefix := ""
	if pkg != "lib" {
		libPrefix = "lib."
	}
	return &generator{
		tokens:       tokens,
		enumVariants: map[string][]string{},
		pkg:          pkg,
		libPrefix:    libPrefix,
	}
}

func (g *generator) write(format string, args ...any) {
	fmt.Fprintf(&g.sb, format+"\n", args...)
}

func (g *generator) peek() string {
	return g.tokens[0].typ
}

func (g *generator) next() token {
	if len(g.tokens) == 0 {
		panic("no more tokens")
	}
	token := g.tokens[0]
	g.tokens = g.tokens[1:]
	return token
}

func (g *generator) expectNum() int64 {
	v := g.expect("number")
	base := 10
	if strings.HasPrefix(v, "0x") {
		v = v[2:]
		base = 16
	}
	result, err := strconv.ParseInt(v, base, 64)
	if err != nil {
		panic(err)
	}
	return result
}

func (g *generator) expect(typ string) string {
	if g.tokens[0].typ != typ {
		panic(fmt.Errorf("expected %q, got %s", typ, g.tokens[0]))
	}
	return g.next().val
}

func (g *generator) genEnum() {
	name := g.expect("word")
	g.enums = append(g.enums, name)
	g.write("type %s uint32\n", name)
	g.write("const (")
	g.expect("{")
	for g.peek() != "}" {
		variant := snakeToPascal(g.expect("word"))
		g.expect("=")
		value := g.expect("number")
		if variant != "FileModeEmpty" {
			// FileModeEmpty is only there to please `protoc`, which we obviously not use but
			// we still want the protobuf declarations be compatible with the protobuf standard.
			g.write("%s %s = %s", variant, name, value)
			g.enumVariants[name] = append(g.enumVariants[name], variant)
		}
		g.expect(";")
	}
	g.write(")\n")
	g.expect("}")
}

type constraints struct {
	typ          string
	length       int64
	max_length   int64
	required     string
	inner_typ    string
	inner_length int64
	bitmask      bool
}

func (g *generator) parseConstraints() constraints {
	g.expect("[")
	g.expect("(")
	name := g.expect("word")
	if name != "cling" && name != "lib.cling" {
		panic(fmt.Errorf("unknown constraint option: %s", name))
	}
	g.expect(")")
	g.expect("=")
	g.expect("{")
	c := constraints{}
	for g.peek() != "}" {
		name := g.expect("word")
		g.expect(":")
		switch name {
		case "type":
			c.typ = g.expect("string")
		case "inner_type":
			c.inner_typ = g.expect("string")
		case "length":
			c.length = g.expectNum()
		case "max_length":
			c.max_length = g.expectNum()
		case "inner_length":
			c.inner_length = g.expectNum()
		case "required":
			c.required = g.expect("string")
		case "bitmask":
			c.bitmask = g.expect("word") == "true"
		}
		if g.peek() == "," {
			g.next()
		}
	}
	g.expect("}")
	g.expect("]")
	return c
}

func (g *generator) parseTyp() (string, bool) {
	t := g.expect("word")
	switch t {
	case "repeated":
		t, _ := g.parseTyp()
		return t, true
	}
	return t, false
}

type field struct {
	name        string
	protoTyp    string
	repeated    bool
	tag         int64
	constraints constraints
}

func (g *generator) goTyp(f field) string {
	typ := f.protoTyp
	if typ == "bytes" {
		typ = "[]byte"
	}
	if f.constraints.typ != "" {
		typ = f.constraints.typ
	}
	if f.constraints.inner_typ != "" {
		typ = f.constraints.inner_typ
	}
	if f.repeated {
		if g.isMessage(f.protoTyp) {
			typ = "[]*" + typ
		} else {
			typ = "[]" + typ
		}
	}
	if f.constraints.required != "" {
		typ = "*" + typ
	}
	return typ
}

func qualifiedFunc(prefix, typ string) string {
	if i := strings.LastIndex(typ, "."); i >= 0 {
		return typ[:i+1] + prefix + typ[i+1:]
	}
	return prefix + typ
}

func (g *generator) genFieldValidate(structName string, f field) {
	errorf := g.libPrefix + "Errorf"
	if f.constraints.length != 0 && f.constraints.typ == "" {
		g.write(
			"if len(o.%[1]s) != %[2]d { return %[4]s(\"%[3]s.%[1]s must have length %[2]d\")}",
			f.name,
			f.constraints.length,
			structName,
			errorf,
		)
	}
	if f.constraints.max_length != 0 {
		g.write(
			"if len(o.%[1]s) > %[2]d { return %[4]s(\"%[3]s.%[1]s must not be longer than %[2]d\")}",
			f.name,
			f.constraints.max_length,
			structName,
			errorf,
		)
	}
	if f.constraints.required != "" && f.constraints.required != "false" {
		g.write(
			"if o.%[1]s == nil && %[2]s { return %[4]s(\"%[3]s.%[1]s must be set\")}",
			f.name,
			strings.ReplaceAll(f.constraints.required, "this.", "o."),
			structName,
			errorf,
		)
	}
	if f.constraints.inner_length != 0 && f.constraints.inner_typ == "" {
		g.write("for _, v := range o.%s {", f.name)
		g.write("if len(v) != %[1]d { return %[4]s(\"every entry in %[2]s.%[3]s must have length %[1]d\")}",
			f.constraints.inner_length,
			structName,
			f.name,
			errorf,
		)
		g.write("}")
	}
	if variants := g.enumVariants[f.protoTyp]; len(variants) > 0 && !f.constraints.bitmask {
		g.write("switch o.%s {", f.name)
		g.write("case %s:", strings.Join(variants, ", "))
		g.write("default: return %[3]s(\"%[1]s.%[2]s has invalid value %%d\", o.%[2]s)",
			structName, f.name, errorf)
		g.write("}")
	}
}

func (g *generator) isMessage(typ string) bool {
	return slices.Contains(g.messages, typ) || strings.Contains(typ, ".")
}

func (g *generator) isEnum(typ string) bool {
	return slices.Contains(g.enums, typ)
}

// wireTypeFor returns the protobuf wire type a field's tag must carry on
// the wire. 0 = varint, 2 = length-delimited.
func (g *generator) wireTypeFor(f field) int {
	switch f.protoTyp {
	case "string", "bytes":
		return 2
	case "uint32", "uint64", "int64":
		return 0
	}
	if g.isMessage(f.protoTyp) {
		return 2
	}
	if g.isEnum(f.protoTyp) {
		return 0
	}
	panic(fmt.Errorf("unknown type for wire type: %s", f.protoTyp))
}

func (g *generator) genUnmarshall(structName string, fields []field) {
	assign := func(f field, expr string) {
		switch {
		case f.constraints.required != "":
			g.write("o.%s = &%s", f.name, expr)
		case f.repeated:
			g.write("o.%[1]s = append(o.%[1]s, %s)", f.name, expr)
		default:
			g.write("o.%s = %s", f.name, expr)
		}
	}
	read := func(local, call string) {
		g.write("%s, err := %s", local, call)
		g.write("if err != nil { return nil, err }")
	}
	readExpr := func(f field) string {
		switch f.protoTyp {
		case "string":
			read("b", "r.ReadBytes()")
			if f.constraints.typ != "" {
				// Custom string-backed type: call `New<Type>(string)` which returns `(<Type>, error)`.
				read("pv", fmt.Sprintf("%s(string(b))", qualifiedFunc("New", f.constraints.typ)))
				return "pv"
			}
			return "string(b)"
		case "uint32":
			read("u", "r.ReadUint32()")
			return "u"
		case "int64":
			read("i", "r.ReadVarint()")
			return "i"
		case "uint64":
			read("u", "r.ReadUint64()")
			return "u"
		case "bytes":
			cast, length := f.constraints.typ, f.constraints.length
			if f.repeated {
				cast, length = f.constraints.inner_typ, f.constraints.inner_length
			}
			read("b", "r.ReadBytes()")
			if length != 0 {
				label := fmt.Sprintf("%s.%s", structName, f.name)
				if f.repeated {
					label = "every entry in " + label
				}
				g.write("if len(b) != %d { return nil, %s(\"%s must have length %d\") }",
					length, g.libPrefix+"Errorf", label, length)
			}
			if cast == "" {
				return "b"
			}
			return fmt.Sprintf("%s(b)", cast)
		}
		if g.isEnum(f.protoTyp) {
			read("u", "r.ReadUint32()")
			return fmt.Sprintf("%s(u)", f.protoTyp)
		}
		panic(fmt.Errorf("unknown type: %s", f.protoTyp))
	}
	g.write("for !r.AtEnd() {")
	g.write("tag, wireType, err := r.ReadTag()")
	g.write("if err != nil { return nil, err }")
	g.write("switch tag {")
	for _, f := range fields {
		g.write("case %d:", f.tag)
		g.write(
			"if wireType != %d { return nil, %s(\"%s.%s: unexpected wire type %%d, want %d\", wireType) }",
			g.wireTypeFor(f), g.libPrefix+"Errorf", structName, f.name, g.wireTypeFor(f),
		)
		if g.isMessage(f.protoTyp) {
			read("b", "r.ReadBytes()")
			g.write("v, err := %s(%sNewProtobufReader(b))",
				qualifiedFunc("Unmarshall", f.protoTyp), g.libPrefix)
			g.write("if err != nil { return nil, err }")
			switch {
			case f.constraints.required != "":
				g.write("o.%s = v", f.name)
			case f.repeated:
				g.write("o.%[1]s = append(o.%[1]s, v)", f.name)
			default:
				g.write("o.%s = *v", f.name)
			}
			continue
		}
		expr := readExpr(f)
		if f.constraints.required != "" {
			g.write("v := %s", expr)
			g.write("o.%s = &v", f.name)
		} else {
			assign(f, expr)
		}
	}
	g.write("default:")
	g.write("if err := r.Skip(wireType); err != nil { return nil, err }")
	g.write("}")
	g.write("}")
}

func (g *generator) genFieldMarshall(f field) {
	write := func(typ string, variable string) {
		if f.constraints.required != "" {
			g.write("if %s != nil {", variable)
			variable = fmt.Sprintf("(*%s)", variable)
		}
		checkErr := func(call string) {
			g.write("if err := %s; err != nil { return err }", call)
		}
		varint := func(expr string) {
			checkErr(fmt.Sprintf("w.WriteTag(%d, 0)", f.tag))
			checkErr(fmt.Sprintf("w.WriteVarint(%s)", expr))
		}
		switch typ {
		case "string":
			expr := variable
			if f.constraints.typ != "" {
				// Custom string-backed type: marshal via its `String()` method.
				expr = fmt.Sprintf("%s.String()", variable)
			}
			checkErr(fmt.Sprintf("w.WriteBytes(%d, []byte(%s))", f.tag, expr))
		case "uint32":
			varint(fmt.Sprintf("int64(%s)", variable))
		case "int64":
			varint(variable)
		case "uint64":
			checkErr(fmt.Sprintf("w.WriteUint64(%d, %s)", f.tag, variable))
		case "bytes":
			checkErr(fmt.Sprintf("w.WriteBytes(%d, %s[:])", f.tag, variable))
		default:
			if g.isMessage(typ) {
				checkErr(fmt.Sprintf("w.WriteMessage(%d, %s.Marshall)", f.tag, variable))
			} else if g.isEnum(typ) {
				varint(fmt.Sprintf("int64(%s)", variable))
			} else {
				panic(fmt.Errorf("unknown type: %s", typ))
			}
		}
		if f.constraints.required != "" {
			g.write("}")
		}
	}
	if f.repeated {
		g.write("for _, v := range o.%s {", f.name)
		write(f.protoTyp, "v")
		g.write("}")
		return
	}
	write(f.protoTyp, fmt.Sprintf("o.%s", f.name))
}

func (g *generator) genMessage() {
	structName := g.expect("word")
	g.messages = append(g.messages, structName)
	g.expect("{")
	fields := []field{}
	for g.peek() != "}" {
		fieldTyp, fieldRepeated := g.parseTyp()
		fieldName := snakeToPascal(g.expect("word"))
		g.expect("=")
		fieldTag := g.expectNum()
		var constr constraints
		if g.peek() == "[" {
			constr = g.parseConstraints()
		}
		g.expect(";")
		fields = append(fields, field{fieldName, fieldTyp, fieldRepeated, fieldTag, constr})
	}
	g.expect("}")

	// Write struct.
	g.write("type %s struct {", structName)
	for _, field := range fields {
		g.write("%s %s", field.name, g.goTyp(field))
	}
	g.write("}\n")

	// Write validation function.
	g.write("func (o *%s) Validate() error {", structName)
	for _, field := range fields {
		g.genFieldValidate(structName, field)
	}
	g.write("return nil")
	g.write("}\n")

	// Write marshal function.
	g.write("func (o *%s) Marshall(w %sProtobufWriter) error {", structName, g.libPrefix)
	g.write("if err := o.Validate(); err != nil { return err }")
	for _, field := range fields {
		g.genFieldMarshall(field)
	}
	g.write("return nil")
	g.write("}\n")

	// Write marshalled-size function.
	g.write("func (o *%s) MarshallSize() int {", structName)
	g.write("sw := %sNewProtobufSizeWriter()", g.libPrefix)
	g.write("_ = o.Marshall(sw)")
	g.write("return sw.Size()")
	g.write("}\n")

	// Write unmarshal function.
	g.write("func Unmarshall%[1]s(r * %[2]sProtobufReader) (*%[1]s, error) {", structName, g.libPrefix)
	g.write("o := &%s{}", structName)
	g.genUnmarshall(structName, fields)
	g.write("if err := o.Validate(); err != nil { return nil, err }")
	g.write("return o, nil")
	g.write("}\n")
}

func (g *generator) gen() string {
	g.write("// Generated from format.proto - DO NOT EDIT\n")
	g.write("//nolint:gocritic,exhaustruct,funlen,wrapcheck,nolintlint")
	g.write("package %s\n", g.pkg)
	if g.pkg != "lib" {
		g.write("import \"github.com/flunderpero/cling-sync/lib\"\n")
	}
	for len(g.tokens) > 0 {
		switch g.expect("word") {
		case "syntax":
			g.expect("=")
			g.expect("string")
			g.expect(";")
		case "package":
			g.expect("word")
			g.expect(";")
		case "import":
			g.expect("string")
			g.expect(";")
		case "enum":
			g.genEnum()
		case "message":
			g.genMessage()
		default:
			panic(fmt.Errorf("unexpected token: %s", g.tokens[0]))
		}
	}
	return g.sb.String()
}

func snakeToPascal(s string) string {
	parts := strings.Split(s, "_")
	var sb strings.Builder
	for _, p := range parts {
		sb.WriteRune(unicode.ToUpper(rune(p[0])))
		sb.WriteString(p[1:])
	}
	return sb.String()
}

// goPackageName extracts the Go package name from the proto's
// `option go_package = "<path>";` directive.
func goPackageName(src string) string {
	re := regexp.MustCompile(`option\s+go_package\s*=\s*"([^"]+)"`)
	m := re.FindStringSubmatch(src)
	if len(m) < 2 {
		log.Fatal("format.proto must declare `option go_package = \"...\"`")
	}
	return path.Base(m[1])
}

func main() {
	src, err := os.ReadFile("format.proto")
	if err != nil {
		log.Fatal(err)
	}
	pkg := goPackageName(string(src))
	tokens := tokenize(string(src))
	code := newGenerator(tokens, pkg).gen()
	formatted, err := format.Source([]byte(code))
	if err != nil {
		log.Fatal("error formatting generated code: ", err.Error(), "\n", code)
	}
	if err := os.WriteFile("format.go", formatted, 0o600); err != nil {
		log.Fatal("error writing output file:", err.Error())
	}
}
