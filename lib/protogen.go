//go:build ignore

package main

import (
	"fmt"
	"go/format"
	"log"
	"os"
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
			for unicode.IsLetter(rune(src[0])) || unicode.IsDigit(rune(src[0])) || src[0] == '_' {
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
}

func newGenerator(tokens []token) *generator {
	return &generator{tokens: tokens, enumVariants: map[string][]string{}}
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

func (g *generator) expectVal(typ string, val string) string {
	if g.tokens[0].typ != typ {
		panic(fmt.Errorf("expected %q, got %s", typ, g.tokens[0]))
	}
	if g.tokens[0].val != val {
		panic(fmt.Errorf("expected %q, got %q", val, g.tokens[0].val))
	}
	return g.next().val
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
	g.expectVal("word", "cling")
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

func (f field) goTyp() string {
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
		typ = "[]" + typ
	}
	if f.constraints.required != "" {
		typ = "*" + typ
	}
	return typ
}

func (g *generator) genFieldValidate(structName string, f field) {
	if f.constraints.length != 0 && f.constraints.typ == "" {
		g.write(
			"if len(o.%[1]s) != %[2]d { return Errorf(\"%[3]s.%[1]s must have length %[2]d\")}",
			f.name,
			f.constraints.length,
			structName,
		)
	}
	if f.constraints.max_length != 0 {
		g.write(
			"if len(o.%[1]s) > %[2]d { return Errorf(\"%[3]s.%[1]s must not be longer than %[2]d\")}",
			f.name,
			f.constraints.max_length,
			structName,
		)
	}
	if f.constraints.required != "" && f.constraints.required != "false" {
		g.write(
			"if o.%[1]s == nil && %[2]s { return Errorf(\"%[3]s.%[1]s must be set\")}",
			f.name,
			strings.ReplaceAll(f.constraints.required, "this.", "o."),
			structName,
		)
	}
	if f.constraints.inner_length != 0 && f.constraints.inner_typ == "" {
		g.write("for _, v := range o.%s {", f.name)
		g.write("if len(v) != %[1]d { return Errorf(\"every entry in %[2]s.%[3]s must have length %[1]d\")}",
			f.constraints.inner_length,
			structName,
			f.name,
		)
		g.write("}")
	}
	if variants := g.enumVariants[f.protoTyp]; len(variants) > 0 && !f.constraints.bitmask {
		g.write("switch o.%s {", f.name)
		g.write("case %s:", strings.Join(variants, ", "))
		g.write("default: return Errorf(\"%[1]s.%[2]s has invalid value %%d\", o.%[2]s)", structName, f.name)
		g.write("}")
	}
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
		g.write("if err != nil { return %s{}, err }", structName)
	}
	readExpr := func(f field) string {
		switch f.protoTyp {
		case "string":
			read("b", "r.ReadBytes()")
			if f.constraints.typ != "" {
				// Custom string-backed type: call `New<Type>(string)` which returns `(<Type>, error)`.
				read("pv", fmt.Sprintf("New%s(string(b))", f.constraints.typ))
				return "pv"
			}
			return "string(b)"
		case "uint32":
			read("u", "r.ReadUint32()")
			return "u"
		case "int64":
			read("i", "r.ReadVarint()")
			return "i"
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
				g.write("if len(b) != %d { return %s{}, Errorf(\"%s must have length %d\") }",
					length, structName, label, length)
			}
			if cast == "" {
				return "b"
			}
			return fmt.Sprintf("%s(b)", cast)
		}
		if slices.Contains(g.enums, f.protoTyp) {
			read("u", "r.ReadUint32()")
			return fmt.Sprintf("%s(u)", f.protoTyp)
		}
		panic(fmt.Errorf("unknown type: %s", f.protoTyp))
	}
	g.write("for !r.AtEnd() {")
	g.write("tag, wireType, err := r.ReadTag()")
	g.write("if err != nil { return %s{}, err }", structName)
	g.write("switch tag {")
	for _, f := range fields {
		g.write("case %d:", f.tag)
		if slices.Contains(g.messages, f.protoTyp) {
			read("b", "r.ReadBytes()")
			g.write("v, err := Unmarshall%s(NewProtobufReader(b))", f.protoTyp)
			g.write("if err != nil { return %s{}, err }", structName)
			assign(f, "v")
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
	g.write("if err := r.Skip(wireType); err != nil { return %s{}, err }", structName)
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
		case "bytes":
			checkErr(fmt.Sprintf("w.WriteBytes(%d, %s[:])", f.tag, variable))
		default:
			if slices.Contains(g.messages, typ) {
				checkErr(fmt.Sprintf("w.WriteMessage(%d, %s.Marshall)", f.tag, variable))
			} else if slices.Contains(g.enums, typ) {
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
		g.write("%s %s", field.name, field.goTyp())
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
	g.write("func (o *%s) Marshall(w ProtobufWriter) error {", structName)
	g.write("if err := o.Validate(); err != nil { return err }")
	for _, field := range fields {
		g.genFieldMarshall(field)
	}
	g.write("return nil")
	g.write("}\n")

	// Write marshalled-size function.
	g.write("func (o *%s) MarshallSize() int {", structName)
	g.write("sw := NewProtobufSizeWriter()")
	g.write("_ = o.Marshall(sw)")
	g.write("return sw.Size()")
	g.write("}\n")

	// Write unmarshal function.
	g.write("func Unmarshall%[1]s(r * ProtobufReader) (%[1]s, error) {", structName)
	g.write("o := %s{}", structName)
	g.genUnmarshall(structName, fields)
	g.write("if err := o.Validate(); err != nil { return %s{}, err }", structName)
	g.write("return o, nil")
	g.write("}\n")
}

func (g *generator) gen() string {
	g.write("// Generated from format.proto - DO NOT EDIT\n")
	g.write("//nolint:gocritic,exhaustruct,funlen,wrapcheck")
	g.write("package lib\n")
	for len(g.tokens) > 0 {
		switch g.expect("word") {
		case "syntax":
			// Ignore
			g.expect("=")
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

func main() {
	src, err := os.ReadFile("format.proto")
	if err != nil {
		log.Fatal(err)
	}
	tokens := tokenize(string(src))
	code := newGenerator(tokens).gen()
	formatted, err := format.Source([]byte(code))
	if err != nil {
		log.Fatal("error formatting generated code: ", err.Error(), "\n", code)
	}
	if err := os.WriteFile("format.go", formatted, 0o600); err != nil {
		log.Fatal("error writing output file:", err.Error())
	}
}
