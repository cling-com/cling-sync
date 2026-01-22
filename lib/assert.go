package lib

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var justAny = &struct{}{} //nolint:gochecknoglobals

type Assert struct {
	tb  testing.TB
	Any any
}

func NewAssert(tb testing.TB) Assert {
	tb.Helper()
	return Assert{tb: tb, Any: justAny}
}

func areEqual(expected, actual any) bool {
	if expected == justAny || actual == justAny {
		return true
	}
	if expected == nil || actual == nil {
		return expected != actual
	}
	expectedMockCall, ok := expected.(MockCall)
	if ok {
		actualMockCall, ok := actual.(MockCall)
		if !ok {
			return false
		}
		return expectedMockCall.Equal(actualMockCall)
	}
	expectedBytes, ok := expected.([]byte)
	if ok {
		actualBytes, ok := actual.([]byte)
		if !ok {
			return false
		}
		return bytes.Equal(expectedBytes, actualBytes)
	}
	if expectedTime, ok := expected.(time.Time); ok {
		actualTime, ok := actual.(time.Time)
		if !ok {
			return false
		}
		return expectedTime.Equal(actualTime)
	}
	return reflect.DeepEqual(expected, actual)
}

func (a Assert) Equal(expected, actual any, msg ...any) {
	a.tb.Helper()
	if areEqual(expected, actual) {
		return
	}
	expectedStr := Stringify(expected)
	if strings.Contains(expectedStr, "\n") {
		expectedStr = "\n" + expectedStr
	}
	actualStr := Stringify(actual)
	if strings.Contains(actualStr, "\n") {
		actualStr = "\n" + actualStr
	}
	expectedStr, actualStr = diff(expectedStr, actualStr)
	a.tb.Fatalf(
		"%sexpected: %v (%T), got: %v (%T)",
		details(msg),
		expectedStr,
		expected,
		actualStr,
		actual,
	)
}

func (a Assert) NotEqual(expected, actual any, msg ...any) {
	a.tb.Helper()
	if !areEqual(expected, actual) {
		return
	}
	expectedStr := Stringify(expected)
	if strings.Contains(expectedStr, "\n") {
		expectedStr = "\n" + expectedStr
	}
	actualStr := Stringify(actual)
	if strings.Contains(actualStr, "\n") {
		actualStr = "\n" + actualStr
	}
	expectedStr, actualStr = diff(expectedStr, actualStr)
	a.tb.Fatalf(
		"%sexpected: %v (%T) not to equal: %v (%T)",
		details(msg),
		expectedStr,
		expected,
		actualStr,
		actual,
	)
}

type MockCall struct {
	Name string
	Args []any
}

func NewMockCall(name string, args ...any) MockCall {
	return MockCall{Name: name, Args: args}
}

func (m MockCall) String() string {
	var s strings.Builder
	s.WriteString(m.Name + "(")
	for i, arg := range m.Args {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(Stringify(arg))
	}
	s.WriteString(")")
	return s.String()
}

func (m MockCall) Equal(other MockCall) bool {
	if m.Name != other.Name {
		return false
	}
	if len(m.Args) != len(other.Args) {
		return false
	}
	for i, arg := range m.Args {
		if !areEqual(arg, other.Args[i]) {
			return false
		}
	}
	return true
}

// Make sure at least one to the given function is found.
func (a Assert) Call(expected MockCall, calls []MockCall, msg ...any) {
	a.tb.Helper()
	if slices.ContainsFunc(calls, expected.Equal) {
		return
	}
	var callsStr strings.Builder
	for _, call := range calls {
		callsStr.WriteString(call.String() + "\n")
	}
	a.tb.Fatalf("%sexpected call:\nwant: %s\ngot:\n%s", details(msg), expected, callsStr.String())
}

func (a Assert) Calls(expected []MockCall, calls []MockCall, msg ...any) {
	a.tb.Helper()
	var callsStr strings.Builder
	for _, call := range calls {
		callsStr.WriteString(call.String() + "\n")
	}
	if len(expected) != len(calls) {
		a.tb.Fatalf("%sexpected %d calls, got %d\n%s", details(msg), len(expected), len(calls), callsStr.String())
	}
	for i, call := range calls {
		a.Equal(expected[i], call, msg...)
	}
}

// Just mark different lines.
func diff(a, b string) (string, string) {
	if a == b {
		return a, b
	}
	aLines := strings.Split(a, "\n")
	bLines := strings.Split(b, "\n")
	for i := 0; i < len(aLines) && i < len(bLines); i++ {
		if aLines[i] != bLines[i] {
			aLines[i] = "\033[32m" + aLines[i] + "\033[0m"
			bLines[i] = "\033[31m" + bLines[i] + "\033[0m"
		}
	}
	// Join the lines back together.
	a = strings.Join(aLines, "\n")
	b = strings.Join(bLines, "\n")
	return a, b
}

func Stringify(v any) string {
	return stringifyInternal(v, 0)
}

func stringifyInternal(v any, indent int) string {
	if t, ok := v.(time.Time); ok {
		return t.Format(time.RFC3339Nano)
	}
	if t, ok := v.(string); ok {
		return t
	}
	return stringifyValue(reflect.ValueOf(v), indent)
}

func stringifyValue(v reflect.Value, indent int) string { //nolint:funlen
	if !v.IsValid() {
		return "nil"
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return "nil"
		}
		v = v.Elem()
		if !v.IsValid() {
			return "nil"
		}
	}

	t := v.Type()
	kind := t.Kind()
	// Check for byte array or byte slice (including aliases like [32]byte or Sha256).
	if (kind == reflect.Slice || kind == reflect.Array) && t.Elem().Kind() == reflect.Uint8 {
		n := v.Len()
		if n == 0 {
			return fmt.Sprintf("%s{}", t.String())
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s{ ", t.String())
		for i := range min(n, 500) {
			if i > 0 {
				sb.WriteString(" ")
			}
			fmt.Fprintf(&sb, "%02x", v.Index(i).Uint())
		}
		if n > 500 {
			fmt.Fprintf(&sb, " ... %d more bytes }", n-500)
		}
		sb.WriteString(" }")
		return sb.String()
	}

	if kind == reflect.Pointer {
		if v.IsNil() {
			return "nil"
		}
		return "&" + stringifyValue(v.Elem(), indent)
	}

	switch kind { //nolint:exhaustive
	case reflect.Slice, reflect.Array:
		if v.IsNil() {
			return "nil"
		}
		if v.Len() == 0 {
			return "[]"
		}
		n := v.Len()
		parts := make([]string, n)
		for i := range n {
			parts[i] = stringifyValue(v.Index(i), indent+1)
		}
		inline := "[ " + strings.Join(parts, ", ") + " ]"
		if len(inline)+(indent*2) <= 100 {
			return inline
		}
		var sb strings.Builder
		sb.WriteString("[\n")
		for i, part := range parts {
			sb.WriteString(strings.Repeat("  ", indent+1))
			sb.WriteString(part)
			if i < len(parts)-1 {
				sb.WriteString(",\n")
			} else {
				sb.WriteString("\n")
			}
		}
		sb.WriteString(strings.Repeat("  ", indent))
		sb.WriteString("]")
		return sb.String()

	case reflect.Struct:
		numFields := v.NumField()
		parts := make([]string, 0, numFields)

		for i := range numFields {
			field := t.Field(i)
			valStr := stringifyValue(v.Field(i), indent+1)
			parts = append(parts, fmt.Sprintf("%s: %s", field.Name, valStr))
		}
		prefix := t.Name() + "{ "
		inline := prefix + strings.Join(parts, ", ") + " }"
		if len(inline)+(indent*2) <= 100 {
			return inline
		}
		var sb strings.Builder
		sb.WriteString(t.Name())
		sb.WriteString("{\n")
		for _, part := range parts {
			sb.WriteString(strings.Repeat("  ", indent+1))
			sb.WriteString(part)
			sb.WriteString(",\n")
		}
		sb.WriteString(strings.Repeat("  ", indent))
		sb.WriteString("}")
		return sb.String()

	case reflect.String:
		return fmt.Sprintf("%q", v.String())

	default:
		if v.CanInterface() {
			return fmt.Sprintf("%v", v.Interface())
		}
		return fmt.Sprintf("%v", v)
	}
}

func (a Assert) Greater(x, y any, msg ...any) {
	a.tb.Helper()
	a.compare(x, y, func(xv, yv reflect.Value) bool {
		switch xv.Kind() { //nolint:exhaustive
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return xv.Int() > yv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return xv.Uint() > yv.Uint()
		case reflect.Float32, reflect.Float64:
			return xv.Float() > yv.Float()
		case reflect.String:
			return xv.String() > yv.String()
		default:
			return false
		}
	}, msg...)
}

func (a Assert) Less(x, y any, msg ...any) {
	a.tb.Helper()
	a.compare(x, y, func(xv, yv reflect.Value) bool {
		switch xv.Kind() { //nolint:exhaustive
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return xv.Int() < yv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return xv.Uint() < yv.Uint()
		case reflect.Float32, reflect.Float64:
			return xv.Float() < yv.Float()
		case reflect.String:
			return xv.String() < yv.String()
		default:
			return false
		}
	}, msg...)
}

func (a Assert) Error(err error, contains string, msg ...any) {
	a.tb.Helper()
	if err == nil {
		a.tb.Fatalf("%sexpected error, got nil", details(msg))
	}
	if contains != "" && !strings.Contains(err.Error(), contains) {
		a.tb.Fatalf("%sexpected error containing %q, got %v", details(msg), contains, err)
	}
}

func (a Assert) ErrorIs(err, target error, msg ...any) {
	a.tb.Helper()
	if err == nil {
		a.tb.Fatalf("%sexpected error, got nil", details(msg))
	}
	if !errors.Is(err, target) {
		a.tb.Fatalf("%sexpected error %v, got %v", details(msg), target, err)
	}
}

func (a Assert) Contains(haystack any, needle any, msg ...any) {
	if haystack == nil {
		a.tb.Fatalf("%sexpected non-nil haystack, got nil", details(msg))
	}
	if arr, ok := haystack.([]any); ok {
		if slices.Contains(arr, needle) {
			return
		}
		a.tb.Fatalf("%sexpected %v in %v", details(msg), needle, arr)
	}
	if str, ok := haystack.(string); ok {
		needleStr, ok := needle.(string)
		if !ok {
			a.tb.Fatalf("%sexpected needle to a string, got %T", details(msg), needle)
		}
		if strings.Contains(str, needleStr) {
			return
		}
		a.tb.Fatalf("%sexpected %q in %q", details(msg), needle, str)
	}
}

func (a Assert) Nil(v any, msg ...any) {
	a.tb.Helper()
	if v == nil {
		return
	}
	if reflect.ValueOf(v).IsNil() {
		return
	}
	a.tb.Fatalf("%sexpected nil, got %v (%T)", details(msg), v, v)
}

func (a Assert) NotNil(v any, msg ...any) {
	a.tb.Helper()
	if v != nil && !reflect.ValueOf(v).IsNil() {
		return
	}
	a.tb.Fatalf("%sexpected non-nil, got %v (%T)", details(msg), v, v)
}

func (a Assert) NoError(err error, msg ...any) {
	a.tb.Helper()
	if err == nil {
		return
	}
	if reflect.ValueOf(err).IsNil() {
		return
	}
	a.tb.Fatalf("%sexpected no error, got %v", details(msg), err)
}

func (a Assert) compare(x, y any, compare func(x, y reflect.Value) bool, msg ...any) {
	a.tb.Helper()
	if x == nil || y == nil {
		a.tb.Fatalf("%snil values cannot be compared: %v and %v", details(msg), x, y)
	}
	xv := reflect.ValueOf(x)
	yv := reflect.ValueOf(y)
	if xv.Kind() != yv.Kind() {
		a.tb.Fatalf("%sexpected same type kind, got %T and %T", details(msg), x, y)
	}
	if !compare(xv, yv) {
		a.tb.Fatalf("%sexpected %v (%T) > %v (%T)", details(msg), x, x, y, y)
	}
}

func details(msg []any) string {
	if len(msg) == 0 {
		return ""
	}
	if len(msg) == 1 {
		return fmt.Sprintf("%v: ", msg[0])
	}
	return fmt.Sprintf(msg[0].(string), msg[1:]...) + ": " //nolint:forcetypeassert
}
