//go:build !wasm

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

type Assert struct {
	tb testing.TB
}

func NewAssert(tb testing.TB) Assert {
	tb.Helper()
	return Assert{tb: tb}
}

func areEqual(expected, actual any) bool {
	if expected == nil || actual == nil {
		return expected != actual
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
	expectedStr := stringify(expected)
	if strings.Contains(expectedStr, "\n") {
		expectedStr = "\n" + expectedStr
	}
	actualStr := stringify(actual)
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
	expectedStr := stringify(expected)
	if strings.Contains(expectedStr, "\n") {
		expectedStr = "\n" + expectedStr
	}
	actualStr := stringify(actual)
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

func stringify(v any) string {
	return stringifyInternal(v, 0)
}

func stringifyInternal(v any, indent int) string { //nolint:funlen
	if t, ok := v.(time.Time); ok {
		return t.Format(time.RFC3339Nano)
	}
	if t, ok := v.(string); ok {
		return t
	}
	reflectV := reflect.ValueOf(v)
	t := reflectV.Type()
	kind := t.Kind()
	// Check for byte array or byte slice (including aliases like [32]byte or Sha256).
	if (kind == reflect.Slice || kind == reflect.Array) && t.Elem().Kind() == reflect.Uint8 {
		n := reflectV.Len()
		if n == 0 {
			return fmt.Sprintf("%s{}", t.String())
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("%s{ ", t.String()))
		for i := range min(n, 500) {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(fmt.Sprintf("%02x", reflectV.Index(i).Uint()))
		}
		if n > 500 {
			sb.WriteString(fmt.Sprintf(" ... %d more bytes }", n-500))
		}
		sb.WriteString(" }")
		return sb.String()
	}

	if reflectV.Kind() == reflect.Ptr {
		if reflectV.IsNil() {
			return "nil"
		}
		reflectV = reflectV.Elem()
		t = reflectV.Type()
		kind = t.Kind()
	}

	switch kind { //nolint:exhaustive
	case reflect.Slice, reflect.Array:
		if reflectV.Len() == 0 {
			return "[]"
		}
		parts := make([]string, reflectV.Len())
		for i := range reflectV.Len() {
			parts[i] = stringifyInternal(reflectV.Index(i).Interface(), indent+1)
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
		numFields := reflectV.NumField()
		parts := make([]string, 0, numFields)
		for i := range numFields {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue // unexported
			}
			valStr := stringifyInternal(reflectV.Field(i).Interface(), indent+1)
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
		return fmt.Sprintf("%q", reflectV.String())

	default:
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

func (a Assert) Nil(v interface{}, msg ...any) {
	a.tb.Helper()
	if v == nil {
		return
	}
	if reflect.ValueOf(v).IsNil() {
		return
	}
	a.tb.Fatalf("%sexpected nil, got %v (%T)", details(msg), v, v)
}

func (a Assert) NoError(err error, msg ...any) {
	a.tb.Helper()
	if err != nil {
		a.tb.Fatalf("%sexpected no error, got %v", details(msg), err)
	}
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
