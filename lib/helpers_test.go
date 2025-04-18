package lib

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type Assert struct {
	tb testing.TB
}

func NewAssert(tb testing.TB) Assert {
	tb.Helper()
	return Assert{tb: tb}
}

func (a Assert) Equal(expected, actual any, msg ...any) {
	a.tb.Helper()
	areEqual := func() bool {
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
		return reflect.DeepEqual(expected, actual)
	}()
	if !areEqual {
		a.tb.Fatalf("%sexpected %v (%T), got: %v (%T)", details(msg), expected, expected, actual, actual)
	}
}

func (a Assert) Greater(x, y any, msg ...any) {
	a.tb.Helper()
	at := reflect.TypeOf(x)
	bt := reflect.TypeOf(y)
	if at != bt {
		a.tb.Fatalf("%sexpected same type, got %T and %T", details(msg), x, y)
	}
	isTrue := func() bool {
		if x == nil || y == nil {
			return x != y
		}
		if x == nil {
			return false
		}
		if y == nil {
			return true
		}
		switch at.Kind() { //nolint:exhaustive
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return x.(int64) > y.(int64) //nolint:forcetypeassert
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return x.(uint64) > y.(uint64) //nolint:forcetypeassert
		case reflect.Float32, reflect.Float64:
			return x.(float64) > y.(float64) //nolint:forcetypeassert
		case reflect.String:
			return x.(string) > y.(string) //nolint:forcetypeassert
		default:
			return false
		}
	}()
	if !isTrue {
		a.tb.Fatalf("%sexpected %v (%T) > %v (%T)", details(msg), x, x, y, y)
	}
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

func details(msg []any) string {
	if len(msg) == 0 {
		return ""
	}
	if len(msg) == 1 {
		return fmt.Sprintf("%v: ", msg[0])
	}
	return fmt.Sprintf(msg[0].(string), msg[1:]...) + ": " //nolint:forcetypeassert
}

//nolint:unused
func fakeRawKey(suffix string) RawKey {
	return RawKey([]byte(strings.Repeat("k", RawKeySize-len(suffix)) + suffix))
}

func fakeSHA256(suffix string) Sha256 {
	return Sha256([]byte(strings.Repeat("s", 32-len(suffix)) + suffix))
}

func fakeEncryptedKey(suffix string) EncryptedKey { //nolint:unparam
	return EncryptedKey([]byte(strings.Repeat("e", EncryptedKeySize-len(suffix)) + suffix))
}

func fakeBlockId(suffix string) BlockId {
	return BlockId([]byte(strings.Repeat("b", 32-len(suffix)) + suffix))
}
