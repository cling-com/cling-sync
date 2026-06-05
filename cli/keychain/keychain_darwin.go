//go:build darwin && !mock

//nolint:gocritic,govet
package keychain

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation

#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
*/
import "C"

import (
	"context"
	"unsafe"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
	ErrKeychainLocked             = lib.Errorf(
		"macOS keychain is locked, unlock it by running: security unlock-keychain",
	)
)

// createCFString is a helper to convert a Go string to a CFStringRef.
// The caller is responsible for releasing the returned CFStringRef.
func createCFString(s string) C.CFStringRef {
	cStr := C.CString(s)
	defer C.free(unsafe.Pointer(cStr))
	return C.CFStringCreateWithCString(C.kCFAllocatorDefault, cStr, C.kCFStringEncodingUTF8)
}

// createCFData is a helper to convert a Go string to a CFDataRef.
// The caller is responsible for releasing the returned CFDataRef.
func createCFData(data string) C.CFDataRef {
	cStr := C.CString(data)
	defer C.free(unsafe.Pointer(cStr))
	return C.CFDataCreate(C.kCFAllocatorDefault, (*C.UInt8)(unsafe.Pointer(cStr)), C.CFIndex(len(data)))
}

func buildQueryDict(service, account string) C.CFMutableDictionaryRef {
	cService := createCFString(service)
	defer C.CFRelease(C.CFTypeRef(cService))
	cAccount := createCFString(account)
	defer C.CFRelease(C.CFTypeRef(cAccount))

	query := C.CFDictionaryCreateMutable(
		C.kCFAllocatorDefault,
		0,
		&C.kCFTypeDictionaryKeyCallBacks,
		&C.kCFTypeDictionaryValueCallBacks,
	)

	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecClass), unsafe.Pointer(C.kSecClassGenericPassword))
	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecAttrService), unsafe.Pointer(cService))
	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecAttrAccount), unsafe.Pointer(cAccount))
	return query
}

func AddKeychainEntry(ctx context.Context, service, account, secret string) error {
	query := buildQueryDict(service, account)
	defer C.CFRelease(C.CFTypeRef(query))

	cSecretData := createCFData(secret)
	defer C.CFRelease(C.CFTypeRef(cSecretData))
	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecValueData), unsafe.Pointer(cSecretData))

	addStatus := C.SecItemAdd(C.CFDictionaryRef(query), nil)
	if addStatus == C.errSecDuplicateItem {
		return ErrKeychainEntryAlreadyExists
	}
	if addStatus != C.errSecSuccess {
		return keychainError(int32(addStatus), "store")
	}
	return nil
}

func GetKeychainEntry(ctx context.Context, service, account string) (string, error) {
	query := buildQueryDict(service, account)
	defer C.CFRelease(C.CFTypeRef(query))

	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecReturnData), unsafe.Pointer(C.kCFBooleanTrue))
	C.CFDictionarySetValue(query, unsafe.Pointer(C.kSecMatchLimit), unsafe.Pointer(C.kSecMatchLimitOne))

	var result C.CFTypeRef
	status := C.SecItemCopyMatching(C.CFDictionaryRef(query), &result)

	if status == C.errSecItemNotFound {
		return "", ErrKeychainEntryNotFound
	}
	if status != C.errSecSuccess {
		return "", keychainError(int32(status), "lookup")
	}
	defer C.CFRelease(result)

	data := C.CFDataRef(result)
	length := C.CFDataGetLength(data)
	ptr := C.CFDataGetBytePtr(data)

	goBytes := C.GoBytes(unsafe.Pointer(ptr), C.int(length))
	return string(goBytes), nil
}

func DeleteKeychainEntry(ctx context.Context, service, account string) error {
	query := buildQueryDict(service, account)
	defer C.CFRelease(C.CFTypeRef(query))

	status := C.SecItemDelete(C.CFDictionaryRef(query))
	if status != C.errSecSuccess && status != C.errSecItemNotFound {
		return keychainError(int32(status), "delete")
	}
	return nil
}

const (
	errSecInteractionNotAllowed = int32(C.errSecInteractionNotAllowed)
	errSecInteractionRequired   = int32(C.errSecInteractionRequired)
)

// keychainError translates a macOS OSStatus into a readable error. It takes int32, not the
// natural C.OSStatus, so the mapping stays testable: Go forbids cgo in _test.go files.
// SecCopyErrorMessageString supplies Apple's own description for any code we do not special-case.
func keychainError(status int32, operation string) error {
	// errSecInteractionNotAllowed (-25308) and errSecInteractionRequired (-25315) both mean
	// the keychain needed unlocking but no prompt could be shown, which from a CLI almost
	// always means it is locked.
	if status == errSecInteractionNotAllowed || status == errSecInteractionRequired {
		return ErrKeychainLocked
	}
	if msg := secErrorMessage(status); msg != "" {
		return lib.Errorf("failed to %s keychain entry: %s (%d)", operation, msg, status)
	}
	return lib.Errorf("failed to %s keychain entry: %d", operation, status)
}

func secErrorMessage(status int32) string {
	cMsg := C.SecCopyErrorMessageString(C.OSStatus(status), nil)
	if cMsg == 0 {
		return ""
	}
	defer C.CFRelease(C.CFTypeRef(cMsg))
	if ptr := C.CFStringGetCStringPtr(cMsg, C.kCFStringEncodingUTF8); ptr != nil {
		return C.GoString(ptr)
	}
	length := C.CFStringGetLength(cMsg)
	maxSize := C.CFStringGetMaximumSizeForEncoding(length, C.kCFStringEncodingUTF8) + 1
	buf := (*C.char)(C.malloc(C.size_t(maxSize)))
	defer C.free(unsafe.Pointer(buf))
	if C.CFStringGetCString(cMsg, buf, maxSize, C.kCFStringEncodingUTF8) != 0 {
		return C.GoString(buf)
	}
	return ""
}
