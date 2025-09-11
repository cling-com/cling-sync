//go:build darwin && !mock

//nolint:gocritic,govet
package main

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation

#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
*/
import "C"

import (
	"unsafe"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
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

func AddKeychainEntry(service, account, secret string) error {
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
		return lib.Errorf("failed to add keychain entry: %d", addStatus)
	}
	return nil
}

func GetKeychainEntry(service, account string) (string, error) {
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
		return "", lib.Errorf("failed to get keychain entry: %d", status)
	}
	defer C.CFRelease(result)

	data := C.CFDataRef(result)
	length := C.CFDataGetLength(data)
	ptr := C.CFDataGetBytePtr(data)

	goBytes := C.GoBytes(unsafe.Pointer(ptr), C.int(length))
	return string(goBytes), nil
}

func DeleteKeychainEntry(service, account string) error {
	query := buildQueryDict(service, account)
	defer C.CFRelease(C.CFTypeRef(query))

	status := C.SecItemDelete(C.CFDictionaryRef(query))
	if status != C.errSecSuccess && status != C.errSecItemNotFound {
		return lib.Errorf("failed to delete keychain entry: %d", status)
	}
	return nil
}
