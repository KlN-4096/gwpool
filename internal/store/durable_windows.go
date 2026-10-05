//go:build windows

package store

import (
	"syscall"
	"unsafe"
)

var moveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func durableReplace(source, target string) error {
	from, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	const replaceExistingAndWriteThrough = 0x1 | 0x8
	result, _, callErr := moveFileEx.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), replaceExistingAndWriteThrough)
	if result == 0 {
		return callErr
	}
	return nil
}

// Windows directory handles do not support FlushFileBuffers. File.Sync flushes
// file contents; snapshot/WAL replacements use MoveFileEx WRITE_THROUGH.
func syncDirectory(string) error { return nil }
