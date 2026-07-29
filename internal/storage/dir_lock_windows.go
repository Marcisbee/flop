//go:build windows

package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

type windowsDirLock struct {
	f          *os.File
	overlapped windows.Overlapped
}

func AcquireDirLock(dataDir string) (DirLock, error) {
	lockPath := filepath.Join(dataDir, ".flop.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	lock := &windowsDirLock{f: f}
	handle := windows.Handle(f.Fd())
	if err := windows.LockFileEx(
		handle,
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&lock.overlapped,
	); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("database already open by another process: %w", err)
	}
	return lock, nil
}

func (l *windowsDirLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	errUnlock := windows.UnlockFileEx(
		windows.Handle(l.f.Fd()),
		0,
		1,
		0,
		&l.overlapped,
	)
	errClose := l.f.Close()
	l.f = nil
	if errUnlock != nil {
		return errUnlock
	}
	return errClose
}
