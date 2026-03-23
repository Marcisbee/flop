//go:build unix

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type DirLock interface {
	Close() error
}

type fileDirLock struct {
	f *os.File
}

func AcquireDirLock(dataDir string) (DirLock, error) {
	lockPath := filepath.Join(dataDir, ".flop.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("database already open by another process: %w", err)
	}
	return &fileDirLock{f: f}, nil
}

func (l *fileDirLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	errUnlock := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	errClose := l.f.Close()
	l.f = nil
	if errUnlock != nil {
		return errUnlock
	}
	return errClose
}
