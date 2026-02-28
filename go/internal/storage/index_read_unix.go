//go:build unix

package storage

import (
	"os"
	"syscall"
)

func readIndexFileBytes(path string) ([]byte, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, func() {}, err
	}
	size := fi.Size()
	if size == 0 {
		_ = f.Close()
		return nil, func() {}, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	_ = f.Close()
	if err != nil {
		return nil, func() {}, err
	}
	return data, func() { _ = syscall.Munmap(data) }, nil
}
