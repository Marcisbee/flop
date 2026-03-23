//go:build !unix

package storage

type DirLock interface {
	Close() error
}

type noopDirLock struct{}

func AcquireDirLock(dataDir string) (DirLock, error) {
	return noopDirLock{}, nil
}

func (noopDirLock) Close() error {
	return nil
}
