//go:build !unix && !windows

package storage

type noopDirLock struct{}

func AcquireDirLock(dataDir string) (DirLock, error) {
	return noopDirLock{}, nil
}

func (noopDirLock) Close() error {
	return nil
}
