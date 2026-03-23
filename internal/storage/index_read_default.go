//go:build !unix

package storage

import "os"

func readIndexFileBytes(path string) ([]byte, func(), error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, func() {}, err
	}
	return data, func() {}, nil
}
