package storage

import "errors"

var (
	ErrShortBuffer = errors.New("buffer too short for deserialization")
)
