package storage

import "errors"

var (
	ErrShortBuffer      = errors.New("buffer too short for deserialization")
	ErrRowTooLarge      = errors.New("row too large for page")
	ErrPageSizeMismatch = errors.New("page size mismatch")
)
