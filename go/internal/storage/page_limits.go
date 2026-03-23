package storage

import (
	"fmt"

	"github.com/marcisbee/flop/internal/schema"
)

// MaxRowDataSize returns the maximum serialized row payload that can fit into a
// fresh page alongside one slot entry.
func MaxRowDataSize() int {
	return schema.PageSize - schema.PageHeaderSize - schema.SlotSize
}

// ValidateRowDataSize reports whether a serialized row payload can fit into a page.
func ValidateRowDataSize(rowDataSize int) error {
	max := MaxRowDataSize()
	if rowDataSize > max {
		return fmt.Errorf("%w: size=%d max=%d", ErrRowTooLarge, rowDataSize, max)
	}
	return nil
}
