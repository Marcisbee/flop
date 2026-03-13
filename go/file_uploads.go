package flop

import (
	"fmt"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

// StoreFileForField validates and stores a file according to the field schema.
func (d *Database) StoreFileForField(tableName, rowID, fieldName, filename string, data []byte, mime string) (*FileRef, error) {
	if d == nil || d.db == nil {
		return nil, fmt.Errorf("flop: database is not open")
	}
	ti := d.db.GetTable(tableName)
	if ti == nil {
		return nil, fmt.Errorf("flop: table not found: %s", tableName)
	}
	field := ti.GetDef().CompiledSchema.FieldMap[fieldName]
	if field == nil || (field.Kind != schema.KindFileSingle && field.Kind != schema.KindFileMulti) {
		return nil, fmt.Errorf("flop: field %s.%s is not a file field", tableName, fieldName)
	}
	ref, err := storage.StoreFileWithField(d.db.GetDataDir(), tableName, rowID, fieldName, filename, data, mime, field)
	if err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, nil
	}
	return &FileRef{
		Path: ref.Path,
		Name: ref.Name,
		URL:  ref.URL,
		Mime: ref.Mime,
		Size: ref.Size,
	}, nil
}
