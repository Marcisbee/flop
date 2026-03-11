package flop

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB is the main database engine.
type DB struct {
	dir       string
	tables    map[string]*Table
	schemas   map[string]*Schema
	tableIDs  map[string]uint16
	nextTblID uint16
	mu        sync.RWMutex
	fts       map[string]*ftsEntry
}

// ftsEntry holds the FTS index and the searchable field names for a table.
type ftsEntry struct {
	index  *FTSIndex
	fields []string // field names marked Searchable
	once   sync.Once
	ready  bool
}

// OpenDB opens or creates a database at the given directory.
func OpenDB(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	return &DB{
		dir:       dir,
		tables:    make(map[string]*Table),
		schemas:   make(map[string]*Schema),
		tableIDs:  make(map[string]uint16),
		nextTblID: 1,
		fts:       make(map[string]*ftsEntry),
	}, nil
}

// CreateTable registers and creates a table.
func (db *DB) CreateTable(schema *Schema) (*Table, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if t, exists := db.tables[schema.Name]; exists {
		return t, nil
	}

	tableID := db.nextTblID
	db.nextTblID++

	dataPath := filepath.Join(db.dir, schema.Name+".db")
	archivePath := filepath.Join(db.dir, schema.Name+".archive")

	table, err := NewTable(schema, tableID, dataPath, archivePath)
	if err != nil {
		return nil, err
	}

	db.tables[schema.Name] = table
	db.schemas[schema.Name] = schema
	db.tableIDs[schema.Name] = tableID

	// Eagerly build secondary indexes if the table already has data.
	// This avoids a slow first-request penalty from lazy index building.
	if len(table.indexes) > 0 && table.primary.Count() > 0 {
		table.indexOnce.Do(func() {
			table.buildIndexes()
		})
	}

	var searchFields []string
	for _, f := range schema.Fields {
		if f.Searchable {
			searchFields = append(searchFields, f.Name)
		}
	}
	if len(searchFields) > 0 {
		db.fts[schema.Name] = &ftsEntry{
			index:  NewFTSIndex(),
			fields: searchFields,
		}
	}

	return table, nil
}

// Table returns a table by name.
func (db *DB) Table(name string) *Table {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.tables[name]
}

// Insert inserts a row, updating full-text indexes.
func (db *DB) Insert(tableName string, data map[string]any) (*Row, error) {
	table := db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	row, err := table.Insert(data)
	if err != nil {
		return nil, err
	}

	if entry, ok := db.fts[tableName]; ok && entry.ready {
		var texts []string
		for _, fname := range entry.fields {
			if s, ok := data[fname].(string); ok {
				texts = append(texts, s)
			}
		}
		if len(texts) > 0 {
			entry.index.Index(row.ID, texts...)
		}
	}

	return row, nil
}

// Update updates a row, refreshing full-text indexes.
func (db *DB) Update(tableName string, id uint64, updates map[string]any) (*Row, error) {
	table := db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	row, err := table.Update(id, updates)
	if err != nil {
		return nil, err
	}

	if entry, ok := db.fts[tableName]; ok && entry.ready {
		// Re-index with current data (Index handles replace)
		var texts []string
		for _, fname := range entry.fields {
			if s, ok := row.Data[fname].(string); ok {
				texts = append(texts, s)
			}
		}
		if len(texts) > 0 {
			entry.index.Index(row.ID, texts...)
		}
	}

	return row, nil
}

// Delete soft-deletes a row with optional cascade.
func (db *DB) Delete(tableName string, id uint64) error {
	table := db.Table(tableName)
	if table == nil {
		return fmt.Errorf("table %q not found", tableName)
	}

	schema := db.schemas[tableName]

	// Cascade delete
	for _, cascadeTable := range schema.CascadeDeletes {
		ct := db.Table(cascadeTable)
		if ct == nil {
			continue
		}
		var toDelete []uint64
		ct.Scan(func(row *Row) bool {
			for _, f := range db.schemas[cascadeTable].Fields {
				if f.RefTable == tableName {
					if ref, ok := row.Data[f.Name]; ok {
						if toUint64(ref) == id {
							toDelete = append(toDelete, row.ID)
						}
					}
				}
			}
			return true
		})
		for _, did := range toDelete {
			db.Delete(cascadeTable, did)
		}
	}

	if entry, ok := db.fts[tableName]; ok && entry.ready {
		entry.index.Delete(id)
	}

	return table.Delete(id)
}

// Search performs full-text search on a table.
func (db *DB) Search(tableName, query string, limit int) ([]*Row, error) {
	entry, ok := db.fts[tableName]
	if !ok {
		return nil, fmt.Errorf("no full-text index on table %q", tableName)
	}

	table := db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}
	db.ensureFTS(tableName, table)

	ids := entry.index.Search(query, limit)
	rows := make([]*Row, 0, len(ids))
	for _, id := range ids {
		row, err := table.Get(id)
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// SearchFullText searches a full-text index and returns row data maps.
// This mirrors the flop-go SearchFullText API.
func (db *DB) SearchFullText(tableName string, fields []string, query string, limit int) ([]map[string]any, error) {
	entry, ok := db.fts[tableName]
	if !ok {
		return nil, fmt.Errorf("no full-text index on table %q", tableName)
	}

	table := db.Table(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}
	db.ensureFTS(tableName, table)

	ids := entry.index.Search(query, limit)
	results := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		row, err := table.Get(id)
		if err != nil {
			return nil, err
		}
		if row != nil {
			data := make(map[string]any, len(row.Data)+1)
			data["id"] = row.ID
			for k, v := range row.Data {
				data[k] = v
			}
			results = append(results, data)
		}
	}
	return results, nil
}

func (db *DB) ensureFTS(tableName string, table *Table) {
	entry, ok := db.fts[tableName]
	if !ok || entry == nil {
		return
	}
	entry.once.Do(func() {
		table.Scan(func(row *Row) bool {
			var texts []string
			for _, fname := range entry.fields {
				if s, ok := row.Data[fname].(string); ok {
					texts = append(texts, s)
				}
			}
			if len(texts) > 0 {
				entry.index.Index(row.ID, texts...)
			}
			return true
		})
		entry.index.Finalize()
		entry.ready = true
	})
}

// Flush persists all tables to disk.
func (db *DB) Flush() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for name, table := range db.tables {
		if err := table.Flush(); err != nil {
			return fmt.Errorf("flush %s: %w", name, err)
		}
	}
	// FTS is in-memory only, no flush needed
	return nil
}

// Close closes all tables and flushes to disk.
func (db *DB) Close() error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, table := range db.tables {
		table.Close()
	}
	return nil
}

// Backup copies the database directory to a destination.
func (db *DB) Backup(destDir string) error {
	if err := db.Flush(); err != nil {
		return err
	}
	if err := os.MkdirAll(destDir, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		src := filepath.Join(db.dir, entry.Name())
		dst := filepath.Join(destDir, entry.Name())
		data, err := os.ReadFile(src)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0600); err != nil {
			return err
		}
	}
	return nil
}
