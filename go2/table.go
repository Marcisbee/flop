package flop

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Table represents a single table in the database.
type Table struct {
	schema    *Schema
	tableID   uint16
	primary   *Store       // primary storage: encoded(ID) -> encoded(Row)
	indexes   []*StoreIndex // secondary indexes
	nextID    atomic.Uint64
	mu        sync.RWMutex
	archive   *Store       // shadow storage for soft-deleted rows
	indexOnce sync.Once    // lazy index building
	encoder   *RowEncoder  // schema-aware binary encoder
}

func NewTable(schema *Schema, tableID uint16, dataPath, archivePath string) (*Table, error) {
	t := &Table{
		schema:  schema,
		tableID: tableID,
		primary: NewStore(dataPath),
		archive: NewStore(archivePath),
		encoder: NewRowEncoder(schema),
	}

	// Create indexes for unique fields
	for _, f := range schema.Fields {
		if f.Unique {
			idx := &StoreIndex{
				Name:   f.Name + "_unique",
				Fields: []string{f.Name},
				Unique: true,
				store:  NewStore(""), // in-memory only, rebuilt on load
			}
			t.indexes = append(t.indexes, idx)
		}
		if f.Indexed {
			idx := &StoreIndex{
				Name:   f.Name + "_idx",
				Fields: []string{f.Name},
				Unique: false,
				store:  NewStore(""),
			}
			t.indexes = append(t.indexes, idx)
		}
	}

	// Create composite unique indexes
	for i, fields := range schema.UniqueConstraints {
		idx := &StoreIndex{
			Name:   fmt.Sprintf("composite_unique_%d", i),
			Fields: fields,
			Unique: true,
			store:  NewStore(""),
		}
		t.indexes = append(t.indexes, idx)
	}

	// Find max ID only — indexes are built lazily on first use
	maxID := uint64(0)
	t.primary.ScanReverse(func(key, val []byte) bool {
		maxID = DecodeUint64(key)
		return false // first key in reverse order is the max
	})
	t.nextID.Store(maxID + 1)

	return t, nil
}

// Insert adds a new row. Returns the inserted row with generated ID.
func (t *Table) Insert(data map[string]any) (*Row, error) {
	t.ensureIndexes()
	t.mu.Lock()
	defer t.mu.Unlock()

	// Normalize data types to match schema (e.g., string "1" → uint64 for FieldRef)
	t.normalizeData(data)

	// Validate required fields
	for _, f := range t.schema.Fields {
		if f.Required {
			if _, ok := data[f.Name]; !ok {
				return nil, fmt.Errorf("field %q is required", f.Name)
			}
		}
		if f.MaxLen > 0 && f.Type == FieldString {
			if s, ok := data[f.Name].(string); ok && len(s) > f.MaxLen {
				return nil, fmt.Errorf("field %q exceeds max length %d", f.Name, f.MaxLen)
			}
		}
		if len(f.EnumValues) > 0 {
			if v, ok := data[f.Name]; ok {
				s := fmt.Sprintf("%v", v)
				valid := false
				for _, ev := range f.EnumValues {
					if s == ev {
						valid = true
						break
					}
				}
				if !valid {
					return nil, fmt.Errorf("field %q value %q not in enum %v", f.Name, s, f.EnumValues)
				}
			}
		}
	}

	// Check unique constraints
	for _, idx := range t.indexes {
		if idx.Unique {
			key := buildIdxKey(idx.Fields, data)
			if existing := idx.store.Get(key); existing != nil {
				return nil, fmt.Errorf("unique constraint violated on %s", idx.Name)
			}
		}
	}

	now := time.Now()
	row := &Row{
		ID:        t.nextID.Add(1),
		TableID:   t.tableID,
		Data:      data,
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}

	encoded := t.encodeRow(row)
	key := EncodeUint64(row.ID)
	t.primary.Put(key, encoded)

	// Update indexes
	for _, idx := range t.indexes {
		t.indexPut(idx, data, row.ID)
	}

	return row, nil
}

// Get retrieves a row by ID.
func (t *Table) Get(id uint64) (*Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.get(id)
}

func (t *Table) get(id uint64) (*Row, error) {
	val := t.primary.Get(EncodeUint64(id))
	if val == nil {
		return nil, nil
	}
	return t.decodeRow(val)
}

// decodeRow decodes a row using the schema-aware binary format.
func (t *Table) decodeRow(data []byte) (*Row, error) {
	return t.encoder.DecodeRow(data)
}

// encodeRow encodes a row using the schema-aware binary format.
func (t *Table) encodeRow(row *Row) []byte {
	b, _ := t.encoder.EncodeRow(row)
	return b
}

// Update modifies an existing row.
func (t *Table) Update(id uint64, updates map[string]any) (*Row, error) {
	t.ensureIndexes()
	t.mu.Lock()
	defer t.mu.Unlock()

	row, err := t.get(id)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("row %d not found", id)
	}

	// Remove old index entries
	for _, idx := range t.indexes {
		t.indexDelete(idx, row.Data, row.ID)
	}

	// Normalize and apply updates
	t.normalizeData(updates)
	for k, v := range updates {
		row.Data[k] = v
	}
	row.UpdatedAt = time.Now()
	row.Version++

	// Check unique constraints with new data
	for _, idx := range t.indexes {
		if idx.Unique {
			idxKey := buildIdxKey(idx.Fields, row.Data)
			existing := idx.store.Get(idxKey)
			if existing != nil {
				existingID := DecodeUint64(existing)
				if existingID != id {
					return nil, fmt.Errorf("unique constraint violated on %s", idx.Name)
				}
			}
		}
	}

	encoded := t.encodeRow(row)
	key := EncodeUint64(id)
	t.primary.Put(key, encoded)

	for _, idx := range t.indexes {
		t.indexPut(idx, row.Data, row.ID)
	}

	return row, nil
}

// Delete soft-deletes a row (moves to archive).
func (t *Table) Delete(id uint64) error {
	t.ensureIndexes()
	t.mu.Lock()
	defer t.mu.Unlock()

	key := EncodeUint64(id)
	data := t.primary.Get(key)
	if data == nil {
		return fmt.Errorf("row %d not found", id)
	}

	row, err := t.decodeRow(data)
	if err != nil {
		return fmt.Errorf("decode row %d: %w", id, err)
	}

	// Archive the row
	t.archive.Put(key, data)

	// Remove from primary
	t.primary.Delete(key)

	// Remove from indexes
	for _, idx := range t.indexes {
		t.indexDelete(idx, row.Data, row.ID)
	}

	return nil
}

// Restore moves a row from archive back to the main table.
func (t *Table) Restore(id uint64) (*Row, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := EncodeUint64(id)
	data := t.archive.Get(key)
	if data == nil {
		return nil, fmt.Errorf("archived row %d not found", id)
	}

	row, err := t.decodeRow(data)
	if err != nil {
		return nil, err
	}

	t.primary.Put(key, data)
	t.archive.Delete(key)

	for _, idx := range t.indexes {
		t.indexPut(idx, row.Data, row.ID)
	}

	return row, nil
}

// Scan iterates all rows in primary key order.
func (t *Table) Scan(fn func(*Row) bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.primary.Scan(func(key, val []byte) bool {
		row, err := t.decodeRow(val)
		if err != nil {
			return true
		}
		return fn(row)
	})
	return nil
}

// Count returns the number of rows.
func (t *Table) Count() (int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.primary.Count(), nil
}

// Flush persists all changes to disk.
func (t *Table) Flush() error {
	if err := t.primary.Flush(); err != nil {
		return err
	}
	return t.archive.Flush()
}

// Close closes the table.
func (t *Table) Close() error {
	return t.Flush()
}

// ScanLast iterates the last N rows in reverse insertion order.
func (t *Table) ScanLast(limit int, fn func(*Row) bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	count := 0
	t.primary.ScanReverse(func(key, val []byte) bool {
		if count >= limit {
			return false
		}
		row, err := t.decodeRow(val)
		if err != nil {
			return true
		}
		count++
		return fn(row)
	})
	return nil
}

// ScanByField iterates rows matching a field value using a secondary index.
// Falls back to full scan if no index exists for the field.
func (t *Table) ScanByField(field string, value any, fn func(*Row) bool) error {
	t.ensureIndexes()
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Normalize value to match JSON-deserialized types (what's stored in index)
	normValue := normalizeForIndex(value)

	// Find non-unique index for this field
	for _, idx := range t.indexes {
		if !idx.Unique && len(idx.Fields) == 1 && idx.Fields[0] == field {
			prefix := buildIdxKey(idx.Fields, map[string]any{field: normValue})
			idx.store.ScanPrefix(prefix, func(key, val []byte) bool {
				// val is empty for non-unique; row ID is in the key suffix
				rowID := DecodeUint64(key[len(prefix):])
				row, err := t.get(rowID)
				if err != nil || row == nil {
					return true
				}
				return fn(row)
			})
			return nil
		}
	}

	// Fallback: full scan
	t.primary.Scan(func(key, val []byte) bool {
		row, err := t.decodeRow(val)
		if err != nil {
			return true
		}
		if fmt.Sprintf("%v", row.Data[field]) == fmt.Sprintf("%v", value) {
			return fn(row)
		}
		return true
	})
	return nil
}

// ScanByFieldRange iterates rows where the indexed field value is in [start, end).
// start or end may be nil to indicate unbounded. Uses the non-unique index if available.
// Returns true if an index was used, false if not (caller should fall back to full scan).
func (t *Table) ScanByFieldRange(field string, start, end any, includeStart, includeEnd bool, fn func(*Row) bool) bool {
	t.ensureIndexes()
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, idx := range t.indexes {
		if len(idx.Fields) != 1 || idx.Fields[0] != field {
			continue
		}
		if idx.Unique {
			continue // range scan on unique indexes needs different key layout
		}

		var startKey, endKey []byte
		if start != nil {
			startKey = buildIdxKey(idx.Fields, map[string]any{field: normalizeForIndex(start)})
		}
		if end != nil {
			endKey = buildIdxKey(idx.Fields, map[string]any{field: normalizeForIndex(end)})
		}

		idx.store.ScanRange(startKey, nil, func(key, val []byte) bool {
			// Key format: [lenPrefix][encodedValue][8-byte rowID]
			// Extract the value portion (everything before the last 8 bytes)
			if len(key) < 8 {
				return true
			}
			valPart := key[:len(key)-8]

			// Check bounds
			if startKey != nil {
				cmp := compareBytes(valPart, startKey)
				if cmp < 0 || (!includeStart && cmp == 0) {
					return true
				}
			}
			if endKey != nil {
				cmp := compareBytes(valPart, endKey)
				if cmp > 0 || (!includeEnd && cmp == 0) {
					return false // past end, stop
				}
			}

			rowID := DecodeUint64(key[len(key)-8:])
			row, err := t.get(rowID)
			if err != nil || row == nil {
				return true
			}
			return fn(row)
		})
		return true
	}

	return false
}

func compareBytes(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}

// ScanSortField scans all rows extracting only the ID and a numeric field value.
// Much faster than full Scan when only one field is needed (no map allocations).
func (t *Table) ScanSortField(field string, fn func(id uint64, val float64) bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.primary.Scan(func(key, val []byte) bool {
		id, sv, ok := t.encoder.ExtractSortFloat(val, field)
		if !ok {
			return true
		}
		return fn(id, sv)
	})
	return nil
}

// normalizeData coerces data values to match schema field types.
// This ensures consistent index keys and encoding regardless of input types.
func (t *Table) normalizeData(data map[string]any) {
	for _, f := range t.schema.Fields {
		v, ok := data[f.Name]
		if !ok || v == nil {
			continue
		}
		switch f.Type {
		case FieldRef:
			switch n := v.(type) {
			case uint64:
				// already correct
			case float64:
				data[f.Name] = uint64(n)
			case int:
				data[f.Name] = uint64(n)
			case int64:
				data[f.Name] = uint64(n)
			case string:
				if n != "" {
					if parsed, err := strconv.ParseUint(n, 10, 64); err == nil {
						data[f.Name] = parsed
					}
				}
			}
		case FieldInt:
			switch n := v.(type) {
			case int64:
				// already correct
			case int:
				data[f.Name] = int64(n)
			case uint64:
				data[f.Name] = int64(n)
			case float64:
				data[f.Name] = int64(n)
			case string:
				if parsed, err := strconv.ParseInt(n, 10, 64); err == nil {
					data[f.Name] = parsed
				}
			}
		case FieldFloat:
			switch n := v.(type) {
			case float64:
				// already correct
			case float32:
				data[f.Name] = float64(n)
			case int:
				data[f.Name] = float64(n)
			case int64:
				data[f.Name] = float64(n)
			case string:
				if parsed, err := strconv.ParseFloat(n, 64); err == nil {
					data[f.Name] = parsed
				}
			}
		case FieldBool:
			switch b := v.(type) {
			case bool:
				// already correct
			case string:
				data[f.Name] = b == "true" || b == "1"
			case int:
				data[f.Name] = b != 0
			case float64:
				data[f.Name] = b != 0
			}
		}
	}
}

// ensureIndexes lazily builds all secondary indexes on first use.
func (t *Table) ensureIndexes() {
	if len(t.indexes) == 0 {
		return
	}
	t.indexOnce.Do(func() {
		t.buildIndexes()
	})
}

// buildIndexes scans all rows and populates secondary indexes.
// Called by ensureIndexes (lazy) or eagerly from CreateTable.
func (t *Table) buildIndexes() {
	t.primary.Scan(func(key, val []byte) bool {
		row, err := t.decodeRow(val)
		if err != nil {
			return true
		}
		for _, idx := range t.indexes {
			t.indexPut(idx, row.Data, row.ID)
		}
		return true
	})
}

// indexPut adds a row to an index.
func (t *Table) indexPut(idx *StoreIndex, data map[string]any, rowID uint64) {
	if idx.Unique {
		idxKey := buildIdxKey(idx.Fields, data)
		idx.store.Put(idxKey, EncodeUint64(rowID))
	} else {
		// Non-unique: key = fieldValue + rowID, value = empty
		idxKey := append(buildIdxKey(idx.Fields, data), EncodeUint64(rowID)...)
		idx.store.Put(idxKey, []byte{})
	}
}

// indexDelete removes a row from an index.
func (t *Table) indexDelete(idx *StoreIndex, data map[string]any, rowID uint64) {
	if idx.Unique {
		idxKey := buildIdxKey(idx.Fields, data)
		idx.store.Delete(idxKey)
	} else {
		idxKey := append(buildIdxKey(idx.Fields, data), EncodeUint64(rowID)...)
		idx.store.Delete(idxKey)
	}
}

// normalizeForIndex converts integer types to float64 to match JSON deserialization.
// JSON unmarshalling turns all numbers into float64, so index keys must be consistent.
func normalizeForIndex(v any) any {
	switch n := v.(type) {
	case uint64:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return v
}

func buildIdxKey(fields []string, data map[string]any) []byte {
	var key []byte
	for _, field := range fields {
		val := data[field]
		var encoded []byte
		switch v := val.(type) {
		case string:
			encoded = EncodeString(v)
		case float64:
			encoded = EncodeFloat64(v)
		case uint64:
			// Normalize to float64 for consistency with JSON deserialization
			encoded = EncodeFloat64(float64(v))
		case int:
			encoded = EncodeFloat64(float64(v))
		case int64:
			encoded = EncodeFloat64(float64(v))
		default:
			encoded = []byte(fmt.Sprintf("%v", v))
		}
		key = append(key, byte(len(encoded)))
		key = append(key, encoded...)
	}
	return key
}
