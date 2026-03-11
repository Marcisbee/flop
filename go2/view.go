package flop

import (
	"container/heap"
	"sort"
)

// SortOrder specifies sort direction.
type SortOrder int

const (
	Asc SortOrder = iota
	Desc
)

// Filter represents a query predicate.
type Filter struct {
	Field string
	Op    FilterOp
	Value any
}

type FilterOp int

const (
	OpEq FilterOp = iota
	OpNeq
	OpGt
	OpGte
	OpLt
	OpLte
	OpIn
	OpContains
)

// ViewDef defines a precompiled view (read query).
type ViewDef struct {
	Name       string
	Table      string
	Filters    []Filter
	OrderBy    string
	Order      SortOrder
	Limit      int
	Offset     int
	Includes   []string // ref fields to resolve
	SearchQ    string   // full-text search query
	PermFilter func(auth any, row *Row) bool
}

// ViewResult holds the result of executing a view.
type ViewResult struct {
	Rows  []*Row
	Total int
}

// ExecuteView runs a precompiled view against the database.
func (db *DB) ExecuteView(v *ViewDef) (*ViewResult, error) {
	table := db.Table(v.Table)
	if table == nil {
		return nil, nil
	}

	// Index lookup path: all filters are OpEq and match a unique index → direct lookup.
	if v.SearchQ == "" && len(v.Filters) > 0 && v.PermFilter == nil {
		if rows, ok := db.executeViewIndexed(v, table); ok {
			db.resolveIncludes(v, rows)
			return &ViewResult{Rows: rows, Total: len(rows)}, nil
		}
	}

	// Fast path: no filters/search, numeric sort + limit.
	// Extracts only the sort field per row (no map allocs), then decodes only the result rows.
	if v.SearchQ == "" && len(v.Filters) == 0 && v.PermFilter == nil && v.OrderBy != "" && v.Limit > 0 {
		return db.executeViewFast(v, table)
	}

	// Simple limit path: no filters, no order, no search — just take the first N rows.
	if v.SearchQ == "" && len(v.Filters) == 0 && v.PermFilter == nil && v.OrderBy == "" && v.Limit > 0 {
		total, _ := table.Count()
		rows := make([]*Row, 0, v.Limit)
		skip := v.Offset
		table.Scan(func(row *Row) bool {
			if skip > 0 {
				skip--
				return true
			}
			rows = append(rows, row)
			return len(rows) < v.Limit
		})
		db.resolveIncludes(v, rows)
		return &ViewResult{Rows: rows, Total: total}, nil
	}

	var rows []*Row

	// If search query, use FTS
	if v.SearchQ != "" {
		searchRows, err := db.Search(v.Table, v.SearchQ, 0)
		if err != nil {
			return nil, err
		}
		rows = searchRows
	} else {
		// Full scan
		table.Scan(func(row *Row) bool {
			rows = append(rows, row)
			return true
		})
	}

	// Apply filters
	filtered := make([]*Row, 0, len(rows))
	for _, row := range rows {
		if matchesFilters(row, v.Filters) {
			if v.PermFilter == nil || v.PermFilter(nil, row) {
				filtered = append(filtered, row)
			}
		}
	}

	total := len(filtered)

	// Sort
	if v.OrderBy != "" {
		sort.Slice(filtered, func(i, j int) bool {
			var a, b any
			if v.OrderBy == "id" {
				a, b = filtered[i].ID, filtered[j].ID
			} else {
				a = filtered[i].Data[v.OrderBy]
				b = filtered[j].Data[v.OrderBy]
			}
			cmp := compareValues(a, b)
			if v.Order == Desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	// Pagination
	if v.Offset > 0 && v.Offset < len(filtered) {
		filtered = filtered[v.Offset:]
	} else if v.Offset >= len(filtered) {
		filtered = nil
	}
	if v.Limit > 0 && len(filtered) > v.Limit {
		filtered = filtered[:v.Limit]
	}

	db.resolveIncludes(v, filtered)

	return &ViewResult{Rows: filtered, Total: total}, nil
}

// executeViewIndexed tries to use a secondary index for OpEq filter lookups.
// Returns (rows, true) if the index was used, (nil, false) if no index matched.
func (db *DB) executeViewIndexed(v *ViewDef, table *Table) ([]*Row, bool) {
	table.ensureIndexes()

	// Check for single-field unique index match (most common: lookup by slug, email, etc.)
	for _, f := range v.Filters {
		if f.Op != OpEq || f.Value == nil {
			continue
		}
		for _, idx := range table.indexes {
			if !idx.Unique || len(idx.Fields) != 1 || idx.Fields[0] != f.Field {
				continue
			}
			// Found a unique index match — do direct lookup
			normValue := normalizeForIndex(f.Value)
			idxKey := buildIdxKey(idx.Fields, map[string]any{f.Field: normValue})
			rawID := idx.store.Get(idxKey)
			if rawID == nil {
				return []*Row{}, true
			}
			rowID := DecodeUint64(rawID)
			row, err := table.Get(rowID)
			if err != nil || row == nil {
				return []*Row{}, true
			}
			// Verify all remaining filters match
			if matchesFilters(row, v.Filters) {
				return []*Row{row}, true
			}
			return []*Row{}, true
		}
	}

	// Check for single-field non-unique index match
	for _, f := range v.Filters {
		if f.Op != OpEq || f.Value == nil {
			continue
		}
		for _, idx := range table.indexes {
			if idx.Unique || len(idx.Fields) != 1 || idx.Fields[0] != f.Field {
				continue
			}
			// Non-unique index match — scan by prefix
			var rows []*Row
			table.ScanByField(f.Field, f.Value, func(row *Row) bool {
				if matchesFilters(row, v.Filters) {
					rows = append(rows, row)
				}
				return v.Limit == 0 || len(rows) < v.Limit
			})
			return rows, true
		}
	}

	return nil, false
}

// executeViewFast handles views with no filters — extracts only the sort field
// from raw bytes (zero alloc per row). Uses heap selection for small limits
// (O(n log k) instead of O(n log n)), or full sort for large result sets.
func (db *DB) executeViewFast(v *ViewDef, table *Table) (*ViewResult, error) {
	total, _ := table.Count()
	need := v.Offset + v.Limit

	// Use heap selection when limit+offset is much smaller than total rows.
	// This avoids sorting all 600k entries for a top-36 query.
	if need > 0 && need < total/4 {
		return db.executeViewHeap(v, table, total, need)
	}

	type entry struct {
		id  uint64
		val float64
	}

	entries := make([]entry, 0, total)
	table.ScanSortField(v.OrderBy, func(id uint64, val float64) bool {
		entries = append(entries, entry{id, val})
		return true
	})

	total = len(entries)

	// Sort
	if v.Order == Desc {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].val > entries[j].val
		})
	} else {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].val < entries[j].val
		})
	}

	// Pagination
	start := v.Offset
	if start >= len(entries) {
		return &ViewResult{Rows: nil, Total: total}, nil
	}
	end := start + v.Limit
	if end > len(entries) {
		end = len(entries)
	}

	// Decode only the needed rows
	rows := make([]*Row, 0, end-start)
	for _, e := range entries[start:end] {
		row, err := table.Get(e.id)
		if err != nil || row == nil {
			continue
		}
		rows = append(rows, row)
	}

	db.resolveIncludes(v, rows)

	return &ViewResult{Rows: rows, Total: total}, nil
}

// sortEntry is used by the heap for top-K selection.
type sortEntry struct {
	id  uint64
	val float64
}

// For Desc order, we want a min-heap so we can evict the smallest value.
// For Asc order, we want a max-heap so we can evict the largest value.
type minHeap []sortEntry

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].val < h[j].val }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)         { *h = append(*h, x.(sortEntry)) }
func (h *minHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

type maxHeap []sortEntry

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].val > h[j].val }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)         { *h = append(*h, x.(sortEntry)) }
func (h *maxHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// executeViewHeap uses heap selection to find the top-K entries without sorting all rows.
func (db *DB) executeViewHeap(v *ViewDef, table *Table, total, need int) (*ViewResult, error) {
	if v.Order == Desc {
		// For Desc: keep the K largest values using a min-heap of size K
		h := &minHeap{}
		heap.Init(h)
		table.ScanSortField(v.OrderBy, func(id uint64, val float64) bool {
			if h.Len() < need {
				heap.Push(h, sortEntry{id, val})
			} else if val > (*h)[0].val {
				(*h)[0] = sortEntry{id, val}
				heap.Fix(h, 0)
			}
			return true
		})
		// Extract in sorted order (heap gives smallest first, so reverse)
		n := h.Len()
		sorted := make([]sortEntry, n)
		for i := n - 1; i >= 0; i-- {
			sorted[i] = heap.Pop(h).(sortEntry)
		}
		// Apply offset
		start := v.Offset
		if start >= len(sorted) {
			return &ViewResult{Rows: nil, Total: total}, nil
		}
		end := start + v.Limit
		if end > len(sorted) {
			end = len(sorted)
		}
		rows := make([]*Row, 0, end-start)
		for _, e := range sorted[start:end] {
			row, err := table.Get(e.id)
			if err != nil || row == nil {
				continue
			}
			rows = append(rows, row)
		}
		db.resolveIncludes(v, rows)
		return &ViewResult{Rows: rows, Total: total}, nil
	}

	// For Asc: keep the K smallest values using a max-heap of size K
	h := &maxHeap{}
	heap.Init(h)
	table.ScanSortField(v.OrderBy, func(id uint64, val float64) bool {
		if h.Len() < need {
			heap.Push(h, sortEntry{id, val})
		} else if val < (*h)[0].val {
			(*h)[0] = sortEntry{id, val}
			heap.Fix(h, 0)
		}
		return true
	})
	n := h.Len()
	sorted := make([]sortEntry, n)
	for i := n - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(h).(sortEntry)
	}
	// Reverse to get ascending order
	for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}
	start := v.Offset
	if start >= len(sorted) {
		return &ViewResult{Rows: nil, Total: total}, nil
	}
	end := start + v.Limit
	if end > len(sorted) {
		end = len(sorted)
	}
	rows := make([]*Row, 0, end-start)
	for _, e := range sorted[start:end] {
		row, err := table.Get(e.id)
		if err != nil || row == nil {
			continue
		}
		rows = append(rows, row)
	}
	db.resolveIncludes(v, rows)
	return &ViewResult{Rows: rows, Total: total}, nil
}

// resolveIncludes resolves foreign key references for the given rows.
func (db *DB) resolveIncludes(v *ViewDef, rows []*Row) {
	if len(v.Includes) == 0 {
		return
	}
	schema := db.schemas[v.Table]
	for _, row := range rows {
		for _, inc := range v.Includes {
			for _, f := range schema.Fields {
				if f.Name == inc && f.Type == FieldRef {
					refID := toUint64(row.Data[inc])
					if refID > 0 {
						refTable := db.Table(f.RefTable)
						if refTable != nil {
							refRow, _ := refTable.Get(refID)
							if refRow != nil {
								row.Data["_ref_"+inc] = refRow.Data
							}
						}
					}
				}
			}
		}
	}
}

func matchesFilters(row *Row, filters []Filter) bool {
	for _, f := range filters {
		var val any
		if f.Field == "id" {
			val = row.ID
		} else {
			val = row.Data[f.Field]
		}
		switch f.Op {
		case OpEq:
			if !valuesEqual(val, f.Value) {
				return false
			}
		case OpNeq:
			if valuesEqual(val, f.Value) {
				return false
			}
		case OpGt:
			if compareValues(val, f.Value) <= 0 {
				return false
			}
		case OpGte:
			if compareValues(val, f.Value) < 0 {
				return false
			}
		case OpLt:
			if compareValues(val, f.Value) >= 0 {
				return false
			}
		case OpLte:
			if compareValues(val, f.Value) > 0 {
				return false
			}
		}
	}
	return true
}

func valuesEqual(a, b any) bool {
	return compareValues(a, b) == 0
}

func compareValues(a, b any) int {
	af := toFloat(a)
	bf := toFloat(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	// Try string comparison
	as := toString(a)
	bs := toString(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ToUint64 converts various numeric types to uint64.
func ToUint64(v any) uint64 {
	return toUint64(v)
}

func toUint64(v any) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int:
		return uint64(n)
	case int64:
		return uint64(n)
	case float64:
		return uint64(n)
	}
	return 0
}
