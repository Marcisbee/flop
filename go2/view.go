package flop

import (
	"container/heap"
	"fmt"
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
	compiled   *CompiledQuery // precompiled bytecode (set by CompileViews)
}

// ViewResult holds the result of executing a view.
type ViewResult struct {
	Rows  []*Row
	Total int
}

// AutoIndex analyzes a view's filters and creates any missing secondary indexes
// on the table. Called automatically at view registration time.
func (db *DB) AutoIndex(v *ViewDef) {
	table := db.Table(v.Table)
	if table == nil {
		return
	}

	for _, f := range v.Filters {
		if f.Field == "id" {
			continue // primary key, no index needed
		}
		switch f.Op {
		case OpEq, OpGt, OpGte, OpLt, OpLte:
			// Check if an index already covers this field
			found := false
			for _, idx := range table.indexes {
				if len(idx.Fields) == 1 && idx.Fields[0] == f.Field {
					found = true
					break
				}
			}
			if !found {
				idx := &StoreIndex{
					Name:   f.Field + "_auto",
					Fields: []string{f.Field},
					Unique: false,
					store:  NewStore(""),
				}
				table.indexes = append(table.indexes, idx)
				// Populate immediately if table already has data
				if table.primary.RootPageID() != 0 {
					table.BuildIndex(idx)
				}
			}
		}
	}
}

// CompileView precompiles a view into bytecode for faster execution.
// Call this once at registration time. If the view uses features not supported
// by the VM (PermFilter, SearchQ, Includes, OpIn, OpContains), compilation
// is skipped and ExecuteView falls back to the interpreted path.
func (db *DB) CompileView(v *ViewDef) {
	// Auto-create indexes based on view filters
	db.AutoIndex(v)

	// Skip compilation for views the VM can't handle
	if v.PermFilter != nil || v.SearchQ != "" {
		return
	}
	for _, f := range v.Filters {
		if f.Op == OpIn || f.Op == OpContains {
			return
		}
	}
	table := db.Table(v.Table)
	if table == nil {
		return
	}
	v.compiled = CompileView(v, table, db)
}

// RegisterQuery pre-compiles a named ViewDef for later use with DB.Query().
// Unlike RegisterView on Server, this does NOT create an HTTP endpoint.
// Use this when you need precompiled queries inside custom handlers.
func (db *DB) RegisterQuery(name string, v *ViewDef) {
	v.Name = name
	db.CompileView(v)
	if db.queries == nil {
		db.queries = make(map[string]*ViewDef)
	}
	db.queries[name] = v
}

// Query executes a pre-registered query by name with dynamic filter values.
// Pass params as field→value pairs. Returns rows matching the query.
func (db *DB) Query(name string, params map[string]any) ([]*Row, error) {
	v, ok := db.queries[name]
	if !ok {
		return nil, fmt.Errorf("query %q not registered", name)
	}

	// Clone and set filter values from params
	vCopy := *v
	vCopy.Filters = make([]Filter, len(v.Filters))
	copy(vCopy.Filters, v.Filters)
	for i := range vCopy.Filters {
		if val, ok := params[vCopy.Filters[i].Field]; ok {
			vCopy.Filters[i].Value = val
		}
	}

	// Override limit/offset from params if provided
	if lim, ok := params["_limit"]; ok {
		vCopy.Limit = int(toFloat(lim))
	}
	if off, ok := params["_offset"]; ok {
		vCopy.Offset = int(toFloat(off))
	}

	result, err := db.ExecuteView(&vCopy)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

// ExecuteView runs a precompiled view against the database.
func (db *DB) ExecuteView(v *ViewDef) (*ViewResult, error) {
	table := db.Table(v.Table)
	if table == nil {
		return nil, nil
	}

	// Fast path: use bytecode VM if the view was precompiled
	// and doesn't have dynamic features (PermFilter, SearchQ)
	if v.compiled != nil && v.PermFilter == nil && v.SearchQ == "" {
		result, err := v.compiled.Execute(table, db, v)
		if err != nil {
			return nil, err
		}
		// Apply dynamic limit/offset set via Query() params that weren't
		// baked into the compiled bytecode.
		if v.Offset > 0 || v.Limit > 0 {
			rows := result.Rows
			if v.Offset > 0 {
				if v.Offset < len(rows) {
					rows = rows[v.Offset:]
				} else {
					rows = nil
				}
			}
			if v.Limit > 0 && len(rows) > v.Limit {
				rows = rows[:v.Limit]
			}
			result.Rows = rows
		}
		if len(v.Includes) > 0 {
			db.resolveIncludes(v, result.Rows)
		}
		return result, nil
	}

	// Index lookup path: filters match a secondary index → skip full scan.
	if v.SearchQ == "" && len(v.Filters) > 0 {
		if rows, ok := db.executeViewIndexed(v, table); ok {
			total := len(rows)

			// Sort if needed
			if v.OrderBy != "" {
				sort.Slice(rows, func(i, j int) bool {
					var a, b any
					if v.OrderBy == "id" {
						a, b = rows[i].ID, rows[j].ID
					} else {
						a = rows[i].Data[v.OrderBy]
						b = rows[j].Data[v.OrderBy]
					}
					cmp := compareValues(a, b)
					if v.Order == Desc {
						return cmp > 0
					}
					return cmp < 0
				})
			}

			// Pagination
			if v.Offset > 0 && v.Offset < len(rows) {
				rows = rows[v.Offset:]
			} else if v.Offset >= len(rows) {
				rows = nil
			}
			if v.Limit > 0 && len(rows) > v.Limit {
				rows = rows[:v.Limit]
			}

			db.resolveIncludes(v, rows)
			return &ViewResult{Rows: rows, Total: total}, nil
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
			// Verify all remaining filters and permission match
			if matchesFilters(row, v.Filters) {
				if v.PermFilter == nil || v.PermFilter(nil, row) {
					return []*Row{row}, true
				}
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
					if v.PermFilter == nil || v.PermFilter(nil, row) {
						rows = append(rows, row)
					}
				}
				return v.Limit == 0 || len(rows) < v.Limit
			})
			return rows, true
		}
	}

	// Check for range filters (OpGt, OpGte, OpLt, OpLte) on indexed fields
	if rows, ok := db.executeViewRangeIndexed(v, table); ok {
		return rows, true
	}

	return nil, false
}

// executeViewRangeIndexed uses a secondary index for range filter lookups.
// Handles single-field range filters like field > X, field >= X, field < Y, field <= Y,
// and combined ranges like field >= X AND field < Y.
func (db *DB) executeViewRangeIndexed(v *ViewDef, table *Table) ([]*Row, bool) {
	// Group range filters by field
	type rangeBound struct {
		field                            string
		low, high                        any
		includeLow, includeHigh          bool
		hasLow, hasHigh                  bool
		otherFilters                     []Filter
	}

	bounds := make(map[string]*rangeBound)
	var otherFilters []Filter

	for _, f := range v.Filters {
		switch f.Op {
		case OpGt, OpGte, OpLt, OpLte:
			b, ok := bounds[f.Field]
			if !ok {
				b = &rangeBound{field: f.Field}
				bounds[f.Field] = b
			}
			switch f.Op {
			case OpGt:
				b.low = f.Value
				b.includeLow = false
				b.hasLow = true
			case OpGte:
				b.low = f.Value
				b.includeLow = true
				b.hasLow = true
			case OpLt:
				b.high = f.Value
				b.includeHigh = false
				b.hasHigh = true
			case OpLte:
				b.high = f.Value
				b.includeHigh = true
				b.hasHigh = true
			}
		default:
			otherFilters = append(otherFilters, f)
		}
	}

	if len(bounds) == 0 {
		return nil, false
	}

	// Try the first range-filterable field that has an index
	for _, b := range bounds {
		var rows []*Row
		used := table.ScanByFieldRange(b.field, b.low, b.high, b.includeLow, b.includeHigh, func(row *Row) bool {
			if len(otherFilters) > 0 && !matchesFilters(row, otherFilters) {
				return true
			}
			if v.PermFilter != nil && !v.PermFilter(nil, row) {
				return true
			}
			rows = append(rows, row)
			// If no sort needed, can stop at limit
			if v.OrderBy == "" && v.Limit > 0 && len(rows) >= v.Limit+v.Offset {
				return false
			}
			return true
		})
		if used {
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
	case bool:
		if n {
			return 1
		}
		return 0
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
