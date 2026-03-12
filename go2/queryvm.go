package flop

import (
	"encoding/binary"
	"math"
	"sort"
)

// QueryVM is a precompiled query plan that executes views without per-row
// map[string]any allocations. It operates directly on encoded row bytes,
// extracting only the fields needed for filtering and sorting.

// Op is a query VM opcode.
type Op uint8

const (
	// Scan strategies
	OpFullScan    Op = iota // scan entire BTree
	OpIndexLookup           // single-key index lookup (unique)
	OpIndexScan             // prefix scan on non-unique index
	OpReverseScan           // scan BTree in reverse

	// Field extraction (from raw encoded bytes)
	OpExtractFloat // extract float64 field → register
	OpExtractInt   // extract int64 field → register (stored as float64)
	OpExtractUint  // extract uint64 field → register (stored as float64)
	OpExtractStr   // extract string field → str register
	OpExtractID    // extract row ID → register

	// Comparisons (operate on registers)
	OpCmpEqF64  // register == float64 immediate
	OpCmpNeqF64 // register != float64 immediate
	OpCmpGtF64  // register > float64 immediate
	OpCmpGteF64 // register >= float64 immediate
	OpCmpLtF64  // register < float64 immediate
	OpCmpLteF64 // register <= float64 immediate
	OpCmpEqStr  // str register == string immediate
	OpCmpNeqStr // str register != string immediate

	// Dynamic comparisons (value resolved from ViewDef at runtime)
	OpCmpDynF64 // dynamic float comparison, Op stored in IntImm
	OpCmpDynStr // dynamic string comparison, Op stored in IntImm

	// Control flow
	OpSkipIfFalse // skip row if last comparison was false
	OpCollect     // add current row (id + sort value) to result set
	OpCollectAll  // add current row (id only, no sort) to result set

	// Post-processing
	OpSortAsc  // sort results ascending by collected sort value
	OpSortDesc // sort results descending by collected sort value
	OpOffset   // skip first N results
	OpLimit    // keep only N results
	OpDecode   // decode final row IDs to full Row objects
)

// Instruction is a single VM instruction.
type Instruction struct {
	Op        Op
	FieldName string  // for extract ops
	FloatImm  float64 // for comparison ops (static value)
	StrImm    string  // for string comparison ops (static value)
	IntImm    int     // for limit/offset, or sub-op for dynamic comparisons
	// Index lookup fields
	IdxRef      *StoreIndex
	IdxKey      []byte // pre-built key (when filter value known at compile time)
	FilterField string // field name for dynamic index key resolution
	FilterIdx   int    // index into ViewDef.Filters for runtime value (-1 = use IdxKey/FloatImm/StrImm)
}

// CompiledQuery is a precompiled query ready for execution.
type CompiledQuery struct {
	Instructions []Instruction
	SortField    string
	HasFilters   bool
	HasSort      bool
	NeedsDecode  bool
	// Cached schema info for fast field extraction
	encoder *RowEncoder
}

// CompileView compiles a ViewDef into a CompiledQuery.
// This is called once at registration time, not per request.
// Filter values may be nil at compile time (set per-request); the VM resolves them dynamically.
func CompileView(v *ViewDef, table *Table, db *DB) *CompiledQuery {
	cq := &CompiledQuery{
		encoder:     table.encoder,
		HasSort:     v.OrderBy != "",
		HasFilters:  len(v.Filters) > 0,
		SortField:   v.OrderBy,
		NeedsDecode: true,
	}

	var instrs []Instruction

	// --- Scan strategy ---
	scanOp := OpFullScan

	// Try to use an index for the primary filter
	indexUsed := false
	if len(v.Filters) > 0 && v.SearchQ == "" {
		table.ensureIndexes()
		for fi, f := range v.Filters {
			if f.Op != OpEq {
				continue
			}
			for _, idx := range table.indexes {
				if len(idx.Fields) != 1 || idx.Fields[0] != f.Field {
					continue
				}
				if idx.Unique {
					instr := Instruction{
						Op:          OpIndexLookup,
						IdxRef:      idx,
						FilterField: f.Field,
						FilterIdx:   fi,
					}
					if f.Value != nil {
						instr.IdxKey = buildIdxKey(idx.Fields, map[string]any{f.Field: normalizeForIndex(f.Value)})
						instr.FilterIdx = -1
					}
					instrs = append(instrs, instr)
					indexUsed = true
					for i, rf := range v.Filters {
						if i == fi {
							continue
						}
						instrs = append(instrs, compileFilter(rf, i)...)
					}
					break
				}
				if !idx.Unique {
					instr := Instruction{
						Op:          OpIndexScan,
						IdxRef:      idx,
						FilterField: f.Field,
						FilterIdx:   fi,
					}
					if f.Value != nil {
						instr.IdxKey = buildIdxKey(idx.Fields, map[string]any{f.Field: normalizeForIndex(f.Value)})
						instr.FilterIdx = -1
					}
					instrs = append(instrs, instr)
					indexUsed = true
					for i, rf := range v.Filters {
						if i == fi {
							continue
						}
						instrs = append(instrs, compileFilter(rf, i)...)
					}
					break
				}
			}
			if indexUsed {
				break
			}
		}
	}

	if !indexUsed {
		instrs = append(instrs, Instruction{Op: scanOp})
		// Add all filters
		for i, f := range v.Filters {
			instrs = append(instrs, compileFilter(f, i)...)
		}
	}

	// --- Collection ---
	if v.OrderBy != "" {
		instrs = append(instrs, Instruction{Op: OpCollect, FieldName: v.OrderBy})
	} else {
		instrs = append(instrs, Instruction{Op: OpCollectAll})
	}

	// --- Post-processing ---
	if v.OrderBy != "" {
		if v.Order == Desc {
			instrs = append(instrs, Instruction{Op: OpSortDesc})
		} else {
			instrs = append(instrs, Instruction{Op: OpSortAsc})
		}
	}

	if v.Offset > 0 {
		instrs = append(instrs, Instruction{Op: OpOffset, IntImm: v.Offset})
	}
	if v.Limit > 0 {
		instrs = append(instrs, Instruction{Op: OpLimit, IntImm: v.Limit})
	}

	instrs = append(instrs, Instruction{Op: OpDecode})

	cq.Instructions = instrs
	return cq
}

// compileFilter generates extraction + comparison instructions for a single filter.
// filterIdx is the position in ViewDef.Filters for dynamic value resolution.
func compileFilter(f Filter, filterIdx int) []Instruction {
	var instrs []Instruction

	// If value is nil at compile time, emit dynamic comparison ops
	if f.Value == nil {
		if f.Field == "id" {
			instrs = append(instrs, Instruction{Op: OpExtractID})
		} else {
			instrs = append(instrs, Instruction{Op: OpExtractFloat, FieldName: f.Field})
		}
		instrs = append(instrs, Instruction{
			Op:        OpCmpDynF64,
			IntImm:    int(f.Op),
			FilterIdx: filterIdx,
		})
		instrs = append(instrs, Instruction{Op: OpSkipIfFalse})
		return instrs
	}

	if f.Field == "id" {
		instrs = append(instrs, Instruction{Op: OpExtractID})
	} else {
		instrs = append(instrs, Instruction{Op: OpExtractFloat, FieldName: f.Field})
	}

	// String equality needs special handling
	if s, ok := f.Value.(string); ok && (f.Op == OpEq || f.Op == OpNeq) {
		instrs = instrs[:0]
		instrs = append(instrs, Instruction{Op: OpExtractStr, FieldName: f.Field})
		switch f.Op {
		case OpEq:
			instrs = append(instrs, Instruction{Op: OpCmpEqStr, StrImm: s})
		case OpNeq:
			instrs = append(instrs, Instruction{Op: OpCmpNeqStr, StrImm: s})
		}
		instrs = append(instrs, Instruction{Op: OpSkipIfFalse})
		return instrs
	}

	fv := toFloat(f.Value)
	switch f.Op {
	case OpEq:
		instrs = append(instrs, Instruction{Op: OpCmpEqF64, FloatImm: fv})
	case OpNeq:
		instrs = append(instrs, Instruction{Op: OpCmpNeqF64, FloatImm: fv})
	case OpGt:
		instrs = append(instrs, Instruction{Op: OpCmpGtF64, FloatImm: fv})
	case OpGte:
		instrs = append(instrs, Instruction{Op: OpCmpGteF64, FloatImm: fv})
	case OpLt:
		instrs = append(instrs, Instruction{Op: OpCmpLtF64, FloatImm: fv})
	case OpLte:
		instrs = append(instrs, Instruction{Op: OpCmpLteF64, FloatImm: fv})
	}

	instrs = append(instrs, Instruction{Op: OpSkipIfFalse})
	return instrs
}

// vmEntry holds a row reference collected during scan.
type vmEntry struct {
	id      uint64
	sortVal float64
}

// Execute runs the compiled query against the database.
// The ViewDef provides runtime filter values (set per-request by the server).
func (cq *CompiledQuery) Execute(table *Table, db *DB, v *ViewDef) (*ViewResult, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

	var results []vmEntry
	var scanInstr Instruction
	var filterInstrs []Instruction
	var collectInstr Instruction
	var postInstrs []Instruction

	// Parse instruction stream into phases
	phase := 0 // 0=scan, 1=filter, 2=collect, 3=post
	for _, instr := range cq.Instructions {
		switch {
		case phase == 0 && (instr.Op == OpFullScan || instr.Op == OpReverseScan || instr.Op == OpIndexLookup || instr.Op == OpIndexScan):
			scanInstr = instr
			phase = 1
		case instr.Op == OpCollect || instr.Op == OpCollectAll:
			collectInstr = instr
			phase = 3
		case phase < 3:
			filterInstrs = append(filterInstrs, instr)
		default:
			postInstrs = append(postInstrs, instr)
		}
	}

	// Resolve dynamic index key if needed
	if scanInstr.FilterIdx >= 0 && scanInstr.FilterIdx < len(v.Filters) {
		f := v.Filters[scanInstr.FilterIdx]
		if f.Value != nil {
			scanInstr.IdxKey = buildIdxKey(scanInstr.IdxRef.Fields, map[string]any{f.Field: normalizeForIndex(f.Value)})
		}
	}

	// --- Scan + Filter + Collect phase ---
	switch scanInstr.Op {
	case OpFullScan:
		results = cq.scanAndFilter(table, filterInstrs, collectInstr, false, v)
	case OpReverseScan:
		results = cq.scanAndFilter(table, filterInstrs, collectInstr, true, v)
	case OpIndexLookup:
		results = cq.indexLookupAndFilter(table, scanInstr, filterInstrs, collectInstr, v)
	case OpIndexScan:
		results = cq.indexScanAndFilter(table, scanInstr, filterInstrs, collectInstr, v)
	}

	total := len(results)

	// --- Post-processing ---
	for _, instr := range postInstrs {
		switch instr.Op {
		case OpSortAsc:
			sort.Slice(results, func(i, j int) bool {
				return results[i].sortVal < results[j].sortVal
			})
		case OpSortDesc:
			sort.Slice(results, func(i, j int) bool {
				return results[i].sortVal > results[j].sortVal
			})
		case OpOffset:
			if instr.IntImm < len(results) {
				results = results[instr.IntImm:]
			} else {
				results = nil
			}
		case OpLimit:
			if instr.IntImm < len(results) {
				results = results[:instr.IntImm]
			}
		case OpDecode:
			rows := make([]*Row, 0, len(results))
			for _, e := range results {
				row, err := table.get(e.id)
				if err != nil || row == nil {
					continue
				}
				rows = append(rows, row)
			}
			return &ViewResult{Rows: rows, Total: total}, nil
		}
	}

	return &ViewResult{Total: total}, nil
}

// scanAndFilter performs a full BTree scan, filtering and collecting in one pass.
func (cq *CompiledQuery) scanAndFilter(table *Table, filters []Instruction, collect Instruction, reverse bool, v *ViewDef) []vmEntry {
	var results []vmEntry

	scanFn := func(key, val []byte) bool {
		if !cq.evalFilters(val, filters, v) {
			return true
		}
		id := binary.BigEndian.Uint64(val[0:8])
		if collect.Op == OpCollect {
			_, sv, _ := cq.encoder.ExtractSortFloat(val, collect.FieldName)
			results = append(results, vmEntry{id: id, sortVal: sv})
		} else {
			results = append(results, vmEntry{id: id})
		}
		return true
	}

	if reverse {
		table.primary.ScanReverse(scanFn)
	} else {
		table.primary.Scan(scanFn)
	}
	return results
}

// indexLookupAndFilter does a unique index lookup then filters remaining predicates.
func (cq *CompiledQuery) indexLookupAndFilter(table *Table, scan Instruction, filters []Instruction, collect Instruction, v *ViewDef) []vmEntry {
	rawID := scan.IdxRef.store.Get(scan.IdxKey)
	if rawID == nil {
		return nil
	}
	rowID := DecodeUint64(rawID)
	rowKey := EncodeUint64(rowID)
	val, err := table.primary.Get(rowKey)
	if err != nil || val == nil {
		return nil
	}
	if !cq.evalFilters(val, filters, v) {
		return nil
	}
	if collect.Op == OpCollect {
		_, sv, _ := cq.encoder.ExtractSortFloat(val, collect.FieldName)
		return []vmEntry{{id: rowID, sortVal: sv}}
	}
	return []vmEntry{{id: rowID}}
}

// indexScanAndFilter does a non-unique index prefix scan then filters.
func (cq *CompiledQuery) indexScanAndFilter(table *Table, scan Instruction, filters []Instruction, collect Instruction, v *ViewDef) []vmEntry {
	var results []vmEntry
	prefix := scan.IdxKey
	scan.IdxRef.store.ScanPrefix(prefix, func(key, val []byte) bool {
		if len(key) < 8 {
			return true
		}
		rowID := DecodeUint64(key[len(prefix):])
		rowKey := EncodeUint64(rowID)
		rowVal, err := table.primary.Get(rowKey)
		if err != nil || rowVal == nil {
			return true
		}
		if !cq.evalFilters(rowVal, filters, v) {
			return true
		}
		if collect.Op == OpCollect {
			_, sv, _ := cq.encoder.ExtractSortFloat(rowVal, collect.FieldName)
			results = append(results, vmEntry{id: rowID, sortVal: sv})
		} else {
			results = append(results, vmEntry{id: rowID})
		}
		return true
	})
	return results
}

// evalFilters runs the filter instruction sequence against raw row bytes.
// The ViewDef provides runtime filter values for dynamic comparisons.
func (cq *CompiledQuery) evalFilters(val []byte, filters []Instruction, v *ViewDef) bool {
	var fReg float64
	var sReg string
	var cmpResult bool

	for _, instr := range filters {
		switch instr.Op {
		case OpExtractID:
			if len(val) < 8 {
				return false
			}
			fReg = float64(binary.BigEndian.Uint64(val[0:8]))

		case OpExtractFloat:
			_, sv, ok := cq.encoder.ExtractSortFloat(val, instr.FieldName)
			if !ok {
				fReg = 0
			} else {
				fReg = sv
			}

		case OpExtractStr:
			sReg = cq.extractString(val, instr.FieldName)

		case OpCmpEqF64:
			cmpResult = fReg == instr.FloatImm
		case OpCmpNeqF64:
			cmpResult = fReg != instr.FloatImm
		case OpCmpGtF64:
			cmpResult = fReg > instr.FloatImm
		case OpCmpGteF64:
			cmpResult = fReg >= instr.FloatImm
		case OpCmpLtF64:
			cmpResult = fReg < instr.FloatImm
		case OpCmpLteF64:
			cmpResult = fReg <= instr.FloatImm
		case OpCmpEqStr:
			cmpResult = sReg == instr.StrImm
		case OpCmpNeqStr:
			cmpResult = sReg != instr.StrImm

		case OpCmpDynF64:
			// Resolve value from ViewDef at runtime
			if instr.FilterIdx >= 0 && instr.FilterIdx < len(v.Filters) {
				dynVal := toFloat(v.Filters[instr.FilterIdx].Value)
				op := FilterOp(instr.IntImm)
				switch op {
				case OpEq:
					cmpResult = fReg == dynVal
				case OpNeq:
					cmpResult = fReg != dynVal
				case OpGt:
					cmpResult = fReg > dynVal
				case OpGte:
					cmpResult = fReg >= dynVal
				case OpLt:
					cmpResult = fReg < dynVal
				case OpLte:
					cmpResult = fReg <= dynVal
				default:
					cmpResult = false
				}
			}

		case OpCmpDynStr:
			if instr.FilterIdx >= 0 && instr.FilterIdx < len(v.Filters) {
				dynStr := toString(v.Filters[instr.FilterIdx].Value)
				op := FilterOp(instr.IntImm)
				switch op {
				case OpEq:
					cmpResult = sReg == dynStr
				case OpNeq:
					cmpResult = sReg != dynStr
				default:
					cmpResult = false
				}
			}

		case OpSkipIfFalse:
			if !cmpResult {
				return false
			}
		}
	}
	return true
}

// extractString extracts a string field from raw encoded row bytes without full decode.
func (cq *CompiledQuery) extractString(data []byte, fieldName string) string {
	if len(data) < 34 {
		return ""
	}
	offset := 32 // skip ID + CreatedAt + UpdatedAt + Version
	numFields := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	fnLen := len(fieldName)
	for i := 0; i < numFields; i++ {
		if offset+2 > len(data) {
			return ""
		}
		nameLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		match := nameLen == fnLen
		if match {
			for j := 0; j < fnLen; j++ {
				if offset+j >= len(data) || data[offset+j] != fieldName[j] {
					match = false
					break
				}
			}
		}
		offset += nameLen

		if offset+4 > len(data) {
			return ""
		}
		dataLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		if match {
			if dataLen == 0 {
				return ""
			}
			end := offset + dataLen
			if end > len(data) {
				return ""
			}
			b := data[offset:end]
			if len(b) > 0 && b[len(b)-1] == 0 {
				return string(b[:len(b)-1])
			}
			return string(b)
		}
		offset += dataLen
	}
	return ""
}

// extractSortValue extracts a numeric value for sorting from raw bytes.
func extractSortValue(encoder *RowEncoder, val []byte, field string) float64 {
	if field == "id" {
		if len(val) >= 8 {
			return float64(binary.BigEndian.Uint64(val[0:8]))
		}
		return 0
	}
	_, sv, _ := encoder.ExtractSortFloat(val, field)
	return sv
}

// --- Heap-based top-K for VM ---

type vmMinHeap []vmEntry

func (h vmMinHeap) Len() int            { return len(h) }
func (h vmMinHeap) Less(i, j int) bool  { return h[i].sortVal < h[j].sortVal }
func (h vmMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *vmMinHeap) Push(x any)         { *h = append(*h, x.(vmEntry)) }
func (h *vmMinHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

type vmMaxHeap []vmEntry

func (h vmMaxHeap) Len() int            { return len(h) }
func (h vmMaxHeap) Less(i, j int) bool  { return h[i].sortVal > h[j].sortVal }
func (h vmMaxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *vmMaxHeap) Push(x any)         { *h = append(*h, x.(vmEntry)) }
func (h *vmMaxHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

var _ = math.MaxFloat64
