package flop

import (
	"bytes"
	"encoding/binary"
)

// BTree is a B+ tree backed by a Pager.
// Keys and values are arbitrary byte slices.
// Leaf nodes store key-value pairs.
// Internal nodes store keys and child page IDs.
type BTree struct {
	pager      *Pager
	rootPageID uint64
	order      int // max entries per node before split
}

const btreeOrder = 64 // tuned for 4KB pages with typical key sizes

func NewBTree(pager *Pager, rootPageID uint64) *BTree {
	return &BTree{
		pager:      pager,
		rootPageID: rootPageID,
		order:      btreeOrder,
	}
}

// Get retrieves a value by key. Returns nil if not found.
func (bt *BTree) Get(key []byte) ([]byte, error) {
	if bt.rootPageID == 0 {
		return nil, nil
	}
	return bt.search(bt.rootPageID, key)
}

func (bt *BTree) search(pageID uint64, key []byte) ([]byte, error) {
	pg, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	n := int(pg.NumEntries())
	if pg.Type() == PageLeaf {
		// Binary search in leaf
		lo, hi := 0, n-1
		for lo <= hi {
			mid := (lo + hi) / 2
			k, v := pg.EntryAt(mid)
			cmp := bytes.Compare(key, k)
			if cmp == 0 {
				result := make([]byte, len(v))
				copy(result, v)
				return result, nil
			} else if cmp < 0 {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		return nil, nil
	}

	// Internal node: find child
	childIdx := n // default to last child
	for i := 0; i < n; i++ {
		k, _ := pg.EntryAt(i)
		if bytes.Compare(key, k) < 0 {
			childIdx = i
			break
		}
	}

	// Child pointer is stored in the value of the entry (or first child ptr for leftmost)
	var childPageID uint64
	if childIdx == 0 {
		// First child pointer stored in overflow field
		childPageID = pg.OverflowID()
	} else {
		_, v := pg.EntryAt(childIdx - 1)
		childPageID = binary.BigEndian.Uint64(v)
	}

	return bt.search(childPageID, key)
}

// Put inserts or updates a key-value pair. Returns the new root page ID.
func (bt *BTree) Put(key, val []byte) (uint64, error) {
	if bt.rootPageID == 0 {
		// Create first leaf
		leaf := bt.pager.AllocPage(PageLeaf)
		leaf.AppendEntry(key, val)
		bt.pager.WritePage(leaf)
		bt.rootPageID = leaf.PageID()
		return bt.rootPageID, nil
	}

	newKey, newChildID, err := bt.insert(bt.rootPageID, key, val)
	if err != nil {
		return bt.rootPageID, err
	}

	if newKey != nil {
		// Root was split, create new root
		newRoot := bt.pager.AllocPage(PageInternal)
		newRoot.SetOverflowID(bt.rootPageID) // left child
		childBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(childBuf, newChildID)
		newRoot.AppendEntry(newKey, childBuf)
		bt.pager.WritePage(newRoot)
		bt.rootPageID = newRoot.PageID()
	}

	return bt.rootPageID, nil
}

// insert returns (splitKey, splitChildPageID) if the node was split, or (nil, 0) if not.
func (bt *BTree) insert(pageID uint64, key, val []byte) ([]byte, uint64, error) {
	pg, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, 0, err
	}

	// Copy-on-write: always work on a copy
	cow := bt.copyPage(pg)

	if cow.Type() == PageLeaf {
		return bt.insertLeaf(cow, key, val)
	}

	return bt.insertInternal(cow, key, val)
}

func (bt *BTree) insertLeaf(pg *Page, key, val []byte) ([]byte, uint64, error) {
	n := int(pg.NumEntries())

	// Find insertion point
	idx := 0
	for idx < n {
		k, _ := pg.EntryAt(idx)
		cmp := bytes.Compare(key, k)
		if cmp == 0 {
			// Update existing
			pg.SetEntryAt(idx, key, val)
			bt.pager.WritePage(pg)
			return nil, 0, nil
		} else if cmp < 0 {
			break
		}
		idx++
	}

	// Try to insert
	if pg.FreeSpace() >= 4+len(key)+len(val) {
		keys, vals := pg.AllEntries()
		pg.ClearEntries()
		for i := 0; i < idx; i++ {
			pg.AppendEntry(keys[i], vals[i])
		}
		pg.AppendEntry(key, val)
		for i := idx; i < n; i++ {
			pg.AppendEntry(keys[i], vals[i])
		}
		bt.pager.WritePage(pg)
		return nil, 0, nil
	}

	// Need to split
	return bt.splitLeaf(pg, key, val, idx)
}

func (bt *BTree) splitLeaf(pg *Page, key, val []byte, insertIdx int) ([]byte, uint64, error) {
	keys, vals := pg.AllEntries()

	// Insert new entry into the collected entries
	allKeys := make([][]byte, 0, len(keys)+1)
	allVals := make([][]byte, 0, len(vals)+1)
	for i := 0; i < len(keys); i++ {
		if i == insertIdx {
			allKeys = append(allKeys, key)
			allVals = append(allVals, val)
		}
		allKeys = append(allKeys, keys[i])
		allVals = append(allVals, vals[i])
	}
	if insertIdx == len(keys) {
		allKeys = append(allKeys, key)
		allVals = append(allVals, val)
	}

	mid := len(allKeys) / 2

	// Left page gets first half
	pg.ClearEntries()
	for i := 0; i < mid; i++ {
		pg.AppendEntry(allKeys[i], allVals[i])
	}
	bt.pager.WritePage(pg)

	// Right page gets second half
	right := bt.pager.AllocPage(PageLeaf)
	for i := mid; i < len(allKeys); i++ {
		right.AppendEntry(allKeys[i], allVals[i])
	}
	bt.pager.WritePage(right)

	// Promote the first key of the right page
	splitKey := make([]byte, len(allKeys[mid]))
	copy(splitKey, allKeys[mid])

	return splitKey, right.PageID(), nil
}

func (bt *BTree) insertInternal(pg *Page, key, val []byte) ([]byte, uint64, error) {
	n := int(pg.NumEntries())

	// Find child to descend into
	childIdx := n
	for i := 0; i < n; i++ {
		k, _ := pg.EntryAt(i)
		if bytes.Compare(key, k) < 0 {
			childIdx = i
			break
		}
	}

	var childPageID uint64
	if childIdx == 0 {
		childPageID = pg.OverflowID()
	} else {
		_, v := pg.EntryAt(childIdx - 1)
		childPageID = binary.BigEndian.Uint64(v)
	}

	newKey, newChildPageID, err := bt.insert(childPageID, key, val)
	if err != nil {
		return nil, 0, err
	}

	if newKey == nil {
		// No split below, just update our child pointer (CoW may have changed it)
		bt.pager.WritePage(pg)
		return nil, 0, nil
	}

	// Child was split, insert new key + right child pointer
	childBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(childBuf, newChildPageID)

	if pg.FreeSpace() >= 4+len(newKey)+8 {
		keys, vals := pg.AllEntries()
		pg.ClearEntries()
		inserted := false
		for i := 0; i < n; i++ {
			if !inserted && bytes.Compare(newKey, keys[i]) < 0 {
				pg.AppendEntry(newKey, childBuf)
				inserted = true
			}
			pg.AppendEntry(keys[i], vals[i])
		}
		if !inserted {
			pg.AppendEntry(newKey, childBuf)
		}
		bt.pager.WritePage(pg)
		return nil, 0, nil
	}

	// Split internal node
	return bt.splitInternal(pg, newKey, childBuf, childIdx)
}

func (bt *BTree) splitInternal(pg *Page, newKey, newChildPtr []byte, insertAfter int) ([]byte, uint64, error) {
	keys, vals := pg.AllEntries()

	allKeys := make([][]byte, 0, len(keys)+1)
	allVals := make([][]byte, 0, len(vals)+1)
	inserted := false
	for i := 0; i < len(keys); i++ {
		if !inserted && bytes.Compare(newKey, keys[i]) < 0 {
			allKeys = append(allKeys, newKey)
			allVals = append(allVals, newChildPtr)
			inserted = true
		}
		allKeys = append(allKeys, keys[i])
		allVals = append(allVals, vals[i])
	}
	if !inserted {
		allKeys = append(allKeys, newKey)
		allVals = append(allVals, newChildPtr)
	}

	mid := len(allKeys) / 2
	promoteKey := make([]byte, len(allKeys[mid]))
	copy(promoteKey, allKeys[mid])

	// Left gets [0, mid)
	pg.ClearEntries()
	for i := 0; i < mid; i++ {
		pg.AppendEntry(allKeys[i], allVals[i])
	}
	bt.pager.WritePage(pg)

	// Right gets (mid, end], with OverflowID = child pointer from promoted key
	right := bt.pager.AllocPage(PageInternal)
	right.SetOverflowID(binary.BigEndian.Uint64(allVals[mid]))
	for i := mid + 1; i < len(allKeys); i++ {
		right.AppendEntry(allKeys[i], allVals[i])
	}
	bt.pager.WritePage(right)

	return promoteKey, right.PageID(), nil
}

// Delete removes a key. Returns the new root page ID.
func (bt *BTree) Delete(key []byte) (uint64, error) {
	if bt.rootPageID == 0 {
		return 0, nil
	}

	_, err := bt.delete(bt.rootPageID, key)
	if err != nil {
		return bt.rootPageID, err
	}

	// Check if root is empty internal node
	if bt.rootPageID != 0 {
		pg, err := bt.pager.ReadPage(bt.rootPageID)
		if err != nil {
			return bt.rootPageID, err
		}
		if pg.Type() == PageInternal && pg.NumEntries() == 0 {
			bt.rootPageID = pg.OverflowID()
		}
	}

	return bt.rootPageID, nil
}

func (bt *BTree) delete(pageID uint64, key []byte) (bool, error) {
	pg, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return false, err
	}

	cow := bt.copyPage(pg)

	if cow.Type() == PageLeaf {
		n := int(cow.NumEntries())
		for i := 0; i < n; i++ {
			k, _ := cow.EntryAt(i)
			if bytes.Equal(key, k) {
				cow.RemoveEntryAt(i)
				bt.pager.WritePage(cow)
				return true, nil
			}
		}
		return false, nil
	}

	// Internal node
	n := int(cow.NumEntries())
	childIdx := n
	for i := 0; i < n; i++ {
		k, _ := cow.EntryAt(i)
		if bytes.Compare(key, k) < 0 {
			childIdx = i
			break
		}
	}

	var childPageID uint64
	if childIdx == 0 {
		childPageID = cow.OverflowID()
	} else {
		_, v := cow.EntryAt(childIdx - 1)
		childPageID = binary.BigEndian.Uint64(v)
	}

	found, err := bt.delete(childPageID, key)
	if err != nil || !found {
		return found, err
	}

	bt.pager.WritePage(cow)
	return true, nil
}

// copyPage creates a writable copy of a page (same page ID, in-place through cache).
// Safe for single-writer architecture — the cache acts as a write buffer.
func (bt *BTree) copyPage(pg *Page) *Page {
	cow := &Page{}
	*cow = *pg
	// Keep same page ID — we're modifying in-place through the cache
	bt.pager.CachePage(cow)
	return cow
}

// Scan iterates over all key-value pairs in sorted order.
func (bt *BTree) Scan(fn func(key, val []byte) bool) error {
	if bt.rootPageID == 0 {
		return nil
	}
	return bt.scan(bt.rootPageID, fn)
}

func (bt *BTree) scan(pageID uint64, fn func(key, val []byte) bool) error {
	pg, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return err
	}

	if pg.Type() == PageLeaf {
		n := int(pg.NumEntries())
		for i := 0; i < n; i++ {
			k, v := pg.EntryAt(i)
			if !fn(k, v) {
				return nil
			}
		}
		return nil
	}

	// Internal node: traverse children in order
	n := int(pg.NumEntries())

	// First child (leftmost)
	if err := bt.scan(pg.OverflowID(), fn); err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		_, v := pg.EntryAt(i)
		childID := binary.BigEndian.Uint64(v)
		if err := bt.scan(childID, fn); err != nil {
			return err
		}
	}

	return nil
}

// ScanRange iterates over key-value pairs in [start, end) range.
func (bt *BTree) ScanRange(start, end []byte, fn func(key, val []byte) bool) error {
	if bt.rootPageID == 0 {
		return nil
	}
	return bt.scanRange(bt.rootPageID, start, end, fn)
}

func (bt *BTree) scanRange(pageID uint64, start, end []byte, fn func(key, val []byte) bool) error {
	pg, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return err
	}

	if pg.Type() == PageLeaf {
		n := int(pg.NumEntries())
		for i := 0; i < n; i++ {
			k, v := pg.EntryAt(i)
			if start != nil && bytes.Compare(k, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(k, end) >= 0 {
				return nil
			}
			if !fn(k, v) {
				return nil
			}
		}
		return nil
	}

	n := int(pg.NumEntries())

	// Check leftmost child
	if start == nil || (n > 0 && func() bool {
		k, _ := pg.EntryAt(0)
		return bytes.Compare(start, k) < 0
	}()) {
		if err := bt.scanRange(pg.OverflowID(), start, end, fn); err != nil {
			return err
		}
	}

	for i := 0; i < n; i++ {
		k, v := pg.EntryAt(i)
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		childID := binary.BigEndian.Uint64(v)
		if start != nil && i+1 < n {
			nextK, _ := pg.EntryAt(i + 1)
			if bytes.Compare(start, nextK) >= 0 {
				continue
			}
		}
		if err := bt.scanRange(childID, start, end, fn); err != nil {
			return err
		}
	}

	return nil
}

// Count returns the total number of entries.
func (bt *BTree) Count() (int, error) {
	count := 0
	err := bt.Scan(func(key, val []byte) bool {
		count++
		return true
	})
	return count, err
}
