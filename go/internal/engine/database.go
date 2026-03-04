package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/marcisbee/flop/internal/failpoint"
	"github.com/marcisbee/flop/internal/reqtrace"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

// DatabaseConfig holds configuration for the database.
type DatabaseConfig struct {
	DataDir               string
	MaxCachePages         int
	SyncMode              string // "full" or "normal"
	AsyncSecondaryIndexes bool
}

// Database manages all table instances and coordinates group commit.
type Database struct {
	Tables                map[string]*TableInstance
	dataDir               string
	meta                  *schema.StoredMeta
	pubsub                *PubSub
	opened                bool
	syncMode              string
	asyncSecondaryIndexes bool

	maxCachePages int

	// Group commit
	commitMu       sync.Mutex
	commitQueue    []commitSlot
	commitDraining bool
}

type commitSlot struct {
	walBuffers map[string]*walBufEntry
	done       chan error
}

// WalBufEntry holds buffered WAL records for a single table.
// Exported so Bridge can manage transaction buffers.
type WalBufEntry = walBufEntry

type walBufEntry struct {
	records [][]byte
	txIDs   []uint32
}

func NewDatabase(config DatabaseConfig) *Database {
	if config.DataDir == "" {
		config.DataDir = "./data"
	}
	if config.SyncMode == "" {
		config.SyncMode = "full"
	}
	if config.MaxCachePages == 0 {
		config.MaxCachePages = 256
	}
	return &Database{
		Tables:                make(map[string]*TableInstance),
		dataDir:               config.DataDir,
		syncMode:              config.SyncMode,
		asyncSecondaryIndexes: config.AsyncSecondaryIndexes,
		maxCachePages:         config.MaxCachePages,
		pubsub:                NewPubSub(),
	}
}

// Open initializes the database, opens all tables, replays WALs.
func (db *Database) Open(tableDefs map[string]*schema.TableDef) error {
	if db.opened {
		return nil
	}
	db.opened = true

	if err := os.MkdirAll(db.dataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(db.dataDir, "_files"), 0755); err != nil {
		return fmt.Errorf("create files dir: %w", err)
	}

	metaPath := filepath.Join(db.dataDir, "_meta.flop")
	meta, err := storage.ReadMetaFile(metaPath)
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}
	db.meta = meta

	for name, def := range tableDefs {
		instance, err := newTableInstance(name, def, db)
		if err != nil {
			return fmt.Errorf("init table %q: %w", name, err)
		}
		if err := instance.open(db.dataDir, db.meta, db.pubsub, db.maxCachePages); err != nil {
			return fmt.Errorf("open table %q: %w", name, err)
		}
		db.Tables[name] = instance
	}

	// Save meta (may have new schema versions)
	if err := storage.WriteMetaFile(metaPath, db.meta); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	return nil
}

func (db *Database) GetTable(name string) *TableInstance {
	return db.Tables[name]
}

func (db *Database) GetPubSub() *PubSub {
	return db.pubsub
}

func (db *Database) GetDataDir() string {
	return db.dataDir
}

func (db *Database) GetMeta() *schema.StoredMeta {
	return db.meta
}

// GetAuthTable returns the table with Auth=true.
func (db *Database) GetAuthTable() *TableInstance {
	for _, t := range db.Tables {
		if t.def.Auth {
			return t
		}
	}
	return nil
}

// EnqueueCommit buffers WAL records for group commit.
// In "full" syncMode, uses the commit queue to batch fsyncs.
// In "normal" syncMode, flushes WAL records directly (no fsync needed).
// The locksHeld parameter indicates the caller already holds table locks (non-tx single-op mode).
func (db *Database) EnqueueCommit(walBuffers map[string]*walBufEntry) error {
	if db.syncMode != "full" {
		return db.directFlush(walBuffers, false)
	}
	return db.enqueueCommitQueued(walBuffers)
}

// EnqueueCommitLocked is like EnqueueCommit but the caller already holds the table lock.
// Used by Insert/Update/Delete for non-transaction single-table operations.
func (db *Database) EnqueueCommitLocked(walBuffers map[string]*walBufEntry) error {
	if db.syncMode != "full" {
		return db.directFlush(walBuffers, true)
	}
	return db.enqueueCommitQueued(walBuffers)
}

// directFlush writes WAL records directly without the commit queue.
// If locksHeld is true, skips table lock acquisition (caller already holds it).
func (db *Database) directFlush(walBuffers map[string]*walBufEntry, locksHeld bool) error {
	for tableName, entry := range walBuffers {
		table := db.Tables[tableName]
		if table == nil {
			continue
		}
		if !locksHeld {
			table.mu.Lock()
		}
		err := table.wal.FlushBatch(entry.records, entry.txIDs)
		table.walEntryCount += len(entry.records) + len(entry.txIDs)
		needCheckpoint := table.walEntryCount >= walCheckpointThreshold
		if needCheckpoint {
			table.walEntryCount = 0
		}
		if !locksHeld {
			table.mu.Unlock()
		}
		if err != nil {
			return err
		}
		if needCheckpoint {
			go table.Checkpoint()
		}
	}
	return nil
}

func (db *Database) enqueueCommitQueued(walBuffers map[string]*walBufEntry) error {
	slot := commitSlot{
		walBuffers: walBuffers,
		done:       make(chan error, 1),
	}

	db.commitMu.Lock()
	db.commitQueue = append(db.commitQueue, slot)
	shouldDrain := !db.commitDraining
	if shouldDrain {
		db.commitDraining = true
	}
	db.commitMu.Unlock()

	if shouldDrain {
		go db.drainCommitQueue()
	}

	return <-slot.done
}

func (db *Database) drainCommitQueue() {
	for {
		db.commitMu.Lock()
		if len(db.commitQueue) == 0 {
			db.commitDraining = false
			db.commitMu.Unlock()
			return
		}
		batch := db.commitQueue
		db.commitQueue = nil
		db.commitMu.Unlock()

		// Merge all WAL buffers by table
		merged := make(map[string]*walBufEntry)
		for _, slot := range batch {
			for tableName, entry := range slot.walBuffers {
				m := merged[tableName]
				if m == nil {
					m = &walBufEntry{}
					merged[tableName] = m
				}
				m.records = append(m.records, entry.records...)
				m.txIDs = append(m.txIDs, entry.txIDs...)
			}
		}

		// Flush all WALs in parallel
		var flushErr error
		doFsync := db.syncMode == "full"
		var checkpointTables []*TableInstance

		if len(merged) == 1 {
			// Single table — no goroutine overhead
			for tableName, entry := range merged {
				table := db.Tables[tableName]
				if table == nil {
					continue
				}
				if err := table.wal.FlushBatch(entry.records, entry.txIDs); err != nil {
					flushErr = err
				} else if doFsync {
					if err := table.wal.Fsync(); err != nil {
						flushErr = err
					}
				}
				table.walEntryCount += len(entry.records) + len(entry.txIDs)
				if table.walEntryCount >= walCheckpointThreshold {
					checkpointTables = append(checkpointTables, table)
				}
			}
		} else {
			// Multiple tables — flush + fsync in parallel
			type tableResult struct {
				table          *TableInstance
				entryCount     int
				needCheckpoint bool
				err            error
			}
			results := make(chan tableResult, len(merged))
			for tableName, entry := range merged {
				table := db.Tables[tableName]
				if table == nil {
					continue
				}
				go func(t *TableInstance, e *walBufEntry) {
					var err error
					if err = t.wal.FlushBatch(e.records, e.txIDs); err == nil && doFsync {
						err = t.wal.Fsync()
					}
					count := len(e.records) + len(e.txIDs)
					t.walEntryCount += count
					results <- tableResult{
						table:          t,
						entryCount:     count,
						needCheckpoint: t.walEntryCount >= walCheckpointThreshold,
						err:            err,
					}
				}(table, entry)
			}
			for range merged {
				r := <-results
				if r.err != nil && flushErr == nil {
					flushErr = r.err
				}
				if r.needCheckpoint {
					checkpointTables = append(checkpointTables, r.table)
				}
			}
		}

		// Notify waiters
		for _, slot := range batch {
			slot.done <- flushErr
		}

		// Auto-checkpoint — run async to avoid deadlock with table.mu
		if len(checkpointTables) > 0 {
			go func(tables []*TableInstance) {
				for _, table := range tables {
					table.walEntryCount = 0
					table.Checkpoint() // ignore error
				}
			}(checkpointTables)
		}
	}
}

// Checkpoint flushes all tables and writes meta.
func (db *Database) Checkpoint() error {
	for _, table := range db.Tables {
		if err := table.Checkpoint(); err != nil {
			return err
		}
	}
	return storage.WriteMetaFile(filepath.Join(db.dataDir, "_meta.flop"), db.meta)
}

// Close closes all tables.
func (db *Database) Close() error {
	// Wait for any in-flight commit drains
	for {
		db.commitMu.Lock()
		draining := db.commitDraining || len(db.commitQueue) > 0
		db.commitMu.Unlock()
		if !draining {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	var firstErr error
	for _, table := range db.Tables {
		if err := table.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.pubsub != nil {
		db.pubsub.Close()
	}
	return firstErr
}

// --- TableInstance ---

const walCheckpointThreshold = 10000
const updateLockShards = 256

// TableInstance manages a single table's storage, indexes, and WAL.
type TableInstance struct {
	Name string

	def              *schema.TableDef
	tableFile        *storage.TableFile
	wal              *storage.WAL
	primaryIndex     *storage.HashIndex
	secondaryIdxs    map[string]interface{} // *HashIndex, *MultiIndex, or *FullTextIndex
	indexDefsByKey   map[string]schema.IndexDef
	indexStateMu     sync.RWMutex
	indexesReady     bool
	indexBuildDone   chan struct{}
	indexesToRebuild map[string]bool
	migChains        map[int]*schema.MigrationChain
	currentVersion   int
	tableMeta        *schema.StoredTableMeta
	dataDir          string
	pubsub           *PubSub
	db               *Database

	// mu coordinates checkpoints/schema-changing writes.
	// Insert/Delete/Checkpoint take Lock; Update fast path uses RLock.
	mu            sync.RWMutex
	rowLocks      [updateLockShards]sync.Mutex
	pageLocks     [updateLockShards]sync.Mutex
	walEntryCount int
	autoIDNext    map[string]int64
}

func newTableInstance(name string, def *schema.TableDef, db *Database) (*TableInstance, error) {
	return &TableInstance{
		Name:             name,
		def:              def,
		primaryIndex:     storage.NewHashIndex(),
		secondaryIdxs:    make(map[string]interface{}),
		indexDefsByKey:   make(map[string]schema.IndexDef),
		indexesToRebuild: make(map[string]bool),
		migChains:        make(map[int]*schema.MigrationChain),
		autoIDNext:       make(map[string]int64),
		db:               db,
	}, nil
}

func (ti *TableInstance) open(dataDir string, meta *schema.StoredMeta, pubsub *PubSub, maxCachePages int) error {
	ti.dataDir = dataDir
	ti.pubsub = pubsub

	flopPath := filepath.Join(dataDir, ti.Name+".flop")
	walPath := filepath.Join(dataDir, ti.Name+".wal")
	idxPath := filepath.Join(dataDir, ti.Name+".idx")
	midxPath := filepath.Join(dataDir, ti.Name+".midx")

	currentStored := schema.CompiledToStored(ti.def.CompiledSchema)

	tableMeta := meta.Tables[ti.Name]
	if tableMeta == nil {
		// New table
		tableMeta = storage.CreateTableMeta(currentStored)
		meta.Tables[ti.Name] = tableMeta

		tf, err := storage.CreateTableFile(flopPath, 1, maxCachePages)
		if err != nil {
			return err
		}
		ti.tableFile = tf
	} else {
		// Existing table — check for schema changes
		latestVersion := tableMeta.CurrentSchemaVersion
		latestSchema := tableMeta.Schemas[latestVersion]

		if latestSchema != nil && !schema.SchemasEqual(latestSchema, ti.def.CompiledSchema) {
			changes := schema.DiffSchemas(latestSchema, ti.def.CompiledSchema)
			newVersion := latestVersion + 1
			errors := schema.ValidateMigration(changes, ti.def.Migrations, newVersion)
			if len(errors) > 0 {
				return fmt.Errorf("schema migration errors for table %q:\n%s", ti.Name, strings.Join(errors, "\n"))
			}
			storage.AddSchemaVersion(tableMeta, currentStored)
		}

		tf, err := storage.OpenTableFile(flopPath, maxCachePages)
		if err != nil {
			return err
		}
		ti.tableFile = tf
	}

	ti.tableMeta = tableMeta
	ti.currentVersion = tableMeta.CurrentSchemaVersion

	// Build migration chains for older versions
	for v := 1; v < ti.currentVersion; v++ {
		ti.migChains[v] = schema.BuildMigrationChain(v, ti.currentVersion, ti.def.Migrations, tableMeta.Schemas)
	}

	// Open WAL and replay
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		return err
	}
	ti.wal = wal
	ti.primaryIndex = storage.NewHashIndex()
	replayed, err := ti.replayWAL()
	if err != nil {
		return err
	}

	if !replayed {
		// Load primary index from mapped .midx first, then fallback to .idx, or rebuild.
		idx, err := storage.ReadMappedIndexFile(midxPath)
		if err == nil && idx.Size() > 0 {
			ti.primaryIndex = idx
		} else {
			idx, err = storage.ReadIndexFile(idxPath)
			if err == nil && idx.Size() > 0 {
				ti.primaryIndex = idx
				// Best effort: seed mapped index so next restart can load without full deserialize.
				_ = storage.WriteMappedIndexFile(midxPath, ti.primaryIndex)
			} else {
				if err := ti.rebuildIndex(); err != nil {
					return err
				}
			}
		}
	}
	ti.initializeAutoIncrementCounters()

	forceSecondaryRebuild := replayed

	// Auto-create unique indexes for fields with Unique flag
	for _, field := range ti.def.CompiledSchema.Fields {
		if field.Unique && field.AutoGenPattern == "" {
			alreadyDefined := false
			for _, idx := range ti.def.Indexes {
				if len(idx.Fields) == 1 && idx.Fields[0] == field.Name {
					alreadyDefined = true
					break
				}
			}
			if !alreadyDefined {
				ti.def.Indexes = append(ti.def.Indexes, schema.IndexDef{
					Fields: []string{field.Name},
					Unique: true,
					Type:   schema.IndexTypeHash,
				})
			}
		}
	}

	// Set up secondary indexes
	ti.indexDefsByKey = make(map[string]schema.IndexDef, len(ti.def.Indexes))
	ti.indexesToRebuild = make(map[string]bool, len(ti.def.Indexes))
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		ti.indexDefsByKey[indexKey] = indexDef
		if forceSecondaryRebuild {
			if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
				ti.secondaryIdxs[indexKey] = storage.NewFullTextIndex()
			} else if indexDef.Unique {
				ti.secondaryIdxs[indexKey] = storage.NewHashIndex()
			} else {
				ti.secondaryIdxs[indexKey] = storage.NewMultiIndex()
			}
			ti.indexesToRebuild[indexKey] = true
			continue
		}
		if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
			ti.secondaryIdxs[indexKey] = storage.NewFullTextIndex()
			ti.indexesToRebuild[indexKey] = true
		} else if indexDef.Unique {
			persistPath := secondaryIndexDiskPath(dataDir, ti.Name, indexKey, true)
			idx, lerr := storage.ReadMappedIndexFile(persistPath)
			if lerr == nil && idx.Size() > 0 {
				ti.secondaryIdxs[indexKey] = idx
			} else {
				ti.secondaryIdxs[indexKey] = storage.NewHashIndex()
				ti.indexesToRebuild[indexKey] = true
			}
		} else {
			persistPath := secondaryIndexDiskPath(dataDir, ti.Name, indexKey, true)
			idx, lerr := storage.ReadMappedMultiIndexFile(persistPath)
			if lerr == nil {
				ti.secondaryIdxs[indexKey] = idx
				if idx.Stats().Entries == 0 {
					ti.indexesToRebuild[indexKey] = true
				}
			} else {
				ti.secondaryIdxs[indexKey] = storage.NewMultiIndex()
				ti.indexesToRebuild[indexKey] = true
			}
		}
	}

	ti.setIndexesReady(len(ti.def.Indexes) == 0)

	// Populate secondary indexes.
	if len(ti.def.Indexes) == 0 {
		return nil
	}
	if len(ti.indexesToRebuild) == 0 {
		ti.setIndexesReady(true)
		return nil
	}
	if !ti.db.asyncSecondaryIndexes {
		if err := ti.rebuildSecondaryIndexesByKeys(ti.indexesToRebuild); err != nil {
			return err
		}
		ti.indexesToRebuild = make(map[string]bool)
		ti.setIndexesReady(true)
		return nil
	}

	ti.indexBuildDone = make(chan struct{})
	go ti.rebuildSecondaryIndexesAsync()

	return nil
}

func (ti *TableInstance) replayWAL() (bool, error) {
	entries, err := ti.wal.Replay()
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}

	checkpointLSN := ti.wal.CheckpointLSN()
	committedLSN := storage.FindCommittedTxLSN(entries)
	if len(committedLSN) == 0 {
		if err := ti.wal.Truncate(); err != nil {
			return false, err
		}
		return false, nil
	}

	// Start from on-disk state.
	if err := ti.rebuildIndex(); err != nil {
		return false, err
	}

	applied := false
	for _, entry := range entries {
		if entry.Op == storage.WALOpBegin || entry.Op == storage.WALOpCommit {
			continue
		}
		commitLSN, committed := committedLSN[entry.TxID]
		if !committed {
			continue
		}
		if checkpointLSN > 0 && commitLSN > 0 && commitLSN <= checkpointLSN {
			continue
		}
		if err := ti.applyWALEntry(entry); err != nil {
			return false, err
		}
		applied = true
	}

	if applied {
		if err := ti.tableFile.Flush(); err != nil {
			return false, err
		}
	}
	if err := ti.wal.Truncate(); err != nil {
		return false, err
	}
	return applied, nil
}

func (ti *TableInstance) applyWALEntry(entry storage.WALEntry) error {
	switch entry.Op {
	case storage.WALOpInsert:
		return ti.applyWALInsert(entry.Data)
	case storage.WALOpUpdate:
		return ti.applyWALUpdate(entry.Data)
	case storage.WALOpDelete:
		return ti.applyWALDelete(string(entry.Data))
	default:
		return nil
	}
}

func (ti *TableInstance) applyWALInsert(serialized []byte) error {
	row, err := ti.deserializeCurrentRow(serialized)
	if err != nil {
		return nil
	}
	pk := toString(row[ti.primaryKeyField()])
	if pk == "" {
		return nil
	}
	if ti.primaryIndex.Has(pk) {
		return nil
	}

	pageNum, page, err := ti.tableFile.FindOrAllocatePage(len(serialized))
	if err != nil {
		return err
	}
	slotIndex := page.InsertRow(serialized)
	if slotIndex == -1 {
		return fmt.Errorf("wal replay insert failed: no slot")
	}
	ti.tableFile.MarkPageDirty(pageNum)
	ti.tableFile.TotalRows++
	ti.primaryIndex.Set(pk, schema.RowPointer{PageNumber: pageNum, SlotIndex: uint16(slotIndex)})
	return nil
}

func (ti *TableInstance) applyWALUpdate(serialized []byte) error {
	row, err := ti.deserializeCurrentRow(serialized)
	if err != nil {
		return nil
	}
	pk := toString(row[ti.primaryKeyField()])
	if pk == "" {
		return nil
	}

	pointer, ok := ti.primaryIndex.Get(pk)
	if !ok {
		return ti.applyWALInsert(serialized)
	}

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return err
	}
	if page.UpdateRow(int(pointer.SlotIndex), serialized) {
		ti.tableFile.MarkPageDirty(pointer.PageNumber)
		return nil
	}

	page.DeleteRow(int(pointer.SlotIndex))
	ti.tableFile.MarkPageDirty(pointer.PageNumber)

	newPageNum, newPage, err := ti.tableFile.FindOrAllocatePage(len(serialized))
	if err != nil {
		return err
	}
	newSlot := newPage.InsertRow(serialized)
	if newSlot == -1 {
		return fmt.Errorf("wal replay update failed: reinsert")
	}
	ti.tableFile.MarkPageDirty(newPageNum)
	ti.primaryIndex.Set(pk, schema.RowPointer{PageNumber: newPageNum, SlotIndex: uint16(newSlot)})
	return nil
}

func (ti *TableInstance) applyWALDelete(pk string) error {
	if pk == "" {
		return nil
	}
	pointer, ok := ti.primaryIndex.Get(pk)
	if !ok {
		return nil
	}
	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return err
	}
	page.DeleteRow(int(pointer.SlotIndex))
	ti.tableFile.MarkPageDirty(pointer.PageNumber)
	if ti.tableFile.TotalRows > 0 {
		ti.tableFile.TotalRows--
	}
	ti.primaryIndex.Delete(pk)
	return nil
}

func (ti *TableInstance) rebuildIndex() error {
	ti.primaryIndex.Clear()

	pkField := ti.primaryKeyField()
	if pkField == "" {
		return nil
	}

	return ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		key := toString(row[pkField])
		if key != "" {
			ti.primaryIndex.Set(key, schema.RowPointer{
				PageNumber: scanned.PageNumber,
				SlotIndex:  uint16(scanned.SlotIndex),
			})
		}
		return true
	})
}

func (ti *TableInstance) rebuildSecondaryIndexes() error {
	return ti.rebuildSecondaryIndexesByKeys(nil)
}

func (ti *TableInstance) rebuildSecondaryIndexesByKeys(keys map[string]bool) error {
	for indexKey, idx := range ti.secondaryIdxs {
		if keys != nil && !keys[indexKey] {
			continue
		}
		switch idx := idx.(type) {
		case *storage.FullTextIndex:
			idx.Clear()
		case *storage.HashIndex:
			idx.Clear()
		case *storage.MultiIndex:
			idx.Clear()
		}
	}

	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}

		pointer := schema.RowPointer{
			PageNumber: scanned.PageNumber,
			SlotIndex:  uint16(scanned.SlotIndex),
		}

		for _, indexDef := range ti.def.Indexes {
			indexKey := secondaryIndexKey(indexDef)
			if keys != nil && !keys[indexKey] {
				continue
			}
			idx := ti.secondaryIdxs[indexKey]
			if idx == nil {
				continue
			}

			switch idx := idx.(type) {
			case *storage.FullTextIndex:
				idx.Index(toString(row[ti.primaryKeyField()]), textValuesForFields(row, indexDef.Fields)...)
			case *storage.HashIndex:
				key := storage.CompositeKeyFromRow(row, indexDef.Fields)
				idx.Set(key, pointer)
			case *storage.MultiIndex:
				if allIndexFieldsUnset(row, indexDef.Fields) {
					continue
				}
				key := storage.CompositeKeyFromRow(row, indexDef.Fields)
				idx.Add(key, pointer)
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	// Eagerly finalize full-text indexes so the first search is fast.
	for indexKey, idx := range ti.secondaryIdxs {
		if keys != nil && !keys[indexKey] {
			continue
		}
		if fti, ok := idx.(*storage.FullTextIndex); ok {
			fti.Finalize()
		}
	}
	return nil
}

func (ti *TableInstance) rebuildSecondaryIndexesAsync() {
	start := time.Now()
	defer close(ti.indexBuildDone)
	ti.mu.Lock()
	defer ti.mu.Unlock()
	if err := ti.rebuildSecondaryIndexesByKeys(ti.indexesToRebuild); err != nil {
		fmt.Fprintf(os.Stderr, "flop: %s: secondary index build failed in %s: %v\n", ti.Name, time.Since(start).Round(time.Millisecond), err)
		ti.setIndexesReady(false)
		return
	}
	fmt.Fprintf(os.Stderr, "flop: %s: secondary indexes ready in %s\n", ti.Name, time.Since(start).Round(time.Millisecond))
	ti.indexesToRebuild = make(map[string]bool)
	ti.setIndexesReady(true)
}

func (ti *TableInstance) setIndexesReady(ready bool) {
	ti.indexStateMu.Lock()
	ti.indexesReady = ready
	ti.indexStateMu.Unlock()
}

func (ti *TableInstance) secondaryIndexesReady() bool {
	ti.indexStateMu.RLock()
	ready := ti.indexesReady
	ti.indexStateMu.RUnlock()
	return ready
}

func (ti *TableInstance) waitForSecondaryIndexBuild() {
	done := ti.indexBuildDone
	if done == nil {
		return
	}
	<-done
}

func (ti *TableInstance) primaryKeyField() string {
	for _, f := range ti.def.CompiledSchema.Fields {
		if f.AutoGenPattern != "" {
			return f.Name
		}
	}
	if len(ti.def.CompiledSchema.Fields) > 0 {
		return ti.def.CompiledSchema.Fields[0].Name
	}
	return "id"
}

func (ti *TableInstance) initializeAutoIncrementCounters() {
	if ti.autoIDNext == nil {
		ti.autoIDNext = make(map[string]int64)
	}
	for _, f := range ti.def.CompiledSchema.Fields {
		if strings.ToLower(strings.TrimSpace(f.AutoIDStrategy)) != "autoincrement" {
			continue
		}
		maxSeen := int64(0)
		if f.Name == ti.primaryKeyField() && ti.primaryIndex != nil {
			ti.primaryIndex.Range(func(key string, _ schema.RowPointer) bool {
				n, ok := parseInt64Like(key)
				if ok && n > maxSeen {
					maxSeen = n
				}
				return true
			})
		}
		ti.autoIDNext[f.Name] = maxSeen + 1
	}
}

func (ti *TableInstance) nextAutoIDValue(field schema.CompiledField) interface{} {
	next := ti.autoIDNext[field.Name]
	if next <= 0 {
		next = 1
	}
	ti.autoIDNext[field.Name] = next + 1
	if field.Kind == schema.KindInteger {
		return next
	}
	return float64(next)
}

// GetDef returns the table definition.
func (ti *TableInstance) GetDef() *schema.TableDef {
	return ti.def
}

// Insert adds a new row to the table.
// Insert inserts a new row. If txBuf is non-nil, WAL records are buffered
// into it for batch commit later (transaction mode). Otherwise commits immediately.
func (ti *TableInstance) Insert(data map[string]interface{}, txBuf map[string]*walBufEntry) (map[string]interface{}, error) {
	ti.mu.Lock()
	var change *ChangeEvent
	defer func() {
		ti.mu.Unlock()
		if change != nil {
			ti.pubsub.Publish(*change)
		}
	}()

	row := copyRow(data)

	// Strip cached fields — engine manages these values
	for _, field := range ti.def.CompiledSchema.Fields {
		if field.Cached {
			delete(row, field.Name)
		}
	}

	// Apply autogenerate, defaults, and validation
	for _, field := range ti.def.CompiledSchema.Fields {
		val := row[field.Name]
		if val == nil {
			if field.AutoIDStrategy != "" {
				generated, err := generateAutoID(field, ti)
				if err != nil {
					return nil, err
				}
				row[field.Name] = generated
			} else if field.AutoGenPattern != "" {
				generated, err := generateFromPattern(field.AutoGenPattern)
				if err != nil {
					return nil, err
				}
				row[field.Name] = generated
			} else if field.Kind == schema.KindTimestamp && field.DefaultValue == "now" {
				row[field.Name] = float64(time.Now().UnixMilli())
			} else if field.DefaultValue != nil {
				row[field.Name] = field.DefaultValue
			}
		}

		if field.Required && (row[field.Name] == nil) {
			return nil, fmt.Errorf("field %q is required", field.Name)
		}

		if row[field.Name] != nil {
			if err := validateFieldValue(&field, row[field.Name]); err != nil {
				return nil, err
			}
			if strings.EqualFold(field.AutoIDStrategy, "autoincrement") {
				if n, ok := parseInt64Like(row[field.Name]); ok {
					if ti.autoIDNext[field.Name] <= n {
						ti.autoIDNext[field.Name] = n + 1
					}
				}
			}
			// Deduplicate sets
			if field.Kind == schema.KindSet {
				if arr, ok := row[field.Name].([]interface{}); ok {
					row[field.Name] = deduplicateStrings(arr)
				}
			}
		}
	}

	// Check unique constraints
	pk := toString(row[ti.primaryKeyField()])
	var rowLock *sync.Mutex
	if pk != "" {
		rowLock = ti.rowLockForKey(pk)
		rowLock.Lock()
		defer rowLock.Unlock()
	}
	if pk != "" && ti.primaryIndex.Has(pk) {
		return nil, fmt.Errorf("duplicate primary key: %s", pk)
	}

	for _, indexDef := range ti.def.Indexes {
		if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText || !indexDef.Unique {
			continue
		}
		key := secondaryIndexRowKey(indexDef.Fields, row)
		if ti.secondaryIndexesReady() {
			indexKey := secondaryIndexKey(indexDef)
			idx := ti.secondaryIdxs[indexKey]
			if hi, ok := idx.(*storage.HashIndex); ok {
				if hi.Has(key) {
					return nil, fmt.Errorf("duplicate unique constraint on (%s)", strings.Join(indexDef.Fields, ", "))
				}
			}
			continue
		}
		conflict, err := ti.uniqueConflictByScan(indexDef.Fields, key, "")
		if err != nil {
			return nil, err
		}
		if conflict {
			return nil, fmt.Errorf("duplicate unique constraint on (%s)", strings.Join(indexDef.Fields, ", "))
		}
	}

	// Serialize
	serialized := storage.SerializeRow(row, ti.def.CompiledSchema, uint16(ti.currentVersion))

	// WAL
	txID := ti.wal.BeginTransaction()
	beginRecord := ti.wal.BuildBeginRecord(txID)
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpInsert, serialized)
	failpoint.Hit("insert_after_wal_record")

	// Write to page
	pageNum, page, err := ti.tableFile.FindOrAllocatePage(len(serialized))
	if err != nil {
		return nil, err
	}
	slotIndex := page.InsertRow(serialized)
	if slotIndex == -1 {
		return nil, fmt.Errorf("failed to insert row into page")
	}
	ti.tableFile.MarkPageDirty(pageNum)
	ti.tableFile.TotalRows++
	failpoint.Hit("insert_after_page_write")

	// Update indexes
	pointer := schema.RowPointer{PageNumber: pageNum, SlotIndex: uint16(slotIndex)}
	if pk != "" {
		ti.primaryIndex.Set(pk, pointer)
	}
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		idx := ti.secondaryIdxs[indexKey]
		switch idx := idx.(type) {
		case *storage.FullTextIndex:
			idx.Index(pk, textValuesForFields(row, indexDef.Fields)...)
		case *storage.HashIndex:
			key := storage.CompositeKeyFromRow(row, indexDef.Fields)
			idx.Set(key, pointer)
		case *storage.MultiIndex:
			if allIndexFieldsUnset(row, indexDef.Fields) {
				continue
			}
			key := storage.CompositeKeyFromRow(row, indexDef.Fields)
			idx.Add(key, pointer)
		}
	}
	failpoint.Hit("insert_after_index_update")

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, beginRecord)
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{beginRecord, walRecord}, txIDs: []uint32{txID}},
		}
		failpoint.Hit("insert_before_commit")
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
		failpoint.Hit("insert_after_commit")
	}

	change = &ChangeEvent{Table: ti.Name, Op: "insert", RowID: pk, Data: row}

	return row, nil
}

// BulkInsert inserts many rows using buffered WAL flushes for higher throughput.
// Returns how many rows were successfully inserted.
func (ti *TableInstance) BulkInsert(rows []map[string]interface{}, flushEvery int) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if flushEvery <= 0 {
		flushEvery = 2000
	}

	txBuf := make(map[string]*walBufEntry)
	inserted := 0

	flush := func() error {
		if len(txBuf) == 0 {
			return nil
		}
		if err := ti.db.EnqueueCommit(txBuf); err != nil {
			return err
		}
		txBuf = make(map[string]*walBufEntry)
		return nil
	}

	for _, row := range rows {
		if _, err := ti.Insert(row, txBuf); err != nil {
			return inserted, err
		}
		inserted++
		if inserted%flushEvery == 0 {
			if err := flush(); err != nil {
				return inserted, err
			}
		}
	}

	if err := flush(); err != nil {
		return inserted, err
	}
	return inserted, nil
}

// Get retrieves a row by primary key.
func (ti *TableInstance) Get(key string) (map[string]interface{}, error) {
	rowLock := ti.rowLockForKey(key)
	rowLock.Lock()
	defer rowLock.Unlock()

	return ti.getUnlocked(key)
}

func (ti *TableInstance) getUnlocked(key string) (map[string]interface{}, error) {
	pointer, ok := ti.primaryIndex.Get(key)
	if !ok {
		return nil, nil
	}

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return nil, err
	}
	rawData := page.ReadRow(int(pointer.SlotIndex))
	if rawData == nil {
		return nil, nil
	}

	row, sv, _, err := storage.DeserializeRow(rawData, 0, ti.def.CompiledSchema)
	if err != nil {
		return nil, err
	}

	if int(sv) < ti.currentVersion {
		chain := ti.migChains[int(sv)]
		if chain != nil {
			values, sv2, _, err := storage.DeserializeRawFields(rawData, 0)
			if err == nil {
				oldSchema := ti.tableMeta.Schemas[int(sv2)]
				if oldSchema != nil {
					oldRow := schema.DeserializeWithSchema(values, oldSchema)
					return chain.Migrate(oldRow), nil
				}
			}
		}
	}

	return row, nil
}

// Update modifies an existing row. If txBuf is non-nil, WAL records are buffered
// into it for batch commit later (transaction mode). Otherwise commits immediately.
func (ti *TableInstance) Update(key string, updates map[string]interface{}, txBuf map[string]*walBufEntry) (map[string]interface{}, error) {
	return ti.update(key, updates, txBuf, false)
}

// UpdateSilent modifies an existing row without publishing a PubSub event.
// Used by the cached field system to avoid cascading recomputation.
func (ti *TableInstance) UpdateSilent(key string, updates map[string]interface{}) (map[string]interface{}, error) {
	return ti.update(key, updates, nil, true)
}

func (ti *TableInstance) update(key string, updates map[string]interface{}, txBuf map[string]*walBufEntry, silent bool) (map[string]interface{}, error) {
	// Fast path: allow concurrent in-place updates on different rows/pages.
	// Complex updates that require row relocation fall back to the locked path.
	ti.mu.RLock()
	rowLock := ti.rowLockForKey(key)
	rowLock.Lock()
	locked := true
	defer func() {
		if locked {
			rowLock.Unlock()
			ti.mu.RUnlock()
		}
	}()

	existing, err := ti.getUnlocked(key)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}

	pointer, _ := ti.primaryIndex.Get(key)
	newRow := copyRow(existing)
	for k, v := range updates {
		newRow[k] = v
	}

	// Validate
	for _, field := range ti.def.CompiledSchema.Fields {
		val := newRow[field.Name]
		if val != nil {
			if err := validateFieldValue(&field, val); err != nil {
				return nil, err
			}
		}
		if field.Kind == schema.KindSet {
			if arr, ok := val.([]interface{}); ok {
				newRow[field.Name] = deduplicateStrings(arr)
			}
		}
	}

	// Handle file cleanup for file fields
	for _, field := range ti.def.CompiledSchema.Fields {
		if (field.Kind == schema.KindFileSingle || field.Kind == schema.KindFileMulti) && updates[field.Name] != nil {
			// Simplified: delete old file refs that aren't in the new set
			// Full implementation would compare FileRef paths
		}
	}

	// Validate unique constraints before mutating anything
	if err := ti.validateIndexChanges(existing, newRow); err != nil {
		return nil, err
	}

	// Serialize
	serialized := storage.SerializeRow(newRow, ti.def.CompiledSchema, uint16(ti.currentVersion))

	txID := ti.wal.BeginTransaction()
	beginRecord := ti.wal.BuildBeginRecord(txID)
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpUpdate, serialized)
	failpoint.Hit("update_after_wal_record")

	pageLock := ti.pageLockFor(pointer.PageNumber)
	pageLock.Lock()

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		pageLock.Unlock()
		return nil, err
	}
	_, oldLen := page.GetSlot(int(pointer.SlotIndex))
	if oldLen == 0 || uint16(len(serialized)) > oldLen {
		pageLock.Unlock()
		rowLock.Unlock()
		ti.mu.RUnlock()
		locked = false
		return ti.updateSlowLocked(key, updates, txBuf, silent)
	}

	if !page.UpdateRow(int(pointer.SlotIndex), serialized) {
		pageLock.Unlock()
		rowLock.Unlock()
		ti.mu.RUnlock()
		locked = false
		return ti.updateSlowLocked(key, updates, txBuf, silent)
	}
	ti.tableFile.MarkPageDirty(pointer.PageNumber)
	pageLock.Unlock()
	failpoint.Hit("update_after_page_write")

	// Apply index changes after successful page write
	ti.applyIndexChanges(existing, newRow, pointer)
	failpoint.Hit("update_after_index_update")

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, beginRecord)
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{beginRecord, walRecord}, txIDs: []uint32{txID}},
		}
		failpoint.Hit("update_before_commit")
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
		failpoint.Hit("update_after_commit")
	}

	rowLock.Unlock()
	ti.mu.RUnlock()
	locked = false
	if !silent {
		ti.pubsub.Publish(ChangeEvent{Table: ti.Name, Op: "update", RowID: key, Data: newRow})
	}

	return newRow, nil
}

// updateSlowLocked performs the original update flow under the table write lock.
// Used as a fallback when an in-place concurrent update cannot be applied.
func (ti *TableInstance) updateSlowLocked(key string, updates map[string]interface{}, txBuf map[string]*walBufEntry, silent bool) (map[string]interface{}, error) {
	ti.mu.Lock()
	rowLock := ti.rowLockForKey(key)
	rowLock.Lock()
	var change *ChangeEvent
	defer func() {
		rowLock.Unlock()
		ti.mu.Unlock()
		if change != nil && !silent {
			ti.pubsub.Publish(*change)
		}
	}()

	existing, err := ti.getUnlocked(key)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}

	pointer, _ := ti.primaryIndex.Get(key)
	newRow := copyRow(existing)
	for k, v := range updates {
		newRow[k] = v
	}

	for _, field := range ti.def.CompiledSchema.Fields {
		val := newRow[field.Name]
		if val != nil {
			if err := validateFieldValue(&field, val); err != nil {
				return nil, err
			}
		}
		if field.Kind == schema.KindSet {
			if arr, ok := val.([]interface{}); ok {
				newRow[field.Name] = deduplicateStrings(arr)
			}
		}
	}

	// Validate unique constraints before mutating anything
	if err := ti.validateIndexChanges(existing, newRow); err != nil {
		return nil, err
	}

	serialized := storage.SerializeRow(newRow, ti.def.CompiledSchema, uint16(ti.currentVersion))
	txID := ti.wal.BeginTransaction()
	beginRecord := ti.wal.BuildBeginRecord(txID)
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpUpdate, serialized)
	failpoint.Hit("update_slow_after_wal_record")

	// Apply index changes (remove old keys, add new keys with current pointer)
	ti.applyIndexChanges(existing, newRow, pointer)

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return nil, err
	}
	updated := page.UpdateRow(int(pointer.SlotIndex), serialized)

	if !updated {
		page.DeleteRow(int(pointer.SlotIndex))
		ti.tableFile.MarkPageDirty(pointer.PageNumber)

		newPageNum, newPage, err := ti.tableFile.FindOrAllocatePage(len(serialized))
		if err != nil {
			return nil, err
		}
		newSlot := newPage.InsertRow(serialized)
		if newSlot == -1 {
			return nil, fmt.Errorf("failed to re-insert row during update")
		}
		ti.tableFile.MarkPageDirty(newPageNum)

		newPointer := schema.RowPointer{PageNumber: newPageNum, SlotIndex: uint16(newSlot)}
		ti.primaryIndex.Set(key, newPointer)

		// Fix pointers in secondary indexes for the relocated row
		for _, indexDef := range ti.def.Indexes {
			indexKey := secondaryIndexKey(indexDef)
			idx := ti.secondaryIdxs[indexKey]
			if _, isFullText := idx.(*storage.FullTextIndex); isFullText {
				continue
			}
			k := storage.CompositeKeyFromRow(newRow, indexDef.Fields)
			switch idx := idx.(type) {
			case *storage.HashIndex:
				idx.Set(k, newPointer)
			case *storage.MultiIndex:
				idx.Delete(k, pointer)
				idx.Add(k, newPointer)
			}
		}
	} else {
		ti.tableFile.MarkPageDirty(pointer.PageNumber)
	}
	failpoint.Hit("update_slow_after_page_write")

	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, beginRecord)
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{beginRecord, walRecord}, txIDs: []uint32{txID}},
		}
		failpoint.Hit("update_slow_before_commit")
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
		failpoint.Hit("update_slow_after_commit")
	}

	change = &ChangeEvent{Table: ti.Name, Op: "update", RowID: key, Data: newRow}
	return newRow, nil
}

// validateIndexChanges checks unique constraints for changed indexed fields.
func (ti *TableInstance) validateIndexChanges(existing, newRow map[string]interface{}) error {
	for _, indexDef := range ti.def.Indexes {
		if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText || !indexDef.Unique {
			continue
		}
		indexKey := secondaryIndexKey(indexDef)
		idx := ti.secondaryIdxs[indexKey]

		oldValues := make([]interface{}, len(indexDef.Fields))
		newValues := make([]interface{}, len(indexDef.Fields))
		for i, f := range indexDef.Fields {
			oldValues[i] = existing[f]
			newValues[i] = newRow[f]
		}
		oldKey := storage.CompositeKey(oldValues)
		newKey := storage.CompositeKey(newValues)
		oldHas := !allIndexValuesUnset(oldValues)
		newHas := !allIndexValuesUnset(newValues)

		if oldHas == newHas && oldKey == newKey {
			continue
		}

		if ti.secondaryIndexesReady() {
			if hi, ok := idx.(*storage.HashIndex); ok {
				if hi.Has(newKey) {
					return fmt.Errorf("duplicate unique constraint on (%s)", strings.Join(indexDef.Fields, ", "))
				}
			}
			continue
		}

		excludePK := toString(existing[ti.primaryKeyField()])
		conflict, err := ti.uniqueConflictByScan(indexDef.Fields, newKey, excludePK)
		if err != nil {
			return err
		}
		if conflict {
			return fmt.Errorf("duplicate unique constraint on (%s)", strings.Join(indexDef.Fields, ", "))
		}
	}
	return nil
}

// applyIndexChanges removes old index entries and adds new ones for changed fields.
func (ti *TableInstance) applyIndexChanges(existing, newRow map[string]interface{}, pointer schema.RowPointer) {
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		idx := ti.secondaryIdxs[indexKey]

		if fti, ok := idx.(*storage.FullTextIndex); ok {
			fti.Index(toString(newRow[ti.primaryKeyField()]), textValuesForFields(newRow, indexDef.Fields)...)
			continue
		}

		oldValues := make([]interface{}, len(indexDef.Fields))
		newValues := make([]interface{}, len(indexDef.Fields))
		for i, f := range indexDef.Fields {
			oldValues[i] = existing[f]
			newValues[i] = newRow[f]
		}
		oldKey := storage.CompositeKey(oldValues)
		newKey := storage.CompositeKey(newValues)
		oldHas := !allIndexValuesUnset(oldValues)
		newHas := !allIndexValuesUnset(newValues)

		if oldHas == newHas && oldKey == newKey {
			continue
		}

		switch idx := idx.(type) {
		case *storage.HashIndex:
			idx.Delete(oldKey)
			idx.Set(newKey, pointer)
		case *storage.MultiIndex:
			if oldHas {
				idx.Delete(oldKey, pointer)
			}
			if newHas {
				idx.Add(newKey, pointer)
			}
		}
	}
}

// Delete removes a row by primary key.
// Delete removes a row by primary key. If txBuf is non-nil, WAL records are buffered
// into it for batch commit later (transaction mode). Otherwise commits immediately.
func (ti *TableInstance) Delete(key string, txBuf map[string]*walBufEntry) (bool, error) {
	ti.mu.Lock()
	rowLock := ti.rowLockForKey(key)
	rowLock.Lock()
	var change *ChangeEvent
	defer func() {
		rowLock.Unlock()
		ti.mu.Unlock()
		if change != nil {
			ti.pubsub.Publish(*change)
		}
	}()

	existing, err := ti.getUnlocked(key)
	if err != nil {
		return false, err
	}
	if existing == nil {
		return false, nil
	}

	pointer, ok := ti.primaryIndex.Get(key)
	if !ok {
		return false, nil
	}

	// File cleanup
	storage.DeleteRowFiles(ti.dataDir, ti.Name, key)

	txID := ti.wal.BeginTransaction()
	beginRecord := ti.wal.BuildBeginRecord(txID)
	deleteData := []byte(key)
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpDelete, deleteData)
	failpoint.Hit("delete_after_wal_record")

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return false, err
	}
	page.DeleteRow(int(pointer.SlotIndex))
	ti.tableFile.MarkPageDirty(pointer.PageNumber)
	ti.tableFile.TotalRows--
	failpoint.Hit("delete_after_page_write")

	// Remove from indexes
	ti.primaryIndex.Delete(key)
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		idx := ti.secondaryIdxs[indexKey]
		switch idx := idx.(type) {
		case *storage.FullTextIndex:
			idx.Delete(key)
		case *storage.HashIndex:
			k := storage.CompositeKeyFromRow(existing, indexDef.Fields)
			idx.Delete(k)
		case *storage.MultiIndex:
			if allIndexFieldsUnset(existing, indexDef.Fields) {
				continue
			}
			k := storage.CompositeKeyFromRow(existing, indexDef.Fields)
			idx.Delete(k, pointer)
		}
	}
	failpoint.Hit("delete_after_index_update")

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, beginRecord)
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{beginRecord, walRecord}, txIDs: []uint32{txID}},
		}
		failpoint.Hit("delete_before_commit")
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return false, err
		}
		failpoint.Hit("delete_after_commit")
	}

	change = &ChangeEvent{Table: ti.Name, Op: "delete", RowID: key, Data: existing}

	return true, nil
}

// Count returns the number of rows.
func (ti *TableInstance) Count() int {
	return ti.primaryIndex.Size()
}

// SecondaryIndexesReady reports whether non-primary indexes are fully built.
func (ti *TableInstance) SecondaryIndexesReady() bool {
	return ti.secondaryIndexesReady()
}

// Scan returns rows with limit/offset.
func (ti *TableInstance) Scan(limit, offset int) ([]map[string]interface{}, error) {
	start := time.Now()
	if limit <= 0 {
		limit = 100
	}

	var results []map[string]interface{}
	skipped := 0
	count := 0
	scannedCount := 0

	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		scannedCount++
		if skipped < offset {
			skipped++
			return true
		}
		if count >= limit {
			return false
		}

		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}

		results = append(results, row)
		count++
		return true
	})
	if err != nil {
		return nil, err
	}
	reqtrace.AddDuration("table_scan", ti.Name, "", len(results), scannedCount, "", start)

	return results, nil
}

// ScanFilter iterates all rows and returns those matching the predicate.
// It counts all matches but only collects rows in the [offset, offset+limit) window.
// Pass limit <= 0 to collect all matches (no pagination).
// Returns (matched rows, total match count, error).
func (ti *TableInstance) ScanFilter(match func(map[string]interface{}) bool, limit, offset int) ([]map[string]interface{}, int, error) {
	start := time.Now()
	var results []map[string]interface{}
	total := 0
	scannedCount := 0

	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		scannedCount++
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		if !match(row) {
			return true
		}
		total++
		if limit > 0 {
			if total <= offset {
				return true // skip rows before the page
			}
			if total > offset+limit {
				return true // past the page, but keep counting for total
			}
		}
		results = append(results, row)
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	reqtrace.AddDuration("table_scan_filter", ti.Name, "", total, scannedCount, "", start)

	return results, total, nil
}

// LookupByField performs an index-based lookup for field=value.
// It checks the primary key first, then secondary indexes.
// Returns (rows, total, used) — used is false if no index exists for the field,
// in which case the caller should fall back to ScanFilter.
func (ti *TableInstance) LookupByField(field, value string, limit, offset int) ([]map[string]interface{}, int, bool) {
	traceStart := time.Now()
	pkField := ti.def.CompiledSchema.Fields[0].Name

	if field == pkField {
		row, err := ti.Get(value)
		if err != nil || row == nil {
			reqtrace.AddDuration("primary_lookup", ti.Name, pkField, 0, 1, "not found", traceStart)
			return nil, 0, true
		}
		// Single PK match — total is always 1
		if offset >= 1 {
			reqtrace.AddDuration("primary_lookup", ti.Name, pkField, 1, 1, "offset excluded row", traceStart)
			return nil, 1, true
		}
		reqtrace.AddDuration("primary_lookup", ti.Name, pkField, 1, 1, "", traceStart)
		return []map[string]interface{}{row}, 1, true
	}

	// Check secondary index
	indexKey := field // single-field index key
	if _, exists := ti.indexDefsByKey[indexKey]; !exists {
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 0, 0, "index missing; fallback scan", traceStart)
		return nil, 0, false // no index — caller should scan
	}

	pointers := ti.FindAllByIndex([]string{field}, value)
	total := len(pointers)
	if total == 0 {
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 0, 0, "", traceStart)
		return nil, 0, true
	}

	// Apply pagination over the pointer list
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if limit <= 0 || end > total {
		end = total
	}
	page := pointers[start:end]

	rows := make([]map[string]interface{}, 0, len(page))
	for _, ptr := range page {
		row, err := ti.GetByPointer(ptr)
		if err == nil && row != nil {
			rows = append(rows, row)
		}
	}
	reqtrace.AddDuration("index_lookup", ti.Name, indexKey, total, len(page), "", traceStart)

	return rows, total, true
}

// BuildAutocompleteEntries builds reusable autocomplete entries from this table.
func (ti *TableInstance) BuildAutocompleteEntries(keyField, textField string, payloadFields ...string) ([]AutocompleteEntry, error) {
	keyField = strings.TrimSpace(keyField)
	textField = strings.TrimSpace(textField)
	if keyField == "" || textField == "" {
		return nil, fmt.Errorf("keyField and textField are required")
	}

	cleanPayload := make([]string, 0, len(payloadFields))
	for _, field := range payloadFields {
		field = strings.TrimSpace(field)
		if field != "" {
			cleanPayload = append(cleanPayload, field)
		}
	}

	out := make([]AutocompleteEntry, 0, ti.Count())
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}

		key := toString(row[keyField])
		text := toString(row[textField])
		if key == "" || text == "" {
			return true
		}

		var data interface{}
		switch len(cleanPayload) {
		case 0:
			data = nil
		case 1:
			data = row[cleanPayload[0]]
		default:
			payload := make(map[string]interface{}, len(cleanPayload))
			for _, field := range cleanPayload {
				payload[field] = row[field]
			}
			if len(payload) > 0 {
				data = payload
			}
		}

		out = append(out, AutocompleteEntry{
			Key:  key,
			Text: text,
			Data: data,
		})
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FindByIndex finds a row by a secondary unique index.
func (ti *TableInstance) FindByIndex(fields []string, value interface{}) (schema.RowPointer, bool) {
	start := time.Now()
	indexKey := strings.Join(fields, ",")
	indexDef, exists := ti.indexDefsByKey[indexKey]
	if !exists || normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
		return schema.RowPointer{}, false
	}

	if !ti.secondaryIndexesReady() {
		matchKey := toString(value)
		if len(fields) > 1 {
			matchKey = storage.CompositeKey(anySlice(value))
		}
		ptrs, err := ti.scanPointersByIndexKey(fields, matchKey, 1)
		if err != nil || len(ptrs) == 0 {
			reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 0, 0, "fallback scan", start)
			return schema.RowPointer{}, false
		}
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 1, 1, "fallback scan", start)
		return ptrs[0], true
	}

	idx := ti.secondaryIdxs[indexKey]
	if hi, ok := idx.(*storage.HashIndex); ok {
		matchKey := toString(value)
		if len(fields) > 1 {
			matchKey = storage.CompositeKey(anySlice(value))
		}
		ptr, ok := hi.Get(matchKey)
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, btoi(ok), 1, "", start)
		return ptr, ok
	}
	return schema.RowPointer{}, false
}

// FindAllByIndex returns all row pointers for a non-unique index value.
func (ti *TableInstance) FindAllByIndex(fields []string, value interface{}) []schema.RowPointer {
	start := time.Now()
	indexKey := strings.Join(fields, ",")
	indexDef, exists := ti.indexDefsByKey[indexKey]
	if !exists || normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
		return nil
	}

	if !ti.secondaryIndexesReady() {
		matchKey := toString(value)
		if len(fields) > 1 {
			matchKey = storage.CompositeKey(anySlice(value))
		}
		ptrs, err := ti.scanPointersByIndexKey(fields, matchKey, 0)
		if err != nil {
			reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 0, 0, "fallback scan error", start)
			return nil
		}
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, len(ptrs), len(ptrs), "fallback scan", start)
		return ptrs
	}

	idx := ti.secondaryIdxs[indexKey]
	switch idx := idx.(type) {
	case *storage.MultiIndex:
		matchKey := toString(value)
		if len(fields) > 1 {
			matchKey = storage.CompositeKey(anySlice(value))
		}
		ptrs := idx.GetAll(matchKey)
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, len(ptrs), len(ptrs), "multi-index", start)
		return ptrs
	case *storage.HashIndex:
		matchKey := toString(value)
		if len(fields) > 1 {
			matchKey = storage.CompositeKey(anySlice(value))
		}
		p, ok := idx.Get(matchKey)
		if ok {
			reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 1, 1, "unique-index", start)
			return []schema.RowPointer{p}
		}
		reqtrace.AddDuration("index_lookup", ti.Name, indexKey, 0, 1, "unique-index", start)
	}
	return nil
}

// SearchFullText searches a full-text secondary index over the given fields.
func (ti *TableInstance) SearchFullText(fields []string, query string, limit int) ([]map[string]interface{}, error) {
	start := time.Now()
	indexKey := fullTextIndexKey(fields)
	indexDef, exists := ti.indexDefsByKey[indexKey]
	if !exists || normalizeIndexType(indexDef.Type) != schema.IndexTypeFullText {
		return nil, fmt.Errorf("full-text index not found on fields (%s)", strings.Join(fields, ", "))
	}
	if !ti.secondaryIndexesReady() {
		rows, err := ti.searchFullTextByScan(fields, query, limit)
		if err == nil {
			reqtrace.AddDuration("fulltext_search", ti.Name, indexKey, len(rows), 0, "fallback scan", start)
		}
		return rows, err
	}
	idx := ti.secondaryIdxs[indexKey]
	fti, ok := idx.(*storage.FullTextIndex)
	if !ok {
		return nil, fmt.Errorf("full-text index not found on fields (%s)", strings.Join(fields, ", "))
	}

	pks := fti.Search(query, limit)
	if len(pks) == 0 {
		reqtrace.AddDuration("fulltext_search", ti.Name, indexKey, 0, 0, "", start)
		return []map[string]interface{}{}, nil
	}

	results := make([]map[string]interface{}, 0, len(pks))
	for _, pk := range pks {
		row, err := ti.Get(pk)
		if err != nil || row == nil {
			continue // skip rows on unreadable/corrupted pages
		}
		results = append(results, row)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	reqtrace.AddDuration("fulltext_search", ti.Name, indexKey, len(results), len(pks), "", start)
	return results, nil
}

// GetByPointer reads a row from a direct page/slot pointer.
func (ti *TableInstance) GetByPointer(pointer schema.RowPointer) (map[string]interface{}, error) {
	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return nil, err
	}
	rawData := page.ReadRow(int(pointer.SlotIndex))
	if rawData == nil {
		return nil, nil
	}
	return ti.deserializeCurrentRow(rawData)
}

func (ti *TableInstance) deserializeCurrentRow(rawData []byte) (map[string]interface{}, error) {
	row, sv, _, err := storage.DeserializeRow(rawData, 0, ti.def.CompiledSchema)
	if err != nil {
		return nil, err
	}

	if int(sv) < ti.currentVersion {
		chain := ti.migChains[int(sv)]
		if chain != nil {
			values, sv2, _, err := storage.DeserializeRawFields(rawData, 0)
			if err == nil {
				oldSchema := ti.tableMeta.Schemas[int(sv2)]
				if oldSchema != nil {
					oldRow := schema.DeserializeWithSchema(values, oldSchema)
					return chain.Migrate(oldRow), nil
				}
			}
		}
	}

	return row, nil
}

func (ti *TableInstance) uniqueConflictByScan(fields []string, matchKey, excludePK string) (bool, error) {
	var conflict bool
	pkField := ti.primaryKeyField()
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		if excludePK != "" && toString(row[pkField]) == excludePK {
			return true
		}
		if secondaryIndexRowKey(fields, row) == matchKey {
			conflict = true
			return false
		}
		return true
	})
	if err != nil {
		return false, err
	}
	return conflict, nil
}

func (ti *TableInstance) scanPointersByIndexKey(fields []string, matchKey string, limit int) ([]schema.RowPointer, error) {
	start := time.Now()
	out := make([]schema.RowPointer, 0, 16)
	scannedCount := 0
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		scannedCount++
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		if secondaryIndexRowKey(fields, row) != matchKey {
			return true
		}
		out = append(out, schema.RowPointer{
			PageNumber: scanned.PageNumber,
			SlotIndex:  uint16(scanned.SlotIndex),
		})
		if limit > 0 && len(out) >= limit {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	reqtrace.AddDuration("table_scan_index_match", ti.Name, strings.Join(fields, ","), len(out), scannedCount, "", start)
	return out, nil
}

func (ti *TableInstance) searchFullTextByScan(fields []string, query string, limit int) ([]map[string]interface{}, error) {
	start := time.Now()
	queryTokens := tokenizeFullTextLike(query)
	if len(queryTokens) == 0 {
		return []map[string]interface{}{}, nil
	}

	results := make([]map[string]interface{}, 0, 16)
	scannedCount := 0
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		scannedCount++
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		docTokens := tokenizeFullTextLikeSet(textValuesForFields(row, fields)...)
		match := true
		for _, token := range queryTokens {
			if _, ok := docTokens[token]; !ok {
				match = false
				break
			}
		}
		if !match {
			return true
		}
		results = append(results, row)
		if limit > 0 && len(results) >= limit {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	reqtrace.AddDuration("fulltext_scan", ti.Name, fullTextIndexKey(fields), len(results), scannedCount, "", start)
	return results, nil
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

// Checkpoint flushes all dirty pages, indexes, and WAL.
func (ti *TableInstance) Checkpoint() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	failpoint.Hit("checkpoint_start")
	if err := ti.tableFile.Flush(); err != nil {
		return err
	}
	failpoint.Hit("checkpoint_after_table_flush")
	idxPath := filepath.Join(ti.dataDir, ti.Name+".idx")
	if err := storage.WriteIndexFile(idxPath, ti.primaryIndex); err != nil {
		return err
	}
	midxPath := filepath.Join(ti.dataDir, ti.Name+".midx")
	if err := storage.WriteMappedIndexFile(midxPath, ti.primaryIndex); err != nil {
		return err
	}
	for indexKey, indexDef := range ti.indexDefsByKey {
		if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
			continue
		}
		path := secondaryIndexDiskPath(ti.dataDir, ti.Name, indexKey, true)
		switch idx := ti.secondaryIdxs[indexKey].(type) {
		case *storage.HashIndex:
			if err := storage.WriteMappedIndexFile(path, idx); err != nil {
				return err
			}
		case *storage.MultiIndex:
			if err := storage.WriteMappedMultiIndexFile(path, idx); err != nil {
				return err
			}
		}
	}
	failpoint.Hit("checkpoint_after_index_write")
	if err := ti.wal.Fsync(); err != nil {
		return err
	}
	failpoint.Hit("checkpoint_after_wal_fsync")
	if err := ti.wal.SetCheckpointLSN(ti.wal.CurrentLSN()); err != nil {
		return err
	}
	return ti.wal.Truncate()
}

// Close checkpoints and closes the table.
func (ti *TableInstance) Close() error {
	ti.waitForSecondaryIndexBuild()
	if err := ti.Checkpoint(); err != nil {
		return err
	}
	ti.tableFile.Close()
	return ti.wal.Close()
}

// --- Helpers ---

func normalizeIndexType(t schema.IndexType) schema.IndexType {
	if t == "" {
		return schema.IndexTypeHash
	}
	return t
}

func secondaryIndexKey(indexDef schema.IndexDef) string {
	if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
		return fullTextIndexKey(indexDef.Fields)
	}
	return strings.Join(indexDef.Fields, ",")
}

func fullTextIndexKey(fields []string) string {
	return "ft:" + strings.Join(fields, ",")
}

func secondaryIndexDiskPath(dataDir, tableName, indexKey string, mapped bool) string {
	encoded := hex.EncodeToString([]byte(indexKey))
	if mapped {
		return filepath.Join(dataDir, tableName+"."+encoded+".smidx")
	}
	return filepath.Join(dataDir, tableName+"."+encoded+".sidx")
}

func textValuesForFields(row map[string]interface{}, fields []string) []string {
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		values = append(values, toString(row[field]))
	}
	return values
}

func (ti *TableInstance) rowLockForKey(key string) *sync.Mutex {
	return &ti.rowLocks[hashString(key)%updateLockShards]
}

func (ti *TableInstance) pageLockFor(pageNumber uint32) *sync.Mutex {
	return &ti.pageLocks[pageNumber%updateLockShards]
}

func hashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func copyRow(row map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{}, len(row))
	for k, v := range row {
		cp[k] = v
	}
	return cp
}

func secondaryIndexRowKey(fields []string, row map[string]interface{}) string {
	return storage.CompositeKeyFromRow(row, fields)
}

func allIndexValuesUnset(values []interface{}) bool {
	for _, v := range values {
		if v == nil {
			continue
		}
		if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		// Any non-empty value means the index key should be materialized.
		if v != nil {
			return false
		}
	}
	return true
}

func allIndexFieldsUnset(row map[string]interface{}, fields []string) bool {
	for _, f := range fields {
		v := row[f]
		if v == nil {
			continue
		}
		if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		return false
	}
	return true
}

func anySlice(value interface{}) []interface{} {
	switch v := value.(type) {
	case []interface{}:
		return v
	case []string:
		out := make([]interface{}, len(v))
		for i := range v {
			out[i] = v[i]
		}
		return out
	default:
		return []interface{}{value}
	}
}

var fullTextStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "by": {}, "for": {}, "from": {},
	"has": {}, "he": {}, "in": {}, "is": {}, "it": {}, "its": {}, "of": {}, "on": {}, "or": {}, "that": {},
	"the": {}, "to": {}, "was": {}, "were": {}, "will": {}, "with": {},
}

func tokenizeFullTextLike(texts ...string) []string {
	set := tokenizeFullTextLikeSet(texts...)
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for token := range set {
		out = append(out, token)
	}
	return out
}

func tokenizeFullTextLikeSet(texts ...string) map[string]struct{} {
	const minTokenLen = 2
	seen := make(map[string]struct{}, 16)
	var token []rune
	flush := func() {
		if len(token) < minTokenLen {
			token = token[:0]
			return
		}
		t := string(token)
		if _, stop := fullTextStopWords[t]; !stop {
			seen[t] = struct{}{}
		}
		token = token[:0]
	}
	for _, text := range texts {
		for _, r := range text {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				token = append(token, unicode.ToLower(r))
				continue
			}
			flush()
		}
		flush()
	}
	return seen
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func validateFieldValue(field *schema.CompiledField, value interface{}) error {
	switch field.Kind {
	case schema.KindEnum:
		if len(field.EnumValues) > 0 {
			s := toString(value)
			found := false
			for _, ev := range field.EnumValues {
				if ev == s {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("invalid value %q for enum field %q. Allowed: %s",
					s, field.Name, strings.Join(field.EnumValues, ", "))
			}
		}
	case schema.KindInteger:
		n, ok := toNumber(value)
		if !ok {
			return fmt.Errorf("field %q must be an integer", field.Name)
		}
		if n != float64(int32(n)) {
			return fmt.Errorf("field %q must be a 32-bit integer", field.Name)
		}
	case schema.KindVector:
		arr, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("field %q must be an array of numbers", field.Name)
		}
		if field.VectorDimensions > 0 && len(arr) != field.VectorDimensions {
			return fmt.Errorf("field %q requires exactly %d dimensions, got %d",
				field.Name, field.VectorDimensions, len(arr))
		}
	case schema.KindSet:
		if _, ok := value.([]interface{}); !ok {
			if _, ok := value.([]string); !ok {
				return fmt.Errorf("field %q must be an array of strings", field.Name)
			}
		}
	case schema.KindTimestamp:
		if _, ok := toNumber(value); !ok {
			return fmt.Errorf("field %q must be a number (epoch ms)", field.Name)
		}
	}
	return nil
}

func toNumber(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

func parseInt64Like(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float64:
		if val == float64(int64(val)) {
			return int64(val), true
		}
	case float32:
		if val == float32(int64(val)) {
			return int64(val), true
		}
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return 0, false
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && f == float64(int64(f)) {
			return int64(f), true
		}
	}
	return 0, false
}

func deduplicateStrings(arr []interface{}) []interface{} {
	seen := make(map[string]struct{}, len(arr))
	result := make([]interface{}, 0, len(arr))
	for _, v := range arr {
		s := toString(v)
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// generateFromPattern generates a time-sortable ID from a pattern like "[a-z0-9]{12}".
// The first 8 characters encode the current millisecond timestamp in base36
// for lexicographic ordering; the remaining characters are random.
func generateFromPattern(pattern string) (string, error) {
	// Parse pattern: [charset]{length}
	if len(pattern) < 5 || pattern[0] != '[' {
		return "", fmt.Errorf("autogenerate pattern must be in format [charset]{length}")
	}

	closeBracket := strings.Index(pattern, "]")
	if closeBracket < 0 {
		return "", fmt.Errorf("autogenerate pattern must be in format [charset]{length}")
	}

	charsetSpec := pattern[1:closeBracket]
	rest := pattern[closeBracket+1:]

	if len(rest) < 3 || rest[0] != '{' || rest[len(rest)-1] != '}' {
		return "", fmt.Errorf("autogenerate pattern must be in format [charset]{length}")
	}

	var length int
	if _, err := fmt.Sscanf(rest, "{%d}", &length); err != nil {
		return "", fmt.Errorf("invalid length in pattern: %w", err)
	}

	charset := expandCharset(charsetSpec)
	if len(charset) == 0 {
		return "", fmt.Errorf("empty charset in pattern")
	}

	result := make([]byte, length)

	// Encode timestamp prefix using the charset for sortable ordering.
	// Use 8 chars for the timestamp portion (enough for milliseconds in base-36 until year ~3000).
	ts := time.Now().UnixMilli()
	base := len(charset)
	tsLen := 8
	if tsLen > length {
		tsLen = length
	}
	for i := tsLen - 1; i >= 0; i-- {
		result[i] = charset[ts%int64(base)]
		ts /= int64(base)
	}

	// Fill the rest with random characters
	if length > tsLen {
		randBytes := make([]byte, length-tsLen)
		if _, err := rand.Read(randBytes); err != nil {
			return "", err
		}
		for i := range randBytes {
			result[tsLen+i] = charset[randBytes[i]%byte(len(charset))]
		}
	}

	return string(result), nil
}

func generateAutoID(field schema.CompiledField, ti *TableInstance) (interface{}, error) {
	switch strings.ToLower(strings.TrimSpace(field.AutoIDStrategy)) {
	case "uuidv7":
		return generateUUIDv7()
	case "ulid":
		return generateULID()
	case "nanoid":
		return generateNanoID(21)
	case "random":
		return generateFromPattern("[a-z0-9]{12}")
	case "autoincrement":
		if ti == nil {
			return nil, fmt.Errorf("autoincrement requires table context")
		}
		return ti.nextAutoIDValue(field), nil
	default:
		return nil, fmt.Errorf("unsupported autogen strategy: %s", field.AutoIDStrategy)
	}
}

func generateUUIDv7() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	ts := uint64(time.Now().UnixMilli())
	b[0] = byte(ts >> 40)
	b[1] = byte(ts >> 32)
	b[2] = byte(ts >> 24)
	b[3] = byte(ts >> 16)
	b[4] = byte(ts >> 8)
	b[5] = byte(ts)

	// Version 7
	b[6] = (b[6] & 0x0f) | 0x70
	// RFC 4122 variant (10xxxxxx)
	b[8] = (b[8] & 0x3f) | 0x80

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:]), nil
}

func generateULID() (string, error) {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var entropy [10]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	ts := uint64(time.Now().UnixMilli())
	var out [26]byte

	for i := 9; i >= 0; i-- {
		out[i] = alphabet[ts&0x1f]
		ts >>= 5
	}

	var bits uint64
	var bitCount uint
	pos := 10
	for i := 0; i < len(entropy); i++ {
		bits = (bits << 8) | uint64(entropy[i])
		bitCount += 8
		for bitCount >= 5 && pos < 26 {
			shift := bitCount - 5
			idx := (bits >> shift) & 0x1f
			out[pos] = alphabet[idx]
			bits &= (1 << shift) - 1
			bitCount -= 5
			pos++
		}
	}
	for pos < 26 {
		out[pos] = alphabet[0]
		pos++
	}
	return string(out[:]), nil
}

func generateNanoID(length int) (string, error) {
	if length <= 0 {
		length = 21
	}
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-"
	buf := make([]byte, length)
	r := make([]byte, length)
	if _, err := rand.Read(r); err != nil {
		return "", err
	}
	for i := 0; i < length; i++ {
		buf[i] = alphabet[int(r[i])%len(alphabet)]
	}
	return string(buf), nil
}

func expandCharset(spec string) []byte {
	var result []byte
	i := 0
	for i < len(spec) {
		if i+2 < len(spec) && spec[i+1] == '-' {
			start := spec[i]
			end := spec[i+2]
			for c := start; c <= end; c++ {
				result = append(result, c)
			}
			i += 3
		} else {
			result = append(result, spec[i])
			i++
		}
	}
	return result
}
