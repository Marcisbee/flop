package engine

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

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

	def            *schema.TableDef
	tableFile      *storage.TableFile
	wal            *storage.WAL
	primaryIndex   *storage.HashIndex
	secondaryIdxs  map[string]interface{} // *HashIndex, *MultiIndex, or *FullTextIndex
	indexDefsByKey map[string]schema.IndexDef
	indexStateMu   sync.RWMutex
	indexesReady   bool
	indexBuildDone chan struct{}
	migChains      map[int]*schema.MigrationChain
	currentVersion int
	tableMeta      *schema.StoredTableMeta
	dataDir        string
	pubsub         *PubSub
	db             *Database

	// mu coordinates checkpoints/schema-changing writes.
	// Insert/Delete/Checkpoint take Lock; Update fast path uses RLock.
	mu            sync.RWMutex
	rowLocks      [updateLockShards]sync.Mutex
	pageLocks     [updateLockShards]sync.Mutex
	walEntryCount int
}

func newTableInstance(name string, def *schema.TableDef, db *Database) (*TableInstance, error) {
	return &TableInstance{
		Name:           name,
		def:            def,
		primaryIndex:   storage.NewHashIndex(),
		secondaryIdxs:  make(map[string]interface{}),
		indexDefsByKey: make(map[string]schema.IndexDef),
		migChains:      make(map[int]*schema.MigrationChain),
		db:             db,
	}, nil
}

func (ti *TableInstance) open(dataDir string, meta *schema.StoredMeta, pubsub *PubSub, maxCachePages int) error {
	ti.dataDir = dataDir
	ti.pubsub = pubsub

	flopPath := filepath.Join(dataDir, ti.Name+".flop")
	walPath := filepath.Join(dataDir, ti.Name+".wal")
	idxPath := filepath.Join(dataDir, ti.Name+".idx")

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
	if err := ti.replayWAL(); err != nil {
		return err
	}

	// Load primary index from .idx or rebuild
	idx, err := storage.ReadIndexFile(idxPath)
	if err == nil && idx.Size() > 0 {
		ti.primaryIndex = idx
	} else {
		if err := ti.rebuildIndex(); err != nil {
			return err
		}
	}

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
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		ti.indexDefsByKey[indexKey] = indexDef
		if normalizeIndexType(indexDef.Type) == schema.IndexTypeFullText {
			ti.secondaryIdxs[indexKey] = storage.NewFullTextIndex()
		} else if indexDef.Unique {
			ti.secondaryIdxs[indexKey] = storage.NewHashIndex()
		} else {
			ti.secondaryIdxs[indexKey] = storage.NewMultiIndex()
		}
	}

	ti.setIndexesReady(len(ti.def.Indexes) == 0)

	// Populate secondary indexes.
	if len(ti.def.Indexes) == 0 {
		return nil
	}
	if !ti.db.asyncSecondaryIndexes {
		if err := ti.rebuildSecondaryIndexes(); err != nil {
			return err
		}
		ti.setIndexesReady(true)
		return nil
	}

	ti.indexBuildDone = make(chan struct{})
	go ti.rebuildSecondaryIndexesAsync()

	return nil
}

func (ti *TableInstance) replayWAL() error {
	entries, err := ti.wal.Replay()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	// For now, truncate after replay (simplified — entries were already applied)
	return ti.wal.Truncate()
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
	for _, idx := range ti.secondaryIdxs {
		switch idx := idx.(type) {
		case *storage.FullTextIndex:
			idx.Clear()
		case *storage.HashIndex:
			idx.Clear()
		case *storage.MultiIndex:
			idx.Clear()
		}
	}

	return ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
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
			idx := ti.secondaryIdxs[indexKey]
			if idx == nil {
				continue
			}

			switch idx := idx.(type) {
			case *storage.FullTextIndex:
				idx.Index(toString(row[ti.primaryKeyField()]), textValuesForFields(row, indexDef.Fields)...)
			case *storage.HashIndex:
				keyValues := make([]interface{}, len(indexDef.Fields))
				for i, f := range indexDef.Fields {
					keyValues[i] = row[f]
				}
				key := storage.CompositeKey(keyValues)
				idx.Set(key, pointer)
			case *storage.MultiIndex:
				keyValues := make([]interface{}, len(indexDef.Fields))
				for i, f := range indexDef.Fields {
					keyValues[i] = row[f]
				}
				key := storage.CompositeKey(keyValues)
				idx.Add(key, pointer)
			}
		}
		return true
	})
}

func (ti *TableInstance) rebuildSecondaryIndexesAsync() {
	defer close(ti.indexBuildDone)
	ti.mu.Lock()
	defer ti.mu.Unlock()
	if err := ti.rebuildSecondaryIndexes(); err != nil {
		ti.setIndexesReady(false)
		return
	}
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
			if field.AutoGenPattern != "" {
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
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpInsert, serialized)

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
			keyValues := make([]interface{}, len(indexDef.Fields))
			for i, f := range indexDef.Fields {
				keyValues[i] = row[f]
			}
			key := storage.CompositeKey(keyValues)
			idx.Set(key, pointer)
		case *storage.MultiIndex:
			keyValues := make([]interface{}, len(indexDef.Fields))
			for i, f := range indexDef.Fields {
				keyValues[i] = row[f]
			}
			key := storage.CompositeKey(keyValues)
			idx.Add(key, pointer)
		}
	}

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{walRecord}, txIDs: []uint32{txID}},
		}
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
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

	existing, err := ti.Get(key)
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
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpUpdate, serialized)

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

	// Apply index changes after successful page write
	ti.applyIndexChanges(existing, newRow, pointer)

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{walRecord}, txIDs: []uint32{txID}},
		}
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
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
	var change *ChangeEvent
	defer func() {
		ti.mu.Unlock()
		if change != nil && !silent {
			ti.pubsub.Publish(*change)
		}
	}()

	existing, err := ti.Get(key)
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
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpUpdate, serialized)

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
			keyValues := make([]interface{}, len(indexDef.Fields))
			for i, f := range indexDef.Fields {
				keyValues[i] = newRow[f]
			}
			k := storage.CompositeKey(keyValues)
			if hi, ok := idx.(*storage.HashIndex); ok {
				hi.Set(k, newPointer)
			}
		}
	} else {
		ti.tableFile.MarkPageDirty(pointer.PageNumber)
	}

	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{walRecord}, txIDs: []uint32{txID}},
		}
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return nil, err
		}
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

		if oldKey == newKey {
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

		if oldKey == newKey {
			continue
		}

		switch idx := idx.(type) {
		case *storage.HashIndex:
			idx.Delete(oldKey)
			idx.Set(newKey, pointer)
		case *storage.MultiIndex:
			idx.Delete(oldKey, pointer)
			idx.Add(newKey, pointer)
		}
	}
}

// Delete removes a row by primary key.
// Delete removes a row by primary key. If txBuf is non-nil, WAL records are buffered
// into it for batch commit later (transaction mode). Otherwise commits immediately.
func (ti *TableInstance) Delete(key string, txBuf map[string]*walBufEntry) (bool, error) {
	ti.mu.Lock()
	var change *ChangeEvent
	defer func() {
		ti.mu.Unlock()
		if change != nil {
			ti.pubsub.Publish(*change)
		}
	}()

	existing, err := ti.Get(key)
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
	deleteData := []byte(key)
	walRecord := ti.wal.BuildRecord(txID, storage.WALOpDelete, deleteData)

	page, err := ti.tableFile.GetPage(pointer.PageNumber)
	if err != nil {
		return false, err
	}
	page.DeleteRow(int(pointer.SlotIndex))
	ti.tableFile.MarkPageDirty(pointer.PageNumber)
	ti.tableFile.TotalRows--

	// Remove from indexes
	ti.primaryIndex.Delete(key)
	for _, indexDef := range ti.def.Indexes {
		indexKey := secondaryIndexKey(indexDef)
		idx := ti.secondaryIdxs[indexKey]
		switch idx := idx.(type) {
		case *storage.FullTextIndex:
			idx.Delete(key)
		case *storage.HashIndex:
			keyValues := make([]interface{}, len(indexDef.Fields))
			for i, f := range indexDef.Fields {
				keyValues[i] = existing[f]
			}
			k := storage.CompositeKey(keyValues)
			idx.Delete(k)
		case *storage.MultiIndex:
			keyValues := make([]interface{}, len(indexDef.Fields))
			for i, f := range indexDef.Fields {
				keyValues[i] = existing[f]
			}
			k := storage.CompositeKey(keyValues)
			idx.Delete(k, pointer)
		}
	}

	// WAL commit: buffer into transaction or commit immediately
	if txBuf != nil {
		entry := txBuf[ti.Name]
		if entry == nil {
			entry = &walBufEntry{}
			txBuf[ti.Name] = entry
		}
		entry.records = append(entry.records, walRecord)
		entry.txIDs = append(entry.txIDs, txID)
	} else {
		walBuf := map[string]*walBufEntry{
			ti.Name: {records: [][]byte{walRecord}, txIDs: []uint32{txID}},
		}
		if err := ti.db.EnqueueCommitLocked(walBuf); err != nil {
			return false, err
		}
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
	if limit <= 0 {
		limit = 100
	}

	var results []map[string]interface{}
	skipped := 0
	count := 0

	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
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

	return results, nil
}

// ScanFilter iterates all rows and returns those matching the predicate.
func (ti *TableInstance) ScanFilter(match func(map[string]interface{}) bool) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
		row, err := ti.deserializeCurrentRow(scanned.Data)
		if err != nil {
			return true
		}
		if match(row) {
			results = append(results, row)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return results, nil
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
			return schema.RowPointer{}, false
		}
		return ptrs[0], true
	}

	idx := ti.secondaryIdxs[indexKey]
	if hi, ok := idx.(*storage.HashIndex); ok {
		return hi.Get(toString(value))
	}
	return schema.RowPointer{}, false
}

// FindAllByIndex returns all row pointers for a non-unique index value.
func (ti *TableInstance) FindAllByIndex(fields []string, value interface{}) []schema.RowPointer {
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
			return nil
		}
		return ptrs
	}

	idx := ti.secondaryIdxs[indexKey]
	switch idx := idx.(type) {
	case *storage.MultiIndex:
		return idx.GetAll(toString(value))
	case *storage.HashIndex:
		p, ok := idx.Get(toString(value))
		if ok {
			return []schema.RowPointer{p}
		}
	}
	return nil
}

// SearchFullText searches a full-text secondary index over the given fields.
func (ti *TableInstance) SearchFullText(fields []string, query string, limit int) ([]map[string]interface{}, error) {
	indexKey := fullTextIndexKey(fields)
	indexDef, exists := ti.indexDefsByKey[indexKey]
	if !exists || normalizeIndexType(indexDef.Type) != schema.IndexTypeFullText {
		return nil, fmt.Errorf("full-text index not found on fields (%s)", strings.Join(fields, ", "))
	}
	if !ti.secondaryIndexesReady() {
		return ti.searchFullTextByScan(fields, query, limit)
	}
	idx := ti.secondaryIdxs[indexKey]
	fti, ok := idx.(*storage.FullTextIndex)
	if !ok {
		return nil, fmt.Errorf("full-text index not found on fields (%s)", strings.Join(fields, ", "))
	}

	pks := fti.Search(query, limit)
	if len(pks) == 0 {
		return []map[string]interface{}{}, nil
	}

	results := make([]map[string]interface{}, 0, len(pks))
	for _, pk := range pks {
		row, err := ti.Get(pk)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		results = append(results, row)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
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
	out := make([]schema.RowPointer, 0, 16)
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
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
	return out, nil
}

func (ti *TableInstance) searchFullTextByScan(fields []string, query string, limit int) ([]map[string]interface{}, error) {
	queryTokens := tokenizeFullTextLike(query)
	if len(queryTokens) == 0 {
		return []map[string]interface{}{}, nil
	}

	results := make([]map[string]interface{}, 0, 16)
	err := ti.tableFile.ForEachRow(func(scanned storage.ScannedRow) bool {
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
	return results, nil
}

// Checkpoint flushes all dirty pages, indexes, and WAL.
func (ti *TableInstance) Checkpoint() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	if err := ti.tableFile.Flush(); err != nil {
		return err
	}
	idxPath := filepath.Join(ti.dataDir, ti.Name+".idx")
	if err := storage.WriteIndexFile(idxPath, ti.primaryIndex); err != nil {
		return err
	}
	if err := ti.wal.Fsync(); err != nil {
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
	values := make([]interface{}, len(fields))
	for i, f := range fields {
		values[i] = row[f]
	}
	return storage.CompositeKey(values)
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
