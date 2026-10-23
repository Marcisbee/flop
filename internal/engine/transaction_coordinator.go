package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/marcisbee/flop/internal/storage"
)

const transactionJournalVersion = 1
const transactionRedoVersion = 2

const (
	transactionPrepared  = "prepared"
	transactionCommitted = "committed"
	transactionAborted   = "aborted"
)

type transactionJournal struct {
	Version      int                      `json:"version"`
	Transactions []coordinatedTransaction `json:"transactions"`
}

type coordinatedTransaction struct {
	ID      string              `json:"id"`
	State   string              `json:"state"`
	Tables  map[string][]uint32 `json:"tables"`
	Records map[string][][]byte `json:"records,omitempty"`
}

func (db *Database) transactionJournalPath() string {
	return filepath.Join(db.dataDir, "_transactions.flop")
}

func (db *Database) loadTransactionJournal() error {
	data, err := os.ReadFile(db.transactionJournalPath())
	if err != nil {
		if os.IsNotExist(err) {
			db.txJournal = transactionJournal{Version: transactionJournalVersion}
			return db.loadTransactionDecisions()
		}
		return fmt.Errorf("read transaction journal: %w", err)
	}

	var journal transactionJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return fmt.Errorf("decode transaction journal: %w", err)
	}
	if journal.Version != transactionJournalVersion && journal.Version != transactionRedoVersion {
		return fmt.Errorf("unsupported transaction journal version: %d", journal.Version)
	}
	for _, tx := range journal.Transactions {
		if tx.ID == "" {
			return fmt.Errorf("invalid transaction journal: empty transaction id")
		}
		switch tx.State {
		case transactionPrepared, transactionCommitted, transactionAborted:
		default:
			return fmt.Errorf("invalid transaction journal state %q", tx.State)
		}
	}
	db.txJournal = journal
	return db.loadTransactionDecisions()

}

func (db *Database) persistTransactionJournalLocked() error {
	db.txJournal.Version = transactionJournalVersion
	if db.txDecisions != nil {
		db.txJournal.Version = transactionRedoVersion
	}
	data, err := json.Marshal(db.txJournal)
	if err != nil {
		return err
	}
	if err := storage.WriteFileAtomic(db.transactionJournalPath(), data, 0o600); err != nil {
		return fmt.Errorf("write transaction journal: %w", err)
	}
	return nil
}

func (db *Database) clearTransactionJournalLocked() error {
	db.txJournal = transactionJournal{Version: transactionJournalVersion}
	if err := db.persistTransactionJournalLocked(); err != nil {
		return err
	}
	if db.txDecisions != nil {
		return db.txDecisions.Truncate()
	}
	return nil
}

func (db *Database) ignoredRecoveryTxIDs(tableName string) map[uint32]bool {
	ignored := make(map[uint32]bool)
	for _, tx := range db.txJournal.Transactions {
		if tx.State == transactionCommitted {
			continue
		}
		for _, txID := range tx.Tables[tableName] {
			ignored[txID] = true
		}
	}
	return ignored
}

func transactionTableTxIDs(walBuffers map[string]*walBufEntry) map[string][]uint32 {
	refs := make(map[string][]uint32, len(walBuffers))
	for tableName, entry := range walBuffers {
		if entry == nil {
			continue
		}
		seen := make(map[uint32]struct{}, len(entry.txIDs))
		for _, txID := range entry.txIDs {
			if _, ok := seen[txID]; ok {
				continue
			}
			seen[txID] = struct{}{}
			refs[tableName] = append(refs[tableName], txID)
		}
	}
	return refs
}

func (db *Database) coordinatedFlush(walBuffers map[string]*walBufEntry) error {
	if len(walBuffers) == 0 {
		return nil
	}

	slot := commitSlot{walBuffers: walBuffers, done: make(chan error, 1)}
	db.txCommitMu.Lock()
	db.txCommitQueue = append(db.txCommitQueue, slot)
	start := !db.txCommitDraining
	db.txCommitDraining = true
	db.txCommitMu.Unlock()
	if start {
		go db.drainTransactionCommits()
	}
	return <-slot.done
}

// Drain only already-waiting callers: an uncontended commit has no batching
// delay. Bound each batch so checkpoint callers can acquire transactionMu
// between batches even under sustained write load.
func (db *Database) drainTransactionCommits() {
	const maxBatch = 64
	for {
		db.txCommitMu.Lock()
		if len(db.txCommitQueue) == 0 {
			db.txCommitDraining = false
			db.txCommitQueue = nil
			db.txCommitMu.Unlock()
			return
		}
		n := min(len(db.txCommitQueue), maxBatch)
		batch := db.txCommitQueue[:n:n]
		db.txCommitQueue = db.txCommitQueue[n:]
		db.txCommitMu.Unlock()

		merged := make(map[string]*walBufEntry)
		for _, slot := range batch {
			for name, entry := range slot.walBuffers {
				m := merged[name]
				if m == nil {
					m = &walBufEntry{}
					merged[name] = m
				}
				m.records = append(m.records, entry.records...)
				m.txIDs = append(m.txIDs, entry.txIDs...)
				m.pending = append(m.pending, entry.pending...)
			}
		}
		// One durable redo record covers every transaction in the batch.
		// Every caller is acknowledged only after the complete record syncs.
		err := db.flushTransactionBatch(merged)
		for i := range batch {
			batch[i].done <- err
			batch[i] = commitSlot{}
		}
	}
}

func (db *Database) flushTransactionBatch(walBuffers map[string]*walBufEntry) error {
	db.transactionMu.Lock()
	defer db.transactionMu.Unlock()

	// A single durable redo record contains every participant's writes and
	// commit markers. Table WALs need not be synced separately: recovery merges
	// this record by LSN, and checkpoints retire it only after all tables persist.
	tx := coordinatedTransaction{ID: randomHex(16), State: transactionCommitted, Tables: transactionTableTxIDs(walBuffers), Records: make(map[string][][]byte, len(walBuffers))}
	checkpointNeeded := false
	for name, entry := range walBuffers {
		table := db.Tables[name]
		if table == nil {
			return fmt.Errorf("transaction references unknown table %q", name)
		}
		records := append([][]byte(nil), entry.records...)
		for _, id := range entry.txIDs {
			records = append(records, table.wal.BuildRecord(id, storage.WALOpCommit, nil))
		}
		tx.Records[name] = records
	}
	if err := db.persistTransactionDecision(tx); err != nil {
		return err
	}
	// Redo stays on disk, not in an ever-growing in-memory copy.
	for name, entry := range walBuffers {
		table := db.Tables[name]
		if table.bumpWALEntryCount(len(entry.records) + len(entry.txIDs)) {
			checkpointNeeded = true
		}
	}

	for tableName, entry := range walBuffers {
		if table := db.Tables[tableName]; table != nil {
			table.clearPendingKeys(entry.pending)
		}
	}
	if checkpointNeeded {
		go func() { _ = db.Checkpoint() }()
	}
	return nil
}

// Transactions use the existing checksummed WAL framing. Each complete record
// contains its redo data and commit decision, made durable with one sync.
// The v2 JSON marker makes older engines fail closed instead of ignoring this log.
func (db *Database) loadTransactionDecisions() error {
	path := filepath.Join(db.dataDir, "_transactions.wal")
	if db.txJournal.Version == transactionRedoVersion {
		stat, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("transaction redo log unavailable: %w", err)
		}
		if stat.Size() == 0 {
			return fmt.Errorf("transaction redo log is empty")
		}
	}
	w, err := storage.OpenWAL(path)
	if err != nil {
		return err
	}
	entries, err := w.Replay()
	if err != nil {
		w.Close()
		return err
	}
	positions := make(map[string]int, len(db.txJournal.Transactions))
	for i, tx := range db.txJournal.Transactions {
		positions[tx.ID] = i
	}
	for _, entry := range entries {
		if !storage.WALOpIsAutoCommit(entry.Op) {
			continue
		}
		var tx coordinatedTransaction
		if err := json.Unmarshal(entry.Data, &tx); err != nil {
			w.Close()
			return fmt.Errorf("decode transaction decision: %w", err)
		}
		if tx.ID == "" || (tx.State != transactionPrepared && tx.State != transactionCommitted && tx.State != transactionAborted) {
			w.Close()
			return fmt.Errorf("invalid transaction decision")
		}
		if len(tx.Records) > 0 {
			if tx.State != transactionCommitted {
				w.Close()
				return fmt.Errorf("redo transaction is not committed")
			}
			if db.txRecovery == nil {
				db.txRecovery = make(map[string][]storage.WALEntry)
			}
			for name, records := range tx.Records {
				for _, raw := range records {
					decoded, err := storage.DecodeWALRecords(raw)
					if err != nil {
						w.Close()
						return err
					}
					db.txRecovery[name] = append(db.txRecovery[name], decoded...)
				}
			}
			continue
		}
		if i, ok := positions[tx.ID]; ok {
			db.txJournal.Transactions[i] = tx
		} else {
			positions[tx.ID] = len(db.txJournal.Transactions)
			db.txJournal.Transactions = append(db.txJournal.Transactions, tx)
		}
	}
	if err := w.Fsync(); err != nil {
		w.Close()
		return err
	}
	db.txDecisions = w
	// Atomic replacement also syncs the directory containing the new WAL.
	if err := db.persistTransactionJournalLocked(); err != nil {
		w.Close()
		db.txDecisions = nil
		return err
	}
	return nil
}

func (db *Database) persistTransactionDecision(tx coordinatedTransaction) error {
	data, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	id := db.txDecisions.BeginTransaction()
	record, _ := db.txDecisions.BuildRecordAutoCommit(id, storage.WALOpInsert, data)
	if err := db.txDecisions.FlushBatch([][]byte{record}, nil); err != nil {
		return err
	}
	return db.txDecisions.Fsync()
}
