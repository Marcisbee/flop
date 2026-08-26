package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/marcisbee/flop/internal/storage"
)

const transactionJournalVersion = 1

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
	ID     string              `json:"id"`
	State  string              `json:"state"`
	Tables map[string][]uint32 `json:"tables"`
}

func (db *Database) transactionJournalPath() string {
	return filepath.Join(db.dataDir, "_transactions.flop")
}

func (db *Database) loadTransactionJournal() error {
	data, err := os.ReadFile(db.transactionJournalPath())
	if err != nil {
		if os.IsNotExist(err) {
			db.txJournal = transactionJournal{Version: transactionJournalVersion}
			return nil
		}
		return fmt.Errorf("read transaction journal: %w", err)
	}

	var journal transactionJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return fmt.Errorf("decode transaction journal: %w", err)
	}
	if journal.Version != transactionJournalVersion {
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
	return nil
}

func (db *Database) persistTransactionJournalLocked() error {
	db.txJournal.Version = transactionJournalVersion
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
	return db.persistTransactionJournalLocked()
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

	db.transactionMu.Lock()
	defer db.transactionMu.Unlock()

	tx := coordinatedTransaction{
		ID:     randomHex(16),
		State:  transactionPrepared,
		Tables: transactionTableTxIDs(walBuffers),
	}
	db.txJournal.Transactions = append(db.txJournal.Transactions, tx)
	txIndex := len(db.txJournal.Transactions) - 1
	if err := db.persistTransactionJournalLocked(); err != nil {
		db.txJournal.Transactions = db.txJournal.Transactions[:txIndex]
		return err
	}

	names := make([]string, 0, len(walBuffers))
	for name := range walBuffers {
		names = append(names, name)
	}
	sort.Strings(names)

	var checkpointTables []*TableInstance
	for _, tableName := range names {
		entry := walBuffers[tableName]
		table := db.Tables[tableName]
		if table == nil {
			db.txJournal.Transactions[txIndex].State = transactionAborted
			_ = db.persistTransactionJournalLocked()
			return fmt.Errorf("transaction references unknown table %q", tableName)
		}
		if err := table.wal.FlushBatch(entry.records, entry.txIDs); err != nil {
			db.txJournal.Transactions[txIndex].State = transactionAborted
			_ = db.persistTransactionJournalLocked()
			return err
		}
		// Cross-table commits always sync every participant before the global
		// commit decision, including when normal sync mode is configured.
		if err := table.wal.Fsync(); err != nil {
			db.txJournal.Transactions[txIndex].State = transactionAborted
			_ = db.persistTransactionJournalLocked()
			return err
		}
		if table.bumpWALEntryCount(len(entry.records) + len(entry.txIDs)) {
			checkpointTables = append(checkpointTables, table)
		}
	}

	db.txJournal.Transactions[txIndex].State = transactionCommitted
	if err := db.persistTransactionJournalLocked(); err != nil {
		return err
	}
	for tableName, entry := range walBuffers {
		if table := db.Tables[tableName]; table != nil {
			table.clearPendingKeys(entry.pending)
		}
	}
	if len(checkpointTables) > 0 {
		go func(tables []*TableInstance) {
			for _, table := range tables {
				_ = table.Checkpoint()
			}
		}(checkpointTables)
	}
	return nil
}
