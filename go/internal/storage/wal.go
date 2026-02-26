package storage

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/util"
)

// WAL operation codes.
const (
	WALOpInsert = 1
	WALOpUpdate = 2
	WALOpDelete = 3
	WALOpCommit = 4
)

const walHeaderSize = 16

// WALEntry is a single WAL record after replay.
type WALEntry struct {
	TxID uint32
	Op   byte
	Data []byte
}

// WAL manages a per-table write-ahead log.
type WAL struct {
	Path      string
	file      *os.File
	txCounter atomic.Uint32
	mu        sync.Mutex
	closed    bool
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w := &WAL{Path: path, file: f}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if stat.Size() == 0 {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := w.readHeader(); err != nil {
			// Reset on bad header
			f.Truncate(0)
			w.writeHeader()
		}
	}

	return w, nil
}

func (w *WAL) writeHeader() error {
	buf := make([]byte, walHeaderSize)
	copy(buf[0:4], schema.WALFileMagic[:])
	binary.LittleEndian.PutUint32(buf[4:8], 1)   // version
	binary.LittleEndian.PutUint32(buf[8:12], 0)  // checkpoint LSN
	binary.LittleEndian.PutUint32(buf[12:16], 0) // reserved
	_, err := w.file.WriteAt(buf, 0)
	return err
}

func (w *WAL) readHeader() error {
	buf := make([]byte, walHeaderSize)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		return err
	}
	for i := 0; i < 4; i++ {
		if buf[i] != schema.WALFileMagic[i] {
			return ErrShortBuffer
		}
	}
	return nil
}

// BeginTransaction returns a new transaction ID.
func (w *WAL) BeginTransaction() uint32 {
	return w.txCounter.Add(1)
}

// Append writes a WAL entry to disk.
func (w *WAL) Append(txID uint32, op byte, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(txID, op, data)
}

func (w *WAL) appendLocked(txID uint32, op byte, data []byte) error {
	record := w.BuildRecord(txID, op, data)
	_, err := w.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	_, err = w.file.Write(record)
	return err
}

// Commit writes a COMMIT entry and fsyncs.
func (w *WAL) Commit(txID uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.appendLocked(txID, WALOpCommit, nil); err != nil {
		return err
	}
	return w.file.Sync()
}

// BuildRecord creates a WAL record in memory (no I/O).
func (w *WAL) BuildRecord(txID uint32, op byte, data []byte) []byte {
	if data == nil {
		data = []byte{}
	}
	// recordLen(4) + txId(4) + op(1) + dataLen(4) + data + crc32(4)
	recordLen := uint32(4 + 1 + 4 + len(data) + 4)
	buf := make([]byte, 4+recordLen)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], recordLen)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], txID)
	offset += 4
	buf[offset] = op
	offset++
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(data)))
	offset += 4
	copy(buf[offset:], data)
	offset += len(data)

	checksum := util.CRC32(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], checksum)

	return buf
}

// FlushBatch writes multiple pre-built records + commit markers in one write.
func (w *WAL) FlushBatch(records [][]byte, txIDs []uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build commit records
	var commitRecords [][]byte
	for _, txID := range txIDs {
		commitRecords = append(commitRecords, w.BuildRecord(txID, WALOpCommit, nil))
	}

	totalSize := 0
	for _, r := range records {
		totalSize += len(r)
	}
	for _, r := range commitRecords {
		totalSize += len(r)
	}

	buf := make([]byte, 0, totalSize)
	for _, r := range records {
		buf = append(buf, r...)
	}
	for _, r := range commitRecords {
		buf = append(buf, r...)
	}

	if _, err := w.file.Seek(0, os.SEEK_END); err != nil {
		return err
	}
	_, err := w.file.Write(buf)
	return err
}

// Fsync flushes the WAL file.
func (w *WAL) Fsync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Replay reads all WAL entries for crash recovery.
func (w *WAL) Replay() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	stat, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() <= walHeaderSize {
		return nil, nil
	}

	dataSize := stat.Size() - walHeaderSize
	fullBuf := make([]byte, dataSize)
	n, err := w.file.ReadAt(fullBuf, walHeaderSize)
	if err != nil && int64(n) < dataSize {
		fullBuf = fullBuf[:n]
	}

	var entries []WALEntry
	offset := 0

	for offset+4 <= len(fullBuf) {
		recordLen := binary.LittleEndian.Uint32(fullBuf[offset : offset+4])
		if recordLen == 0 || offset+4+int(recordLen) > len(fullBuf) {
			break
		}

		recordStart := offset + 4
		txID := binary.LittleEndian.Uint32(fullBuf[recordStart : recordStart+4])
		op := fullBuf[recordStart+4]
		dataLen := binary.LittleEndian.Uint32(fullBuf[recordStart+5 : recordStart+9])
		data := make([]byte, dataLen)
		copy(data, fullBuf[recordStart+9:recordStart+9+int(dataLen)])

		// Verify CRC32
		expectedCRC := binary.LittleEndian.Uint32(fullBuf[recordStart+9+int(dataLen):])
		actualCRC := util.CRC32(fullBuf[offset : recordStart+9+int(dataLen)])

		if expectedCRC == actualCRC {
			entries = append(entries, WALEntry{TxID: txID, Op: op, Data: data})
		} else {
			break // corrupted entry
		}

		offset += 4 + int(recordLen)
	}

	return entries, nil
}

// FindCommittedTxIDs returns the set of committed transaction IDs.
func FindCommittedTxIDs(entries []WALEntry) map[uint32]bool {
	committed := make(map[uint32]bool)
	for _, e := range entries {
		if e.Op == WALOpCommit {
			committed[e.TxID] = true
		}
	}
	return committed
}

// Truncate resets the WAL to header only.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(walHeaderSize); err != nil {
		return err
	}
	return w.writeHeader()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}
