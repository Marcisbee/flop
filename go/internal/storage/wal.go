package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/util"
)

// WAL operation codes.
const (
	WALOpBegin  = 0
	WALOpInsert = 1
	WALOpUpdate = 2
	WALOpDelete = 3
	WALOpCommit = 4
)

const walHeaderSize = 16
const walVersionLegacyV1 = 1
const walVersionCurrent = 2

// WALEntry is a single WAL record after replay.
type WALEntry struct {
	TxID uint32
	Op   byte
	LSN  uint64
	Data []byte
}

// WAL manages a per-table write-ahead log.
type WAL struct {
	Path          string
	file          *os.File
	txCounter     atomic.Uint32
	lsn           atomic.Uint64
	checkpointLSN atomic.Uint64
	mu            sync.Mutex
	closed        bool
	version       uint32
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w := &WAL{Path: path, file: f, version: walVersionCurrent}

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
			_ = f.Truncate(0)
			w.version = walVersionCurrent
			if err := w.writeHeader(); err != nil {
				f.Close()
				return nil, err
			}
		}
	}

	return w, nil
}

func (w *WAL) writeHeader() error {
	buf := make([]byte, walHeaderSize)
	copy(buf[0:4], schema.WALFileMagic[:])
	binary.LittleEndian.PutUint32(buf[4:8], w.version)
	if w.version >= walVersionCurrent {
		binary.LittleEndian.PutUint64(buf[8:16], w.checkpointLSN.Load())
	} else {
		binary.LittleEndian.PutUint32(buf[8:12], uint32(w.checkpointLSN.Load()))
		binary.LittleEndian.PutUint32(buf[12:16], 0)
	}
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
	version := binary.LittleEndian.Uint32(buf[4:8])
	if version != walVersionLegacyV1 && version != walVersionCurrent {
		return fmt.Errorf("unsupported wal version: %d", version)
	}
	w.version = version
	if version >= walVersionCurrent {
		cp := binary.LittleEndian.Uint64(buf[8:16])
		w.checkpointLSN.Store(cp)
		w.lsn.Store(cp)
	} else {
		cp := uint64(binary.LittleEndian.Uint32(buf[8:12]))
		w.checkpointLSN.Store(cp)
		w.lsn.Store(cp)
	}
	return nil
}

// BeginTransaction returns a new transaction ID.
func (w *WAL) BeginTransaction() uint32 {
	return w.txCounter.Add(1)
}

func (w *WAL) nextLSN() uint64 {
	return w.lsn.Add(1)
}

// CurrentLSN returns the highest LSN allocated in this WAL.
func (w *WAL) CurrentLSN() uint64 {
	return w.lsn.Load()
}

// CheckpointLSN returns the most recently persisted checkpoint LSN.
func (w *WAL) CheckpointLSN() uint64 {
	return w.checkpointLSN.Load()
}

// SetCheckpointLSN persists checkpoint LSN in the WAL header.
func (w *WAL) SetCheckpointLSN(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.version < walVersionCurrent {
		w.version = walVersionCurrent
	}
	w.checkpointLSN.Store(lsn)
	if err := w.writeHeader(); err != nil {
		return err
	}
	return w.file.Sync()
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
	record, _ := w.BuildRecordWithLSN(txID, op, data)
	return record
}

// BuildRecordWithLSN creates a WAL record and returns the assigned record LSN.
func (w *WAL) BuildRecordWithLSN(txID uint32, op byte, data []byte) ([]byte, uint64) {
	if w.version == walVersionLegacyV1 {
		return buildRecordV1(txID, op, data), 0
	}
	lsn := w.nextLSN()
	return buildRecordV2(txID, op, lsn, data), lsn
}

// BuildBeginRecord creates a WAL BEGIN record for the transaction.
func (w *WAL) BuildBeginRecord(txID uint32) []byte {
	record, _ := w.BuildRecordWithLSN(txID, WALOpBegin, nil)
	return record
}

func buildRecordV1(txID uint32, op byte, data []byte) []byte {
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

func buildRecordV2(txID uint32, op byte, lsn uint64, data []byte) []byte {
	if data == nil {
		data = []byte{}
	}
	// recordLen(4) + txID(4) + op(1) + lsn(8) + dataLen(4) + data + crc32(4)
	recordLen := uint32(4 + 1 + 8 + 4 + len(data) + 4)
	buf := make([]byte, 4+recordLen)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], recordLen)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], txID)
	offset += 4
	buf[offset] = op
	offset++
	binary.LittleEndian.PutUint64(buf[offset:], lsn)
	offset += 8
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
	// Build commit records outside the lock to minimize time holding w.mu.
	commitRecords := make([][]byte, len(txIDs))
	for i, txID := range txIDs {
		commitRecords[i] = w.BuildRecord(txID, WALOpCommit, nil)
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

	w.mu.Lock()
	defer w.mu.Unlock()
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

	entries := make([]WALEntry, 0, 64)
	offset := 0
	var maxLSN uint64

	for offset+4 <= len(fullBuf) {
		recordLen := int(binary.LittleEndian.Uint32(fullBuf[offset : offset+4]))
		if recordLen <= 0 || offset+4+recordLen > len(fullBuf) {
			break
		}

		recordStart := offset + 4
		recordEnd := recordStart + recordLen
		if recordEnd-recordStart < 9 {
			break
		}
		if recordEnd-recordStart < 13 {
			break
		}

		var (
			txID         uint32
			op           byte
			lsn          uint64
			dataLen      uint32
			dataStartPos int
		)
		txID = binary.LittleEndian.Uint32(fullBuf[recordStart : recordStart+4])
		op = fullBuf[recordStart+4]

		if w.version >= walVersionCurrent {
			if recordEnd-recordStart < 21 {
				break
			}
			lsn = binary.LittleEndian.Uint64(fullBuf[recordStart+5 : recordStart+13])
			dataLen = binary.LittleEndian.Uint32(fullBuf[recordStart+13 : recordStart+17])
			dataStartPos = recordStart + 17
		} else {
			dataLen = binary.LittleEndian.Uint32(fullBuf[recordStart+5 : recordStart+9])
			dataStartPos = recordStart + 9
		}

		dataEnd := dataStartPos + int(dataLen)
		crcPos := dataEnd
		if dataEnd < dataStartPos || crcPos+4 > recordEnd {
			break
		}

		expectedCRC := binary.LittleEndian.Uint32(fullBuf[crcPos : crcPos+4])
		actualCRC := util.CRC32(fullBuf[offset:crcPos])
		if expectedCRC != actualCRC {
			break // stop at first corrupted/torn record
		}

		data := make([]byte, dataLen)
		copy(data, fullBuf[dataStartPos:dataEnd])
		entries = append(entries, WALEntry{TxID: txID, Op: op, LSN: lsn, Data: data})
		if lsn > maxLSN {
			maxLSN = lsn
		}
		offset += 4 + recordLen
	}

	if maxLSN > 0 {
		w.lsn.Store(maxLSN)
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

// FindCommittedTxLSN returns commit LSN by transaction ID.
func FindCommittedTxLSN(entries []WALEntry) map[uint32]uint64 {
	committed := make(map[uint32]uint64)
	for _, e := range entries {
		if e.Op == WALOpCommit {
			committed[e.TxID] = e.LSN
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
	if w.version < walVersionCurrent {
		w.version = walVersionCurrent
	}
	if err := w.writeHeader(); err != nil {
		return err
	}
	return w.file.Sync()
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
