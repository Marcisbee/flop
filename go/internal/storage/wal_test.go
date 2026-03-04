package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/util"
)

func TestWALReplayV2BeginCommitLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = w.Close() }()

	txID := w.BeginTransaction()
	records := [][]byte{
		w.BuildBeginRecord(txID),
		w.BuildRecord(txID, WALOpInsert, []byte("payload")),
	}
	if err := w.FlushBatch(records, []uint32{txID}); err != nil {
		t.Fatalf("flush batch: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (begin/insert/commit), got %d", len(entries))
	}
	if entries[0].Op != WALOpBegin || entries[1].Op != WALOpInsert || entries[2].Op != WALOpCommit {
		t.Fatalf("unexpected op order: %d %d %d", entries[0].Op, entries[1].Op, entries[2].Op)
	}
	if entries[0].LSN == 0 || entries[1].LSN == 0 || entries[2].LSN == 0 {
		t.Fatalf("expected non-zero v2 LSNs, got %d %d %d", entries[0].LSN, entries[1].LSN, entries[2].LSN)
	}
	if !(entries[0].LSN < entries[1].LSN && entries[1].LSN < entries[2].LSN) {
		t.Fatalf("expected increasing LSNs, got %d %d %d", entries[0].LSN, entries[1].LSN, entries[2].LSN)
	}
	committed := FindCommittedTxIDs(entries)
	if !committed[txID] {
		t.Fatalf("expected tx %d committed", txID)
	}
}

func TestWALReplayLegacyV1CompatibilityAndUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.wal")
	if err := writeLegacyWAL(path); err != nil {
		t.Fatalf("write legacy wal: %v", err)
	}

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open legacy wal: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		_ = w.Close()
		t.Fatalf("replay legacy wal: %v", err)
	}
	if len(entries) != 2 {
		_ = w.Close()
		t.Fatalf("expected 2 legacy entries, got %d", len(entries))
	}
	if entries[0].Op != WALOpInsert || entries[1].Op != WALOpCommit {
		_ = w.Close()
		t.Fatalf("unexpected legacy ops: %d %d", entries[0].Op, entries[1].Op)
	}
	if entries[0].LSN != 0 || entries[1].LSN != 0 {
		_ = w.Close()
		t.Fatalf("expected zero LSNs for legacy entries, got %d %d", entries[0].LSN, entries[1].LSN)
	}

	if err := w.Truncate(); err != nil {
		_ = w.Close()
		t.Fatalf("truncate upgrade: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgraded header: %v", err)
	}
	if len(raw) < walHeaderSize {
		t.Fatalf("wal too short after truncate: %d", len(raw))
	}
	version := binary.LittleEndian.Uint32(raw[4:8])
	if version != walVersionCurrent {
		t.Fatalf("expected upgraded wal version %d, got %d", walVersionCurrent, version)
	}
}

func writeLegacyWAL(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	header := make([]byte, walHeaderSize)
	copy(header[0:4], schema.WALFileMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], walVersionLegacyV1)
	if _, err := f.Write(header); err != nil {
		return err
	}

	insert := buildLegacyRecord(11, WALOpInsert, []byte("legacy"))
	commit := buildLegacyRecord(11, WALOpCommit, nil)
	if _, err := f.Write(insert); err != nil {
		return err
	}
	if _, err := f.Write(commit); err != nil {
		return err
	}
	return nil
}

func buildLegacyRecord(txID uint32, op byte, data []byte) []byte {
	if data == nil {
		data = []byte{}
	}
	recordLen := uint32(4 + 1 + 4 + len(data) + 4)
	buf := make([]byte, 4+recordLen)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:], recordLen)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], txID)
	off += 4
	buf[off] = op
	off++
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(data)))
	off += 4
	copy(buf[off:], data)
	off += len(data)
	binary.LittleEndian.PutUint32(buf[off:], util.CRC32(buf[:off]))
	return buf
}
