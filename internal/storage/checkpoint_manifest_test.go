package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointManifestReadLatestGeneration(t *testing.T) {
	dir := t.TempDir()
	table := "items"

	m1 := &CheckpointManifest{
		Generation:    1,
		CheckpointLSN: 10,
		Files:         map[string]uint32{"items.idx": 123},
	}
	if err := WriteCheckpointManifest(dir, table, m1); err != nil {
		t.Fatalf("write manifest gen1: %v", err)
	}

	m2 := &CheckpointManifest{
		Generation:    2,
		CheckpointLSN: 20,
		Files:         map[string]uint32{"items.idx": 456},
	}
	if err := WriteCheckpointManifest(dir, table, m2); err != nil {
		t.Fatalf("write manifest gen2: %v", err)
	}

	got, err := ReadLatestCheckpointManifest(dir, table)
	if err != nil {
		t.Fatalf("read latest manifest: %v", err)
	}
	if got == nil {
		t.Fatal("expected latest manifest")
	}
	if got.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", got.Generation)
	}
	if got.CheckpointLSN != 20 {
		t.Fatalf("expected checkpoint lsn 20, got %d", got.CheckpointLSN)
	}
}

func TestValidateCheckpointManifest(t *testing.T) {
	dir := t.TempDir()
	table := "items"
	fileName := "items.midx"
	filePath := filepath.Join(dir, fileName)

	if err := os.WriteFile(filePath, []byte("abc123"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	hash, err := ComputeFileCRC32(filePath)
	if err != nil {
		t.Fatalf("hash file: %v", err)
	}

	manifest := &CheckpointManifest{
		Generation:    1,
		CheckpointLSN: 42,
		Files:         map[string]uint32{fileName: hash},
	}
	if !ValidateCheckpointManifest(dir, table, manifest) {
		t.Fatal("expected manifest validation success")
	}

	if err := os.WriteFile(filePath, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("mutate file: %v", err)
	}
	if ValidateCheckpointManifest(dir, table, manifest) {
		t.Fatal("expected manifest validation failure after mutation")
	}
}
