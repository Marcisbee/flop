package storage

import (
	"bytes"
	"fmt"
	"hash/crc32"
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

func TestCheckpointManifestRejectsEscapingPaths(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside.idx")
	if err := os.WriteFile(outside, []byte("123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../outside.idx", "items.midx"} {
		t.Run(name, func(t *testing.T) {
			if name == "items.midx" {
				if err := os.Symlink(outside, filepath.Join(dir, name)); err != nil {
					t.Skip(err)
				}
			}
			m := &CheckpointManifest{Generation: 1, Files: map[string]uint32{name: 0xcbf43926}}
			if ValidateCheckpointManifest(dir, "items", m) {
				t.Fatal("accepted checkpoint file outside database directory")
			}
		})
	}
}

func BenchmarkComputeFileCRC32(b *testing.B) {
	for _, size := range []int{1024, 1024 * 1024, 16 * 1024 * 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "index")
			data := bytes.Repeat([]byte("123456789"), (size+8)/9)[:size]
			want := crc32.ChecksumIEEE(data)
			if err := os.WriteFile(path, data, 0600); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				got, err := ComputeFileCRC32(path)
				if err != nil || got != want {
					b.Fatalf("checksum=%x err=%v", got, err)
				}
			}
		})
	}
}

func TestComputeFileCRC32(t *testing.T) {
	for _, size := range []int{0, 9, 64*1024 - 1, 64 * 1024, 64*1024 + 1, 1024 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte("123456789"), (size+8)/9)[:size]
			path := filepath.Join(t.TempDir(), "index")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			got, err := ComputeFileCRC32(path)
			if want := crc32.ChecksumIEEE(data); err != nil || got != want {
				t.Fatalf("checksum=%x, want=%x err=%v", got, want, err)
			}
		})
	}
}
