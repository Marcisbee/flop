package storage

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/marcisbee/flop/internal/util"
)

const checkpointManifestVersion = 1

// CheckpointManifest captures a durable checkpoint generation and hashes of
// persisted index artifacts so startup can reject partial/mixed checkpoints.
type CheckpointManifest struct {
	Version       int               `json:"version"`
	Generation    uint64            `json:"generation"`
	CheckpointLSN uint64            `json:"checkpoint_lsn"`
	Files         map[string]uint32 `json:"files"`
}

func checkpointManifestPaths(dataDir, tableName string) (string, string) {
	base := filepath.Join(dataDir, tableName+".ckpt")
	return base + ".a", base + ".b"
}

// ReadLatestCheckpointManifest returns the newest valid manifest from the A/B slots.
func ReadLatestCheckpointManifest(dataDir, tableName string) (*CheckpointManifest, error) {
	pathA, pathB := checkpointManifestPaths(dataDir, tableName)
	manifests := make([]*CheckpointManifest, 0, 2)

	for _, path := range []string{pathA, pathB} {
		m, err := readCheckpointManifestFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			// Treat corrupted slots as invalid and continue.
			continue
		}
		manifests = append(manifests, m)
	}
	if len(manifests) == 0 {
		return nil, nil
	}
	best := manifests[0]
	for i := 1; i < len(manifests); i++ {
		if manifests[i].Generation > best.Generation {
			best = manifests[i]
		}
	}
	return best, nil
}

// WriteCheckpointManifest writes a manifest to the generation-selected slot.
func WriteCheckpointManifest(dataDir, tableName string, manifest *CheckpointManifest) error {
	if manifest == nil {
		return nil
	}
	m := *manifest
	if m.Version == 0 {
		m.Version = checkpointManifestVersion
	}
	if m.Files == nil {
		m.Files = map[string]uint32{}
	}

	encoded, err := json.Marshal(&m)
	if err != nil {
		return err
	}

	pathA, pathB := checkpointManifestPaths(dataDir, tableName)
	path := pathA
	if m.Generation%2 == 0 {
		path = pathB
	}
	return writeFileAtomic(path, encoded, 0o600)
}

func readCheckpointManifestFile(path string) (*CheckpointManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m CheckpointManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Version != checkpointManifestVersion || m.Generation == 0 {
		return nil, os.ErrInvalid
	}
	if m.Files == nil {
		m.Files = map[string]uint32{}
	}
	return &m, nil
}

// ComputeFileCRC32 returns CRC32 of file contents.
func ComputeFileCRC32(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return computeOpenFileCRC32(f)
}

// Keep checksum memory bounded even for large checkpoint artifacts.
func computeOpenFileCRC32(f *os.File) (uint32, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, os.ErrInvalid
	}
	buf := make([]byte, min(info.Size(), 64*1024-1)+1)
	var sum uint32
	for {
		n, err := f.Read(buf)
		sum = util.CRC32Update(sum, buf[:n])
		if err == io.EOF {
			return sum, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

// ValidateCheckpointManifest checks whether all recorded file hashes match.
func ValidateCheckpointManifest(dataDir, tableName string, manifest *CheckpointManifest) bool {
	_ = tableName
	if manifest == nil || manifest.Generation == 0 {
		return false
	}
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return false
	}
	defer root.Close()
	for relPath, expected := range manifest.Files {
		// Resolve through the directory handle so traversal and symlink swaps
		// cannot redirect checkpoint validation outside the database.
		f, err := root.Open(relPath)
		if err != nil {
			return false
		}
		got, err := computeOpenFileCRC32(f)
		closeErr := f.Close()
		if closeErr != nil {
			return false
		}
		if err != nil || got != expected {
			return false
		}
	}
	return true
}
