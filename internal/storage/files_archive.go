package storage

import (
	"os"
	"path/filepath"
)

func archiveFilesDir(dataDir, tableName, archiveID string) string {
	return filepath.Join(dataDir, "_archive_files", tableName, archiveID)
}

// ArchiveRowFiles moves row files to archive storage.
func ArchiveRowFiles(dataDir, tableName, rowID, archiveID string) error {
	src := filepath.Join(dataDir, "_files", tableName, rowID)
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	dst := archiveFilesDir(dataDir, tableName, archiveID)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

// RestoreArchivedRowFiles moves archived row files back to live storage.
func RestoreArchivedRowFiles(dataDir, tableName, archiveID, rowID string) error {
	src := archiveFilesDir(dataDir, tableName, archiveID)
	if _, err := os.Stat(src); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	dst := filepath.Join(dataDir, "_files", tableName, rowID)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

// DeleteArchivedRowFiles removes archived file assets permanently.
func DeleteArchivedRowFiles(dataDir, tableName, archiveID string) error {
	return os.RemoveAll(archiveFilesDir(dataDir, tableName, archiveID))
}
