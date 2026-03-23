package storage

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/marcisbee/flop/internal/schema"
)

// MIME type mappings.
var extToMime = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".pdf":  "application/pdf",
	".json": "application/json",
	".txt":  "text/plain",
	".html": "text/html",
	".css":  "text/css",
	".js":   "application/javascript",
	".svg":  "image/svg+xml",
	".mp4":  "video/mp4",
	".mp3":  "audio/mpeg",
	".wav":  "audio/wav",
	".zip":  "application/zip",
}

// MimeFromExtension returns the MIME type for a filename.
func MimeFromExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if m, ok := extToMime[ext]; ok {
		return m
	}
	return "application/octet-stream"
}

// SanitizeFilename removes dangerous characters from a filename.
func SanitizeFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		default:
			return r
		}
	}, name)
	name = strings.ReplaceAll(name, "..", "_")
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}
	return name
}

func hashFilename(data []byte, originalName string) string {
	h := fnv.New32a()
	h.Write(data)
	hash := strconv.FormatUint(uint64(h.Sum32()), 36)
	ext := filepath.Ext(strings.ToLower(originalName))
	return hash + ext
}

// StoreFile saves a file to the data directory and returns a FileRef.
func StoreFile(dataDir, tableName, rowID, fieldName, filename string, data []byte, mime string) (*schema.FileRef, error) {
	hashedName := hashFilename(data, filename)
	dirPath := filepath.Join(dataDir, "_files", tableName, rowID, fieldName)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	filePath := filepath.Join(dirPath, hashedName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return nil, err
	}

	relativePath := fmt.Sprintf("_files/%s/%s/%s/%s", tableName, rowID, fieldName, hashedName)
	return &schema.FileRef{
		Path: relativePath,
		Name: filename,
		Size: int64(len(data)),
		Mime: mime,
		URL:  fmt.Sprintf("/api/files/%s/%s/%s/%s", tableName, rowID, fieldName, hashedName),
	}, nil
}

// DeleteFileRef removes a file from disk.
func DeleteFileRef(dataDir string, ref *schema.FileRef) error {
	filePath := filepath.Join(dataDir, ref.Path)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	parts := strings.Split(filepath.ToSlash(strings.TrimPrefix(ref.Path, "_files/")), "/")
	if len(parts) == 4 {
		thumbDir := filepath.Join(dataDir, "_thumbs", parts[0], parts[1], parts[2])
		pattern := filepath.Join(thumbDir, "*_"+parts[3])
		if matches, err := filepath.Glob(pattern); err == nil {
			for _, match := range matches {
				_ = os.Remove(match)
			}
		}
	}
	return nil
}

// DeleteRowFiles removes the entire row's file directory.
func DeleteRowFiles(dataDir, tableName, rowID string) error {
	dirPath := filepath.Join(dataDir, "_files", tableName, rowID)
	return os.RemoveAll(dirPath)
}

// ValidateMimeType checks if a declared MIME type is in the allowed list.
func ValidateMimeType(declared string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == declared {
			return true
		}
		if strings.HasSuffix(a, "/*") && strings.HasPrefix(declared, a[:len(a)-1]) {
			return true
		}
	}
	return false
}
