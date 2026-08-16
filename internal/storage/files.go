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

// IsSafePathSegment reports whether s is safe to use as a single path
// segment in a filepath.Join chain. It rejects empty values, dot segments,
// path separators, and NUL bytes so the joined path cannot escape its base
// directory.
func IsSafePathSegment(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	if strings.ContainsAny(s, "/\\") || strings.ContainsRune(s, 0) {
		return false
	}
	return true
}

// ResolveContained joins relPath onto base and verifies the result stays
// inside base. The relPath must be clean (no ".", "..", or redundant
// separators) and must not escape base. It returns the joined path.
func ResolveContained(base, relPath string) (string, error) {
	if base == "" || relPath == "" {
		return "", fmt.Errorf("path is empty")
	}
	rel := filepath.FromSlash(relPath)
	if filepath.IsAbs(rel) || filepath.Clean(rel) != rel {
		return "", fmt.Errorf("path %q is not a clean relative path", relPath)
	}
	joined := filepath.Join(base, rel)
	relToBase, err := filepath.Rel(base, joined)
	if err != nil || relToBase == ".." || strings.HasPrefix(relToBase, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes its base directory", relPath)
	}
	return joined, nil
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
	if !IsSafePathSegment(tableName) || !IsSafePathSegment(rowID) || !IsSafePathSegment(fieldName) {
		return nil, fmt.Errorf("invalid table, row, or field name for file storage")
	}
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

// DeleteFileRef removes a file from disk. Refs whose stored path is not a
// clean, contained "_files/" path are skipped so a malicious or corrupted
// ref can never delete files outside the file storage directory.
func DeleteFileRef(dataDir string, ref *schema.FileRef) error {
	filePath, err := ResolveContained(filepath.Join(dataDir, "_files"), strings.TrimPrefix(ref.Path, "_files/"))
	if err != nil || !strings.HasPrefix(ref.Path, "_files/") {
		// Not a path we are allowed to touch — nothing to delete.
		return nil
	}
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	parts := strings.Split(filepath.ToSlash(strings.TrimPrefix(ref.Path, "_files/")), "/")
	if len(parts) == 4 && IsSafePathSegment(parts[0]) && IsSafePathSegment(parts[1]) && IsSafePathSegment(parts[2]) && IsSafePathSegment(parts[3]) {
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
	if !IsSafePathSegment(tableName) || !IsSafePathSegment(rowID) {
		// Refuse to derive a directory from an unsafe name.
		return nil
	}
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
