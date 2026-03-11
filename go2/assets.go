package flop

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// AssetManager handles file storage with content-addressable storage.
type AssetManager struct {
	baseDir    string
	maxSize    int64
	allowTypes map[string]bool
}

func NewAssetManager(baseDir string) *AssetManager {
	if err := os.MkdirAll(baseDir, 0700); err != nil {
		panic(fmt.Sprintf("create asset dir: %v", err))
	}
	return &AssetManager{
		baseDir: baseDir,
		maxSize: 10 * 1024 * 1024, // 10MB default
		allowTypes: map[string]bool{
			"image/jpeg": true,
			"image/png":  true,
			"image/webp": true,
			"image/gif":  true,
		},
	}
}

// SetMaxSize sets the maximum upload size in bytes.
func (am *AssetManager) SetMaxSize(size int64) {
	am.maxSize = size
}

// SetAllowedTypes sets allowed MIME types.
func (am *AssetManager) SetAllowedTypes(types []string) {
	am.allowTypes = make(map[string]bool)
	for _, t := range types {
		am.allowTypes[t] = true
	}
}

// Store saves a file and returns its Asset metadata.
func (am *AssetManager) Store(data []byte, contentType string) (*Asset, error) {
	if int64(len(data)) > am.maxSize {
		return nil, fmt.Errorf("file too large: %d > %d", len(data), am.maxSize)
	}

	if len(am.allowTypes) > 0 && !am.allowTypes[contentType] {
		return nil, fmt.Errorf("content type %q not allowed", contentType)
	}

	// Compute hash
	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])

	// Content-addressable path: assets/ab/cd/abcdef...
	dir := filepath.Join(am.baseDir, hashStr[:2], hashStr[2:4])
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	ext := mimeToExt(contentType)
	filename := hashStr + ext
	path := filepath.Join(dir, filename)

	// Write only if not already exists (dedup)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.WriteFile(path, data, 0600); err != nil {
			return nil, err
		}
	}

	return &Asset{
		Hash:        hash,
		ContentType: contentType,
		Size:        int64(len(data)),
		Path:        "/assets/" + hashStr + ext,
	}, nil
}

// StoreFromRequest handles multipart file upload from HTTP request.
func (am *AssetManager) StoreFromRequest(r *http.Request, field string) (*Asset, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, am.maxSize)

	file, header, err := r.FormFile(field)
	if err != nil {
		return nil, fmt.Errorf("read upload: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}

	return am.Store(data, contentType)
}

// ServeHandler returns an HTTP handler for serving assets.
func (am *AssetManager) ServeHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Path: /assets/{hash}.{ext}
		reqPath := strings.TrimPrefix(r.URL.Path, "/assets/")
		if reqPath == "" {
			http.NotFound(w, r)
			return
		}

		// Extract hash (remove extension)
		hashStr := reqPath
		if idx := strings.LastIndex(hashStr, "."); idx > 0 {
			hashStr = hashStr[:idx]
		}

		if len(hashStr) < 4 {
			http.NotFound(w, r)
			return
		}

		// Find file in content-addressable storage
		dir := filepath.Join(am.baseDir, hashStr[:2], hashStr[2:4])
		entries, err := os.ReadDir(dir)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), hashStr) {
				filePath := filepath.Join(dir, entry.Name())
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				w.Header().Set("ETag", `"`+hashStr+`"`)

				// Check ETag
				if r.Header.Get("If-None-Match") == `"`+hashStr+`"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}

				http.ServeFile(w, r, filePath)
				return
			}
		}

		http.NotFound(w, r)
	})
}

func mimeToExt(contentType string) string {
	switch contentType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "application/pdf":
		return ".pdf"
	default:
		return ".bin"
	}
}
