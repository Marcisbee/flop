package storage

import (
	"bytes"
	"fmt"
	"image"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcisbee/flop/internal/images"
	"github.com/marcisbee/flop/internal/schema"
)

func StoreFileWithField(dataDir, tableName, rowID, fieldName, filename string, data []byte, mime string, field *schema.CompiledField) (*schema.FileRef, error) {
	if field == nil {
		return nil, fmt.Errorf("field configuration is required")
	}
	if mime == "" {
		mime = MimeFromExtension(filename)
	}
	if !ValidateMimeType(mime, field.MimeTypes) {
		return nil, fmt.Errorf("file type %s not allowed", mime)
	}
	if field.MaxUploadBytes > 0 && int64(len(data)) > field.MaxUploadBytes {
		return nil, fmt.Errorf("file exceeds max upload size of %d bytes", field.MaxUploadBytes)
	}

	if !isImageMime(mime) {
		if field.ImageMaxSize != "" || field.DiscardOriginal || len(field.ThumbSizes) > 0 {
			return nil, fmt.Errorf("image processing requires an image mime type")
		}
		return StoreFile(dataDir, tableName, rowID, fieldName, filename, data, mime)
	}

	thumbSizes, err := parseThumbSizes(field.ThumbSizes)
	if err != nil {
		return nil, err
	}

	var canonicalSize *images.ThumbSize
	if field.ImageMaxSize != "" {
		size, err := images.ParseThumbSize(field.ImageMaxSize)
		if err != nil {
			return nil, err
		}
		canonicalSize = &size
	} else if field.DiscardOriginal {
		if len(thumbSizes) == 0 {
			return nil, fmt.Errorf("discard-original requires imageMax or at least one thumb size")
		}
		size := thumbSizes[largestThumbIndex(thumbSizes)]
		canonicalSize = &size
	}

	mode := imageResizeMode(field)
	storedData := data
	storedMime := mime
	if canonicalSize != nil {
		storedData, storedMime, err = resizeImageBytesWithMode(data, mime, *canonicalSize, mode)
		if err != nil {
			return nil, err
		}
	}

	ref, err := StoreFile(dataDir, tableName, rowID, fieldName, filename, storedData, storedMime)
	if err != nil {
		return nil, err
	}

	if len(thumbSizes) == 0 {
		return ref, nil
	}

	thumbDir := filepath.Join(dataDir, "_thumbs", tableName, rowID, fieldName)
	_ = os.RemoveAll(thumbDir)
	for _, size := range thumbSizes {
		thumbPath := images.ThumbPath(dataDir, tableName, rowID, fieldName, filepath.Base(ref.Path), size)
		if err := images.GenerateThumb(filepath.Join(dataDir, ref.Path), thumbPath, size); err != nil {
			return nil, err
		}
	}
	return ref, nil
}

func parseThumbSizes(raw []string) ([]images.ThumbSize, error) {
	sizes := make([]images.ThumbSize, 0, len(raw))
	for _, item := range raw {
		size, err := images.ParseThumbSize(item)
		if err != nil {
			return nil, err
		}
		sizes = append(sizes, size)
	}
	return sizes, nil
}

func largestThumbIndex(sizes []images.ThumbSize) int {
	best := 0
	bestScore := -1
	for i, size := range sizes {
		score := size.Width * size.Height
		if score == 0 {
			score = max(size.Width, size.Height) * 1_000_000
		}
		if score > bestScore {
			best = i
			bestScore = score
		}
	}
	return best
}

func resizeImageBytes(data []byte, mime string, size images.ThumbSize) ([]byte, string, error) {
	return resizeImageBytesWithMode(data, mime, size, images.ResizeContain)
}

func resizeImageBytesWithMode(data []byte, mime string, size images.ThumbSize, mode images.ResizeMode) ([]byte, string, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("decode image: %w", err)
	}
	outExt := outputExtForMime(mime)
	resized, normalizedExt, err := images.ResizeEncodedImage(img, outExt, size, mode)
	if err != nil {
		return nil, "", err
	}
	return resized, mimeForOutputExt(normalizedExt), nil
}

func imageResizeMode(field *schema.CompiledField) images.ResizeMode {
	if field == nil {
		return images.ResizeContain
	}
	if strings.EqualFold(field.ImageFit, string(images.ResizeCover)) {
		return images.ResizeCover
	}
	return images.ResizeContain
}

func outputExtForMime(mime string) string {
	mime = strings.ToLower(strings.TrimSpace(mime))
	if mime == "image/png" {
		return ".png"
	}
	if mime == "image/jpeg" || mime == "image/jpg" {
		return ".jpg"
	}
	return ".jpg"
}

func mimeForOutputExt(ext string) string {
	if ext == ".png" {
		return "image/png"
	}
	return "image/jpeg"
}

func isImageMime(mime string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(mime)), "image/")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
