package server

import (
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	// Register image format decoders.
	_ "image/gif"

	"golang.org/x/image/draw"
)

// ThumbSize represents a parsed thumbnail dimension.
type ThumbSize struct {
	Width  int
	Height int
}

// ParseThumbSize parses "WxH" into a ThumbSize. 0 means preserve aspect ratio.
func ParseThumbSize(s string) (ThumbSize, error) {
	parts := strings.SplitN(strings.ToLower(s), "x", 2)
	if len(parts) != 2 {
		return ThumbSize{}, fmt.Errorf("invalid thumb size %q: expected WxH", s)
	}
	w, err := strconv.Atoi(parts[0])
	if err != nil || w < 0 {
		return ThumbSize{}, fmt.Errorf("invalid thumb width in %q", s)
	}
	h, err := strconv.Atoi(parts[1])
	if err != nil || h < 0 {
		return ThumbSize{}, fmt.Errorf("invalid thumb height in %q", s)
	}
	if w == 0 && h == 0 {
		return ThumbSize{}, fmt.Errorf("thumb size %q: both width and height are 0", s)
	}
	return ThumbSize{Width: w, Height: h}, nil
}

// ThumbPath returns the on-disk path for a cached thumbnail.
// Layout: {dataDir}/_thumbs/{table}/{rowID}/{field}/{WxH}_{filename}
func ThumbPath(dataDir, table, rowID, field, filename string, size ThumbSize) string {
	name := fmt.Sprintf("%dx%d_%s", size.Width, size.Height, filename)
	return filepath.Join(dataDir, "_thumbs", table, rowID, field, name)
}

// GenerateThumb reads the source image, resizes it to the given thumb size,
// and writes the result to destPath. The output format matches the source extension.
func GenerateThumb(srcPath, destPath string, size ThumbSize) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	img, _, err := image.Decode(src)
	if err != nil {
		return fmt.Errorf("decode image: %w", err)
	}

	bounds := img.Bounds()
	origW := bounds.Dx()
	origH := bounds.Dy()

	newW, newH := calcThumbDimensions(origW, origH, size)
	if newW <= 0 || newH <= 0 {
		return fmt.Errorf("invalid computed dimensions: %dx%d", newW, newH)
	}

	// Don't upscale — if the source is smaller, use original dimensions
	if newW > origW && newH > origH {
		newW = origW
		newH = origH
	}

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create dest: %w", err)
	}
	defer out.Close()

	ext := strings.ToLower(filepath.Ext(destPath))
	switch ext {
	case ".png":
		return png.Encode(out, dst)
	case ".jpg", ".jpeg":
		return jpeg.Encode(out, dst, &jpeg.Options{Quality: 90})
	default:
		// Default to JPEG for unknown formats
		return jpeg.Encode(out, dst, &jpeg.Options{Quality: 90})
	}
}

// calcThumbDimensions computes the final thumbnail dimensions, preserving
// aspect ratio when width or height is 0.
func calcThumbDimensions(origW, origH int, size ThumbSize) (int, int) {
	w, h := size.Width, size.Height
	if w == 0 && h == 0 {
		return origW, origH
	}
	if w == 0 {
		// Preserve aspect ratio based on height
		w = origW * h / origH
	} else if h == 0 {
		// Preserve aspect ratio based on width
		h = origH * w / origW
	} else {
		// Fit within both dimensions, preserving aspect ratio
		ratioW := float64(w) / float64(origW)
		ratioH := float64(h) / float64(origH)
		if ratioW < ratioH {
			h = int(float64(origH) * ratioW)
		} else {
			w = int(float64(origW) * ratioH)
		}
	}
	if w < 1 {
		w = 1
	}
	if h < 1 {
		h = 1
	}
	return w, h
}

// IsThumbAllowed checks if a given thumb size string is in the allowed list.
func IsThumbAllowed(size string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(a, size) {
			return true
		}
	}
	return false
}
