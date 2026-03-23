package images

import (
	"bytes"
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

type ThumbSize struct {
	Width  int
	Height int
}

type ResizeMode string

const (
	ResizeContain ResizeMode = "contain"
	ResizeCover   ResizeMode = "cover"
)

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

func ThumbPath(dataDir, table, rowID, field, filename string, size ThumbSize) string {
	name := fmt.Sprintf("%dx%d_%s", size.Width, size.Height, filename)
	return filepath.Join(dataDir, "_thumbs", table, rowID, field, name)
}

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

	data, _, err := ResizeEncodedImage(img, filepath.Ext(destPath), size, ResizeContain)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return fmt.Errorf("write thumb: %w", err)
	}
	return nil
}

func ReadDimensions(path string) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return 0, 0, err
	}
	return cfg.Width, cfg.Height, nil
}

func ResizeEncodedImage(img image.Image, ext string, size ThumbSize, mode ResizeMode) ([]byte, string, error) {
	bounds := img.Bounds()
	newW, newH := calcOutputDimensions(bounds.Dx(), bounds.Dy(), size, mode)
	if newW <= 0 || newH <= 0 {
		return nil, "", fmt.Errorf("invalid computed dimensions: %dx%d", newW, newH)
	}

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	if normalizeResizeMode(mode) == ResizeCover && size.Width > 0 && size.Height > 0 {
		srcRect := coverSourceRect(bounds, newW, newH)
		draw.CatmullRom.Scale(dst, dst.Bounds(), img, srcRect, draw.Over, nil)
	} else {
		draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)
	}

	normalizedExt := normalizeOutputExt(ext)
	var buf bytes.Buffer
	switch normalizedExt {
	case ".png":
		if err := png.Encode(&buf, dst); err != nil {
			return nil, "", err
		}
	default:
		if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85}); err != nil {
			return nil, "", err
		}
		normalizedExt = ".jpg"
	}
	return buf.Bytes(), normalizedExt, nil
}

func IsThumbAllowed(size string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(a, size) {
			return true
		}
	}
	return false
}

func calcOutputDimensions(origW, origH int, size ThumbSize, mode ResizeMode) (int, int) {
	if normalizeResizeMode(mode) == ResizeCover && size.Width > 0 && size.Height > 0 {
		return size.Width, size.Height
	}
	return calcContainDimensions(origW, origH, size)
}

func calcContainDimensions(origW, origH int, size ThumbSize) (int, int) {
	w, h := size.Width, size.Height
	if w == 0 && h == 0 {
		return origW, origH
	}
	if w == 0 {
		w = origW * h / origH
	} else if h == 0 {
		h = origH * w / origW
	} else {
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

func coverSourceRect(bounds image.Rectangle, targetW, targetH int) image.Rectangle {
	origW := bounds.Dx()
	origH := bounds.Dy()
	if origW <= 0 || origH <= 0 || targetW <= 0 || targetH <= 0 {
		return bounds
	}
	srcW := origW
	srcH := origH
	targetRatio := float64(targetW) / float64(targetH)
	srcRatio := float64(origW) / float64(origH)
	if srcRatio > targetRatio {
		srcW = int(float64(origH) * targetRatio)
		if srcW < 1 {
			srcW = 1
		}
	} else if srcRatio < targetRatio {
		srcH = int(float64(origW) / targetRatio)
		if srcH < 1 {
			srcH = 1
		}
	}
	minX := bounds.Min.X + (origW-srcW)/2
	minY := bounds.Min.Y + (origH-srcH)/2
	return image.Rect(minX, minY, minX+srcW, minY+srcH)
}

func normalizeResizeMode(mode ResizeMode) ResizeMode {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case string(ResizeCover):
		return ResizeCover
	default:
		return ResizeContain
	}
}

func normalizeOutputExt(ext string) string {
	switch strings.ToLower(strings.TrimSpace(ext)) {
	case ".png":
		return ".png"
	case ".jpg", ".jpeg":
		return ".jpg"
	default:
		return ".jpg"
	}
}
