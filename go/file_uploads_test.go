package flop

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreFileForFieldStoreOnlyThumbs(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("avatar", "image/png", "image/jpeg").
			MaxUploadBytes(2<<20).
			Thumbs("100x100", "500x500").
			StoreOnlyThumbs()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u1"}); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	data := makeSolidPNG(t, 1200, 800)
	ref, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", data, "image/png")
	if err != nil {
		t.Fatalf("store file: %v", err)
	}
	if ref == nil {
		t.Fatal("expected file ref")
	}

	fullPath := filepath.Join(tmp, ref.Path)
	if _, err := os.Stat(fullPath); err != nil {
		t.Fatalf("expected canonical file on disk: %v", err)
	}
	w, h := readImageSize(t, fullPath)
	if w != 500 || h != 333 {
		t.Fatalf("expected canonical image to be resized to 500x333, got %dx%d", w, h)
	}

	thumbPath := filepath.Join(tmp, "_thumbs", "users", "u1", "avatar", "100x100_"+filepath.Base(ref.Path))
	if _, err := os.Stat(thumbPath); err != nil {
		t.Fatalf("expected precomputed thumb on disk: %v", err)
	}
	tw, th := readImageSize(t, thumbPath)
	if tw != 100 || th != 66 {
		t.Fatalf("expected 100 thumb to preserve aspect ratio as 100x66, got %dx%d", tw, th)
	}
}

func TestStoreFileForFieldMaxUploadBytes(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("avatar", "image/png").MaxUploadBytes(32)
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Table("users").Insert(map[string]any{"id": "u1"}); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	if _, err := db.StoreFileForField("users", "u1", "avatar", "avatar.png", makeSolidPNG(t, 40, 40), "image/png"); err == nil {
		t.Fatal("expected max upload bytes validation error")
	}
}

func makeSolidPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: 100, G: 160, B: 220, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func readImageSize(t *testing.T, path string) (int, int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image %s: %v", path, err)
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode image %s: %v", path, err)
	}
	return img.Bounds().Dx(), img.Bounds().Dy()
}
