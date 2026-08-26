package flop

import "testing"

func TestGetDecodesStoredRowBeforeApplyingCurrentSchema(t *testing.T) {
	dataDir := t.TempDir()
	v1 := New(Config{DataDir: dataDir, SyncMode: "normal"})
	Define(v1, "connections", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.Number("avatar_url")
	})
	db, err := v1.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Table("connections").Insert(map[string]any{
		"id": "connection-1", "avatar_url": float64(42),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	v2 := New(Config{DataDir: dataDir, SyncMode: "normal"})
	Define(v2, "connections", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.String("avatar_url")
		s.Migration(2)
	})
	db, err = v2.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	scanned, err := db.Table("connections").Scan(10, 0)
	if err != nil || len(scanned) != 1 {
		t.Fatalf("schema-aware scan failed: rows=%v err=%v", scanned, err)
	}
	row, err := db.Table("connections").Get("connection-1")
	if err != nil {
		t.Fatalf("schema-aware primary lookup failed: %v", err)
	}
	if row == nil || toString(row["id"]) != "connection-1" {
		t.Fatalf("schema-aware primary lookup returned %v", row)
	}
}
