package flop

import "testing"

func TestArchiveAndRestoreCascade(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})

	users := Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("name").Required()
	})
	posts := Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Ref("authorId", users, "id").Required().CascadeArchive()
		s.String("title").Required()
	})
	Define(app, "comments", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Ref("postId", posts, "id").Required().CascadeArchive()
		s.Ref("authorId", users, "id").Required().CascadeArchive()
		s.String("body").Required()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u1", "name": "Ada"}); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	if _, err := db.Table("posts").Insert(map[string]any{"id": "p1", "authorId": "u1", "title": "Hello"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	if _, err := db.Table("comments").Insert(map[string]any{"id": "c1", "postId": "p1", "authorId": "u1", "body": "First"}); err != nil {
		t.Fatalf("insert comment: %v", err)
	}

	record, err := db.Table("users").Archive("u1")
	if err != nil {
		t.Fatalf("archive user: %v", err)
	}
	if record == nil {
		t.Fatal("expected archive record")
	}

	if got := db.Table("users").Count(); got != 0 {
		t.Fatalf("expected users count=0 after archive, got %d", got)
	}
	if got := db.Table("posts").Count(); got != 0 {
		t.Fatalf("expected posts count=0 after cascade archive, got %d", got)
	}
	if got := db.Table("comments").Count(); got != 0 {
		t.Fatalf("expected comments count=0 after cascade archive, got %d", got)
	}

	if records, _, err := db.db.GetTable("posts").ScanArchived(10, 0); err != nil || len(records) != 1 {
		t.Fatalf("expected 1 archived post, got %d err=%v", len(records), err)
	}
	if records, _, err := db.db.GetTable("comments").ScanArchived(10, 0); err != nil || len(records) != 1 {
		t.Fatalf("expected 1 archived comment, got %d err=%v", len(records), err)
	}

	if err := db.Table("users").RestoreArchive(record.ArchiveID); err != nil {
		t.Fatalf("restore user archive: %v", err)
	}

	if got := db.Table("users").Count(); got != 1 {
		t.Fatalf("expected users count=1 after restore, got %d", got)
	}
	if got := db.Table("posts").Count(); got != 1 {
		t.Fatalf("expected posts count=1 after cascade restore, got %d", got)
	}
	if got := db.Table("comments").Count(); got != 1 {
		t.Fatalf("expected comments count=1 after cascade restore, got %d", got)
	}
}
