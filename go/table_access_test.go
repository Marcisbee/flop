package flop

import (
	"errors"
	"testing"
)

func TestTableAccessPolicies(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("slug").Required().Unique()
		s.String("authorId").Required().Index()
		s.String("title").Required()
		s.String("internalNotes").Access(FieldAccess{
			Read: func(c *TableReadCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
			Write: func(c *FieldWriteCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
		})
		s.Access(TableAccess{
			Read: func(c *TableReadCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return toString(c.Row["authorId"]) == c.Auth.ID
			},
			Insert: func(c *TableInsertCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return toString(c.New["authorId"]) == c.Auth.ID
			},
			Update: func(c *TableUpdateCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return toString(c.Old["authorId"]) == c.Auth.ID
			},
			Delete: func(c *TableDeleteCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return toString(c.Row["authorId"]) == c.Auth.ID
			},
		})
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	raw := db.Table("posts")
	if raw == nil {
		t.Fatal("posts table missing")
	}
	_, err = raw.Insert(map[string]any{
		"id":            "p1",
		"slug":          "post-1",
		"authorId":      "u1",
		"title":         "Post 1",
		"internalNotes": "note-1",
	})
	if err != nil {
		t.Fatalf("insert p1: %v", err)
	}
	_, err = raw.Insert(map[string]any{
		"id":            "p2",
		"slug":          "post-2",
		"authorId":      "u2",
		"title":         "Post 2",
		"internalNotes": "note-2",
	})
	if err != nil {
		t.Fatalf("insert p2: %v", err)
	}

	userPosts := db.trackedAccessor(nil, &AuthContext{
		ID:    "u1",
		Roles: []string{"user"},
	}).Table("posts")
	if userPosts == nil {
		t.Fatal("user posts table missing")
	}
	adminPosts := db.trackedAccessor(nil, &AuthContext{
		ID:    "admin-1",
		Roles: []string{"admin"},
	}).Table("posts")
	if adminPosts == nil {
		t.Fatal("admin posts table missing")
	}

	userRows, err := userPosts.Scan(10, 0)
	if err != nil {
		t.Fatalf("user scan: %v", err)
	}
	if len(userRows) != 1 {
		t.Fatalf("expected 1 visible row for user, got %d", len(userRows))
	}
	if got := toString(userRows[0]["id"]); got != "p1" {
		t.Fatalf("expected user to see p1, got %q", got)
	}
	if _, ok := userRows[0]["internalNotes"]; ok {
		t.Fatalf("expected internalNotes to be hidden for non-admin")
	}
	if got := userPosts.Count(); got != 1 {
		t.Fatalf("expected user count=1, got %d", got)
	}
	if _, ok := userPosts.FindByUniqueIndex("slug", "post-2"); ok {
		t.Fatalf("expected post-2 to be hidden for user")
	}

	if _, err := userPosts.Update("p2", map[string]any{"title": "forbidden"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied for cross-owner update, got %v", err)
	}
	if _, err := userPosts.Update("p1", map[string]any{"internalNotes": "forbidden"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied for protected field update, got %v", err)
	}
	if _, err := userPosts.Update("p1", map[string]any{"title": "allowed"}); err != nil {
		t.Fatalf("expected owner update to succeed, got %v", err)
	}

	adminRows, err := adminPosts.Scan(10, 0)
	if err != nil {
		t.Fatalf("admin scan: %v", err)
	}
	if len(adminRows) != 2 {
		t.Fatalf("expected 2 visible rows for admin, got %d", len(adminRows))
	}
	for _, row := range adminRows {
		if _, ok := row["internalNotes"]; !ok {
			t.Fatalf("expected internalNotes visible for admin")
		}
	}
	if _, err := adminPosts.Update("p1", map[string]any{"internalNotes": "admin-write"}); err != nil {
		t.Fatalf("expected admin field update to succeed, got %v", err)
	}
}
