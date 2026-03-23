package flop

import "testing"

type txUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

func TestPublicTransactionCommitsTypedTableWrites(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	users := AutoTable[txUser](app, "users", func(tb *TableBuilder[txUser]) {
		tb.Field("ID").Primary()
		tb.Field("Email").Required().Unique()
		tb.Field("Name").Required()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
	created, err := Transaction(ctx, func(tx *Tx) (txUser, error) {
		if _, err := users.Insert(tx, txUser{ID: "u1", Email: "ada@example.com", Name: "Ada"}); err != nil {
			return txUser{}, err
		}
		return users.Insert(tx, txUser{ID: "u2", Email: "linus@example.com", Name: "Linus"})
	})
	if err != nil {
		t.Fatalf("transaction commit: %v", err)
	}
	if created.ID != "u2" {
		t.Fatalf("expected final inserted user u2, got %#v", created)
	}
	if got := db.Table("users").Count(); got != 2 {
		t.Fatalf("expected count=2 after commit, got %d", got)
	}
}

func TestPublicTransactionRollsBackTypedTableWrites(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	users := AutoTable[txUser](app, "users", func(tb *TableBuilder[txUser]) {
		tb.Field("ID").Primary()
		tb.Field("Email").Required().Unique()
		tb.Field("Name").Required()
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Table("users").Insert(map[string]any{"id": "u0", "email": "taken@example.com", "name": "Taken"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
	if _, err := Transaction(ctx, func(tx *Tx) (txUser, error) {
		if _, err := users.Insert(tx, txUser{ID: "u1", Email: "fresh@example.com", Name: "Fresh"}); err != nil {
			return txUser{}, err
		}
		return users.Insert(tx, txUser{ID: "u2", Email: "taken@example.com", Name: "Dup"})
	}); err == nil {
		t.Fatal("expected transaction to fail")
	}

	if got := db.Table("users").Count(); got != 1 {
		t.Fatalf("expected rollback to leave count=1, got %d", got)
	}
	row, ok := db.Table("users").FindByUniqueIndex("email", "fresh@example.com")
	if ok || row != nil {
		t.Fatalf("expected fresh@example.com insert to roll back")
	}
}
