package flop

import (
	"errors"
	"testing"
)

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

func TestPublicTransactionRollsBackArchives(t *testing.T) {
	for _, failure := range []string{"callback", "commit"} {
		t.Run(failure, func(t *testing.T) {
			app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
			users := AutoTable[txUser](app, "users", func(tb *TableBuilder[txUser]) {
				tb.Field("ID").Primary()
				tb.Field("Email").Required().Unique()
				tb.Field("Name").Required()
			})
			db, err := app.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
			want := txUser{ID: "u1", Email: "ada@example.com", Name: "Ada"}
			if _, err := users.Insert(ctx, want); err != nil {
				t.Fatal(err)
			}
			failureErr := errors.New("transaction failed after archive")
			if failure == "commit" {
				testArchiveCommitHook = func() error { return failureErr }
				defer func() { testArchiveCommitHook = nil }()
			}
			_, err = Transaction(ctx, func(tx *Tx) (bool, error) {
				record, err := users.Archive(tx, want.ID)
				if err != nil {
					return false, err
				}
				if record == nil {
					t.Fatal("expected archived row")
				}
				if failure == "callback" {
					return false, failureErr
				}
				return true, nil
			})
			if !errors.Is(err, failureErr) {
				t.Fatalf("expected injected failure, got %v", err)
			}
			got, err := users.Get(ctx, want.ID)
			if err != nil || got == nil || *got != want {
				t.Fatalf("archived row must be readable after rollback: got %#v, err %v", got, err)
			}
			rows, err := users.Scan(ctx, 10, 0)
			if err != nil || len(rows) != 1 || rows[0] != want {
				t.Fatalf("scan after rollback: got %#v, err %v", rows, err)
			}
			indexed, ok := db.Table("users").FindByUniqueIndex("email", want.Email)
			if !ok || indexed["id"] != want.ID {
				t.Fatalf("unique lookup after rollback: %#v", indexed)
			}
			records, _, err := db.db.GetTable("users").ScanArchived(10, 0)
			if err != nil || len(records) != 0 {
				t.Fatalf("archive after rollback: %#v, err %v", records, err)
			}
		})
	}
}
