package flop

import (
	"errors"
	"testing"
)

func TestTransactionCommitFailureRollsBackBufferedWrites(t *testing.T) {
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

	testArchiveCommitHook = func() error { return errors.New("commit failed") }
	t.Cleanup(func() { testArchiveCommitHook = nil })

	ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
	if _, err := Transaction(ctx, func(tx *Tx) (txUser, error) {
		return users.Insert(tx, txUser{ID: "u1", Email: "ada@example.com", Name: "Ada"})
	}); err == nil {
		t.Fatal("expected transaction commit to fail")
	}

	if got := db.Table("users").Count(); got != 0 {
		t.Fatalf("expected rollback after failed commit, got count=%d", got)
	}
}
