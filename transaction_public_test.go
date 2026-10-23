package flop

import (
	"errors"
	"strconv"
	"testing"
	"time"
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

// Concurrent callbacks must not mistake another transaction's buffered update
// for a missing row, or save that uncommitted update as their rollback image.
func TestPublicTransactionConcurrentSameKey(t *testing.T) {
	for _, failure := range []string{"none", "callback", "panic"} {
		t.Run(failure, func(t *testing.T) {
			app := New(Config{DataDir: t.TempDir(), SyncMode: "full"})
			tables := []*Table[txUser]{
				AutoTable[txUser](app, "left", func(tb *TableBuilder[txUser]) { tb.Field("ID").Primary() }),
				AutoTable[txUser](app, "right", func(tb *TableBuilder[txUser]) { tb.Field("ID").Primary() }),
			}
			db, err := app.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
			for _, table := range tables {
				if _, err := table.Insert(ctx, txUser{ID: "same", Name: "0"}); err != nil {
					t.Fatal(err)
				}
			}
			abortErr := errors.New("abort callback")
			start := make(chan struct{})
			errs := make(chan error, 8)
			for writer := range 8 {
				go func() {
					<-start
					deadline := time.Now().Add(time.Minute)
					for attempt := 0; attempt < 125; attempt++ {
						err := func() (err error) {
							defer func() {
								if p := recover(); p != nil {
									if p != abortErr {
										panic(p)
									}
									err = abortErr
								}
							}()
							_, err = Transaction(ctx, func(tx *Tx) (bool, error) {
								for _, table := range tables {
									row, err := table.Get(tx, "same")
									if err != nil {
										return false, err
									}
									if row == nil {
										return false, errors.New("existing row unavailable")
									}
									count, err := strconv.Atoi(row.Name)
									if err != nil {
										return false, err
									}
									if err := table.Update(tx, "same", Set("name", strconv.Itoa(count+1))); err != nil {
										return false, err
									}
									// Hold the public callback open after the buffered write.
									time.Sleep(time.Millisecond)
								}
								if failure != "none" && (writer+attempt)%2 == 0 {
									if failure == "panic" {
										panic(abortErr)
									}
									return false, abortErr
								}
								return true, nil
							})
							return err
						}()
						if errors.Is(err, ErrTransactionConflict) && time.Now().Before(deadline) {
							attempt--
							time.Sleep(time.Millisecond)
							continue
						}
						if err != nil && !errors.Is(err, abortErr) {
							errs <- err
							return
						}
					}
					errs <- nil
				}()
			}
			close(start)
			for range 8 {
				if err := <-errs; err != nil {
					t.Errorf("contending transaction: %v", err)
				}
			}
			want := "1000"
			if failure != "none" {
				want = "500"
			}
			check := func() {
				for _, table := range tables {
					row, err := table.Get(ctx, "same")
					if err != nil || row == nil || row.Name != want {
						t.Fatalf("%s: got %#v, err %v", table.name, row, err)
					}
				}
			}
			check()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = app.Open()
			if err != nil {
				t.Fatal(err)
			}
			ctx = &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
			check()
		})
	}
}

// Disjoint callbacks must be able to finish while another callback is open;
// a contending callback must return a retryable conflict rather than block.
func TestPublicTransactionRowReservations(t *testing.T) {
	for _, secondKey := range []string{"b", "a"} {
		t.Run(secondKey, func(t *testing.T) {
			app := New(Config{DataDir: t.TempDir(), SyncMode: "full"})
			table := AutoTable[txUser](app, "rows", func(tb *TableBuilder[txUser]) { tb.Field("ID").Primary() })
			db, err := app.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, key := range []string{"a", "b"} {
				if _, err := table.Insert(db, txUser{ID: key, Name: "old"}); err != nil {
					t.Fatal(err)
				}
			}
			ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
			held := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := Transaction(ctx, func(tx *Tx) (bool, error) {
					if err := table.Update(tx, "a", Set("name", "first")); err != nil {
						return false, err
					}
					close(held)
					<-release
					return true, nil
				})
				done <- err
			}()
			<-held
			second := make(chan error, 1)
			go func() {
				_, err := Transaction(ctx, func(tx *Tx) (bool, error) { return true, table.Update(tx, secondKey, Set("name", "second")) })
				second <- err
			}()
			select {
			case err := <-second:
				if secondKey == "a" && !errors.Is(err, ErrTransactionConflict) {
					t.Errorf("same-key write: %v", err)
				}
				if secondKey == "b" && err != nil {
					t.Errorf("disjoint write: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("second callback blocked behind unrelated/open callback")
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			// The reservation must have been released after the first commit.
			if _, err := Transaction(ctx, func(tx *Tx) (bool, error) { return true, table.Update(tx, "a", Set("name", "released")) }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
