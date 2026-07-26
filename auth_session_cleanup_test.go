package flop

import (
	"testing"
	"time"
)

func TestAuthSessionCleanupLoopUsesStableStopChannel(t *testing.T) {
	app := New(Config{
		DataDir:            t.TempDir(),
		SyncMode:           "normal",
		AuthSessionCleanup: time.Hour,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	close(db.backgroundStop)
	db.backgroundWG.Wait()
	db.backgroundStop = nil

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		db.runAuthSessionCleanupLoop(stop)
		close(done)
	}()
	close(stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("auth session cleanup did not stop through its stable channel")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestDatabaseCloseRepeatedlyStopsBackgroundWorkers(t *testing.T) {
	const cycles = 25
	for cycle := 0; cycle < cycles; cycle++ {
		app := New(Config{
			DataDir:            t.TempDir(),
			SyncMode:           "normal",
			AuthSessionCleanup: time.Hour,
		})
		db, err := app.Open()
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}

		done := make(chan error, 1)
		go func() {
			done <- db.Close()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("cycle %d close: %v", cycle, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cycle %d close timed out", cycle)
		}
	}
}

func TestCleanupExpiredAuthSessionsDeletesOnlyOldDeadRows(t *testing.T) {
	app := New(Config{
		DataDir:              t.TempDir(),
		SyncMode:             "normal",
		AuthSessionRetention: 24 * time.Hour,
		AuthSessionCleanup:   time.Hour,
	})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	sessions := db.Table(systemAuthSessionTableName)
	if sessions == nil {
		t.Fatalf("expected %s table", systemAuthSessionTableName)
	}

	now := time.Unix(1_800_000_000, 0)
	rows := []map[string]any{
		{
			"id":             "active-1",
			"principal_type": "user",
			"principal_id":   "u1",
			"instance_id":    "i1",
			"created_at":     now.Unix() - 300,
			"last_used_at":   now.Unix() - 60,
			"expires_at":     now.Unix() + 3600,
		},
		{
			"id":             "expired-old",
			"principal_type": "user",
			"principal_id":   "u2",
			"instance_id":    "i1",
			"created_at":     now.Unix() - int64((72 * time.Hour).Seconds()),
			"last_used_at":   now.Unix() - int64((48 * time.Hour).Seconds()),
			"expires_at":     now.Unix() - int64((48 * time.Hour).Seconds()),
		},
		{
			"id":             "revoked-old",
			"principal_type": "user",
			"principal_id":   "u3",
			"instance_id":    "i1",
			"created_at":     now.Unix() - int64((72 * time.Hour).Seconds()),
			"last_used_at":   now.Unix() - int64((48 * time.Hour).Seconds()),
			"expires_at":     now.Unix() + 3600,
			"revoked_at":     now.Unix() - int64((36 * time.Hour).Seconds()),
		},
		{
			"id":             "expired-recent",
			"principal_type": "user",
			"principal_id":   "u4",
			"instance_id":    "i1",
			"created_at":     now.Unix() - 7200,
			"last_used_at":   now.Unix() - 3600,
			"expires_at":     now.Unix() - 1800,
		},
		{
			"id":             "revoked-recent",
			"principal_type": "user",
			"principal_id":   "u5",
			"instance_id":    "i1",
			"created_at":     now.Unix() - 7200,
			"last_used_at":   now.Unix() - 3600,
			"expires_at":     now.Unix() + 3600,
			"revoked_at":     now.Unix() - 1800,
		},
	}
	for _, row := range rows {
		if _, err := sessions.Insert(row); err != nil {
			t.Fatalf("insert %s: %v", row["id"], err)
		}
	}

	deleted, err := db.cleanupExpiredAuthSessions(now)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted=%d want 2", deleted)
	}

	for _, id := range []string{"expired-old", "revoked-old"} {
		row, err := sessions.Get(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if row != nil {
			t.Fatalf("expected %s to be deleted", id)
		}
	}
	for _, id := range []string{"active-1", "expired-recent", "revoked-recent"} {
		row, err := sessions.Get(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if row == nil {
			t.Fatalf("expected %s to remain", id)
		}
	}
}
