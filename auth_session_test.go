package flop_test

import (
	"sync"
	"testing"
	"time"

	"github.com/marcisbee/flop"
)

func sessionApp(t *testing.T) (*flop.App, *flop.Database) {
	t.Helper()
	app := flop.New(flop.Config{DataDir: t.TempDir(), SyncMode: "normal"})
	flop.Define(app, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary()
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4)
		s.Boolean("verified").Default(true)
		s.Boolean("disabled").Default(false)
		s.Roles("roles")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, id := range []string{"alice", "bob"} {
		if _, err := db.Table("users").Insert(map[string]any{"id": id, "email": id + "@example.com", "verified": true, "roles": []string{"member"}}); err != nil {
			t.Fatal(err)
		}
	}
	return app, db
}

func TestEmbeddedSessionLifecycle(t *testing.T) {
	app, db := sessionApp(t)
	token, refresh, err := db.CreateSession("alice")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := db.ValidateAccessToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if auth.ID != "alice" || auth.Email != "alice@example.com" || auth.PrincipalType != "user" || auth.SessionID == "" || len(auth.Roles) != 1 || auth.Roles[0] != "member" {
		t.Fatalf("unexpected principal: %+v", auth)
	}
	if _, err := db.ValidateAccessToken(refresh); err == nil {
		t.Fatal("refresh accepted as access")
	}
	if _, _, err := db.RefreshSession(token); err == nil {
		t.Fatal("access accepted as refresh")
	}
	if err := db.RevokeSession("bob", auth.SessionID); err == nil {
		t.Fatal("foreign owner accepted")
	}
	if _, err := db.ValidateAccessToken(token); err != nil {
		t.Fatal("foreign revocation affected session:", err)
	}
	other, _, err := db.CreateSession("alice")
	if err != nil {
		t.Fatal(err)
	}
	next, nextRefresh, err := db.RefreshSession(refresh)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ValidateAccessToken(token); err == nil {
		t.Fatal("rotated access remains valid")
	}

	auth, err = db.ValidateAccessToken(next)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Cross the JWT timestamp resolution so retries cannot accidentally pass
	// by minting a new refresh token in the same second.
	time.Sleep(time.Second)
	// The first renewal response was lost. Only the original refresh token
	// remains at the client, even after the server has restarted.
	recovered, recoveredRefresh, err := db.RefreshSession(refresh)
	if err != nil {
		t.Fatal("lost renewal response could not be recovered:", err)
	}
	recoveredAuth, err := db.ValidateAccessToken(recovered)
	if err != nil || recoveredAuth.SessionID != auth.SessionID || recoveredRefresh != nextRefresh {
		t.Fatalf("retry did not recover the same replacement: auth=%+v err=%v", recoveredAuth, err)
	}
	// Once the replacement is itself renewed, the original token must not
	// follow a chain of rotations to regain access.
	previousRefresh := recoveredRefresh
	next, nextRefresh, err = db.RefreshSession(recoveredRefresh)
	if err != nil {
		t.Fatal(err)
	}
	auth, err = db.ValidateAccessToken(next)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.RefreshSession(refresh); err == nil {
		t.Fatal("old refresh followed a second rotation")
	}
	for i := 0; i < 2; i++ {
		if err := db.RevokeSession(auth.ID, auth.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ValidateAccessToken(next); err == nil {
		t.Fatal("revoked access accepted")
	}
	if _, _, err := db.RefreshSession(previousRefresh); err == nil {
		t.Fatal("retry recovered a revoked replacement")
	}
	if _, _, err := db.RefreshSession(nextRefresh); err == nil {
		t.Fatal("revoked refresh accepted")
	}
	if _, err := db.ValidateAccessToken(other); err != nil {
		t.Fatal("unrelated session revoked:", err)
	}
}

func TestEmbeddedSessionAccountValidation(t *testing.T) {
	_, db := sessionApp(t)
	_, refresh, err := db.CreateSession("alice")
	if err != nil {
		t.Fatal(err)
	}
	_, nextRefresh, err := db.RefreshSession(refresh)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Table("users").Update("alice", map[string]any{"disabled": true}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "missing", "alice"} {
		if token, refresh, err := db.CreateSession(id); err == nil || token != "" || refresh != "" {
			t.Fatalf("created session for unavailable account %q", id)
		}
	}
	for _, credential := range []string{refresh, nextRefresh} {
		if _, _, err := db.RefreshSession(credential); err == nil {
			t.Fatal("disabled account refreshed")
		}
	}
	app := flop.New(flop.Config{DataDir: t.TempDir()})
	unconfigured, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer unconfigured.Close()
	for _, unavailable := range []*flop.Database{nil, unconfigured} {
		if _, _, err := unavailable.CreateSession("alice"); err == nil {
			t.Fatal("unconfigured creation accepted")
		}
		if _, _, err := unavailable.RefreshSession(refresh); err == nil {
			t.Fatal("unconfigured refresh accepted")
		}
		if err := unavailable.RevokeSession("alice", "session"); err == nil {
			t.Fatal("unconfigured revocation accepted")
		}
	}
}

func TestEmbeddedSessionConcurrentRefresh(t *testing.T) {
	_, db := sessionApp(t)
	_, refresh, err := db.CreateSession("alice")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	type result struct {
		token, refresh string
		err            error
	}
	results := make(chan result, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, next, err := db.RefreshSession(refresh)
			results <- result{token, next, err}
		}()
	}
	wg.Wait()
	close(results)
	var replacement string
	for result := range results {
		if result.err != nil {
			t.Fatal("concurrent renewal failed:", result.err)
		}
		if _, err := db.ValidateAccessToken(result.token); err != nil {
			t.Fatal(err)
		}
		if replacement != "" && replacement != result.refresh {
			t.Fatal("concurrent renewals forked the session")
		}
		replacement = result.refresh
	}
}
