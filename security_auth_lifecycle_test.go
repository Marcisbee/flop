package flop

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTokenUseSeparation(t *testing.T) {
	app, db := securityTestApp(t)
	h := app.APIHandler(db)
	access, refresh, _, err := db.authService.Register("uses@example.com", "password123", "User", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, access, refresh string
		refreshFn             func(string) (string, string, error)
	}{
		{"user", access, refresh, db.authService.Refresh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/auth/me", nil)
			req.Header.Set("Authorization", "Bearer "+tc.refresh)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("refresh accepted as access: %d", rec.Code)
			}
			if _, _, err := tc.refreshFn(tc.access); err == nil {
				t.Error("access accepted for refresh")
			}
		})
	}
	if _, _, err := db.superadminService.Register("admin@example.com", "password123", "Admin"); err != nil {
		t.Fatal(err)
	}
	admin, adminRefresh, _, err := db.superadminService.Login("admin@example.com", "password123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.superadminService.ValidateAccessToken(adminRefresh); err == nil {
		t.Error("admin refresh accepted as access")
	}
	if _, _, err := db.superadminService.Refresh(admin); err == nil {
		t.Error("admin access accepted for refresh")
	}
}

func TestPurposeTokenReplayAndAccountBinding(t *testing.T) {
	for _, purpose := range []string{"password-reset", "verification", "email-change"} {
		t.Run(purpose, func(t *testing.T) {
			app, db := securityTestApp(t)
			h := app.APIHandler(db)
			_, _, auth, err := db.authService.Register("before@example.com", "password123", "User", nil)
			if err != nil {
				t.Fatal(err)
			}
			var token, path, body string
			switch purpose {
			case "password-reset":
				token, err = db.authService.RequestPasswordReset(auth.Email)
				path = "/api/auth/confirm-password-reset"
			case "verification":
				token, err = db.authService.RequestVerification(auth.ID)
				path = "/api/auth/confirm-verification"
			case "email-change":
				token, err = db.authService.RequestEmailChange(auth.ID, "after@example.com", "password123")
				path = "/api/auth/confirm-email-change"
			}
			if err != nil {
				t.Fatal(err)
			}
			body = fmt.Sprintf(`{"token":%q}`, token)
			if purpose == "password-reset" {
				body = fmt.Sprintf(`{"token":%q,"password":"replacement123"}`, token)
			}
			confirm := func() int {
				req := httptest.NewRequest("POST", path, strings.NewReader(body))
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				return rec.Code
			}
			if status := confirm(); status != 200 {
				t.Fatalf("first confirmation: %d", status)
			}
			// Restore the original account data to ensure one-use is durable, rather
			// than merely comparing the token email/verified claims with the current row.
			if purpose == "email-change" {
				if _, err := db.Table("users").Update(auth.ID, map[string]any{"email": "before@example.com"}); err != nil {
					t.Fatal(err)
				}
			}
			if status := confirm(); status == 200 {
				t.Error("used link accepted again")
			}
		})
	}
	t.Run("email-changed", func(t *testing.T) {
		_, db := securityTestApp(t)
		_, _, auth, err := db.authService.Register("old@example.com", "password123", "User", nil)
		if err != nil {
			t.Fatal(err)
		}
		reset, _ := db.authService.RequestPasswordReset(auth.Email)
		verify, _ := db.authService.RequestVerification(auth.ID)
		if _, err := db.Table("users").Update(auth.ID, map[string]any{"email": "new@example.com"}); err != nil {
			t.Fatal(err)
		}
		if err := db.authService.ConfirmPasswordReset(reset, "attacker123"); err == nil {
			t.Error("old mailbox reset accepted")
		}
		if _, _, _, err := db.authService.ConfirmVerification(verify); err == nil {
			t.Error("old mailbox verified new email")
		}
	})
}

func TestConcurrentRefreshIsSingleUse(t *testing.T) {
	_, db := securityTestApp(t)
	_, refresh, _, err := db.authService.Register("race@example.com", "password123", "User", nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 16)
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, _, err := db.authService.Refresh(refresh); results <- err }()
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("refresh succeeded %d times, want once", successes)
	}
}

func TestProviderFlowCannotMoveBetweenBrowsers(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{"attacker": {Provider: "fake", Issuer: "https://issuer.example", Subject: "attacker"}}}
	_, _, h := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	bearer, _ := registerProviderTestUser(t, h, "linked@example.com")
	state, _ := startProviderFlow(t, h, "fake", "link", bearer, "")
	completionCode := callbackProviderFlow(t, h, "fake", state, "attacker")
	linked := completeProviderFlow(t, h, completionCode, bearer, true)
	if linked.Code != 200 {
		t.Fatalf("link fixture: %d %s", linked.Code, linked.Body.String())
	}
	start := httptest.NewRequest("POST", "https://app.example/api/auth/provider/start", strings.NewReader(`{"provider":"fake","intent":"sign_in"}`))
	start.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, start)
	if rec.Code != 200 {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	u, err := url.Parse(decodeProviderResponse(t, rec)["authorizationUrl"].(string))
	if err != nil {
		t.Fatal(err)
	}
	callbackURL := "https://app.example/api/auth/provider/callback?state=" + url.QueryEscape(u.Query().Get("state")) + "&code=attacker"
	// A callback URL obtained in one browser must not authenticate another.
	victim := httptest.NewRecorder()
	h.ServeHTTP(victim, httptest.NewRequest("GET", callbackURL, nil))
	if victim.Code != http.StatusForbidden {
		t.Errorf("foreign callback status=%d, want 403", victim.Code)
	}
	if len(cookies) == 0 {
		t.Fatal("start did not bind browser")
	}
	callback := httptest.NewRequest("GET", callbackURL, nil)
	for _, c := range cookies {
		callback.AddCookie(c)
	}
	valid := httptest.NewRecorder()
	h.ServeHTTP(valid, callback)
	if valid.Code != 200 {
		t.Fatalf("bound callback: %d %s", valid.Code, valid.Body.String())
	}
	completion := decodeProviderResponse(t, valid)["completionCode"].(string)
	body := fmt.Sprintf(`{"completionCode":%q}`, completion)
	foreign := httptest.NewRecorder()
	h.ServeHTTP(foreign, httptest.NewRequest("POST", "https://app.example/api/auth/provider/complete", strings.NewReader(body)))
	if foreign.Code != http.StatusForbidden {
		t.Errorf("foreign completion status=%d, want 403", foreign.Code)
	}
	own := httptest.NewRequest("POST", "https://app.example/api/auth/provider/complete", strings.NewReader(body))
	for _, c := range cookies {
		own.AddCookie(c)
	}
	completed := httptest.NewRecorder()
	h.ServeHTTP(completed, own)
	if completed.Code != 200 {
		t.Fatalf("bound completion: %d %s", completed.Code, completed.Body.String())
	}
}

func TestSSEStopsAfterSessionRevocation(t *testing.T) {
	for _, admin := range []bool{false, true} {
		t.Run(fmt.Sprint("admin=", admin), func(t *testing.T) {
			app, db := securityTestApp(t)
			var token, sessionID string
			var handler http.Handler = app.APIHandler(db)
			path := "/api/sse?tables=notes"
			if admin {
				mux := http.NewServeMux()
				mounts := MountDefaultHandlers(mux, app, db)
				token = setupAndLoginSuperadmin(t, mux, mounts.Admin.SetupToken)
				handler = mux
				path = "/_/api/events"
			} else {
				token = registerSecurityTestUser(t, handler, "stream@example.com", "password123")
			}
			sessionID = decodeJWTClaims(t, token)["sessionId"].(string)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest("GET", path, nil).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := newSynchronizedRecorder()
			done := make(chan struct{})
			go func() { handler.ServeHTTP(rec, req); close(done) }()
			defer func() { cancel(); <-done }()
			deadline := time.Now().Add(time.Second)
			for rec.BodyString() == "" && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if rec.BodyString() == "" {
				t.Fatal("stream not connected")
			}
			if _, err := db.Table(systemAuthSessionTableName).Update(sessionID, map[string]any{"revoked_at": time.Now().Unix()}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Table("notes").Insert(map[string]any{"id": "private-after-revoke", "title": "secret"}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("revoked stream stayed open")
			}
			if strings.Contains(rec.BodyString(), "private-after-revoke") {
				t.Error("revoked stream leaked subsequent event")
			}
		})
	}
}

func TestAdminLoginBodyIsBounded(t *testing.T) {
	app, db := securityTestApp(t)
	mux := http.NewServeMux()
	MountDefaultHandlers(mux, app, db)
	// A valid but oversized credential must be rejected before password work.
	body := `{"email":"admin@example.com","password":"` + strings.Repeat("x", (1<<20)+1) + `"}`
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("POST", "/_/api/login", strings.NewReader(body)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized login: %d", rec.Code)
	}
}

func TestForeignFileReferenceCannotDeleteAnotherRowsFile(t *testing.T) {
	app := New(Config{DataDir: t.TempDir()})
	Define(app, "docs", func(s *SchemaBuilder) { s.String("id").Primary(); s.FileSingle("file") })
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	docs := db.Table("docs")
	for _, id := range []string{"victim", "attacker"} {
		if _, err := docs.Insert(map[string]any{"id": id}); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := db.StoreFileForField("docs", "victim", "file", "safe.txt", []byte("victim content"), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Update("victim", map[string]any{"file": ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Update("attacker", map[string]any{"file": ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := docs.Update("attacker", map[string]any{"file": nil}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(db.GetDataDir(), ref.Path))
	if err != nil || string(content) != "victim content" {
		t.Fatalf("foreign file deleted: %v", err)
	}
	// The owner can still remove its own file.
	if _, err := docs.Update("victim", map[string]any{"file": nil}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.GetDataDir(), ref.Path)); !os.IsNotExist(err) {
		t.Fatalf("owned file not removed: %v", err)
	}
}

func TestConsumedPurposeTokenStaysConsumedAfterReopen(t *testing.T) {
	app, db := securityTestApp(t)
	_, _, auth, err := db.authService.Register("durable@example.com", "password123", "User", nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := db.authService.RequestEmailChange(auth.ID, "next@example.com", "password123")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := db.authService.ConfirmEmailChange(token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Table("users").Update(auth.ID, map[string]any{"email": auth.Email}); err != nil {
		t.Fatal(err)
	}
	db.cleanupExpiredPurposeTokens(time.Now())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, _, _, err := reopened.authService.ConfirmEmailChange(token); err == nil {
		t.Fatal("consumed link replayed after restart and account state restoration")
	}
}

func TestLoginUpgradesLegacyPasswordHash(t *testing.T) {
	_, db := securityTestApp(t)
	_, _, auth, err := db.authService.Register("legacy@example.com", "password123", "User", nil)
	if err != nil {
		t.Fatal(err)
	}
	const legacy = "$pbkdf2$30313233343536373839616263646566$04ec9163868782ee3f213aafa2bf5912c15af25faf886f1527f89b7212e45c2c"
	if _, err := db.Table("users").Update(auth.ID, map[string]any{"password": legacy}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := db.authService.Login(auth.Email, "password123"); err != nil {
		t.Fatal(err)
	}
	row, err := db.Table("users").Get(auth.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(toString(row["password"]), "$pbkdf2$600000$") || !VerifyPassword("password123", toString(row["password"])) {
		t.Fatal("legacy user password not upgraded")
	}
}

func TestAdminLoginCannotResetBudgetByChangingEmail(t *testing.T) {
	app, db := securityTestApp(t)
	mux := http.NewServeMux()
	MountDefaultHandlers(mux, app, db)
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("POST", "/_/api/login", strings.NewReader(fmt.Sprintf(`{"email":"missing%d@example.com","password":"incorrect"}`, i))))
		if rec.Code != 401 {
			t.Fatalf("initial attempt %d: %d", i, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("POST", "/_/api/login", strings.NewReader(`{"email":"another@example.com","password":"incorrect"}`)))
	if rec.Code != 429 {
		t.Fatalf("password spraying bypassed throttle: %d", rec.Code)
	}
}
