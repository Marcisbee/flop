package flop

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/jsonx"
	internalserver "github.com/marcisbee/flop/internal/server"
)

// decodeJWTClaims decodes the payload segment of a JWT without verifying it.
func decodeJWTClaims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token is not a JWT: %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode JWT payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshal JWT payload: %v", err)
	}
	return claims
}

func securityTestApp(t *testing.T) (*App, *Database) {
	t.Helper()
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.Boolean("verified").Default(false)
		s.String("default_role").Default("unverified")
		s.Roles("roles")
	})
	Define(app, "notes", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("title").Required()
		s.Access(TableAccess{
			Read: func(c *TableReadCtx) bool { return c.Auth != nil },
		})
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return app, db
}

func registerSecurityTestUser(t *testing.T, handler http.Handler, email, password string, extra ...string) string {
	t.Helper()
	body := fmt.Sprintf(`{"email":%q,"password":%q`, email, password)
	for _, field := range extra {
		body += "," + field
	}
	body += "}"
	req := httptest.NewRequest(http.MethodPost, "/api/auth/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("register status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Token == "" {
		t.Fatalf("register response missing token: %v %s", err, rec.Body.String())
	}
	return out.Token
}

// The default JWT secret must be a random stored secret, not derivable from
// the instance ID embedded in every token.
func TestDefaultJWTSecretIsNotDerivableFromTokens(t *testing.T) {
	app, db := securityTestApp(t)
	handler := app.APIHandler(db)

	token := registerSecurityTestUser(t, handler, "a@example.com", "password123")
	claims := decodeJWTClaims(t, token)
	instanceID, _ := claims["instanceId"].(string)
	if instanceID == "" {
		t.Fatal("expected instanceId claim in token")
	}

	meta := db.db.GetMeta()
	if meta.AuthSecret == "" {
		t.Fatal("expected a persisted random auth secret")
	}
	if meta.AuthSecret == "flop-"+instanceID || strings.Contains(meta.AuthSecret, instanceID) {
		t.Fatal("auth secret must be independent of the public instance ID")
	}

	// Forge a claims-less token with the old derivation scheme and try to
	// impersonate a superadmin against an authenticated endpoint.
	forged := internalserver.CreateJWT(&internalserver.JWTPayload{
		Sub:   "attacker",
		Roles: []string{"superadmin"},
		Iat:   time.Now().Unix(),
		Exp:   time.Now().Add(time.Hour).Unix(),
	}, "flop-"+instanceID)
	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+forged)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged token status=%d, want 401; body=%s", rec.Code, rec.Body.String())
	}

	// Forge a password-reset purpose token with the derived secret.
	forgedReset := internalserver.CreatePurposeJWT(&internalserver.PurposePayload{
		Sub:     "doesnotmatter",
		Purpose: "password-reset",
		Iat:     time.Now().Unix(),
		Exp:     time.Now().Add(time.Hour).Unix(),
	}, "flop-"+instanceID)
	req = httptest.NewRequest(http.MethodPost, "/api/auth/confirm-password-reset",
		strings.NewReader(fmt.Sprintf(`{"token":%q,"password":"hacked123"}`, forgedReset)))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("forged reset token status=%d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

// Anonymous clients must not be able to subscribe to system tables over SSE:
// _auth_sessions row IDs are the session IDs embedded in live tokens.
func TestSSESystemTablesHiddenFromAnonymous(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.Roles("roles")
	})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("title").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	h := app.APIHandler(db)
	rec := newSynchronizedRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/sse?tables=_auth_sessions,_superadmin,posts", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	if !waitForSSEConnected(rec, time.Second) {
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE subscription; body=%q", rec.BodyString())
	}

	// Trigger writes to the subscribed tables: a session row is created on
	// register, and a posts row is inserted directly.
	registerSecurityTestUser(t, h, "b@example.com", "password123")
	if _, err := db.Table("posts").Insert(map[string]any{"id": "p1", "title": "hi"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	if !waitForMinChangeEvents(rec, 1, 2*time.Second) {
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE events; body=%q", rec.BodyString())
	}
	// Give any wrongly-subscribed system-table events a chance to arrive.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	events := parseSSEChanges(rec.BodyString())
	if len(events) == 0 {
		t.Fatal("expected the posts insert event")
	}
	for _, ev := range events {
		table, _ := ev["table"].(string)
		if strings.HasPrefix(table, "_") {
			t.Fatalf("anonymous SSE received system table event: %#v", ev)
		}
	}
}

// A session-validated superadmin may still subscribe to system tables.
// (Superadmin tokens are honored by the app API when no app auth table is
// defined; with one, they are treated as anonymous by design.)
func TestSSESystemTablesAllowedForSuperadmin(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("title").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	mounts := MountDefaultHandlers(mux, app, db)
	if mounts.Admin == nil || mounts.Admin.SetupToken == "" {
		t.Fatal("expected setup token")
	}
	adminToken := setupAndLoginSuperadmin(t, mux, mounts.Admin.SetupToken)

	rec := newSynchronizedRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/sse?tables=_auth_sessions&_token="+adminToken, nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	if !waitForSSEConnected(rec, time.Second) {
		cancel()
		<-done
		t.Fatalf("timed out waiting for SSE subscription; body=%q", rec.BodyString())
	}

	// Another admin login creates an _auth_sessions row.
	loginBody, _ := jsonx.Marshal(map[string]any{"email": "root@example.com", "password": "supersecret123"})
	loginReq := httptest.NewRequest(http.MethodPost, "/_/api/login", bytes.NewReader(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginRec := httptest.NewRecorder()
	mux.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("admin login status=%d", loginRec.Code)
	}

	if !waitForMinChangeEvents(rec, 1, 2*time.Second) {
		cancel()
		<-done
		t.Fatalf("superadmin did not receive _auth_sessions event; body=%q", rec.BodyString())
	}
	cancel()
	<-done

	events := parseSSEChanges(rec.BodyString())
	found := false
	for _, ev := range events {
		if ev["table"] == "_auth_sessions" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an _auth_sessions event for the superadmin, got %#v", events)
	}
}

func setupAndLoginSuperadmin(t *testing.T, mux http.Handler, setupToken string) string {
	t.Helper()
	setupBody, _ := jsonx.Marshal(map[string]any{
		"token":    setupToken,
		"email":    "root@example.com",
		"password": "supersecret123",
	})
	req := httptest.NewRequest(http.MethodPost, "/_/api/setup", bytes.NewReader(setupBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup status=%d body=%s", rec.Code, rec.Body.String())
	}

	loginBody, _ := jsonx.Marshal(map[string]any{"email": "root@example.com", "password": "supersecret123"})
	req = httptest.NewRequest(http.MethodPost, "/_/api/login", bytes.NewReader(loginBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin login status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Token == "" {
		t.Fatalf("admin login missing token: %v %s", err, rec.Body.String())
	}
	return out.Token
}

// File refs stored in rows must never be able to delete files outside the
// _files directory when the referencing row value changes.
func TestFileRefTraversalCannotDeleteArbitraryFiles(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "docs", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("attachment")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A victim file inside and outside the data dir.
	victimInside := filepath.Join(tmp, "_system", "victim.json")
	if err := os.MkdirAll(filepath.Dir(victimInside), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(victimInside, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	victimOutside := filepath.Join(filepath.Dir(tmp), "victim-outside.txt")
	if err := os.WriteFile(victimOutside, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Table("docs").Insert(map[string]any{
		"id":         "d1",
		"attachment": map[string]any{"path": "_files/../../_system/victim.json", "name": "x"},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Table("docs").Insert(map[string]any{
		"id":         "d2",
		"attachment": map[string]any{"path": "_files/../../../victim-outside.txt", "name": "x"},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Clearing the fields triggers cleanup of the previous refs.
	if _, err := db.Table("docs").Update("d1", map[string]any{"attachment": nil}); err != nil {
		t.Fatalf("update d1: %v", err)
	}
	if _, err := db.Table("docs").Update("d2", map[string]any{"attachment": nil}); err != nil {
		t.Fatalf("update d2: %v", err)
	}

	if _, err := os.Stat(victimInside); err != nil {
		t.Fatalf("victim file inside data dir was deleted: %v", err)
	}
	if _, err := os.Stat(victimOutside); err != nil {
		t.Fatalf("victim file outside data dir was deleted: %v", err)
	}
}

// Primary keys that could escape storage paths must be rejected at insert,
// and row deletion must not remove directories outside _files.
func TestPrimaryKeyPathTraversalRejected(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "docs", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.String("title")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, bad := range []string{"../x", "a/b", `a\b`, "..", ".", "a\x00b"} {
		if _, err := db.Table("docs").Insert(map[string]any{"id": bad, "title": "x"}); err == nil {
			t.Fatalf("insert with primary key %q succeeded, want rejection", bad)
		}
	}

	// A directory that a traversal pk would target through DeleteRowFiles.
	victim := filepath.Join(tmp, "victim-dir")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	// "_files/docs/../../victim-dir" resolves to <dataDir>/victim-dir.
	if _, err := db.Table("docs").Delete("../../victim-dir"); err != nil {
		t.Fatalf("delete traversal pk: %v", err)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("directory outside _files was removed: %v", err)
	}
}

// File storage must reject table/row/field names that escape the data dir.
func TestStoreFileForFieldRejectsTraversalNames(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "docs", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("attachment")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Table("docs").Insert(map[string]any{"id": "d1"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, err := db.StoreFileForField("docs", "../../escape", "attachment", "x.txt", []byte("x"), "text/plain"); err == nil {
		t.Fatal("expected traversal row id to be rejected")
	}
	escape := filepath.Join(filepath.Dir(tmp), "escape")
	if _, err := os.Stat(escape); !os.IsNotExist(err) {
		t.Fatalf("directory outside data dir was created: %s", escape)
	}
}

// Admin writes to bcrypt fields must hash the value, and the API response
// must not echo the hash back.
func TestAdminRowWriteHashesAndRedactsBcryptFields(t *testing.T) {
	_, db := securityTestApp(t)
	provider := &EngineAdminProvider{DB: db}

	row, err := provider.AdminCreateRow("users", map[string]any{
		"email":    "admin-made@example.com",
		"password": "plaintext-secret",
	})
	if err != nil {
		t.Fatalf("admin create row: %v", err)
	}
	if got := row["password"]; got != "[REDACTED]" {
		t.Fatalf("create response leaked password material: %#v", got)
	}

	stored, err := db.Table("users").Get(fmt.Sprint(row["id"]))
	if err != nil || stored == nil {
		t.Fatalf("read stored row: %v", err)
	}
	hash := fmt.Sprint(stored["password"])
	if hash == "plaintext-secret" || !strings.HasPrefix(hash, "$") {
		t.Fatalf("password was not hashed before storage: %q", hash)
	}

	// The user must be able to log in with the plaintext password.
	if _, _, _, err := db.authService.Login("admin-made@example.com", "plaintext-secret"); err != nil {
		t.Fatalf("login with admin-set password failed: %v", err)
	}

	// Updates hash too, and the redacted placeholder never overwrites.
	if err := provider.AdminUpdateRow("users", fmt.Sprint(row["id"]), map[string]any{"password": "second-secret"}); err != nil {
		t.Fatalf("admin update row: %v", err)
	}
	if err := provider.AdminUpdateRow("users", fmt.Sprint(row["id"]), map[string]any{"password": "[REDACTED]"}); err != nil {
		t.Fatalf("admin update redacted: %v", err)
	}
	if _, _, _, err := db.authService.Login("admin-made@example.com", "second-secret"); err != nil {
		t.Fatalf("login after admin password update failed: %v", err)
	}
}

// Archived rows must redact bcrypt fields exactly like live rows.
func TestAdminArchiveRowsRedactBcryptFields(t *testing.T) {
	_, db := securityTestApp(t)
	provider := &EngineAdminProvider{DB: db}

	row, err := provider.AdminCreateRow("users", map[string]any{
		"email":    "gone@example.com",
		"password": "secret123",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	pk := fmt.Sprint(row["id"])
	if err := provider.AdminArchiveRow("users", pk); err != nil {
		t.Fatalf("archive: %v", err)
	}
	page, found, err := provider.AdminArchiveRows("users", 10, 0)
	if err != nil || !found {
		t.Fatalf("archive rows: found=%v err=%v", found, err)
	}
	if len(page.Rows) != 1 {
		t.Fatalf("expected one archived row, got %d", len(page.Rows))
	}
	if got := page.Rows[0]["password"]; got != "[REDACTED]" {
		t.Fatalf("archived row leaked password material: %#v", got)
	}
}

// The admin realtime stream must redact bcrypt fields from change events.
func TestAdminSSERedactsBcryptFields(t *testing.T) {
	app, db := securityTestApp(t)
	mux := http.NewServeMux()
	mounts := MountDefaultHandlers(mux, app, db)
	adminToken := setupAndLoginSuperadmin(t, mux, mounts.Admin.SetupToken)

	rec := newSynchronizedRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/_/api/events", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	// Wait for the initial snapshot before triggering a change.
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(rec.BodyString(), "event: snapshot") {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("timed out waiting for admin SSE snapshot; body=%q", rec.BodyString())
		}
		time.Sleep(5 * time.Millisecond)
	}

	provider := &EngineAdminProvider{DB: db}
	if _, err := provider.AdminCreateRow("users", map[string]any{
		"email":    "watched@example.com",
		"password": "secret123",
	}); err != nil {
		t.Fatalf("create user: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for !strings.Contains(rec.BodyString(), "watched@example.com") {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("timed out waiting for user change event; body=%q", rec.BodyString())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	body := rec.BodyString()
	if strings.Contains(body, "$pbkdf2$") || strings.Contains(body, "$2a$") || strings.Contains(body, "$2b$") {
		t.Fatalf("admin SSE leaked a password hash:\n%s", body)
	}
	if !strings.Contains(body, "[REDACTED]") {
		t.Fatalf("admin SSE event missing redaction placeholder:\n%s", body)
	}
}

// Uploaded HTML/SVG must not be served inline from the application origin.
func TestMediaServingForcesAttachmentForActiveContent(t *testing.T) {
	app := New(Config{DataDir: t.TempDir()})
	Define(app, "docs", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("attachment")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Table("docs").Insert(map[string]any{"id": "d1"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	htmlRef, err := db.StoreFileForField("docs", "d1", "attachment", "evil.html", []byte("<script>alert(1)</script>"), "text/html")
	if err != nil {
		t.Fatalf("store html: %v", err)
	}
	if _, err := db.Table("docs").Update("d1", map[string]any{"attachment": map[string]any{
		"path": htmlRef.Path, "name": htmlRef.Name, "url": htmlRef.URL, "mime": htmlRef.Mime, "size": htmlRef.Size,
	}}); err != nil {
		t.Fatalf("link html ref: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/files/", db.FileHandler())
	req := httptest.NewRequest(http.MethodGet, "/api/files/"+strings.TrimPrefix(htmlRef.Path, "_files/"), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("serve status=%d", rec.Code)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") {
		t.Fatalf("html upload served inline: Content-Disposition=%q", cd)
	}

	pngRef, err := db.StoreFileForField("docs", "d1", "attachment", "pic.png", makeSolidPNG(t, 8, 8), "image/png")
	if err != nil {
		t.Fatalf("store png: %v", err)
	}
	if _, err := db.Table("docs").Update("d1", map[string]any{"attachment": map[string]any{
		"path": pngRef.Path, "name": pngRef.Name, "url": pngRef.URL, "mime": pngRef.Mime, "size": pngRef.Size,
	}}); err != nil {
		t.Fatalf("link png ref: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/files/"+strings.TrimPrefix(pngRef.Path, "_files/"), nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("serve png status=%d", rec.Code)
	}
	if cd := rec.Header().Get("Content-Disposition"); cd != "" {
		t.Fatalf("image should stay inline, got Content-Disposition=%q", cd)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
		t.Fatalf("image content type = %q", ct)
	}
}

// The media index must ignore refs whose path escapes _files.
func TestMediaIndexSkipsTraversalRefs(t *testing.T) {
	tmp := t.TempDir()
	app := New(Config{DataDir: tmp})
	Define(app, "docs", func(s *SchemaBuilder) {
		s.String("id").Primary()
		s.FileSingle("attachment")
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A real file outside _files that a crafted ref can point at.
	outside := filepath.Join(tmp, "_system", "peek.png")
	if err := os.MkdirAll(filepath.Dir(outside), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, makeSolidPNG(t, 4, 4), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Table("docs").Insert(map[string]any{
		"id":         "d1",
		"attachment": map[string]any{"path": "_files/../_system/peek.png", "name": "peek.png", "mime": "image/png"},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	provider := &EngineAdminProvider{DB: db}
	rows, _, err := provider.AdminMediaRows(100, 0, "", false)
	if err != nil {
		t.Fatalf("admin media rows: %v", err)
	}
	for _, row := range rows {
		if strings.Contains(row.Path, "..") || strings.Contains(row.Path, "peek.png") {
			t.Fatalf("media index recorded a traversal ref: %#v", row)
		}
	}
}

// Credential endpoints must be rate limited.
func TestAuthLoginRateLimited(t *testing.T) {
	app, db := securityTestApp(t)
	handler := app.APIHandler(db)
	registerSecurityTestUser(t, handler, "limited@example.com", "password123")

	var lastCode int
	for i := 0; i < authRateLimitPerMinute+5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/auth/password",
			strings.NewReader(`{"email":"limited@example.com","password":"wrong"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		lastCode = rec.Code
	}
	if lastCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after %d attempts, got %d", authRateLimitPerMinute+5, lastCode)
	}
}

// The setup schema endpoint must require the one-time setup token.
func TestSetupSchemaRequiresToken(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	cfg := MountDefaultAdmin(mux, &EngineAdminProvider{DB: db})
	if cfg.SetupToken == "" {
		t.Fatal("expected setup token")
	}

	req := httptest.NewRequest(http.MethodGet, "/_/api/setup-schema", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("setup-schema without token: status=%d, want 403", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/_/api/setup-schema?token="+cfg.SetupToken, nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup-schema with token: status=%d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

// A completed password reset must revoke all existing sessions for the user.
func TestPasswordResetRevokesExistingSessions(t *testing.T) {
	app, db := securityTestApp(t)
	handler := app.APIHandler(db)
	token := registerSecurityTestUser(t, handler, "reset@example.com", "old-password")

	resetToken, err := db.authService.RequestPasswordReset("reset@example.com")
	if err != nil || resetToken == "" {
		t.Fatalf("request reset: token=%q err=%v", resetToken, err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/auth/confirm-password-reset",
		strings.NewReader(fmt.Sprintf(`{"token":%q,"password":"new-password"}`, resetToken)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm reset status=%d body=%s", rec.Code, rec.Body.String())
	}

	// The pre-reset access token must no longer validate.
	req = httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("pre-reset token still valid after reset: status=%d", rec.Code)
	}

	// The new password must work.
	req = httptest.NewRequest(http.MethodPost, "/api/auth/password",
		strings.NewReader(`{"email":"reset@example.com","password":"new-password"}`))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login with new password failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// Registration must ignore attempts to self-assign account-state fields.
func TestRegisterIgnoresAccountStateFields(t *testing.T) {
	app, db := securityTestApp(t)
	handler := app.APIHandler(db)
	registerSecurityTestUser(t, handler, "state@example.com", "password123",
		`"roles":["superadmin"]`, `"status":"banned"`, `"banned":true`, `"verified":true`)

	user, found := db.Table("users").FindByEmail("state@example.com")
	if !found || user == nil {
		t.Fatal("user not found")
	}
	for _, field := range []string{"status", "banned"} {
		if _, ok := user[field]; ok {
			t.Fatalf("registration stored account-state field %q: %#v", field, user[field])
		}
	}
	if roles := toStringSlice(user["roles"]); len(roles) == 0 {
		t.Fatal("expected default role to be applied")
	} else {
		for _, role := range roles {
			if role == "superadmin" {
				t.Fatalf("registration self-assigned superadmin role: %#v", roles)
			}
		}
	}
	if _, _, _, err := db.authService.Login("state@example.com", "password123"); err != nil {
		t.Fatalf("login failed after registration with banned self-assignment: %v", err)
	}
}
