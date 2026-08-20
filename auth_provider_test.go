package flop

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	internalserver "github.com/marcisbee/flop/internal/server"
)

type fakeAuthProvider struct {
	mu         sync.Mutex
	auth       []AuthProviderAuthorizationRequest
	callbacks  []AuthProviderCallbackRequest
	identities map[string]AuthProviderIdentity
	errors     map[string]error
}

func (f *fakeAuthProvider) AuthorizationURL(_ context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	f.mu.Lock()
	f.auth = append(f.auth, request)
	f.mu.Unlock()
	u, _ := url.Parse("https://provider.example/authorize")
	query := u.Query()
	query.Set("state", request.State)
	query.Set("nonce", request.Nonce)
	query.Set("redirect_uri", request.RedirectURI)
	if request.CodeChallenge != "" {
		query.Set("code_challenge", request.CodeChallenge)
		query.Set("code_challenge_method", request.CodeChallengeMethod)
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (f *fakeAuthProvider) Exchange(_ context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, request)
	if err := f.errors[request.Code]; err != nil {
		return AuthProviderIdentity{}, err
	}
	identity, ok := f.identities[request.Code]
	if !ok {
		return AuthProviderIdentity{}, fmt.Errorf("unknown fake code containing secret material")
	}
	return identity, nil
}

func providerTestApp(t *testing.T, providers map[string]AuthProviderConfig) (*App, *Database, http.Handler) {
	t.Helper()
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal", AuthProviders: providers})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.String("name")
		s.Boolean("verified").Default(false)
		s.String("default_role").Default("user")
		s.Roles("roles")
		s.Boolean("disabled").Default(false)
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open provider test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return app, db, app.APIHandler(db)
}

func fakeProviderConfig(adapter AuthProviderAdapter, provider, issuer string) AuthProviderConfig {
	return AuthProviderConfig{
		Adapter: adapter, Issuer: issuer,
		RedirectURI:       "https://app.example/api/auth/provider/callback?provider=" + url.QueryEscape(provider),
		AllowedReturnURLs: []string{"https://client.example/auth/done"},
	}
}

func providerRequest(t *testing.T, handler http.Handler, method, path, body, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeProviderResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response status=%d body=%s: %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func registerProviderTestUser(t *testing.T, handler http.Handler, email string) (string, string) {
	t.Helper()
	rec := providerRequest(t, handler, http.MethodPost, "/api/auth/register", fmt.Sprintf(`{"email":%q,"password":"password123","name":"Test User"}`, email), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("register status=%d body=%s", rec.Code, rec.Body.String())
	}
	out := decodeProviderResponse(t, rec)
	token, _ := out["token"].(string)
	claims := decodeJWTClaims(t, token)
	principalID, _ := claims["sub"].(string)
	return token, principalID
}

func startProviderFlow(t *testing.T, handler http.Handler, provider, intent, bearer, returnTo string) (state string, request AuthProviderAuthorizationRequest) {
	t.Helper()
	body := fmt.Sprintf(`{"provider":%q,"intent":%q`, provider, intent)
	if returnTo != "" {
		body += fmt.Sprintf(`,"returnTo":%q`, returnTo)
	}
	body += "}"
	rec := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", body, bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", rec.Code, rec.Body.String())
	}
	authorizationURL, _ := decodeProviderResponse(t, rec)["authorizationUrl"].(string)
	u, err := url.Parse(authorizationURL)
	if err != nil {
		t.Fatalf("parse authorization URL: %v", err)
	}
	return u.Query().Get("state"), AuthProviderAuthorizationRequest{
		Provider: provider, State: u.Query().Get("state"), Nonce: u.Query().Get("nonce"),
		RedirectURI: u.Query().Get("redirect_uri"), CodeChallenge: u.Query().Get("code_challenge"),
		CodeChallengeMethod: u.Query().Get("code_challenge_method"),
	}
}

func callbackProviderFlow(t *testing.T, handler http.Handler, provider, state, code string) string {
	t.Helper()
	path := "/api/auth/provider/callback?provider=" + url.QueryEscape(provider) + "&state=" + url.QueryEscape(state) + "&code=" + url.QueryEscape(code)
	rec := providerRequest(t, handler, http.MethodGet, path, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("callback status=%d body=%s", rec.Code, rec.Body.String())
	}
	completionCode, _ := decodeProviderResponse(t, rec)["completionCode"].(string)
	if completionCode == "" {
		t.Fatal("callback response omitted completion code")
	}
	return completionCode
}

func completeProviderFlow(t *testing.T, handler http.Handler, completionCode, bearer string, confirm bool) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"completionCode":%q,"confirm":%t}`, completionCode, confirm)
	return providerRequest(t, handler, http.MethodPost, "/api/auth/provider/complete", body, bearer)
}

func TestProviderLinkAndSignInUseIssuerSubjectAndStandardSession(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link-code":  {Provider: "fake", Issuer: "https://issuer.example", Subject: "stable-subject", DisplayName: "Initial", Email: "initial@example.com", EmailVerified: true},
		"login-code": {Provider: "fake", Issuer: "https://issuer.example", Subject: "stable-subject", DisplayName: "Changed", Email: "changed@example.com", EmailVerified: false},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	passwordToken, principalID := registerProviderTestUser(t, handler, "account@example.com")

	state, authorization := startProviderFlow(t, handler, "fake", "link", passwordToken, "")
	if authorization.CodeChallengeMethod != "S256" || authorization.CodeChallenge == "" {
		t.Fatalf("PKCE request = %#v", authorization)
	}
	completionCode := callbackProviderFlow(t, handler, "fake", state, "link-code")
	adapter.mu.Lock()
	callback := adapter.callbacks[len(adapter.callbacks)-1]
	adapter.mu.Unlock()
	challenge := sha256.Sum256([]byte(callback.CodeVerifier))
	if got := base64.RawURLEncoding.EncodeToString(challenge[:]); got != authorization.CodeChallenge {
		t.Fatalf("PKCE challenge mismatch: got %q want %q", got, authorization.CodeChallenge)
	}
	if callback.Nonce == "" || callback.Nonce != authorization.Nonce {
		t.Fatalf("nonce mismatch: callback=%q authorization=%q", callback.Nonce, authorization.Nonce)
	}
	linked := completeProviderFlow(t, handler, completionCode, passwordToken, true)
	if linked.Code != http.StatusOK {
		t.Fatalf("link complete status=%d body=%s", linked.Code, linked.Body.String())
	}
	identityRows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil || len(identityRows) != 1 {
		t.Fatalf("linked identities=%#v err=%v", identityRows, err)
	}
	identityID := toString(identityRows[0]["id"])
	list := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/identities", "", passwordToken)
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "stable-subject") {
		t.Fatalf("identity list exposed unsafe fields or failed: status=%d body=%s", list.Code, list.Body.String())
	}

	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	completionCode = callbackProviderFlow(t, handler, "fake", state, "login-code")
	login := completeProviderFlow(t, handler, completionCode, "", false)
	if login.Code != http.StatusOK {
		t.Fatalf("provider login status=%d body=%s", login.Code, login.Body.String())
	}
	loginOut := decodeProviderResponse(t, login)
	if loginOut["token"] == "" || loginOut["refreshToken"] == "" || loginOut["user"] == nil || loginOut["me"] == nil {
		t.Fatalf("provider login did not preserve password response shape: %#v", loginOut)
	}
	providerToken, _ := loginOut["token"].(string)
	claims := decodeJWTClaims(t, providerToken)
	sessionID, _ := claims["sessionId"].(string)
	session, err := db.Table(systemAuthSessionTableName).Get(sessionID)
	if err != nil || session == nil {
		t.Fatalf("provider session missing: %#v err=%v", session, err)
	}
	if session["auth_method"] != "provider" || session["auth_identity_id"] != identityID {
		t.Fatalf("provider session metadata=%#v", session)
	}
	if identityRows[0]["email"] == "account@example.com" {
		t.Fatal("test setup accidentally used Flop account email as provider identity")
	}
}

func TestProviderSignInRejectsDisabledPrincipalWithoutSessionMutation(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link":  {Provider: "fake", Issuer: "https://issuer.example", Subject: "disabled-subject"},
		"login": {Provider: "fake", Issuer: "https://issuer.example", Subject: "disabled-subject"},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	passwordToken, principalID := registerProviderTestUser(t, handler, "disabled@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", passwordToken, "")
	completionCode := callbackProviderFlow(t, handler, "fake", state, "link")
	if rec := completeProviderFlow(t, handler, completionCode, passwordToken, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := db.db.GetAuthTable().Update(principalID, map[string]any{"disabled": true}, nil); err != nil {
		t.Fatalf("disable principal: %v", err)
	}
	sessionsBefore := db.Table(systemAuthSessionTableName).Count()
	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	completionCode = callbackProviderFlow(t, handler, "fake", state, "login")
	login := completeProviderFlow(t, handler, completionCode, "", false)
	if login.Code != http.StatusUnauthorized || decodeProviderResponse(t, login)["code"] != "principal_unavailable" {
		t.Fatalf("disabled login status=%d body=%s", login.Code, login.Body.String())
	}
	if got := db.Table(systemAuthSessionTableName).Count(); got != sessionsBefore {
		t.Fatalf("disabled login mutated sessions: before=%d after=%d", sessionsBefore, got)
	}
}

func TestUnlinkedVerifiedEmailNeverAutoLinksOrCreatesSession(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"same-email": {Provider: "fake", Issuer: "https://issuer.example", Subject: "unlinked", Email: "account@example.com", EmailVerified: true},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	_, _ = registerProviderTestUser(t, handler, "account@example.com")
	sessionsBefore := db.Table(systemAuthSessionTableName).Count()

	state, _ := startProviderFlow(t, handler, "fake", "sign_in", "", "")
	completionCode := callbackProviderFlow(t, handler, "fake", state, "same-email")
	rec := completeProviderFlow(t, handler, completionCode, "", false)
	if rec.Code != http.StatusConflict || decodeProviderResponse(t, rec)["code"] != "link_required" {
		t.Fatalf("unlinked complete status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := db.Table(systemAuthIdentityTableName).Count(); got != 0 {
		t.Fatalf("unlinked identity mutated identity table: %d", got)
	}
	if got := db.Table(systemAuthSessionTableName).Count(); got != sessionsBefore {
		t.Fatalf("unlinked identity created a session: before=%d after=%d", sessionsBefore, got)
	}
}

func TestProviderIdentityUniquenessAndLinkSessionBinding(t *testing.T) {
	first := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"first": {Provider: "first", Issuer: "https://issuer-one.example", Subject: "shared-subject"},
	}}
	second := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"second": {Provider: "second", Issuer: "https://issuer-two.example", Subject: "shared-subject"},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{
		"first":  fakeProviderConfig(first, "first", "https://issuer-one.example"),
		"second": fakeProviderConfig(second, "second", "https://issuer-two.example"),
	})
	ownerToken, ownerID := registerProviderTestUser(t, handler, "owner@example.com")
	otherToken, _ := registerProviderTestUser(t, handler, "other@example.com")

	state, _ := startProviderFlow(t, handler, "first", "link", ownerToken, "")
	completionCode := callbackProviderFlow(t, handler, "first", state, "first")
	wrongSession := completeProviderFlow(t, handler, completionCode, otherToken, true)
	if wrongSession.Code != http.StatusUnauthorized || decodeProviderResponse(t, wrongSession)["code"] != "link_session_changed" {
		t.Fatalf("wrong link session status=%d body=%s", wrongSession.Code, wrongSession.Body.String())
	}
	if correctSession := completeProviderFlow(t, handler, completionCode, ownerToken, true); correctSession.Code != http.StatusOK {
		t.Fatalf("correct link session status=%d body=%s", correctSession.Code, correctSession.Body.String())
	}

	state, _ = startProviderFlow(t, handler, "second", "link", ownerToken, "")
	completionCode = callbackProviderFlow(t, handler, "second", state, "second")
	if rec := completeProviderFlow(t, handler, completionCode, ownerToken, true); rec.Code != http.StatusOK {
		t.Fatalf("different issuer link status=%d body=%s", rec.Code, rec.Body.String())
	}
	rows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", ownerID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("same subject under distinct issuers rows=%#v err=%v", rows, err)
	}

	state, _ = startProviderFlow(t, handler, "first", "link", otherToken, "")
	completionCode = callbackProviderFlow(t, handler, "first", state, "first")
	collision := completeProviderFlow(t, handler, completionCode, otherToken, true)
	if collision.Code != http.StatusConflict || decodeProviderResponse(t, collision)["code"] != "identity_already_linked" {
		t.Fatalf("cross-principal collision status=%d body=%s", collision.Code, collision.Body.String())
	}
}

func TestProviderFlowRejectsMismatchReplayAndRedactsExchangeFailure(t *testing.T) {
	adapter := &fakeAuthProvider{
		identities: map[string]AuthProviderIdentity{"ok": {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"}},
		errors:     map[string]error{"secret-code": fmt.Errorf("upstream leaked token provider-secret-token")},
	}
	_, _, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	state, _ := startProviderFlow(t, handler, "fake", "sign_in", "", "")
	mismatch := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?provider=wrong&state="+url.QueryEscape(state)+"&code=ok", "", "")
	if mismatch.Code != http.StatusBadRequest || decodeProviderResponse(t, mismatch)["code"] != "provider_mismatch" {
		t.Fatalf("provider mismatch status=%d body=%s", mismatch.Code, mismatch.Body.String())
	}
	completionCode := callbackProviderFlow(t, handler, "fake", state, "secret-code")
	replay := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?provider=fake&state="+url.QueryEscape(state)+"&code=ok", "", "")
	if replay.Code != http.StatusGone {
		t.Fatalf("callback replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	failed := completeProviderFlow(t, handler, completionCode, "", false)
	if failed.Code != http.StatusBadGateway || decodeProviderResponse(t, failed)["code"] != "provider_exchange_failed" {
		t.Fatalf("exchange failure status=%d body=%s", failed.Code, failed.Body.String())
	}
	if strings.Contains(failed.Body.String(), "provider-secret-token") || strings.Contains(failed.Body.String(), "secret-code") {
		t.Fatalf("exchange failure leaked provider details: %s", failed.Body.String())
	}
	replayedComplete := completeProviderFlow(t, handler, completionCode, "", false)
	if replayedComplete.Code != http.StatusGone {
		t.Fatalf("completion replay status=%d body=%s", replayedComplete.Code, replayedComplete.Body.String())
	}
}

func TestProviderFlowRedirectAllowlistAndSecretPersistence(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"ok": {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	rejected := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", `{"provider":"fake","intent":"sign_in","returnTo":"https://evil.example/capture"}`, "")
	if rejected.Code != http.StatusBadRequest || decodeProviderResponse(t, rejected)["code"] != "return_not_allowed" {
		t.Fatalf("unlisted return status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	state, authorization := startProviderFlow(t, handler, "fake", "sign_in", "", "https://client.example/auth/done")
	flows, err := db.Table(systemAuthProviderFlowTableName).Scan(10, 0)
	if err != nil || len(flows) != 1 {
		t.Fatalf("provider flow rows=%#v err=%v", flows, err)
	}
	stored := fmt.Sprint(flows[0])
	if strings.Contains(stored, state) || strings.Contains(stored, authorization.Nonce) {
		t.Fatalf("provider flow persisted raw state or nonce: %s", stored)
	}
	adminTables, err := (&EngineAdminProvider{DB: db}).AdminTables()
	if err != nil {
		t.Fatalf("admin tables: %v", err)
	}
	for _, table := range adminTables {
		if table.Name == systemAuthProviderFlowTableName {
			t.Fatal("provider flow table was exposed through generic admin access")
		}
	}
	callbackPath := "/api/auth/provider/callback?provider=fake&state=" + url.QueryEscape(state) + "&code=ok"
	callback := providerRequest(t, handler, http.MethodGet, callbackPath, "", "")
	if callback.Code != http.StatusSeeOther {
		t.Fatalf("redirect callback status=%d body=%s", callback.Code, callback.Body.String())
	}
	location, err := url.Parse(callback.Header().Get("Location"))
	if err != nil || location.Scheme != "https" || location.Host != "client.example" || location.Query().Get("completionCode") == "" || location.Query().Get("status") != "success" {
		t.Fatalf("callback redirect=%q err=%v", callback.Header().Get("Location"), err)
	}
	if location.Query().Get("code") != "" || strings.Contains(callback.Header().Get("Location"), "Bearer") {
		t.Fatalf("callback redirect leaked authorization material: %s", callback.Header().Get("Location"))
	}
}

func TestProviderIdentityPersistsAcrossReopenAndExpiredFlowsAreCleaned(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link": {Provider: "fake", Issuer: "https://issuer.example", Subject: "persistent"},
	}}
	dataDir := t.TempDir()
	config := Config{DataDir: dataDir, SyncMode: "normal", AuthProviders: map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")}}
	buildApp := func() *App {
		app := New(config)
		Define(app, "users", func(s *SchemaBuilder) {
			s.String("id").Primary("uuidv7")
			s.String("email").Required().Unique().Email()
			s.Bcrypt("password", 4).Required()
			s.String("default_role").Default("user")
			s.Roles("roles")
		})
		return app
	}
	app := buildApp()
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open initial database: %v", err)
	}
	handler := app.APIHandler(db)
	token, principalID := registerProviderTestUser(t, handler, "persistent@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", token, "")
	completionCode := callbackProviderFlow(t, handler, "fake", state, "link")
	if rec := completeProviderFlow(t, handler, completionCode, token, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	_, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	db.providerAuth.now = func() time.Time { return time.Now().Add(providerFlowTTL + time.Minute) }
	if deleted, err := db.cleanupExpiredProviderFlows(db.providerAuth.now()); err != nil || deleted != 2 {
		t.Fatalf("cleanup deleted=%d err=%v", deleted, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close initial database: %v", err)
	}

	app = buildApp()
	db, err = app.Open()
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil || len(rows) != 1 || rows[0]["subject"] != "persistent" {
		t.Fatalf("reopened identities=%#v err=%v", rows, err)
	}
	if got := db.Table(systemAuthProviderFlowTableName).Count(); got != 0 {
		t.Fatalf("expired callback material survived cleanup/reopen: %d rows", got)
	}
}

func TestProviderUnlinkSafeguardAndSessionRevocation(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link":  {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
		"login": {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	passwordToken, principalID := registerProviderTestUser(t, handler, "account@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", passwordToken, "")
	linkedCode := callbackProviderFlow(t, handler, "fake", state, "link")
	if rec := completeProviderFlow(t, handler, linkedCode, passwordToken, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	identityRows, _ := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	identityID := toString(identityRows[0]["id"])

	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	loginCode := callbackProviderFlow(t, handler, "fake", state, "login")
	login := completeProviderFlow(t, handler, loginCode, "", false)
	providerToken, _ := decodeProviderResponse(t, login)["token"].(string)
	providerSessionID, _ := decodeJWTClaims(t, providerToken)["sessionId"].(string)

	userTable := db.db.GetAuthTable()
	if _, err := userTable.Update(principalID, map[string]any{"password": "not-a-usable-hash"}, nil); err != nil {
		t.Fatalf("make password unusable: %v", err)
	}
	blocked := providerRequest(t, handler, http.MethodDelete, "/api/auth/provider/identities/"+identityID, "", passwordToken)
	if blocked.Code != http.StatusConflict || decodeProviderResponse(t, blocked)["code"] != "last_sign_in_method" {
		t.Fatalf("last method unlink status=%d body=%s", blocked.Code, blocked.Body.String())
	}

	validPasswordHash, err := internalserver.HashPassword("replacement-password")
	if err != nil {
		t.Fatalf("hash replacement password: %v", err)
	}
	if _, err := userTable.Update(principalID, map[string]any{"password": validPasswordHash}, nil); err != nil {
		t.Fatalf("restore usable password: %v", err)
	}
	removed := providerRequest(t, handler, http.MethodDelete, "/api/auth/provider/identities/"+identityID, "", passwordToken)
	if removed.Code != http.StatusOK {
		t.Fatalf("unlink status=%d body=%s", removed.Code, removed.Body.String())
	}
	session, err := db.Table(systemAuthSessionTableName).Get(providerSessionID)
	if err != nil || providerUnix(session["revoked_at"]) == 0 {
		t.Fatalf("provider-derived session was not revoked: %#v err=%v", session, err)
	}
}
