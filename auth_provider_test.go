package flop

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	internalserver "github.com/marcisbee/flop/internal/server"
)

type fakeAuthProvider struct {
	mu              sync.Mutex
	auth            []AuthProviderAuthorizationRequest
	callbacks       []AuthProviderCallbackRequest
	identities      map[string]AuthProviderIdentity
	errors          map[string]error
	blockCode       string
	exchangeStarted chan struct{}
	exchangeRelease <-chan struct{}
}

type firstLockGate struct {
	mu       sync.Mutex
	once     sync.Once
	acquired chan struct{}
	release  <-chan struct{}
}

func (g *firstLockGate) Lock() {
	g.mu.Lock()
	g.once.Do(func() {
		close(g.acquired)
		<-g.release
	})
}

func (g *firstLockGate) Unlock() {
	g.mu.Unlock()
}

type blockingExchangeProvider struct {
	base    *fakeAuthProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingExchangeProvider) AuthorizationURL(ctx context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	return p.base.AuthorizationURL(ctx, request)
}

func (p *blockingExchangeProvider) Exchange(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return p.base.Exchange(ctx, request)
	case <-ctx.Done():
		return AuthProviderIdentity{}, ctx.Err()
	}
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
	f.callbacks = append(f.callbacks, request)
	err := f.errors[request.Code]
	identity, ok := f.identities[request.Code]
	block := request.Code == f.blockCode
	started := f.exchangeStarted
	release := f.exchangeRelease
	f.mu.Unlock()
	if block {
		if started != nil {
			close(started)
		}
		if release != nil {
			<-release
		}
	}
	if err != nil {
		return AuthProviderIdentity{}, err
	}
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

func TestProviderDiscoveryAndSchemaReflectConfiguration(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
	_, _, configured := providerTestApp(t, map[string]AuthProviderConfig{
		"zeta": fakeProviderConfig(adapter, "zeta", "https://zeta.example"),
		"alpha": {
			Adapter: adapter, Issuer: "https://alpha.example",
			RedirectURI: "https://app.example/api/auth/provider/callback", PKCEUnsupported: true,
		},
	})
	discovery := providerRequest(t, configured, http.MethodGet, "/api/auth/providers", "", "")
	if discovery.Code != http.StatusOK {
		t.Fatalf("configured discovery status=%d body=%s", discovery.Code, discovery.Body.String())
	}
	providers, _ := decodeProviderResponse(t, discovery)["providers"].([]any)
	if len(providers) != 2 {
		t.Fatalf("configured providers=%#v", providers)
	}
	first, _ := providers[0].(map[string]any)
	second, _ := providers[1].(map[string]any)
	if first["key"] != "alpha" || first["issuer"] != "https://alpha.example" || first["pkceS256"] != false || second["key"] != "zeta" {
		t.Fatalf("provider discovery=%#v", providers)
	}

	schemaResponse := providerRequest(t, configured, http.MethodGet, "/api/schema", "", "")
	if schemaResponse.Code != http.StatusOK {
		t.Fatalf("configured schema status=%d body=%s", schemaResponse.Code, schemaResponse.Body.String())
	}
	endpoints, _ := decodeProviderResponse(t, schemaResponse)["endpoints"].([]any)
	wantPaths := map[string]bool{
		"/api/auth/providers": false, "/api/auth/provider/start": false,
		"/api/auth/provider/callback": false, "/api/auth/provider/complete": false,
		"/api/auth/provider/identities": false, "/api/auth/provider/identities/{identityId}": false,
	}
	for _, raw := range endpoints {
		endpoint, _ := raw.(map[string]any)
		if _, ok := wantPaths[endpoint["path"].(string)]; ok {
			wantPaths[endpoint["path"].(string)] = true
		}
	}
	for path, found := range wantPaths {
		if !found {
			t.Errorf("configured schema omitted %s", path)
		}
	}

	_, _, unconfigured := providerTestApp(t, nil)
	discovery = providerRequest(t, unconfigured, http.MethodGet, "/api/auth/providers", "", "")
	if discovery.Code != http.StatusOK {
		t.Fatalf("unconfigured discovery status=%d body=%s", discovery.Code, discovery.Body.String())
	}
	providers, _ = decodeProviderResponse(t, discovery)["providers"].([]any)
	if len(providers) != 0 {
		t.Fatalf("unconfigured providers=%#v", providers)
	}
	schemaResponse = providerRequest(t, unconfigured, http.MethodGet, "/api/schema", "", "")
	endpoints, _ = decodeProviderResponse(t, schemaResponse)["endpoints"].([]any)
	for _, raw := range endpoints {
		endpoint, _ := raw.(map[string]any)
		path, _ := endpoint["path"].(string)
		if strings.HasPrefix(path, "/api/auth/provider") || path == "/api/auth/providers" {
			t.Fatalf("unconfigured schema advertised provider endpoint %q", path)
		}
	}
}

func TestProviderCallbackResolvesProviderFromState(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"canonical": {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
	}}
	config := fakeProviderConfig(adapter, "fake", "https://issuer.example")
	config.RedirectURI = "https://app.example/api/auth/provider/callback"
	_, _, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": config})
	state, authorization := startProviderFlow(t, handler, "fake", "sign_in", "", "")
	if authorization.RedirectURI != config.RedirectURI {
		t.Fatalf("authorization redirect URI=%q want %q", authorization.RedirectURI, config.RedirectURI)
	}
	rec := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?state="+url.QueryEscape(state)+"&code=canonical", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("canonical callback status=%d body=%s", rec.Code, rec.Body.String())
	}
	if code, _ := decodeProviderResponse(t, rec)["completionCode"].(string); code == "" {
		t.Fatal("canonical callback omitted completion code")
	}
}

func TestProviderConfigurationStartupInvariants(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
	duplicate := New(Config{DataDir: t.TempDir(), AuthProviders: map[string]AuthProviderConfig{
		"first":  fakeProviderConfig(adapter, "first", "https://issuer.example"),
		"second": fakeProviderConfig(adapter, "second", "https://issuer.example"),
	}})
	if _, err := duplicate.Open(); err == nil || !strings.Contains(err.Error(), "same issuer") {
		t.Fatalf("duplicate issuer open error=%v", err)
	}

	withoutAuth := New(Config{DataDir: t.TempDir(), AuthProviders: map[string]AuthProviderConfig{
		"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example"),
	}})
	Define(withoutAuth, "notes", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("body")
	})
	if _, err := withoutAuth.Open(); err == nil || !strings.Contains(err.Error(), "requires an auth table") {
		t.Fatalf("provider without auth table error=%v", err)
	}
}

func TestProviderSystemTablesAreOwnedAndAdminHidden(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
	_, db, _ := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	admin := &EngineAdminProvider{DB: db}
	tables, err := admin.AdminTables()
	if err != nil {
		t.Fatal(err)
	}
	for _, systemTable := range []string{systemAuthIdentityTableName, systemAuthProviderFlowTableName} {
		meta := db.db.GetMeta().Tables[systemTable]
		if meta == nil || meta.SystemOwner != "provider_auth" {
			t.Fatalf("system owner for %s = %#v", systemTable, meta)
		}
		for _, table := range tables {
			if table.Name == systemTable {
				t.Fatalf("admin table listing exposed %s", systemTable)
			}
		}
		if _, ok, err := admin.AdminRows(systemTable, 10, 0); err != nil || ok {
			t.Fatalf("admin rows for %s ok=%t err=%v", systemTable, ok, err)
		}
		if _, err := admin.AdminCreateRow(systemTable, map[string]any{}); err == nil {
			t.Fatalf("admin create allowed for %s", systemTable)
		}
		if err := admin.AdminUpdateRow(systemTable, "id", map[string]any{}); err == nil {
			t.Fatalf("admin update allowed for %s", systemTable)
		}
		if err := admin.AdminDeleteRow(systemTable, "id"); err == nil {
			t.Fatalf("admin delete allowed for %s", systemTable)
		}
	}
}

func TestProviderSystemTableCollisionIsActionable(t *testing.T) {
	for _, tableName := range []string{systemAuthIdentityTableName, systemAuthProviderFlowTableName} {
		t.Run(tableName, func(t *testing.T) {
			dataDir := t.TempDir()
			legacy := engine.NewDatabase(engine.DatabaseConfig{DataDir: dataDir, SyncMode: "normal"})
			err := legacy.Open(map[string]*schema.TableDef{
				tableName: {
					Name: tableName,
					CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
						{Name: "id", Kind: schema.KindString, Required: true, Unique: true},
						{Name: "user_data", Kind: schema.KindString},
					}),
				},
			})
			if err != nil {
				t.Fatalf("create colliding table: %v", err)
			}
			if err := legacy.Close(); err != nil {
				t.Fatalf("close colliding database: %v", err)
			}

			app := New(Config{DataDir: dataDir, SyncMode: "normal"})
			if _, err := app.Open(); err == nil || !strings.Contains(err.Error(), "conflicts with reserved system table owner") {
				t.Fatalf("reserved table collision error=%v", err)
			}
		})
	}
}

func TestProviderLinkAndSignInUseIssuerSubjectAndStandardSession(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link-code":    {Provider: "fake", Issuer: "https://issuer.example", Subject: "stable-subject", DisplayName: "Initial", AvatarURL: "https://cdn.example/initial.png", Email: "initial@example.com", EmailVerified: true},
		"login-code":   {Provider: "fake", Issuer: "https://issuer.example", Subject: "stable-subject", DisplayName: "Changed", AvatarURL: "https://cdn.example/changed.png", Email: "changed@example.com", EmailVerified: false},
		"clear-avatar": {Provider: "fake", Issuer: "https://issuer.example", Subject: "stable-subject", DisplayName: "Latest", AvatarURL: "javascript:alert(1)", Email: "latest@example.com", EmailVerified: true},
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
	linkedOut := decodeProviderResponse(t, linked)
	linkedIdentity, _ := linkedOut["identity"].(map[string]any)
	if linkedIdentity["avatarURL"] != "https://cdn.example/initial.png" || linkedIdentity["subject"] != nil {
		t.Fatalf("link identity payload=%#v", linkedIdentity)
	}
	identityRows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil || len(identityRows) != 1 {
		t.Fatalf("linked identities=%#v err=%v", identityRows, err)
	}
	if identityRows[0]["avatar_url"] != "https://cdn.example/initial.png" {
		t.Fatalf("stored linked identity=%#v", identityRows[0])
	}
	identityID := toString(identityRows[0]["id"])
	list := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/identities", "", passwordToken)
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "stable-subject") || !strings.Contains(list.Body.String(), "https://cdn.example/initial.png") {
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
	if _, ok := loginOut["grant"]; !ok {
		t.Fatalf("provider login dropped grant response key: %#v", loginOut)
	}
	loginIdentity, _ := loginOut["identity"].(map[string]any)
	if loginIdentity["avatarURL"] != "https://cdn.example/changed.png" || loginIdentity["displayName"] != "Changed" || loginIdentity["subject"] != nil {
		t.Fatalf("provider login identity=%#v", loginIdentity)
	}
	if strings.Contains(fmt.Sprint(loginIdentity), "refreshToken") || strings.Contains(fmt.Sprint(loginIdentity), "accessToken") {
		t.Fatalf("provider login identity exposed token material: %#v", loginIdentity)
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
	refreshed, err := db.Table(systemAuthIdentityTableName).Get(identityID)
	if err != nil || refreshed["avatar_url"] != "https://cdn.example/changed.png" || refreshed["display_name"] != "Changed" {
		t.Fatalf("refreshed identity=%#v err=%v", refreshed, err)
	}

	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	completionCode = callbackProviderFlow(t, handler, "fake", state, "clear-avatar")
	cleared := completeProviderFlow(t, handler, completionCode, "", false)
	if cleared.Code != http.StatusOK {
		t.Fatalf("clear avatar login status=%d body=%s", cleared.Code, cleared.Body.String())
	}
	clearedIdentity, _ := decodeProviderResponse(t, cleared)["identity"].(map[string]any)
	if _, exists := clearedIdentity["avatarURL"]; exists {
		t.Fatalf("invalid refreshed avatar was returned: %#v", clearedIdentity)
	}
	refreshed, err = db.Table(systemAuthIdentityTableName).Get(identityID)
	if err != nil || toString(refreshed["avatar_url"]) != "" || refreshed["display_name"] != "Latest" {
		t.Fatalf("cleared identity=%#v err=%v", refreshed, err)
	}
	list = providerRequest(t, handler, http.MethodGet, "/api/auth/provider/identities", "", passwordToken)
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "avatarURL") {
		t.Fatalf("identity list retained invalid stale avatar: status=%d body=%s", list.Code, list.Body.String())
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
		"linked":        {Provider: "fake", Issuer: "https://issuer.example", Subject: "linked", DisplayName: "Shared", AvatarURL: "https://cdn.example/shared.png", Email: "account@example.com", EmailVerified: true},
		"same-metadata": {Provider: "fake", Issuer: "https://issuer.example", Subject: "unlinked", DisplayName: "Shared", AvatarURL: "https://cdn.example/shared.png", Email: "account@example.com", EmailVerified: true},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	token, _ := registerProviderTestUser(t, handler, "account@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", token, "")
	completionCode := callbackProviderFlow(t, handler, "fake", state, "linked")
	if rec := completeProviderFlow(t, handler, completionCode, token, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	sessionsBefore := db.Table(systemAuthSessionTableName).Count()

	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	completionCode = callbackProviderFlow(t, handler, "fake", state, "same-metadata")
	rec := completeProviderFlow(t, handler, completionCode, "", false)
	if rec.Code != http.StatusConflict || decodeProviderResponse(t, rec)["code"] != "link_required" {
		t.Fatalf("unlinked complete status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := db.Table(systemAuthIdentityTableName).Count(); got != 1 {
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

func TestCompletedProviderCallbackCannotFollowIdentityOwnershipChanges(t *testing.T) {
	for _, relink := range []bool{false, true} {
		name := "unlink"
		if relink {
			name = "relink"
		}
		t.Run(name, func(t *testing.T) {
			adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
				"link-a": {Provider: "fake", Issuer: "https://issuer.example", Subject: "moving-subject"},
				"link-b": {Provider: "fake", Issuer: "https://issuer.example", Subject: "moving-subject"},
				"login":  {Provider: "fake", Issuer: "https://issuer.example", Subject: "moving-subject"},
			}}
			_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
			ownerToken, ownerID := registerProviderTestUser(t, handler, "owner@example.com")
			otherToken, otherID := registerProviderTestUser(t, handler, "other@example.com")

			state, _ := startProviderFlow(t, handler, "fake", "link", ownerToken, "")
			linkCode := callbackProviderFlow(t, handler, "fake", state, "link-a")
			if rec := completeProviderFlow(t, handler, linkCode, ownerToken, true); rec.Code != http.StatusOK {
				t.Fatalf("initial link status=%d body=%s", rec.Code, rec.Body.String())
			}
			identityRows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", ownerID)
			if err != nil || len(identityRows) != 1 {
				t.Fatalf("owner identities=%#v err=%v", identityRows, err)
			}

			state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
			staleCompletion := callbackProviderFlow(t, handler, "fake", state, "login")
			removed := providerRequest(t, handler, http.MethodDelete, "/api/auth/provider/identities/"+toString(identityRows[0]["id"]), "", ownerToken)
			if removed.Code != http.StatusOK {
				t.Fatalf("unlink status=%d body=%s", removed.Code, removed.Body.String())
			}

			if relink {
				state, _ = startProviderFlow(t, handler, "fake", "link", otherToken, "")
				otherLinkCode := callbackProviderFlow(t, handler, "fake", state, "link-b")
				if rec := completeProviderFlow(t, handler, otherLinkCode, otherToken, true); rec.Code != http.StatusOK {
					t.Fatalf("relink status=%d body=%s", rec.Code, rec.Body.String())
				}
				otherRows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", otherID)
				if err != nil || len(otherRows) != 1 {
					t.Fatalf("new owner identities=%#v err=%v", otherRows, err)
				}
			}

			sessionsBefore := db.Table(systemAuthSessionTableName).Count()
			stale := completeProviderFlow(t, handler, staleCompletion, "", false)
			if stale.Code != http.StatusUnauthorized || decodeProviderResponse(t, stale)["code"] != "principal_unavailable" {
				t.Fatalf("stale completion status=%d body=%s", stale.Code, stale.Body.String())
			}
			if got := db.Table(systemAuthSessionTableName).Count(); got != sessionsBefore {
				t.Fatalf("stale completion created a session: before=%d after=%d", sessionsBefore, got)
			}
		})
	}
}

func TestBlockedProviderExchangeDoesNotBlockUnrelatedStart(t *testing.T) {
	blockedBase := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"wait": {Provider: "blocked", Issuer: "https://blocked.example", Subject: "subject"},
	}}
	blocked := &blockingExchangeProvider{base: blockedBase, started: make(chan struct{}), release: make(chan struct{})}
	fast := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{
		"blocked": fakeProviderConfig(blocked, "blocked", "https://blocked.example"),
		"fast":    fakeProviderConfig(fast, "fast", "https://fast.example"),
	})
	state, _ := startProviderFlow(t, handler, "blocked", "sign_in", "", "")
	callbackDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		path := "/api/auth/provider/callback?provider=blocked&state=" + url.QueryEscape(state) + "&code=wait"
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		callbackDone <- rec
	}()
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("blocked provider exchange did not start")
	}

	startDone := make(chan error, 1)
	go func() {
		_, err := db.providerAuth.start(context.Background(), "fast", "sign_in", "", nil)
		startDone <- err
	}()
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("unrelated provider start failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked exchange held the provider mutation boundary")
	}
	close(blocked.release)
	select {
	case rec := <-callbackDone:
		if rec.Code != http.StatusOK {
			t.Fatalf("blocked callback status=%d body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("blocked callback did not finish after release")
	}
}

func TestProviderStartIsRateLimitedAndOutstandingFlowsAreBounded(t *testing.T) {
	t.Run("request rate", func(t *testing.T) {
		adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
		_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
		for i := 0; i < authRateLimitPerMinute; i++ {
			rec := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", `{"provider":"fake","intent":"sign_in"}`, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("allowed start %d status=%d body=%s", i, rec.Code, rec.Body.String())
			}
		}
		limited := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", `{"provider":"fake","intent":"sign_in"}`, "")
		if limited.Code != http.StatusTooManyRequests {
			t.Fatalf("rate-limited start status=%d body=%s", limited.Code, limited.Body.String())
		}
		if got := db.Table(systemAuthProviderFlowTableName).Count(); got != authRateLimitPerMinute {
			t.Fatalf("rate limiter persisted %d flows, want %d", got, authRateLimitPerMinute)
		}
	})

	t.Run("persistent bound recovers after expiry", func(t *testing.T) {
		adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
		_, db, _ := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
		base := time.Unix(1_800_000_000, 0)
		db.providerAuth.now = func() time.Time { return base }
		db.providerAuth.maxFlows = 2
		for i := 0; i < 2; i++ {
			if _, err := db.providerAuth.start(context.Background(), "fake", "sign_in", "", nil); err != nil {
				t.Fatalf("start within bound %d: %v", i, err)
			}
		}
		_, err := db.providerAuth.start(context.Background(), "fake", "sign_in", "", nil)
		var authErr *AuthProviderError
		if !errors.As(err, &authErr) || authErr.Code != "provider_flow_limit" || authErr.Status != http.StatusServiceUnavailable {
			t.Fatalf("flow limit error=%#v", err)
		}
		if got := db.Table(systemAuthProviderFlowTableName).Count(); got != 2 {
			t.Fatalf("flow count at bound=%d", got)
		}

		base = base.Add(providerFlowTTL + time.Second)
		if _, err := db.providerAuth.start(context.Background(), "fake", "sign_in", "", nil); err != nil {
			t.Fatalf("start after expiry: %v", err)
		}
		if got := db.Table(systemAuthProviderFlowTableName).Count(); got != 1 {
			t.Fatalf("expired flows were not reclaimed before admission: %d", got)
		}
	})
}

func TestProviderIdentityPersistsAcrossReopenAndExpiredFlowsAreCleaned(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link": {Provider: "fake", Issuer: "https://issuer.example", Subject: "persistent", AvatarURL: "https://cdn.example/persistent.png"},
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
	if err != nil || len(rows) != 1 || rows[0]["subject"] != "persistent" || rows[0]["avatar_url"] != "https://cdn.example/persistent.png" {
		t.Fatalf("reopened identities=%#v err=%v", rows, err)
	}
	if got := db.Table(systemAuthProviderFlowTableName).Count(); got != 0 {
		t.Fatalf("expired callback material survived cleanup/reopen: %d rows", got)
	}
}

func TestProviderAvatarSchemaUpgradePreservesLegacyIdentityAndIndexes(t *testing.T) {
	dataDir := t.TempDir()
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{}}
	app := New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviders: map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")}})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.String("default_role").Default("user")
		s.Roles("roles")
	})
	definitions := app.buildTableDefs()
	withoutField := func(definition *schema.TableDef, fieldName string) *schema.TableDef {
		fields := make([]schema.CompiledField, 0, len(definition.CompiledSchema.Fields)-1)
		for _, field := range definition.CompiledSchema.Fields {
			if field.Name != fieldName {
				fields = append(fields, field)
			}
		}
		legacyDefinition := *definition
		legacyDefinition.CompiledSchema = schema.NewCompiledSchema(fields)
		return &legacyDefinition
	}
	definitions[systemAuthIdentityTableName] = withoutField(definitions[systemAuthIdentityTableName], "avatar_url")
	definitions[systemAuthProviderFlowTableName] = withoutField(definitions[systemAuthProviderFlowTableName], "result_avatar_url")

	legacy := engine.NewDatabase(engine.DatabaseConfig{DataDir: dataDir, SyncMode: "normal"})
	if err := legacy.Open(definitions); err != nil {
		t.Fatalf("open pre-avatar database: %v", err)
	}
	if _, err := legacy.GetTable(systemAuthIdentityTableName).Insert(map[string]any{
		"id": "legacyidentity0000000001", "principal_id": "legacy-principal", "provider": "fake",
		"issuer": "https://issuer.example", "subject": "legacy-subject", "display_name": "Legacy",
		"email": "legacy@example.com", "email_verified": true, "linked_at": int64(100), "last_authenticated_at": int64(200),
	}, nil); err != nil {
		t.Fatalf("insert legacy identity: %v", err)
	}
	if _, err := legacy.GetTable(systemAuthProviderFlowTableName).Insert(map[string]any{
		"id": "legacyflow00000000000001", "state_hash": "legacy-state", "provider": "fake", "intent": "sign_in",
		"secrets_ciphertext": "legacy", "redirect_uri": "https://app.example/callback", "created_at": time.Now().Unix(),
		"expires_at": time.Now().Add(time.Hour).Unix(), "phase": providerFlowPhaseStarted,
	}, nil); err != nil {
		t.Fatalf("insert legacy flow: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close pre-avatar database: %v", err)
	}

	db, err := app.Open()
	if err != nil {
		t.Fatalf("upgrade pre-avatar database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	identity, ok := db.Table(systemAuthIdentityTableName).FindByUniqueCompositeIndex([]string{"issuer", "subject"}, "https://issuer.example", "legacy-subject")
	if !ok || toString(identity["id"]) != "legacyidentity0000000001" || toString(identity["avatar_url"]) != "" {
		t.Fatalf("upgraded identity lookup=%#v ok=%t", identity, ok)
	}
	byPrincipal, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", "legacy-principal")
	if err != nil || len(byPrincipal) != 1 {
		t.Fatalf("upgraded principal index rows=%#v err=%v", byPrincipal, err)
	}
	flow, err := db.Table(systemAuthProviderFlowTableName).Get("legacyflow00000000000001")
	if err != nil || flow == nil || toString(flow["result_avatar_url"]) != "" {
		t.Fatalf("upgraded flow=%#v err=%v", flow, err)
	}
	if _, err := db.Table(systemAuthIdentityTableName).Update("legacyidentity0000000001", map[string]any{"avatar_url": "https://cdn.example/legacy.png"}); err != nil {
		t.Fatalf("write upgraded identity avatar field: %v", err)
	}
	if _, err := db.Table(systemAuthProviderFlowTableName).Update("legacyflow00000000000001", map[string]any{"result_avatar_url": "https://cdn.example/flow.png"}); err != nil {
		t.Fatalf("write upgraded flow avatar field: %v", err)
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

func TestProviderRefreshRotationCannotSurviveConcurrentUnlink(t *testing.T) {
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"link":  {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
		"login": {Provider: "fake", Issuer: "https://issuer.example", Subject: "subject"},
	}}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	passwordToken, principalID := registerProviderTestUser(t, handler, "refresh-race@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", passwordToken, "")
	linkedCode := callbackProviderFlow(t, handler, "fake", state, "link")
	if rec := completeProviderFlow(t, handler, linkedCode, passwordToken, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	identityRows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil || len(identityRows) != 1 {
		t.Fatalf("linked identities=%#v err=%v", identityRows, err)
	}
	identityID := toString(identityRows[0]["id"])

	state, _ = startProviderFlow(t, handler, "fake", "sign_in", "", "")
	loginCode := callbackProviderFlow(t, handler, "fake", state, "login")
	login := completeProviderFlow(t, handler, loginCode, "", false)
	if login.Code != http.StatusOK {
		t.Fatalf("provider login status=%d body=%s", login.Code, login.Body.String())
	}
	refreshToken, _ := decodeProviderResponse(t, login)["refreshToken"].(string)

	gateRelease := make(chan struct{})
	gate := &firstLockGate{acquired: make(chan struct{}), release: gateRelease}
	db.providerAuth.mu = gate
	db.authService.SetProviderSessionLocker(gate)
	type refreshResult struct {
		token string
		err   error
	}
	refreshDone := make(chan refreshResult, 1)
	go func() {
		token, _, err := db.authService.Refresh(refreshToken)
		refreshDone <- refreshResult{token: token, err: err}
	}()
	select {
	case <-gate.acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not reach provider-session rotation gate")
	}

	unlinkStarted := make(chan struct{})
	unlinkDone := make(chan error, 1)
	go func() {
		close(unlinkStarted)
		unlinkDone <- db.providerAuth.unlink(principalID, identityID)
	}()
	<-unlinkStarted
	close(gateRelease)
	refreshed := <-refreshDone
	if refreshed.err != nil || refreshed.token == "" {
		t.Fatalf("refresh that won coordination failed: token=%q err=%v", refreshed.token, refreshed.err)
	}
	if err := <-unlinkDone; err != nil {
		t.Fatalf("concurrent unlink: %v", err)
	}

	derived, err := db.Table(systemAuthSessionTableName).FindByIndex("auth_identity_id", identityID)
	if err != nil || len(derived) < 2 {
		t.Fatalf("provider-derived sessions=%#v err=%v", derived, err)
	}
	for _, session := range derived {
		if providerUnix(session["revoked_at"]) == 0 {
			t.Fatalf("provider-derived session survived unlink: %#v", session)
		}
	}
}

func TestProviderCallbackExchangeDoesNotHoldServiceLock(t *testing.T) {
	exchangeStarted := make(chan struct{})
	exchangeRelease := make(chan struct{})
	adapter := &fakeAuthProvider{
		identities: map[string]AuthProviderIdentity{
			"link": {Provider: "fake", Issuer: "https://issuer.example", Subject: "linked"},
			"slow": {Provider: "fake", Issuer: "https://issuer.example", Subject: "slow"},
		},
		blockCode: "slow", exchangeStarted: exchangeStarted, exchangeRelease: exchangeRelease,
	}
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	passwordToken, principalID := registerProviderTestUser(t, handler, "blocking-callback@example.com")
	state, _ := startProviderFlow(t, handler, "fake", "link", passwordToken, "")
	linkedCode := callbackProviderFlow(t, handler, "fake", state, "link")
	if rec := completeProviderFlow(t, handler, linkedCode, passwordToken, true); rec.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", rec.Code, rec.Body.String())
	}
	identityRows, _ := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	identityID := toString(identityRows[0]["id"])

	slowState, _ := startProviderFlow(t, handler, "fake", "sign_in", "", "")
	callbackDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		path := "/api/auth/provider/callback?provider=fake&state=" + url.QueryEscape(slowState) + "&code=slow"
		callbackDone <- providerRequest(t, handler, http.MethodGet, path, "", "")
	}()
	select {
	case <-exchangeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking adapter exchange did not start")
	}

	flow, ok := db.Table(systemAuthProviderFlowTableName).FindByUniqueIndex("state_hash", hashProviderToken(slowState))
	if !ok || providerUnix(flow["callback_consumed_at"]) == 0 || providerUnix(flow["callback_claim_expires_at"]) == 0 {
		t.Fatalf("callback was not durably claimed before exchange: %#v", flow)
	}
	flowID := toString(flow["id"])
	if _, err := db.Table(systemAuthProviderFlowTableName).Update(flowID, map[string]any{"expires_at": time.Now().Add(-time.Minute).Unix()}); err != nil {
		t.Fatalf("expire original flow deadline: %v", err)
	}
	if _, err := db.cleanupExpiredProviderFlows(time.Now()); err != nil {
		t.Fatalf("cleanup during claimed callback: %v", err)
	}
	if claimed, err := db.Table(systemAuthProviderFlowTableName).Get(flowID); err != nil || claimed == nil {
		t.Fatalf("cleanup removed in-flight callback claim: %#v err=%v", claimed, err)
	}

	unlinkDone := make(chan error, 1)
	go func() { unlinkDone <- db.providerAuth.unlink(principalID, identityID) }()
	select {
	case err := <-unlinkDone:
		if err != nil {
			t.Fatalf("unlink while adapter exchange blocked: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking adapter exchange held the provider service lock")
	}
	close(exchangeRelease)
	select {
	case rec := <-callbackDone:
		if rec.Code != http.StatusOK {
			t.Fatalf("callback after exchange release status=%d body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not finish after adapter exchange was released")
	}
}

func TestProviderUnlinkCountsOnlyCurrentlyUsableIdentities(t *testing.T) {
	first := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"first": {Provider: "first", Issuer: "https://issuer-one.example", Subject: "first-subject"},
	}}
	second := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"second": {Provider: "second", Issuer: "https://issuer-two.example", Subject: "second-subject"},
	}}
	secondConfig := fakeProviderConfig(second, "second", "https://issuer-two.example")
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{
		"first":  fakeProviderConfig(first, "first", "https://issuer-one.example"),
		"second": secondConfig,
	})
	passwordToken, principalID := registerProviderTestUser(t, handler, "provider-removal@example.com")
	for _, link := range []struct {
		provider string
		code     string
	}{{"first", "first"}, {"second", "second"}} {
		state, _ := startProviderFlow(t, handler, link.provider, "link", passwordToken, "")
		completionCode := callbackProviderFlow(t, handler, link.provider, state, link.code)
		if rec := completeProviderFlow(t, handler, completionCode, passwordToken, true); rec.Code != http.StatusOK {
			t.Fatalf("link %s status=%d body=%s", link.provider, rec.Code, rec.Body.String())
		}
	}
	rows, err := db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("linked identities=%#v err=%v", rows, err)
	}
	firstIdentityID := ""
	for _, row := range rows {
		if toString(row["provider"]) == "first" {
			firstIdentityID = toString(row["id"])
		}
	}
	if _, err := db.db.GetAuthTable().Update(principalID, map[string]any{"password": "not-a-usable-hash"}, nil); err != nil {
		t.Fatalf("make password unusable: %v", err)
	}

	delete(db.providerAuth.providers, "second")
	assertProviderErrorCode(t, db.providerAuth.unlink(principalID, firstIdentityID), "last_sign_in_method")

	changedIssuer := secondConfig
	changedIssuer.Issuer = "https://issuer-two-changed.example"
	db.providerAuth.providers["second"] = changedIssuer
	assertProviderErrorCode(t, db.providerAuth.unlink(principalID, firstIdentityID), "last_sign_in_method")

	db.providerAuth.providers["second"] = secondConfig
	if err := db.providerAuth.unlink(principalID, firstIdentityID); err != nil {
		t.Fatalf("unlink with another currently usable identity: %v", err)
	}
}

func TestSetJWTSecretDoesNotReplaceProviderServiceDuringCleanup(t *testing.T) {
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{}}
	db, _ := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()

	providerAuth := db.providerAuth
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 250; i++ {
			if _, err := db.cleanupExpiredProviderFlows(time.Now()); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 250; i++ {
			db.SetJWTSecret(fmt.Sprintf("deployment-secret-%d", i))
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 250; i++ {
			sealed, err := providerAuth.sealSecrets(providerFlowSecrets{Nonce: fmt.Sprintf("nonce-%d", i)})
			if err != nil {
				errs <- err
				return
			}
			// A rekey between sealing and opening intentionally invalidates the
			// legacy flow; either result is valid, but both cipher accesses must
			// remain synchronized.
			_, _ = providerAuth.openSecrets(sealed)
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("cleanup provider flows: %v", err)
	}
	if db.providerAuth != providerAuth {
		t.Fatal("SetJWTSecret replaced the provider service")
	}
}

func TestSetJWTSecretReconfiguresAuthAndProviderFlowCipher(t *testing.T) {
	t.Run("legacy flow and auth secret", func(t *testing.T) {
		legacy := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
			"legacy-code": {Provider: "legacy", Issuer: "https://legacy-issuer.example", Subject: "legacy-subject"},
		}}
		_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{
			"legacy": fakeProviderConfig(legacy, "legacy", "https://legacy-issuer.example"),
		})
		oldSecret := db.jwtSecret
		oldToken, _ := registerProviderTestUser(t, handler, "rekey@example.com")
		legacyState, _ := startProviderFlow(t, handler, "legacy", "sign_in", "", "")
		providerAuth := db.providerAuth
		const deploymentSecret = "configured-deployment-secret"
		db.SetJWTSecret(deploymentSecret)

		if db.providerAuth != providerAuth {
			t.Fatal("SetJWTSecret replaced the provider service")
		}
		if _, err := db.ValidateAccessToken(oldToken); err == nil {
			t.Fatal("token issued with the pre-configuration secret remained valid")
		}
		if internalserver.VerifyJWT(oldToken, deploymentSecret) != nil {
			t.Fatal("pre-configuration token validated with the deployment secret")
		}
		newToken, _, _, err := db.authService.Login("rekey@example.com", "password123")
		if err != nil {
			t.Fatalf("login after SetJWTSecret: %v", err)
		}
		if internalserver.VerifyJWT(newToken, deploymentSecret) == nil {
			t.Fatal("post-configuration token did not validate with the deployment secret")
		}
		if internalserver.VerifyJWT(newToken, oldSecret) != nil {
			t.Fatal("post-configuration token validated with the old secret")
		}
		if _, err := db.ValidateAccessToken(newToken); err != nil {
			t.Fatalf("validate post-configuration token: %v", err)
		}

		legacyCallback := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?provider=legacy&state="+url.QueryEscape(legacyState)+"&code=legacy-code", "", "")
		if legacyCallback.Code != http.StatusBadRequest {
			t.Fatalf("legacy flow survived JWT rekey: status=%d body=%s", legacyCallback.Code, legacyCallback.Body.String())
		}
	})

	t.Run("app-scoped provider secret key", func(t *testing.T) {
		appProvider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
			"app-code": {AccessToken: "app-access", RefreshToken: "app-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
		}}
		db, handler := openGrantTestDatabase(t, t.TempDir(), appProvider, "client")
		defer db.Close()
		_, _ = registerProviderTestUser(t, handler, "provider-rekey@example.com")
		db.SetJWTSecret("configured-app-deployment-secret")
		newToken, _, _, err := db.authService.Login("provider-rekey@example.com", "password123")
		if err != nil {
			t.Fatalf("login after SetJWTSecret: %v", err)
		}
		appState := startAppFlow(t, handler, "app", "link", newToken, "", "identity")
		completionCode := callbackProviderFlow(t, handler, "shared", appState, "app-code")
		completed := completeProviderFlow(t, handler, completionCode, newToken, true)
		if completed.Code != http.StatusOK {
			t.Fatalf("provider flow after SetJWTSecret status=%d body=%s", completed.Code, completed.Body.String())
		}
		grantID := decodeProviderResponse(t, completed)["grant"].(map[string]any)["id"].(string)
		lease, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
		if err != nil || lease.AccessToken != "app-access" {
			t.Fatalf("app-scoped provider token after SetJWTSecret=%+v err=%v", lease, err)
		}
	})
}

func assertProviderErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var providerErr *AuthProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != code {
		t.Fatalf("provider error=%v, want code %q", err, code)
	}
}
