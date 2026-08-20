package flop

import (
	"context"
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
)

type fakeGrantProvider struct {
	mu               sync.Mutex
	issuer           string
	tokens           map[string]AuthProviderTokenSet
	refreshes        int
	refreshScopes    []string
	lastRefresh      AuthProviderRefreshRequest
	refreshStarted   chan struct{}
	refreshRelease   <-chan struct{}
	revocations      int
	lastRevoke       AuthProviderRevokeRequest
	revokeErr        error
	revokeStarted    chan struct{}
	revokeRelease    <-chan struct{}
	revokePreference string
}

func (p *fakeGrantProvider) AuthorizationURL(_ context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	u, _ := url.Parse("https://provider.example/authorize")
	q := u.Query()
	q.Set("state", request.State)
	q.Set("nonce", request.Nonce)
	q.Set("redirect_uri", request.RedirectURI)
	q.Set("scope", fmt.Sprint(request.Scopes))
	u.RawQuery = q.Encode()
	return u.String(), nil
}
func (p *fakeGrantProvider) Exchange(_ context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	result, err := p.ExchangeGrant(context.Background(), request)
	return result.Identity, err
}
func (p *fakeGrantProvider) ExchangeGrant(_ context.Context, request AuthProviderCallbackRequest) (AuthProviderExchangeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tokens, ok := p.tokens[request.Code]
	if !ok {
		return AuthProviderExchangeResult{}, fmt.Errorf("invalid code")
	}
	return AuthProviderExchangeResult{Identity: AuthProviderIdentity{Provider: request.Provider, Issuer: p.issuer, Subject: "shared-subject", DisplayName: "Shared Person"}, Tokens: tokens, GrantedScopes: tokens.Scopes, Capabilities: AuthProviderCapabilities{Refresh: true, Revocation: true}}, nil
}
func (p *fakeGrantProvider) RefreshGrant(_ context.Context, request AuthProviderRefreshRequest) (AuthProviderTokenSet, error) {
	p.mu.Lock()
	p.refreshes++
	p.lastRefresh = request
	scopes := append([]string(nil), request.Scopes...)
	if p.refreshScopes != nil {
		scopes = append([]string(nil), p.refreshScopes...)
	}
	started, release := p.refreshStarted, p.refreshRelease
	p.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	return AuthProviderTokenSet{AccessToken: request.AppID + "-refreshed", RefreshToken: request.RefreshToken + "-rotated", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: scopes}, nil
}
func (p *fakeGrantProvider) RevokeGrant(_ context.Context, request AuthProviderRevokeRequest) error {
	p.mu.Lock()
	p.revocations++
	p.lastRevoke = request
	started, release, revokeErr := p.revokeStarted, p.revokeRelease, p.revokeErr
	p.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	return revokeErr
}
func (p *fakeGrantProvider) RevocationTokenType() string { return p.revokePreference }

func appGrantTestConfig(adapter AuthProviderGrantAdapter, clientID, credential string) AuthProviderAppConfig {
	return AuthProviderAppConfig{AllowedReturnURLs: []string{"https://client.example/done"}, BackendCredentials: []string{credential}, Providers: map[string]AuthProviderConfig{"shared": {Adapter: adapter, Issuer: "https://issuer.example", RedirectURI: "https://flop.example/api/auth/provider/callback?provider=shared", ClientID: clientID, ClientSecret: clientID + "-secret", AllowedScopes: []string{"identity", "read", GoogleScopeYouTubeReadonly}, DefaultScopes: []string{"identity"}, RequiredScopes: []string{"identity"}}}}
}

func defineGrantTestUsers(app *App) {
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.String("name")
		s.String("default_role").Default("user")
		s.Roles("roles")
		s.Boolean("disabled").Default(false)
	})
}

func openGrantTestDatabase(t *testing.T, dataDir string, provider *fakeGrantProvider, clientID string) (*Database, http.Handler) {
	t.Helper()
	app := New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(provider, clientID, "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	return db, app.APIHandler(db)
}

func linkGrantForTest(t *testing.T, db *Database, handler http.Handler, providerCode, email string, scopes ...string) (string, string, string, string) {
	t.Helper()
	token, principalID := registerProviderTestUser(t, handler, email)
	state := startAppFlow(t, handler, "app", "link", token, "", scopes...)
	completion := callbackProviderFlow(t, handler, "shared", state, providerCode)
	response := completeProviderFlow(t, handler, completion, token, true)
	if response.Code != http.StatusOK {
		t.Fatalf("link status=%d body=%s", response.Code, response.Body.String())
	}
	grantID := decodeProviderResponse(t, response)["grant"].(map[string]any)["id"].(string)
	grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil {
		t.Fatalf("grant lookup=%#v err=%v", grant, err)
	}
	return token, principalID, grantID, toString(grant["identity_id"])
}

func startAppFlow(t *testing.T, handler http.Handler, appID, intent, bearer, grantID string, scopes ...string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"appId": appID, "provider": "shared", "intent": intent, "grantId": grantID, "scopes": scopes})
	rec := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", string(body), bearer)
	if rec.Code != 200 {
		t.Fatalf("start %s status=%d body=%s", appID, rec.Code, rec.Body.String())
	}
	authorizationURL, _ := decodeProviderResponse(t, rec)["authorizationUrl"].(string)
	parsed, _ := url.Parse(authorizationURL)
	return parsed.Query().Get("state")
}

func TestProviderGrantsAreIsolatedByApp(t *testing.T) {
	providerA := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
		"a":           {AccessToken: "app-a-token", RefreshToken: "app-a-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}},
		"incremental": {AccessToken: "app-a-youtube", RefreshToken: "app-a-youtube-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read", GoogleScopeYouTubeReadonly}},
	}}
	providerB := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{"b": {AccessToken: "app-b-token", RefreshToken: "app-b-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}}}}
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app-a": appGrantTestConfig(providerA, "client-a", "backend-a"), "app-b": appGrantTestConfig(providerB, "client-b", "backend-b")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.String("name")
		s.String("default_role").Default("user")
		s.Roles("roles")
		s.Boolean("disabled").Default(false)
	})
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := app.APIHandler(db)
	discovery := providerRequest(t, handler, http.MethodGet, "/api/auth/providers?appID=app-a", "", "")
	if strings.Contains(discovery.Body.String(), "client-a") || strings.Contains(discovery.Body.String(), "secret") {
		t.Fatalf("discovery exposed registration credentials: %s", discovery.Body.String())
	}
	passwordToken, principalID := registerProviderTestUser(t, handler, "grants@example.com")
	stateA := startAppFlow(t, handler, "app-a", "link", passwordToken, "", "identity", "read")
	completionA := callbackProviderFlow(t, handler, "shared", stateA, "a")
	linked := completeProviderFlow(t, handler, completionA, passwordToken, true)
	if linked.Code != 200 {
		t.Fatalf("link status=%d body=%s", linked.Code, linked.Body.String())
	}
	if strings.Contains(linked.Body.String(), "app-a-token") || strings.Contains(linked.Body.String(), "app-a-refresh") {
		t.Fatalf("browser completion exposed provider tokens: %s", linked.Body.String())
	}
	grantA := decodeProviderResponse(t, linked)["grant"].(map[string]any)["id"].(string)
	stateB := startAppFlow(t, handler, "app-b", "sign_in", "", "", "identity", "read")
	completionB := callbackProviderFlow(t, handler, "shared", stateB, "b")
	signedIn := completeProviderFlow(t, handler, completionB, "", false)
	if signedIn.Code != 200 {
		t.Fatalf("sign in status=%d body=%s", signedIn.Code, signedIn.Body.String())
	}
	grantB := decodeProviderResponse(t, signedIn)["grant"].(map[string]any)["id"].(string)
	if db.Table(systemAuthIdentityTableName).Count() != 1 {
		t.Fatalf("identities=%d want 1", db.Table(systemAuthIdentityTableName).Count())
	}
	if db.Table(systemAuthProviderGrantTableName).Count() != 2 {
		t.Fatalf("grants=%d want 2", db.Table(systemAuthProviderGrantTableName).Count())
	}
	rowA, _ := db.Table(systemAuthProviderGrantTableName).Get(grantA)
	rowB, _ := db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if toString(rowA["token_ciphertext"]) == toString(rowB["token_ciphertext"]) {
		t.Fatal("app grants reused ciphertext")
	}
	if toString(rowA["principal_id"]) != principalID || toString(rowB["principal_id"]) != principalID {
		t.Fatal("grants did not retain shared principal")
	}
	registrationA, _ := db.Table(systemAuthProviderRegistrationTableName).FindByUniqueCompositeIndex([]string{"app_id", "provider"}, "app-a", "shared")
	if strings.Contains(toString(registrationA["credential_ciphertext"]), "client-a-secret") || strings.Contains(toString(rowA["token_ciphertext"]), "app-a-token") {
		t.Fatal("provider secrets were persisted without encryption")
	}
	appA, _ := db.Table(systemAuthProviderAppTableName).FindByUniqueIndex("app_id", "app-a")
	if strings.Contains(fmt.Sprint(appA["backend_credential_hashes"]), "backend-a") {
		t.Fatal("backend credential was persisted without hashing")
	}
	leaseA, err := db.ProviderToken(context.Background(), "app-a", "backend-a", grantA, "read")
	if err != nil || leaseA.AccessToken != "app-a-token" {
		t.Fatalf("app A lease=%+v err=%v", leaseA, err)
	}
	backendBody, _ := json.Marshal(map[string]any{"appID": "app-a", "grantId": grantA, "requiredScopes": []string{"read"}})
	backendRequest := httptest.NewRequest(http.MethodPost, "/api/auth/provider/backend/token", strings.NewReader(string(backendBody)))
	backendRequest.Header.Set("Content-Type", "application/json")
	backendRequest.Header.Set("X-Flop-Backend-Credential", "backend-a")
	backendResponse := httptest.NewRecorder()
	handler.ServeHTTP(backendResponse, backendRequest)
	if backendResponse.Code != http.StatusOK || backendResponse.Header().Get("Cache-Control") != "no-store" || strings.Contains(backendResponse.Body.String(), "app-a-refresh") {
		t.Fatalf("backend response status=%d cache=%q body=%s", backendResponse.Code, backendResponse.Header().Get("Cache-Control"), backendResponse.Body.String())
	}
	if _, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantA, "read"); err == nil {
		t.Fatal("cross-app grant lookup succeeded")
	}
	consentState := startAppFlow(t, handler, "app-a", "consent", passwordToken, grantA, GoogleScopeYouTubeReadonly)
	consentCompletion := callbackProviderFlow(t, handler, "shared", consentState, "incremental")
	consented := completeProviderFlow(t, handler, consentCompletion, passwordToken, true)
	if consented.Code != http.StatusOK {
		t.Fatalf("incremental consent status=%d body=%s", consented.Code, consented.Body.String())
	}
	if incrementalID := decodeProviderResponse(t, consented)["grant"].(map[string]any)["id"].(string); incrementalID != grantA {
		t.Fatalf("incremental consent created grant %q, want %q", incrementalID, grantA)
	}
	if lease, err := db.ProviderToken(context.Background(), "app-a", "backend-a", grantA, GoogleScopeYouTubeReadonly); err != nil || lease.AccessToken != "app-a-youtube" {
		t.Fatalf("incremental lease=%+v err=%v", lease, err)
	}
	var copied AuthProviderTokenSet
	if err := db.providerAuth.openProviderValue("grant", "app-b", toString(rowA["registration_id"]), grantA, toString(rowA["token_ciphertext"]), toString(rowA["token_key_version"]), &copied); err == nil {
		t.Fatal("ciphertext authenticated under a different app AAD")
	}
	if err := db.providerAuth.revokeGrant(context.Background(), principalID, grantA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ProviderToken(context.Background(), "app-a", "backend-a", grantA); err == nil {
		t.Fatal("revoked grant returned a token")
	}
	if _, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantB, "read"); err != nil {
		t.Fatalf("other app grant affected by revoke: %v", err)
	}
}

func TestProviderGrantRefreshIsSerialized(t *testing.T) {
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{"expired": {AccessToken: "expired", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}}}}
	app := New(Config{DataDir: t.TempDir(), AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(provider, "client", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.String("default_role").Default("user")
		s.Roles("roles")
		s.Boolean("disabled").Default(false)
	})
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := app.APIHandler(db)
	token, _ := registerProviderTestUser(t, handler, "refresh@example.com")
	state := startAppFlow(t, handler, "app", "link", token, "", "identity")
	completion := callbackProviderFlow(t, handler, "shared", state, "expired")
	response := completeProviderFlow(t, handler, completion, token, true)
	grantID := decodeProviderResponse(t, response)["grant"].(map[string]any)["id"].(string)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	provider.mu.Lock()
	refreshes := provider.refreshes
	provider.mu.Unlock()
	if refreshes != 1 {
		t.Fatalf("refreshes=%d want 1", refreshes)
	}
}

func TestProviderExchangeRejectsAllowedButUnrequestedScope(t *testing.T) {
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
		"escalated": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}},
	}}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	token, _ := registerProviderTestUser(t, handler, "exchange-scope@example.com")
	state := startAppFlow(t, handler, "app", "link", token, "", "identity")
	completion := callbackProviderFlow(t, handler, "shared", state, "escalated")
	response := completeProviderFlow(t, handler, completion, token, true)
	if response.Code != http.StatusBadRequest || decodeProviderResponse(t, response)["code"] != "provider_scope_invalid" {
		t.Fatalf("escalated exchange status=%d body=%s", response.Code, response.Body.String())
	}
	if db.Table(systemAuthProviderGrantTableName).Count() != 0 {
		t.Fatal("escalated exchange persisted a grant")
	}
}

func TestProviderRefreshRejectsScopeEscalation(t *testing.T) {
	provider := &fakeGrantProvider{
		issuer:        "https://issuer.example",
		refreshScopes: []string{"identity", "read"},
		tokens: map[string]AuthProviderTokenSet{
			"expired": {AccessToken: "expired", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	_, _, grantID, _ := linkGrantForTest(t, db, handler, "expired", "refresh-scope@example.com", "identity")
	before, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if _, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity"); err == nil {
		t.Fatal("scope-escalating refresh returned a token")
	}
	after, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if !scopeSubset(storedStrings(after["granted_scopes"]), []string{"identity"}) || scopeSubset([]string{"read"}, storedStrings(after["granted_scopes"])) {
		t.Fatalf("refresh persisted escalated scopes: %v", storedStrings(after["granted_scopes"]))
	}
	if toString(after["token_ciphertext"]) != toString(before["token_ciphertext"]) {
		t.Fatal("refresh persisted token material after scope escalation")
	}
}

func TestProviderRefreshAndUnlinkShareGrantSerialization(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	provider := &fakeGrantProvider{
		issuer:         "https://issuer.example",
		refreshStarted: started,
		refreshRelease: release,
		tokens: map[string]AuthProviderTokenSet{
			"expired": {AccessToken: "expired", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	_, principalID, grantID, identityID := linkGrantForTest(t, db, handler, "expired", "refresh-unlink@example.com", "identity")
	leaseDone := make(chan error, 1)
	go func() {
		_, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
		leaseDone <- err
	}()
	<-started
	unlinkDone := make(chan error, 1)
	go func() { unlinkDone <- db.providerAuth.unlink(principalID, identityID) }()
	select {
	case err := <-unlinkDone:
		t.Fatalf("unlink bypassed in-flight grant refresh: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-leaseDone; err != nil {
		t.Fatalf("serialized lease failed: %v", err)
	}
	if err := <-unlinkDone; err != nil {
		t.Fatalf("serialized unlink failed: %v", err)
	}
	if _, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity"); err == nil {
		t.Fatal("unlinked grant returned a token")
	}
}

func TestProviderRefreshAndIncrementalConsentShareGrantSerialization(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	provider := &fakeGrantProvider{
		issuer:         "https://issuer.example",
		refreshStarted: started,
		refreshRelease: release,
		tokens: map[string]AuthProviderTokenSet{
			"expired":     {AccessToken: "expired", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
			"incremental": {AccessToken: "incremental", RefreshToken: "incremental-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	token, _, grantID, _ := linkGrantForTest(t, db, handler, "expired", "refresh-consent@example.com", "identity")
	state := startAppFlow(t, handler, "app", "consent", token, grantID, "read")
	completion := callbackProviderFlow(t, handler, "shared", state, "incremental")
	leaseDone := make(chan error, 1)
	go func() {
		_, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
		leaseDone <- err
	}()
	<-started
	consentDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { consentDone <- completeProviderFlow(t, handler, completion, token, true) }()
	select {
	case response := <-consentDone:
		t.Fatalf("incremental consent bypassed in-flight grant refresh: status=%d body=%s", response.Code, response.Body.String())
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-leaseDone; err != nil {
		t.Fatalf("serialized lease failed: %v", err)
	}
	response := <-consentDone
	if response.Code != http.StatusOK {
		t.Fatalf("serialized consent status=%d body=%s", response.Code, response.Body.String())
	}
	lease, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "read")
	if err != nil || lease.AccessToken != "incremental" {
		t.Fatalf("incremental lease=%+v err=%v", lease, err)
	}
}

func TestProviderIncrementalConsentPreservesRefreshToken(t *testing.T) {
	provider := &fakeGrantProvider{
		issuer: "https://issuer.example",
		tokens: map[string]AuthProviderTokenSet{
			"initial":     {AccessToken: "initial", RefreshToken: "original-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
			"incremental": {AccessToken: "incremental", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity", "read"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	token, _, grantID, _ := linkGrantForTest(t, db, handler, "initial", "incremental-refresh@example.com", "identity")
	state := startAppFlow(t, handler, "app", "consent", token, grantID, "read")
	completion := callbackProviderFlow(t, handler, "shared", state, "incremental")
	if response := completeProviderFlow(t, handler, completion, token, true); response.Code != http.StatusOK {
		t.Fatalf("incremental consent status=%d body=%s", response.Code, response.Body.String())
	}
	lease, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "read")
	if err != nil || lease.AccessToken != "app-refreshed" {
		t.Fatalf("incremental refresh lease=%+v err=%v", lease, err)
	}
	provider.mu.Lock()
	refresh := provider.lastRefresh
	provider.mu.Unlock()
	if refresh.RefreshToken != "original-refresh" {
		t.Fatalf("refresh token=%q want original-refresh", refresh.RefreshToken)
	}
}

func TestProviderSameClientCredentialRotationUsesCurrentSecret(t *testing.T) {
	dataDir := t.TempDir()
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
		"expired": {AccessToken: "expired", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
	}}
	open := func(secret, version string) (*Database, http.Handler) {
		config := appGrantTestConfig(provider, "client", "backend")
		providerConfig := config.Providers["shared"]
		providerConfig.ClientSecret = secret
		providerConfig.CredentialVersion = version
		config.Providers["shared"] = providerConfig
		app := New(Config{DataDir: dataDir, AuthProviderApps: map[string]AuthProviderAppConfig{"app": config}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
		defineGrantTestUsers(app)
		db, err := app.Open()
		if err != nil {
			t.Fatal(err)
		}
		return db, app.APIHandler(db)
	}
	db, handler := open("old-secret", "old-version")
	_, _, grantID, _ := linkGrantForTest(t, db, handler, "expired", "secret-rotation@example.com", "identity")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, _ = open("new-secret", "new-version")
	defer db.Close()
	grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil {
		t.Fatalf("rotated grant=%#v err=%v", grant, err)
	}
	snapshotSecret, err := db.providerAuth.grantClientSecret(grant, "")
	if err != nil || snapshotSecret != "new-secret" {
		t.Fatalf("rotated credential snapshot=%q err=%v", snapshotSecret, err)
	}
	if _, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity"); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	refresh := provider.lastRefresh
	provider.mu.Unlock()
	if refresh.ClientID != "client" || refresh.ClientSecret != "new-secret" {
		t.Fatalf("refresh credentials client=%q secret=%q", refresh.ClientID, refresh.ClientSecret)
	}
}

func TestProviderClientRotationPreservesRevocationCredentials(t *testing.T) {
	for _, operation := range []string{"revoke", "unlink"} {
		t.Run(operation, func(t *testing.T) {
			dataDir := t.TempDir()
			provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
				"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
			}}
			db, handler := openGrantTestDatabase(t, dataDir, provider, "old-client")
			_, principalID, grantID, identityID := linkGrantForTest(t, db, handler, "code", operation+"-rotation@example.com", "identity")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, _ = openGrantTestDatabase(t, dataDir, provider, "new-client")
			defer db.Close()
			grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
			if err != nil || toString(grant["state"]) != "reconnect_required" {
				t.Fatalf("rotated grant=%#v err=%v", grant, err)
			}
			if operation == "revoke" {
				err = db.providerAuth.revokeGrant(context.Background(), principalID, grantID)
			} else {
				err = db.providerAuth.unlink(principalID, identityID)
			}
			if err != nil {
				t.Fatalf("%s after rotation: %v", operation, err)
			}
			provider.mu.Lock()
			revoke := provider.lastRevoke
			provider.mu.Unlock()
			if revoke.ClientID != "old-client" || revoke.ClientSecret != "old-client-secret" {
				t.Fatalf("revocation credentials client=%q secret=%q", revoke.ClientID, revoke.ClientSecret)
			}
		})
	}
}

func TestProviderClientRotationReconnectDoesNotRestoreOldRefreshToken(t *testing.T) {
	dataDir := t.TempDir()
	provider := &fakeGrantProvider{
		issuer:           "https://issuer.example",
		revokePreference: "refresh_token",
		tokens: map[string]AuthProviderTokenSet{
			"initial":   {AccessToken: "old-access", RefreshToken: "old-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
			"reconnect": {AccessToken: "new-access", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
		},
	}
	db, handler := openGrantTestDatabase(t, dataDir, provider, "old-client")
	_, principalID, grantID, _ := linkGrantForTest(t, db, handler, "initial", "client-rotation-reconnect@example.com", "identity")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, handler = openGrantTestDatabase(t, dataDir, provider, "new-client")
	defer db.Close()
	state := startAppFlow(t, handler, "app", "sign_in", "", "", "identity")
	completion := callbackProviderFlow(t, handler, "shared", state, "reconnect")
	response := completeProviderFlow(t, handler, completion, "", false)
	if response.Code != http.StatusOK {
		t.Fatalf("reconnect status=%d body=%s", response.Code, response.Body.String())
	}
	if reconnectedID := decodeProviderResponse(t, response)["grant"].(map[string]any)["id"].(string); reconnectedID != grantID {
		t.Fatalf("reconnect grant=%q want %q", reconnectedID, grantID)
	}

	grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil {
		t.Fatalf("reconnected grant=%#v err=%v", grant, err)
	}
	var tokens AuthProviderTokenSet
	if err := db.providerAuth.openProviderValue("grant", "app", toString(grant["registration_id"]), grantID, toString(grant["token_ciphertext"]), toString(grant["token_key_version"]), &tokens); err != nil {
		t.Fatal(err)
	}
	if tokens.RefreshToken != "" {
		t.Fatalf("reconnect retained refresh token %q", tokens.RefreshToken)
	}
	_, err = db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
	var providerErr *AuthProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "reconnect_required" {
		t.Fatalf("expired reconnect token error=%v", err)
	}
	provider.mu.Lock()
	refreshes := provider.refreshes
	provider.mu.Unlock()
	if refreshes != 0 {
		t.Fatalf("reconnect attempted %d refreshes with an old token", refreshes)
	}
	if err := db.providerAuth.revokeGrant(context.Background(), principalID, grantID); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	revoke := provider.lastRevoke
	provider.mu.Unlock()
	if revoke.Token != "new-access" || revoke.TokenTypeHint != "access_token" || revoke.ClientID != "new-client" || revoke.ClientSecret != "new-client-secret" {
		t.Fatalf("reconnect revocation=%+v", revoke)
	}
}

func TestProviderDatabaseFailsClosedWithoutMatchingKey(t *testing.T) {
	dataDir := t.TempDir()
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{"code": {AccessToken: "encrypted-access", RefreshToken: "encrypted-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}}}}
	config := Config{DataDir: dataDir, AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(provider, "client", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"}
	app := New(config)
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	handler := app.APIHandler(db)
	token, _ := registerProviderTestUser(t, handler, "keyring@example.com")
	state := startAppFlow(t, handler, "app", "link", token, "", "identity")
	completion := callbackProviderFlow(t, handler, "shared", state, "code")
	if response := completeProviderFlow(t, handler, completion, token, true); response.Code != http.StatusOK {
		t.Fatalf("completion status=%d body=%s", response.Code, response.Body.String())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	config.ProviderSecretKeys = map[string][]byte{"v2": []byte("abcdef0123456789abcdef0123456789")}
	config.ActiveProviderSecretKey = "v2"
	reopened := New(config)
	defineGrantTestUsers(reopened)
	if _, err := reopened.Open(); err == nil || !strings.Contains(err.Error(), "unavailable key version") {
		t.Fatalf("wrong-key open error=%v", err)
	}
}

func TestProviderRemoteRevocationRetriesWithoutRestoringAccess(t *testing.T) {
	provider := &fakeGrantProvider{issuer: "https://issuer.example", revokeErr: fmt.Errorf("temporary upstream failure"), tokens: map[string]AuthProviderTokenSet{"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}}}}
	app := New(Config{DataDir: t.TempDir(), AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(provider, "client", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := app.APIHandler(db)
	token, principalID := registerProviderTestUser(t, handler, "revoke@example.com")
	state := startAppFlow(t, handler, "app", "link", token, "", "identity")
	completion := callbackProviderFlow(t, handler, "shared", state, "code")
	response := completeProviderFlow(t, handler, completion, token, true)
	grantID := decodeProviderResponse(t, response)["grant"].(map[string]any)["id"].(string)
	err = db.providerAuth.revokeGrant(context.Background(), principalID, grantID)
	var providerErr *AuthProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "revocation_pending" {
		t.Fatalf("revoke error=%v", err)
	}
	if _, err := db.ProviderToken(context.Background(), "app", "backend", grantID); err == nil {
		t.Fatal("pending remote revocation restored local access")
	}
	retryRows := providerAllRows(db.Table(systemAuthProviderRevocationTableName))
	if len(retryRows) != 1 {
		t.Fatalf("retry rows=%d", len(retryRows))
	}
	_, _ = db.Table(systemAuthProviderRevocationTableName).Update(toString(retryRows[0]["id"]), map[string]any{"next_attempt_at": 0})
	provider.mu.Lock()
	provider.revokeErr = nil
	provider.mu.Unlock()
	completed, err := db.RetryProviderRevocations(context.Background())
	if err != nil || completed != 1 {
		t.Fatalf("retry completed=%d err=%v", completed, err)
	}
	grant, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if toString(grant["token_ciphertext"]) != "" || toString(grant["token_key_version"]) != "" ||
		toString(grant["client_id"]) != "" || toString(grant["credential_ciphertext"]) != "" || toString(grant["credential_key_version"]) != "" ||
		db.Table(systemAuthProviderRevocationTableName).Count() != 0 {
		t.Fatal("successful retry did not scrub provider token and credential material")
	}
}

func TestProviderImmediateRevocationScrubsGrantSecrets(t *testing.T) {
	provider := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
		"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
	}}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	_, principalID, grantID, _ := linkGrantForTest(t, db, handler, "code", "immediate-revoke@example.com", "identity")
	if err := db.providerAuth.revokeGrant(context.Background(), principalID, grantID); err != nil {
		t.Fatal(err)
	}
	grant, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if toString(grant["token_ciphertext"]) != "" || toString(grant["token_key_version"]) != "" ||
		toString(grant["client_id"]) != "" || toString(grant["credential_ciphertext"]) != "" || toString(grant["credential_key_version"]) != "" {
		t.Fatal("immediate revocation did not scrub provider token and credential material")
	}
}

func TestProviderTerminalRevocationScrubsGrantSecrets(t *testing.T) {
	provider := &fakeGrantProvider{
		issuer:    "https://issuer.example",
		revokeErr: &AuthProviderUpstreamError{Code: "invalid_token", Terminal: true},
		tokens: map[string]AuthProviderTokenSet{
			"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	_, principalID, grantID, _ := linkGrantForTest(t, db, handler, "code", "terminal-revoke@example.com", "identity")
	if err := db.providerAuth.revokeGrant(context.Background(), principalID, grantID); err != nil {
		t.Fatal(err)
	}
	grant, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if toString(grant["token_ciphertext"]) != "" || toString(grant["token_key_version"]) != "" ||
		toString(grant["client_id"]) != "" || toString(grant["credential_ciphertext"]) != "" || toString(grant["credential_key_version"]) != "" {
		t.Fatal("terminal revocation did not scrub provider token and credential material")
	}
}

func TestProviderOverlappingRevocationsCallUpstreamOnce(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	provider := &fakeGrantProvider{
		issuer:        "https://issuer.example",
		revokeStarted: started,
		revokeRelease: release,
		tokens: map[string]AuthProviderTokenSet{
			"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	_, principalID, grantID, _ := linkGrantForTest(t, db, handler, "code", "overlapping-revoke@example.com", "identity")
	revokeDone := make(chan error, 1)
	go func() { revokeDone <- db.providerAuth.revokeGrant(context.Background(), principalID, grantID) }()
	<-started
	retryRows := providerAllRows(db.Table(systemAuthProviderRevocationTableName))
	if len(retryRows) != 1 {
		t.Fatalf("retry rows=%d", len(retryRows))
	}
	retryID := toString(retryRows[0]["id"])
	staleGrantID := toString(retryRows[0]["grant_id"])
	retryDone := make(chan error, 1)
	go func() {
		retryDone <- db.providerAuth.attemptRevocationForGrant(context.Background(), retryID, staleGrantID)
	}()
	close(release)
	if err := <-revokeDone; err != nil {
		t.Fatalf("immediate revocation: %v", err)
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("overlapping retry: %v", err)
	}
	provider.mu.Lock()
	revocations := provider.revocations
	provider.mu.Unlock()
	if revocations != 1 {
		t.Fatalf("upstream revocations=%d want 1", revocations)
	}
}
