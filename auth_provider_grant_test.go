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
	mu          sync.Mutex
	issuer      string
	tokens      map[string]AuthProviderTokenSet
	refreshes   int
	revocations int
	revokeErr   error
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
	defer p.mu.Unlock()
	p.refreshes++
	return AuthProviderTokenSet{AccessToken: request.AppID + "-refreshed", RefreshToken: request.RefreshToken + "-rotated", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: request.Scopes}, nil
}
func (p *fakeGrantProvider) RevokeGrant(_ context.Context, _ AuthProviderRevokeRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revocations++
	return p.revokeErr
}

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
	if toString(grant["token_ciphertext"]) != "" || db.Table(systemAuthProviderRevocationTableName).Count() != 0 {
		t.Fatal("successful retry did not scrub revocation token material")
	}
}
