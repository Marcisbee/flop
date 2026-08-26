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
	refreshErr       error
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

type failingProviderRandom struct{}

func (failingProviderRandom) Read([]byte) (int, error) {
	return 0, fmt.Errorf("provider random unavailable")
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
	started, release, refreshErr := p.refreshStarted, p.refreshRelease, p.refreshErr
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
	if refreshErr != nil {
		return AuthProviderTokenSet{}, refreshErr
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

func identityOnlyAppConfig(adapter AuthProviderAdapter, clientID, clientSecret, credential string) AuthProviderAppConfig {
	return AuthProviderAppConfig{
		AllowedReturnURLs:  []string{"https://client.example/done"},
		BackendCredentials: []string{credential},
		Providers: map[string]AuthProviderConfig{"shared": {
			Adapter: adapter, Issuer: "https://issuer.example",
			RedirectURI: "https://flop.example/api/auth/provider/callback?provider=shared",
			ClientID:    clientID, ClientSecret: clientSecret,
		}},
	}
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

func TestIdentityOnlyProviderGrantsResolveForMatchingBackend(t *testing.T) {
	const (
		subject       = "identity-only-subject-secret"
		backendA      = "backend-a-secret"
		backendB      = "backend-b-secret"
		clientSecretA = "provider-client-a-secret"
		clientSecretB = "provider-client-b-secret"
	)
	provider := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"app-a":  {Provider: "shared", Issuer: "https://issuer.example", Subject: subject, DisplayName: "Identity Person", ProfileHandle: " identity-person ", ProfileURL: " https://profiles.example/identity-person "},
		"app-b":  {Provider: "shared", Issuer: "https://issuer.example", Subject: subject, DisplayName: "Identity Person", ProfileHandle: "identity-person", ProfileURL: "https://profiles.example/identity-person"},
		"app-a2": {Provider: "shared", Issuer: "https://issuer.example", Subject: subject, DisplayName: "Identity Person", ProfileHandle: "identity-person", ProfileURL: "https://profiles.example/identity-person"},
	}}
	if _, tokenCapable := any(provider).(AuthProviderGrantAdapter); tokenCapable {
		t.Fatal("identity-only fixture unexpectedly implements token grant capability")
	}
	app := New(Config{
		DataDir: t.TempDir(), SyncMode: "normal",
		AuthProviderApps: map[string]AuthProviderAppConfig{
			"app-a": identityOnlyAppConfig(provider, "client-a", clientSecretA, backendA),
			"app-b": identityOnlyAppConfig(provider, "client-b", clientSecretB, backendB),
		},
		ProviderSecretKeys:      map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")},
		ActiveProviderSecretKey: "v1",
	})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := app.APIHandler(db)

	assertBrowserSafe := func(name string, rec *httptest.ResponseRecorder) {
		t.Helper()
		for _, secret := range []string{subject, backendA, backendB, clientSecretA, clientSecretB, "upstream-access-token", "upstream-refresh-token"} {
			if strings.Contains(rec.Body.String(), secret) || strings.Contains(rec.Header().Get("Location"), secret) {
				t.Fatalf("%s exposed %q: headers=%v body=%s", name, secret, rec.Header(), rec.Body.String())
			}
		}
	}
	discovery := providerRequest(t, handler, http.MethodGet, "/api/auth/providers?appID=app-a", "", "")
	assertBrowserSafe("discovery", discovery)

	passwordToken, principalID := registerProviderTestUser(t, handler, "identity-only@example.com")
	stateA := startAppFlow(t, handler, "app-a", "link", passwordToken, "")
	callbackA := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?provider=shared&state="+url.QueryEscape(stateA)+"&code=app-a", "", "")
	if callbackA.Code != http.StatusOK {
		t.Fatalf("app A callback status=%d body=%s", callbackA.Code, callbackA.Body.String())
	}
	assertBrowserSafe("callback", callbackA)
	completionA := toString(decodeProviderResponse(t, callbackA)["completionCode"])
	linked := completeProviderFlow(t, handler, completionA, passwordToken, true)
	if linked.Code != http.StatusOK {
		t.Fatalf("app A completion status=%d body=%s", linked.Code, linked.Body.String())
	}
	assertBrowserSafe("link completion", linked)
	grantA := toString(decodeProviderResponse(t, linked)["grant"].(map[string]any)["id"])

	stateB := startAppFlow(t, handler, "app-b", "sign_in", "", "")
	callbackB := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?provider=shared&state="+url.QueryEscape(stateB)+"&code=app-b", "", "")
	if callbackB.Code != http.StatusOK {
		t.Fatalf("app B callback status=%d body=%s", callbackB.Code, callbackB.Body.String())
	}
	assertBrowserSafe("sign-in callback", callbackB)
	completionB := toString(decodeProviderResponse(t, callbackB)["completionCode"])
	signedIn := completeProviderFlow(t, handler, completionB, "", false)
	if signedIn.Code != http.StatusOK {
		t.Fatalf("app B completion status=%d body=%s", signedIn.Code, signedIn.Body.String())
	}
	assertBrowserSafe("sign-in completion", signedIn)
	grantB := toString(decodeProviderResponse(t, signedIn)["grant"].(map[string]any)["id"])

	if got := db.Table(systemAuthIdentityTableName).Count(); got != 1 {
		t.Fatalf("identities=%d want 1", got)
	}
	if got := db.Table(systemAuthProviderGrantTableName).Count(); got != 2 {
		t.Fatalf("grants=%d want 2", got)
	}
	rowA, _ := db.Table(systemAuthProviderGrantTableName).Get(grantA)
	rowB, _ := db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if grantA == grantB || toString(rowA["registration_id"]) == toString(rowB["registration_id"]) ||
		toString(rowA["app_id"]) != "app-a" || toString(rowB["app_id"]) != "app-b" {
		t.Fatalf("identity-only grants were not app isolated: A=%#v B=%#v", rowA, rowB)
	}
	for _, row := range []map[string]any{rowA, rowB} {
		if toString(row["principal_id"]) != principalID || len(storedStrings(row["granted_scopes"])) != 0 ||
			toString(row["token_ciphertext"]) != "" || toString(row["token_key_version"]) != "" ||
			toString(row["credential_ciphertext"]) != "" || toString(row["credential_key_version"]) != "" ||
			toString(row["client_id"]) != "" || toString(row["state"]) != "active" {
			t.Fatalf("unsafe identity-only grant row: %#v", row)
		}
	}

	resolvedA, err := db.ProviderIdentity(context.Background(), "app-a", backendA, grantA)
	if err != nil || resolvedA.GrantID != grantA || resolvedA.AppID != "app-a" || resolvedA.Provider != "shared" || resolvedA.Issuer != "https://issuer.example" || resolvedA.Subject != subject || resolvedA.ProfileHandle != "identity-person" || resolvedA.ProfileURL != "https://profiles.example/identity-person" {
		t.Fatalf("app A identity=%+v err=%v", resolvedA, err)
	}
	resolvedB, err := db.ProviderIdentity(context.Background(), "app-b", backendB, grantB)
	if err != nil || resolvedB.Subject != subject || resolvedB.ProfileHandle != "identity-person" || resolvedB.ProfileURL != "https://profiles.example/identity-person" {
		t.Fatalf("app B identity=%+v err=%v", resolvedB, err)
	}
	if _, err := db.ProviderToken(context.Background(), "app-b", backendB, grantB); err == nil {
		t.Fatal("identity-only grant returned a provider token")
	}
	rowB, _ = db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if toString(rowB["state"]) != "active" {
		t.Fatalf("token lookup changed identity-only grant state to %q", rowB["state"])
	}

	resolveHTTP := func(appID, grantID, credential, authorization string, extra bool) *httptest.ResponseRecorder {
		t.Helper()
		body := map[string]any{"appID": appID, "grantId": grantID}
		if extra {
			body["unexpected"] = true
		}
		encoded, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/api/auth/provider/backend/identity", strings.NewReader(string(encoded)))
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("X-Flop-Backend-Credential", credential)
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("identity response cache control=%q", rec.Header().Get("Cache-Control"))
		}
		return rec
	}
	if rec := resolveHTTP("app-a", grantA, backendA, "", false); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), subject) || !strings.Contains(rec.Body.String(), "https://profiles.example/identity-person") || strings.Contains(rec.Body.String(), principalID) {
		t.Fatalf("app A backend identity status=%d body=%s", rec.Code, rec.Body.String())
	} else {
		payload := decodeProviderResponse(t, rec)
		for _, forbidden := range []string{"principalId", "principalID", "identityId", "identityID", "email", "credential", "accessToken", "refreshToken", "token"} {
			if _, exists := payload[forbidden]; exists {
				t.Fatalf("app A backend identity exposed %s: %#v", forbidden, payload)
			}
		}
	}
	if rec := resolveHTTP("app-b", grantB, "", "Bearer "+backendB, false); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), subject) {
		t.Fatalf("app B bearer identity status=%d body=%s", rec.Code, rec.Body.String())
	}
	for name, rec := range map[string]*httptest.ResponseRecorder{
		"missing credential": resolveHTTP("app-a", grantA, "", "", false),
		"wrong credential":   resolveHTTP("app-a", grantA, "wrong", "", false),
		"other credential":   resolveHTTP("app-a", grantA, backendB, "", false),
		"other app grant":    resolveHTTP("app-b", grantA, backendB, "", false),
		"unknown grant":      resolveHTTP("app-a", "unknown", backendA, "", false),
		"unknown JSON field": resolveHTTP("app-a", grantA, backendA, "", true),
	} {
		if rec.Code >= 200 && rec.Code < 300 {
			t.Fatalf("%s unexpectedly succeeded: %s", name, rec.Body.String())
		}
		assertBrowserSafe(name, rec)
	}
	if rec := resolveHTTP("app-a", grantA, "wrong", "", false); rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), "backend_unauthorized") {
		t.Fatalf("wrong credential did not fail before grant lookup: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := resolveHTTP("app-a", "unknown", backendA, "", false); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "grant_not_found") {
		t.Fatalf("authenticated unknown grant status=%d body=%s", rec.Code, rec.Body.String())
	}

	registrationA, _ := db.Table(systemAuthProviderRegistrationTableName).Get(toString(rowA["registration_id"]))
	if _, err := db.Table(systemAuthProviderRegistrationTableName).Update(toString(registrationA["id"]), map[string]any{"enabled": false}); err != nil {
		t.Fatal(err)
	}
	if rec := resolveHTTP("app-a", grantA, backendA, "", false); rec.Code < 400 {
		t.Fatalf("disabled registration resolved identity: %s", rec.Body.String())
	}
	_, _ = db.Table(systemAuthProviderRegistrationTableName).Update(toString(registrationA["id"]), map[string]any{"enabled": true})
	appA, _ := db.Table(systemAuthProviderAppTableName).FindByUniqueIndex("app_id", "app-a")
	_, _ = db.Table(systemAuthProviderAppTableName).Update(toString(appA["id"]), map[string]any{"enabled": false})
	if rec := resolveHTTP("app-a", grantA, backendA, "", false); rec.Code < 400 {
		t.Fatalf("disabled app resolved identity: %s", rec.Body.String())
	}
	_, _ = db.Table(systemAuthProviderAppTableName).Update(toString(appA["id"]), map[string]any{"enabled": true})
	identity, _ := db.Table(systemAuthIdentityTableName).Get(toString(rowA["identity_id"]))
	_, _ = db.Table(systemAuthIdentityTableName).Update(toString(identity["id"]), map[string]any{"principal_id": "inconsistent-principal"})
	if _, err := db.ProviderIdentity(context.Background(), "app-a", backendA, grantA); err == nil {
		t.Fatal("identity ownership mismatch resolved")
	}
	_, _ = db.Table(systemAuthIdentityTableName).Update(toString(identity["id"]), map[string]any{"principal_id": principalID, "issuer": "https://wrong-issuer.example"})
	if _, err := db.ProviderIdentity(context.Background(), "app-a", backendA, grantA); err == nil {
		t.Fatal("identity issuer mismatch resolved")
	}
	_, _ = db.Table(systemAuthIdentityTableName).Update(toString(identity["id"]), map[string]any{"issuer": "https://issuer.example"})
	_, _ = db.Table(systemAuthProviderRegistrationTableName).Update(toString(registrationA["id"]), map[string]any{"provider": "wrong-provider"})
	if _, err := db.ProviderIdentity(context.Background(), "app-a", backendA, grantA); err == nil {
		t.Fatal("registration provider mismatch resolved")
	}
	_, _ = db.Table(systemAuthProviderRegistrationTableName).Update(toString(registrationA["id"]), map[string]any{"provider": "shared"})

	if err := db.providerAuth.revokeGrant(context.Background(), principalID, grantA); err != nil {
		t.Fatal(err)
	}
	if db.Table(systemAuthProviderRevocationTableName).Count() != 0 {
		t.Fatal("identity-only revoke created a remote revocation retry")
	}
	if _, err := db.ProviderIdentity(context.Background(), "app-a", backendA, grantA); err == nil {
		t.Fatal("revoked identity-only grant resolved")
	}
	if _, err := db.ProviderIdentity(context.Background(), "app-b", backendB, grantB); err != nil {
		t.Fatalf("app A revocation affected app B grant: %v", err)
	}

	stateA = startAppFlow(t, handler, "app-a", "sign_in", "", "")
	completionA = callbackProviderFlow(t, handler, "shared", stateA, "app-a2")
	reauthorized := completeProviderFlow(t, handler, completionA, "", false)
	if reauthorized.Code != http.StatusOK || toString(decodeProviderResponse(t, reauthorized)["grant"].(map[string]any)["id"]) != grantA {
		t.Fatalf("identity-only reauthorization status=%d body=%s", reauthorized.Code, reauthorized.Body.String())
	}
	identityID := toString(rowA["identity_id"])
	if err := db.providerAuth.unlink(principalID, identityID); err != nil {
		t.Fatal(err)
	}
	if db.Table(systemAuthProviderRevocationTableName).Count() != 0 || db.Table(systemAuthIdentityTableName).Count() != 0 {
		t.Fatal("identity-only unlink retained identity or created revocation retry")
	}
	for _, grantID := range []string{grantA, grantB} {
		row, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
		if toString(row["state"]) != "revoked" || toString(row["token_ciphertext"]) != "" {
			t.Fatalf("unlinked identity-only grant remained usable: %#v", row)
		}
	}
	for _, session := range providerAllRows(db.Table(systemAuthSessionTableName)) {
		if toString(session["auth_identity_id"]) == identityID && providerUnix(session["revoked_at"]) == 0 {
			t.Fatalf("provider-derived session survived identity unlink: %#v", session)
		}
	}

	identities := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/identities", "", passwordToken)
	grants := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/grants", "", passwordToken)
	assertBrowserSafe("identity list", identities)
	assertBrowserSafe("grant list", grants)
	schemaResponse := providerRequest(t, handler, http.MethodGet, "/api/schema", "", "")
	if !strings.Contains(schemaResponse.Body.String(), "auth_provider_backend_identity") {
		t.Fatalf("generated schema omitted backend identity route: %s", schemaResponse.Body.String())
	}
	assertBrowserSafe("schema", schemaResponse)
}

func TestProviderGrantsAreIsolatedByApp(t *testing.T) {
	providerA := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{
		"a":           {AccessToken: "app-a-token", RefreshToken: "app-a-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}},
		"incremental": {AccessToken: "app-a-youtube", RefreshToken: "app-a-youtube-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read", GoogleScopeYouTubeReadonly}},
	}}
	providerB := &fakeGrantProvider{issuer: "https://issuer.example", tokens: map[string]AuthProviderTokenSet{"b": {AccessToken: "app-b-token", RefreshToken: "app-b-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}}}}
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
	sessionsBeforeLinks := db.Table(systemAuthSessionTableName).Count()
	stateA := startAppFlow(t, handler, "app-a", "link", passwordToken, "", "identity", "read")
	completionA := callbackProviderFlow(t, handler, "shared", stateA, "a")
	linked := completeProviderFlow(t, handler, completionA, passwordToken, true)
	if linked.Code != 200 {
		t.Fatalf("link status=%d body=%s", linked.Code, linked.Body.String())
	}
	if strings.Contains(linked.Body.String(), "app-a-token") || strings.Contains(linked.Body.String(), "app-a-refresh") {
		t.Fatalf("browser completion exposed provider tokens: %s", linked.Body.String())
	}
	linkedOut := decodeProviderResponse(t, linked)
	if linkedOut["token"] != nil || linkedOut["refreshToken"] != nil || linkedOut["user"] != nil || linkedOut["me"] != nil {
		t.Fatalf("link completion exposed a Flop session: %#v", linkedOut)
	}
	grantA := decodeProviderResponse(t, linked)["grant"].(map[string]any)["id"].(string)
	stateB := startAppFlow(t, handler, "app-b", "link", passwordToken, "", "identity")
	completionB := callbackProviderFlow(t, handler, "shared", stateB, "b")
	linkedAgain := completeProviderFlow(t, handler, completionB, passwordToken, true)
	if linkedAgain.Code != 200 {
		t.Fatalf("same-principal link status=%d body=%s", linkedAgain.Code, linkedAgain.Body.String())
	}
	linkedAgainOut := decodeProviderResponse(t, linkedAgain)
	if linkedAgainOut["token"] != nil || linkedAgainOut["refreshToken"] != nil || linkedAgainOut["user"] != nil || linkedAgainOut["me"] != nil ||
		strings.Contains(linkedAgain.Body.String(), "app-b-token") || strings.Contains(linkedAgain.Body.String(), "app-b-refresh") {
		t.Fatalf("same-principal link exposed Flop or provider tokens: %#v", linkedAgainOut)
	}
	grantB := linkedAgainOut["grant"].(map[string]any)["id"].(string)
	if db.Table(systemAuthIdentityTableName).Count() != 1 {
		t.Fatalf("identities=%d want 1", db.Table(systemAuthIdentityTableName).Count())
	}
	if db.Table(systemAuthProviderGrantTableName).Count() != 2 {
		t.Fatalf("grants=%d want 2", db.Table(systemAuthProviderGrantTableName).Count())
	}
	if got := db.Table(systemAuthSessionTableName).Count(); got != sessionsBeforeLinks {
		t.Fatalf("linking another app created or switched a Flop session: before=%d after=%d", sessionsBeforeLinks, got)
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
	if _, err := db.ProviderToken(context.Background(), "app-a", "backend-b", grantA, "read"); err == nil {
		t.Fatal("another app's backend credential was accepted")
	}
	if lease, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantB, "identity"); err != nil || lease.AccessToken != "app-b-token" {
		t.Fatalf("app B lease=%+v err=%v", lease, err)
	}
	if _, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantB, "read"); err == nil {
		t.Fatal("app B grant exceeded its isolated scopes")
	}

	grantBCiphertext := toString(rowB["token_ciphertext"])
	rejectedState := startAppFlow(t, handler, "app-b", "link", passwordToken, "", "identity")
	rejectedCompletion := callbackProviderFlow(t, handler, "shared", rejectedState, "b")
	notConfirmed := completeProviderFlow(t, handler, rejectedCompletion, passwordToken, false)
	if notConfirmed.Code != http.StatusBadRequest || decodeProviderResponse(t, notConfirmed)["code"] != "confirmation_required" {
		t.Fatalf("unconfirmed link status=%d body=%s", notConfirmed.Code, notConfirmed.Body.String())
	}
	if got := db.Table(systemAuthProviderGrantTableName).Count(); got != 2 {
		t.Fatalf("unconfirmed link mutated grants: %d", got)
	}
	rowB, _ = db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if toString(rowB["token_ciphertext"]) != grantBCiphertext {
		t.Fatal("unconfirmed link replaced app B credentials")
	}

	login := providerRequest(t, handler, http.MethodPost, "/api/auth/password", `{"email":"grants@example.com","password":"password123"}`, "")
	if login.Code != http.StatusOK {
		t.Fatalf("second session login status=%d body=%s", login.Code, login.Body.String())
	}
	changedSessionToken, _ := decodeProviderResponse(t, login)["token"].(string)
	sessionsBeforeRejectedComplete := db.Table(systemAuthSessionTableName).Count()
	changedSession := completeProviderFlow(t, handler, rejectedCompletion, changedSessionToken, true)
	if changedSession.Code != http.StatusUnauthorized || decodeProviderResponse(t, changedSession)["code"] != "link_session_changed" {
		t.Fatalf("changed-session link status=%d body=%s", changedSession.Code, changedSession.Body.String())
	}
	rowB, _ = db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if db.Table(systemAuthProviderGrantTableName).Count() != 2 || toString(rowB["token_ciphertext"]) != grantBCiphertext {
		t.Fatal("changed-session link mutated grants")
	}
	if got := db.Table(systemAuthSessionTableName).Count(); got != sessionsBeforeRejectedComplete {
		t.Fatalf("changed-session rejection mutated sessions: before=%d after=%d", sessionsBeforeRejectedComplete, got)
	}

	otherToken, _ := registerProviderTestUser(t, handler, "other-grants@example.com")
	collisionState := startAppFlow(t, handler, "app-b", "link", otherToken, "", "identity")
	collisionCompletion := callbackProviderFlow(t, handler, "shared", collisionState, "b")
	collision := completeProviderFlow(t, handler, collisionCompletion, otherToken, true)
	if collision.Code != http.StatusConflict || decodeProviderResponse(t, collision)["code"] != "identity_already_linked" {
		t.Fatalf("different-principal collision status=%d body=%s", collision.Code, collision.Body.String())
	}
	rowB, _ = db.Table(systemAuthProviderGrantTableName).Get(grantB)
	if db.Table(systemAuthProviderGrantTableName).Count() != 2 || toString(rowB["token_ciphertext"]) != grantBCiphertext {
		t.Fatal("different-principal collision mutated grants")
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
	if _, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantB, "identity"); err != nil {
		t.Fatalf("other app grant affected by revoke: %v", err)
	}
}

func TestReproductionProviderGrantReauthorizationAfterRevocation(t *testing.T) {
	for _, test := range []struct {
		name              string
		pendingRevocation bool
		expectSameGrantID bool
	}{
		{name: "completed revocation", expectSameGrantID: true},
		{name: "pending revocation", pendingRevocation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			providerA := &fakeGrantProvider{
				issuer: "https://issuer.example",
				tokens: map[string]AuthProviderTokenSet{
					"old": {AccessToken: "old-access-secret", RefreshToken: "old-refresh-secret", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
					"new": {AccessToken: "new-access-secret", RefreshToken: "new-refresh-secret", TokenType: "Bearer", Scopes: []string{"identity", "read"}},
				},
			}
			if test.pendingRevocation {
				providerA.revokeErr = fmt.Errorf("temporary upstream failure")
			}
			providerB := &fakeGrantProvider{
				issuer: "https://issuer.example",
				tokens: map[string]AuthProviderTokenSet{
					"app-b": {AccessToken: "app-b-access-secret", RefreshToken: "app-b-refresh-secret", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
				},
			}
			app := New(Config{
				DataDir: t.TempDir(), SyncMode: "normal",
				AuthProviderApps: map[string]AuthProviderAppConfig{
					"app-a": appGrantTestConfig(providerA, "client-a", "backend-a"),
					"app-b": appGrantTestConfig(providerB, "client-b", "backend-b"),
				},
				ProviderSecretKeys:      map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")},
				ActiveProviderSecretKey: "v1",
			})
			defineGrantTestUsers(app)
			db, err := app.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			handler := app.APIHandler(db)

			bearer, principalID := registerProviderTestUser(t, handler, test.name+"@example.com")
			stateA := startAppFlow(t, handler, "app-a", "link", bearer, "", "identity")
			completionA := callbackProviderFlow(t, handler, "shared", stateA, "old")
			linkedA := completeProviderFlow(t, handler, completionA, bearer, true)
			if linkedA.Code != http.StatusOK {
				t.Fatalf("initial app A link status=%d body=%s", linkedA.Code, linkedA.Body.String())
			}
			oldGrantID := toString(decodeProviderResponse(t, linkedA)["grant"].(map[string]any)["id"])
			oldGrant, _ := db.Table(systemAuthProviderGrantTableName).Get(oldGrantID)
			identityID := toString(oldGrant["identity_id"])
			registrationA := toString(oldGrant["registration_id"])
			if _, err := db.Table(systemAuthProviderGrantTableName).Update(oldGrantID, map[string]any{"refreshed_at": time.Now().Add(-time.Minute).Unix()}); err != nil {
				t.Fatal(err)
			}
			if !test.pendingRevocation {
				oldTokens := providerA.tokens["old"]
				oldTokens.ExpiresAt = time.Now().Add(time.Second)
				oldCiphertext, oldVersion, err := db.providerAuth.sealProviderValue("grant", "app-a", registrationA, oldGrantID, oldTokens)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Table(systemAuthProviderGrantTableName).Update(oldGrantID, map[string]any{
					"token_ciphertext": oldCiphertext, "token_key_version": oldVersion, "access_expires_at": oldTokens.ExpiresAt.Unix(),
				}); err != nil {
					t.Fatal(err)
				}
				providerA.mu.Lock()
				providerA.refreshErr = &AuthProviderUpstreamError{Code: "invalid_grant", Terminal: true}
				providerA.mu.Unlock()
				lease, err := db.ProviderToken(context.Background(), "app-a", "backend-a", oldGrantID, "identity")
				var providerErr *AuthProviderError
				if lease != nil || !errors.As(err, &providerErr) || providerErr.Code != "reconnect_required" {
					t.Fatalf("terminal refresh lease=%+v err=%v", lease, err)
				}
				if _, unusable := db.providerAuth.unusableGrants.Load(oldGrantID); !unusable {
					t.Fatal("terminal refresh did not tombstone old grant")
				}
			}

			stateB := startAppFlow(t, handler, "app-b", "link", bearer, "", "identity")
			completionB := callbackProviderFlow(t, handler, "shared", stateB, "app-b")
			linkedB := completeProviderFlow(t, handler, completionB, bearer, true)
			if linkedB.Code != http.StatusOK {
				t.Fatalf("app B link status=%d body=%s", linkedB.Code, linkedB.Body.String())
			}
			grantB := toString(decodeProviderResponse(t, linkedB)["grant"].(map[string]any)["id"])

			revokeErr := db.providerAuth.revokeGrant(context.Background(), principalID, oldGrantID)
			if test.pendingRevocation {
				var providerErr *AuthProviderError
				if !errors.As(revokeErr, &providerErr) || providerErr.Code != "revocation_pending" {
					t.Fatalf("pending revoke error=%v", revokeErr)
				}
			} else if revokeErr != nil {
				t.Fatal(revokeErr)
			}
			oldLifecycle, _ := db.Table(systemAuthProviderGrantTableName).Get(oldGrantID)
			oldCiphertext := toString(oldLifecycle["token_ciphertext"])
			oldRetryCount := db.Table(systemAuthProviderRevocationTableName).Count()
			if test.pendingRevocation {
				retries := providerAllRows(db.Table(systemAuthProviderRevocationTableName))
				if len(retries) != 1 {
					t.Fatalf("pending retry rows=%#v", retries)
				}
				// Simulate a retry written before the optional provider snapshot existed.
				if _, err := db.Table(systemAuthProviderRevocationTableName).Update(toString(retries[0]["id"]), map[string]any{"provider": ""}); err != nil {
					t.Fatal(err)
				}
			}

			reauthState := startAppFlow(t, handler, "app-a", "link", bearer, "", "identity", "read")
			reauthCompletion := callbackProviderFlow(t, handler, "shared", reauthState, "new")
			notConfirmed := completeProviderFlow(t, handler, reauthCompletion, bearer, false)
			if notConfirmed.Code != http.StatusBadRequest || decodeProviderResponse(t, notConfirmed)["code"] != "confirmation_required" {
				t.Fatalf("unconfirmed reauthorization status=%d body=%s", notConfirmed.Code, notConfirmed.Body.String())
			}
			unchanged, _ := db.Table(systemAuthProviderGrantTableName).Get(oldGrantID)
			if toString(unchanged["state"]) != toString(oldLifecycle["state"]) ||
				toString(unchanged["token_ciphertext"]) != oldCiphertext ||
				db.Table(systemAuthProviderRevocationTableName).Count() != oldRetryCount {
				t.Fatalf("unconfirmed reauthorization changed old lifecycle: before=%#v after=%#v", oldLifecycle, unchanged)
			}

			login := providerRequest(t, handler, http.MethodPost, "/api/auth/password", `{"email":`+fmt.Sprintf("%q", test.name+"@example.com")+`,"password":"password123"}`, "")
			changedBearer := toString(decodeProviderResponse(t, login)["token"])
			changedSession := completeProviderFlow(t, handler, reauthCompletion, changedBearer, true)
			if changedSession.Code != http.StatusUnauthorized || decodeProviderResponse(t, changedSession)["code"] != "link_session_changed" {
				t.Fatalf("changed-session reauthorization status=%d body=%s", changedSession.Code, changedSession.Body.String())
			}
			if test.pendingRevocation {
				random := db.providerAuth.random
				db.providerAuth.random = failingProviderRandom{}
				failed := completeProviderFlow(t, handler, reauthCompletion, bearer, true)
				db.providerAuth.random = random
				if failed.Code != http.StatusInternalServerError || decodeProviderResponse(t, failed)["code"] != "provider_grant_failed" {
					t.Fatalf("failed pending reauthorization status=%d body=%s", failed.Code, failed.Body.String())
				}
				rolledBack, err := db.Table(systemAuthProviderGrantTableName).Get(oldGrantID)
				retries := providerAllRows(db.Table(systemAuthProviderRevocationTableName))
				if err != nil || rolledBack == nil || toString(rolledBack["state"]) != "revoked" ||
					len(retries) != 1 || toString(retries[0]["grant_id"]) != oldGrantID || toString(retries[0]["provider"]) != "" {
					t.Fatalf("failed detachment did not roll back: grant=%#v retries=%#v err=%v", rolledBack, retries, err)
				}
			}

			reauthorized := completeProviderFlow(t, handler, reauthCompletion, bearer, true)
			if reauthorized.Code != http.StatusOK {
				t.Fatalf("reauthorization status=%d body=%s", reauthorized.Code, reauthorized.Body.String())
			}
			for _, secret := range []string{"new-access-secret", "new-refresh-secret", "old-access-secret", "old-refresh-secret"} {
				if strings.Contains(reauthorized.Body.String(), secret) {
					t.Fatalf("browser response exposed %q: %s", secret, reauthorized.Body.String())
				}
			}
			newGrantID := toString(decodeProviderResponse(t, reauthorized)["grant"].(map[string]any)["id"])
			if (newGrantID == oldGrantID) != test.expectSameGrantID {
				t.Fatalf("reauthorized grant id=%q old=%q expectSame=%t", newGrantID, oldGrantID, test.expectSameGrantID)
			}
			newGrant, _ := db.Table(systemAuthProviderGrantTableName).Get(newGrantID)
			newScopes := storedStrings(newGrant["granted_scopes"])
			if toString(newGrant["principal_id"]) != principalID || toString(newGrant["identity_id"]) != identityID ||
				toString(newGrant["registration_id"]) != registrationA || toString(newGrant["app_id"]) != "app-a" ||
				toString(newGrant["provider"]) != "shared" || toString(newGrant["state"]) != "active" ||
				providerUnix(newGrant["revoked_at"]) != 0 || providerUnix(newGrant["refreshed_at"]) != 0 ||
				providerUnix(newGrant["access_expires_at"]) != 0 || len(newScopes) != 2 || !scopeSubset([]string{"identity", "read"}, newScopes) {
				t.Fatalf("unsafe reauthorized grant: %#v", newGrant)
			}
			var newTokens AuthProviderTokenSet
			if err := db.providerAuth.openProviderValue("grant", "app-a", registrationA, newGrantID, toString(newGrant["token_ciphertext"]), toString(newGrant["token_key_version"]), &newTokens); err != nil {
				t.Fatal(err)
			}
			if newTokens.AccessToken != "new-access-secret" || newTokens.RefreshToken != "new-refresh-secret" {
				t.Fatalf("reauthorized tokens=%+v", newTokens)
			}
			lease, err := db.ProviderToken(context.Background(), "app-a", "backend-a", newGrantID, "read")
			if err != nil || lease.AccessToken != "new-access-secret" {
				t.Fatalf("reauthorized lease=%+v err=%v", lease, err)
			}
			if _, err := db.ProviderToken(context.Background(), "app-b", "backend-b", newGrantID, "identity"); err == nil {
				t.Fatal("app B resolved app A's reauthorized grant")
			}
			if _, err := db.ProviderToken(context.Background(), "app-a", "backend-a", grantB, "identity"); err == nil {
				t.Fatal("app A resolved app B's grant")
			}
			if leaseB, err := db.ProviderToken(context.Background(), "app-b", "backend-b", grantB, "identity"); err != nil || leaseB.AccessToken != "app-b-access-secret" {
				t.Fatalf("app B grant changed: lease=%+v err=%v", leaseB, err)
			}

			replayed := completeProviderFlow(t, handler, reauthCompletion, bearer, true)
			if replayed.Code != http.StatusGone || decodeProviderResponse(t, replayed)["code"] != "completion_consumed" {
				t.Fatalf("completion replay status=%d body=%s", replayed.Code, replayed.Body.String())
			}
			otherBearer, _ := registerProviderTestUser(t, handler, "other-"+test.name+"@example.com")
			collisionState := startAppFlow(t, handler, "app-a", "link", otherBearer, "", "identity")
			collisionCompletion := callbackProviderFlow(t, handler, "shared", collisionState, "old")
			collision := completeProviderFlow(t, handler, collisionCompletion, otherBearer, true)
			if collision.Code != http.StatusConflict || decodeProviderResponse(t, collision)["code"] != "identity_already_linked" {
				t.Fatalf("cross-principal collision status=%d body=%s", collision.Code, collision.Body.String())
			}

			if test.pendingRevocation {
				retries := providerAllRows(db.Table(systemAuthProviderRevocationTableName))
				if len(retries) != 1 || toString(retries[0]["grant_id"]) != oldGrantID || toString(retries[0]["provider"]) != "shared" {
					t.Fatalf("detached retry rows=%#v", retries)
				}
				if oldGrant, err := db.Table(systemAuthProviderGrantTableName).Get(oldGrantID); err != nil || oldGrant != nil {
					t.Fatalf("detached old grant=%#v err=%v", oldGrant, err)
				}
				_, _ = db.Table(systemAuthProviderRevocationTableName).Update(toString(retries[0]["id"]), map[string]any{"next_attempt_at": 0})
				providerA.mu.Lock()
				providerA.revokeErr = nil
				providerA.mu.Unlock()
				completed, err := db.RetryProviderRevocations(context.Background())
				if err != nil || completed != 1 {
					t.Fatalf("old retry completed=%d err=%v", completed, err)
				}
				providerA.mu.Lock()
				oldRetryToken := providerA.lastRevoke.Token
				providerA.mu.Unlock()
				if oldRetryToken != "old-access-secret" || db.Table(systemAuthProviderRevocationTableName).Count() != 0 {
					t.Fatalf("old retry token=%q retries=%d", oldRetryToken, db.Table(systemAuthProviderRevocationTableName).Count())
				}
				if lease, err := db.ProviderToken(context.Background(), "app-a", "backend-a", newGrantID, "read"); err != nil || lease.AccessToken != "new-access-secret" {
					t.Fatalf("old retry changed new grant: lease=%+v err=%v", lease, err)
				}
				if err := db.providerAuth.revokeGrant(context.Background(), principalID, newGrantID); err != nil {
					t.Fatal(err)
				}
				providerA.mu.Lock()
				newRevokeToken := providerA.lastRevoke.Token
				providerA.mu.Unlock()
				if newRevokeToken != "new-access-secret" {
					t.Fatalf("new revocation token=%q", newRevokeToken)
				}
			}
		})
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

func TestProviderReconnectRequiredGrantCompletesConsentInPlace(t *testing.T) {
	provider := &fakeGrantProvider{
		issuer: "https://issuer.example",
		tokens: map[string]AuthProviderTokenSet{
			"expired":   {AccessToken: "expired", RefreshToken: "expired-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Second), Scopes: []string{"identity"}},
			"reconnect": {AccessToken: "reconnected", RefreshToken: "reconnected-refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity", "read"}},
		},
	}
	db, handler := openGrantTestDatabase(t, t.TempDir(), provider, "client")
	defer db.Close()
	token, _, grantID, _ := linkGrantForTest(t, db, handler, "expired", "reconnect-consent@example.com", "identity")

	provider.mu.Lock()
	provider.refreshErr = &AuthProviderUpstreamError{Code: "invalid_grant", Terminal: true}
	provider.mu.Unlock()
	lease, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
	var providerErr *AuthProviderError
	if lease != nil || !errors.As(err, &providerErr) || providerErr.Code != "reconnect_required" {
		t.Fatalf("terminal refresh lease=%+v err=%v", lease, err)
	}
	grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil || toString(grant["state"]) != "reconnect_required" {
		t.Fatalf("grant after terminal refresh=%#v err=%v", grant, err)
	}

	state := startAppFlow(t, handler, "app", "consent", token, grantID, "read")
	completion := callbackProviderFlow(t, handler, "shared", state, "reconnect")
	response := completeProviderFlow(t, handler, completion, token, true)
	if response.Code != http.StatusOK {
		t.Fatalf("reconnect consent status=%d body=%s", response.Code, response.Body.String())
	}
	completedGrantID := toString(decodeProviderResponse(t, response)["grant"].(map[string]any)["id"])
	if completedGrantID != grantID {
		t.Fatalf("reconnect consent grant=%q want %q", completedGrantID, grantID)
	}
	grant, err = db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil || toString(grant["state"]) != "active" {
		t.Fatalf("grant after reconnect consent=%#v err=%v", grant, err)
	}
	if _, unusable := db.providerAuth.unusableGrants.Load(grantID); unusable {
		t.Fatal("reconnected grant remained unusable")
	}
	lease, err = db.ProviderToken(context.Background(), "app", "backend", grantID, "read")
	if err != nil || lease.AccessToken != "reconnected" {
		t.Fatalf("reconnected lease=%+v err=%v", lease, err)
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

func TestTokenlessMaterializationRejectsGrantWithPendingRevocation(t *testing.T) {
	dataDir := t.TempDir()
	tokenProvider := &fakeGrantProvider{
		issuer:    "https://issuer.example",
		revokeErr: fmt.Errorf("temporary upstream failure"),
		tokens: map[string]AuthProviderTokenSet{
			"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
		},
	}
	app := New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(tokenProvider, "client", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	handler := app.APIHandler(db)
	_, principalID, grantID, _ := linkGrantForTest(t, db, handler, "code", "pending-tokenless@example.com", "identity")
	err = db.providerAuth.revokeGrant(context.Background(), principalID, grantID)
	var providerErr *AuthProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "revocation_pending" {
		t.Fatalf("revoke error=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	identityProvider := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"identity": {Provider: "shared", Issuer: "https://issuer.example", Subject: "shared-subject"},
	}}
	app = New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app": identityOnlyAppConfig(identityProvider, "client", "client-secret", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err = app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler = app.APIHandler(db)

	state := startAppFlow(t, handler, "app", "sign_in", "", "")
	completion := callbackProviderFlow(t, handler, "shared", state, "identity")
	response := completeProviderFlow(t, handler, completion, "", false)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "provider_grant_failed") {
		t.Fatalf("tokenless materialization status=%d body=%s", response.Code, response.Body.String())
	}
	grant, _ := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if toString(grant["state"]) != "revoked" || db.Table(systemAuthProviderRevocationTableName).Count() != 1 {
		t.Fatalf("pending revocation changed by tokenless materialization: grant=%#v retries=%d", grant, db.Table(systemAuthProviderRevocationTableName).Count())
	}
	if _, err := db.ProviderIdentity(context.Background(), "app", "backend", grantID); err == nil {
		t.Fatal("pending revocation was exposed as an identity grant")
	}
}

func TestTokenlessMaterializationRejectsActiveTokenGrant(t *testing.T) {
	dataDir := t.TempDir()
	tokenProvider := &fakeGrantProvider{
		issuer: "https://issuer.example",
		tokens: map[string]AuthProviderTokenSet{
			"code": {AccessToken: "access", RefreshToken: "refresh", TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour), Scopes: []string{"identity"}},
		},
	}
	app := New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app": appGrantTestConfig(tokenProvider, "client", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	handler := app.APIHandler(db)
	_, _, grantID, _ := linkGrantForTest(t, db, handler, "code", "active-tokenless@example.com", "identity")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	identityProvider := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{
		"identity": {Provider: "shared", Issuer: "https://issuer.example", Subject: "shared-subject"},
	}}
	app = New(Config{DataDir: dataDir, SyncMode: "normal", AuthProviderApps: map[string]AuthProviderAppConfig{"app": identityOnlyAppConfig(identityProvider, "client", "client-secret", "backend")}, ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1"})
	defineGrantTestUsers(app)
	db, err = app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler = app.APIHandler(db)
	before, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || before == nil {
		t.Fatalf("grant lookup=%#v err=%v", before, err)
	}

	state := startAppFlow(t, handler, "app", "sign_in", "", "")
	completion := callbackProviderFlow(t, handler, "shared", state, "identity")
	response := completeProviderFlow(t, handler, completion, "", false)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "provider_grant_failed") {
		t.Fatalf("tokenless materialization status=%d body=%s", response.Code, response.Body.String())
	}
	after, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || after == nil {
		t.Fatalf("grant lookup=%#v err=%v", after, err)
	}
	for _, field := range []string{"state", "token_ciphertext", "token_key_version", "client_id", "credential_ciphertext", "credential_key_version"} {
		if toString(after[field]) != toString(before[field]) {
			t.Fatalf("tokenless materialization changed %s: before=%q after=%q", field, before[field], after[field])
		}
	}
	if db.Table(systemAuthProviderRevocationTableName).Count() != 0 {
		t.Fatal("failed-closed tokenless materialization staged an unexpected revocation")
	}
	lease, err := db.ProviderToken(context.Background(), "app", "backend", grantID, "identity")
	if err != nil || lease.AccessToken != "access" {
		t.Fatalf("original backend authorization lease=%#v err=%v", lease, err)
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
