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

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
)

type deterministicProvider struct {
	server *httptest.Server
	mu     sync.Mutex
	auth   ProviderAuthorizationRequest
	ident  ProviderIdentity
}

func newDeterministicProvider(t *testing.T) *deterministicProvider {
	t.Helper()
	p := &deterministicProvider{ident: ProviderIdentity{
		Issuer: "https://issuer.example", Subject: "opaque-subject-1",
		DisplayName: "Provider User", Email: "linked@example.com", EmailVerified: true,
		Claims: map[string]string{"avatar": "avatar-1", "access_token": "claim-token-must-not-be-retained"},
	}}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		auth := p.auth
		identity := p.ident
		p.mu.Unlock()
		if r.Form.Get("code") != "good-code" || r.Form.Get("redirect_uri") != auth.RedirectURI || r.Form.Get("nonce") != auth.Nonce {
			http.Error(w, "exchange binding mismatch", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(sum[:]) != auth.CodeChallenge || auth.CodeChallengeMethod != "S256" {
			http.Error(w, "PKCE mismatch", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "must-not-be-retained", "refresh_token": "must-not-be-retained-either",
			"identity": identity,
		})
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *deterministicProvider) Key() string    { return "test" }
func (p *deterministicProvider) Issuer() string { return "https://issuer.example" }
func (p *deterministicProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{PKCES256: true, OIDCNonce: true}
}
func (p *deterministicProvider) AuthorizationURL(_ context.Context, in ProviderAuthorizationRequest) (string, error) {
	p.mu.Lock()
	p.auth = in
	p.mu.Unlock()
	values := url.Values{
		"response_type":         {"code"},
		"redirect_uri":          {in.RedirectURI},
		"state":                 {in.State},
		"code_challenge":        {in.CodeChallenge},
		"code_challenge_method": {in.CodeChallengeMethod},
		"nonce":                 {in.Nonce},
	}
	return p.server.URL + "/authorize?" + values.Encode(), nil
}
func (p *deterministicProvider) ExchangeCode(ctx context.Context, in ProviderCodeExchangeRequest) (ProviderIdentity, error) {
	form := url.Values{
		"code": {in.Code}, "redirect_uri": {in.RedirectURI},
		"code_verifier": {in.CodeVerifier}, "nonce": {in.ExpectedNonce},
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.server.URL+"/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ProviderIdentity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ProviderIdentity{}, fmt.Errorf("exchange failed")
	}
	var out struct {
		Identity ProviderIdentity `json:"identity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return ProviderIdentity{}, err
	}
	return out.Identity, nil
}

func TestProviderLinkAndSignInFlow(t *testing.T) {
	provider := newDeterministicProvider(t)
	dataDir := t.TempDir()
	app := providerTestApp(Config{
		DataDir: dataDir, SyncMode: "normal", Providers: []ProviderAdapter{provider},
		ProviderCallbackURL: "https://app.example/api/auth/provider/callback",
		ProviderReturnURLs:  []string{"https://app.example/auth/complete"},
		ProviderFlowTTL:     time.Minute,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := app.APIHandler(db)
	userToken := registerSecurityTestUser(t, handler, "linked@example.com", "password123")

	// A verified email match remains display data: an unlinked identity cannot
	// sign into the existing user and receives no Flop token.
	state, startURL := startProviderFlow(t, handler, ProviderIntentSignIn, "", "https://app.example/auth/complete")
	assertProviderStartBindings(t, provider, startURL, state)
	assertProviderSecretsNotStoredRaw(t, db, provider)
	resultCode := completeProviderCallback(t, handler, state, "https://app.example/auth/complete")
	result := redeemProviderResult(t, handler, resultCode, http.StatusOK)
	if result["outcome"] != ProviderOutcomeLinkRequired || result["token"] != nil || result["refreshToken"] != nil {
		t.Fatalf("unexpected unknown-identity result: %#v", result)
	}
	redeemProviderResult(t, handler, resultCode, http.StatusBadRequest)

	// Explicit link mode is bound to the initiating principal and session.
	linkState, _ := startProviderFlow(t, handler, ProviderIntentLink, userToken, "https://app.example/auth/complete")
	linkResultCode := completeProviderCallback(t, handler, linkState, "https://app.example/auth/complete")
	linkResult := redeemProviderResult(t, handler, linkResultCode, http.StatusOK)
	confirmationCode, _ := linkResult["confirmationCode"].(string)
	if linkResult["outcome"] != ProviderOutcomeLinkPending || confirmationCode == "" {
		t.Fatalf("unexpected link result: %#v", linkResult)
	}
	otherToken := registerSecurityTestUser(t, handler, "other@example.com", "password123")
	crossSession := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/link/confirm",
		fmt.Sprintf(`{"confirmationCode":%q}`, confirmationCode), otherToken)
	if crossSession.Code != http.StatusBadRequest || !strings.Contains(crossSession.Body.String(), ProviderOutcomeInvalidFlow) {
		t.Fatalf("cross-session confirmation status=%d body=%s", crossSession.Code, crossSession.Body.String())
	}
	confirm := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/link/confirm",
		fmt.Sprintf(`{"confirmationCode":%q}`, confirmationCode), userToken)
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm link status=%d body=%s", confirm.Code, confirm.Body.String())
	}
	assertProviderTokensNotStored(t, db)

	list := providerJSONRequest(handler, http.MethodGet, "/api/auth/provider/identities", "", userToken)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "opaque-subject-1") {
		t.Fatalf("list identities status=%d body=%s", list.Code, list.Body.String())
	}

	// Once linked, the same identity signs in through the normal session path.
	signInState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	signInCode := completeProviderCallback(t, handler, signInState, "")
	signIn := redeemProviderResult(t, handler, signInCode, http.StatusOK)
	providerToken, _ := signIn["token"].(string)
	refreshToken, _ := signIn["refreshToken"].(string)
	if signIn["outcome"] != ProviderOutcomeSignInReady || providerToken == "" || refreshToken == "" || signIn["user"] == nil || signIn["me"] == nil {
		t.Fatalf("unexpected sign-in response: %#v", signIn)
	}
	refresh := providerJSONRequest(handler, http.MethodPost, "/api/auth/refresh",
		fmt.Sprintf(`{"refreshToken":%q}`, refreshToken), "")
	if refresh.Code != http.StatusOK || !strings.Contains(refresh.Body.String(), "refreshToken") {
		t.Fatalf("refresh provider session status=%d body=%s", refresh.Code, refresh.Body.String())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before persistence check: %v", err)
	}
	db, err = app.Open()
	if err != nil {
		t.Fatalf("reopen with linked identity: %v", err)
	}
	handler = app.APIHandler(db)
	persisted := providerJSONRequest(handler, http.MethodGet, "/api/auth/provider/identities", "", userToken)
	if persisted.Code != http.StatusOK || !strings.Contains(persisted.Body.String(), "opaque-subject-1") {
		t.Fatalf("identity did not persist across reopen: status=%d body=%s", persisted.Code, persisted.Body.String())
	}
	userAuth := hAuthFromToken(t, db, userToken)
	if _, err := db.db.GetAuthTable().Update(userAuth.ID, map[string]any{"archived": true}, nil); err != nil {
		t.Fatalf("archive provider-linked principal: %v", err)
	}
	blockedState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	blockedCode := completeProviderCallback(t, handler, blockedState, "")
	blocked := redeemProviderResult(t, handler, blockedCode, http.StatusOK)
	if blocked["outcome"] != ProviderOutcomePrincipalUnavailable || blocked["token"] != nil {
		t.Fatalf("blocked principal received provider session: %#v", blocked)
	}
	if _, err := db.db.GetAuthTable().Update(userAuth.ID, map[string]any{"archived": false}, nil); err != nil {
		t.Fatalf("restore provider-linked principal: %v", err)
	}

	var confirmBody struct {
		Identity LinkedProviderIdentity `json:"identity"`
	}
	if err := json.Unmarshal(confirm.Body.Bytes(), &confirmBody); err != nil {
		t.Fatalf("decode linked identity: %v", err)
	}
	unlink := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/unlink",
		fmt.Sprintf(`{"identityId":%q}`, confirmBody.Identity.ID), userToken)
	if unlink.Code != http.StatusOK {
		t.Fatalf("unlink status=%d body=%s", unlink.Code, unlink.Body.String())
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err = app.Open()
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, _, _, err := db.authService.Login("linked@example.com", "password123"); err != nil {
		t.Fatalf("password login after provider-table migration/reopen: %v", err)
	}
}

func TestProviderCallbackRejectsIssuerReplayAndLastMethodRemoval(t *testing.T) {
	provider := newDeterministicProvider(t)
	app := providerTestApp(Config{
		DataDir: t.TempDir(), SyncMode: "normal", Providers: []ProviderAdapter{provider},
		ProviderCallbackURL: "https://app.example/api/auth/provider/callback", ProviderFlowTTL: time.Minute,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	handler := app.APIHandler(db)
	token := registerSecurityTestUser(t, handler, "owner@example.com", "password123")

	wrongIssuerState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	wrongIssuer := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/auth/provider/callback?state="+url.QueryEscape(wrongIssuerState)+"&code=good-code&iss="+url.QueryEscape("https://wrong.example"), nil)
	handler.ServeHTTP(wrongIssuer, req)
	if wrongIssuer.Code != http.StatusOK {
		t.Fatalf("wrong issuer callback status=%d body=%s", wrongIssuer.Code, wrongIssuer.Body.String())
	}
	var callback map[string]any
	_ = json.Unmarshal(wrongIssuer.Body.Bytes(), &callback)
	wrongIssuerResult := redeemProviderResult(t, handler, fmt.Sprint(callback["resultCode"]), http.StatusOK)
	if wrongIssuerResult["outcome"] != ProviderOutcomeInvalidFlow {
		t.Fatalf("wrong issuer outcome: %#v", wrongIssuerResult)
	}
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, req)
	if replay.Code != http.StatusBadRequest || !strings.Contains(replay.Body.String(), ProviderOutcomeInvalidFlow) {
		t.Fatalf("callback replay status=%d body=%s", replay.Code, replay.Body.String())
	}

	identity := linkProviderIdentity(t, handler, token)
	auth := hAuthFromToken(t, db, token)
	if _, err := db.db.GetAuthTable().Update(auth.ID, map[string]any{"password": ""}, nil); err != nil {
		t.Fatalf("clear password hash for last-method test: %v", err)
	}
	unlink := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/unlink",
		fmt.Sprintf(`{"identityId":%q}`, identity.ID), token)
	if unlink.Code != http.StatusConflict || !strings.Contains(unlink.Body.String(), ProviderOutcomeLastMethod) {
		t.Fatalf("last-method unlink status=%d body=%s", unlink.Code, unlink.Body.String())
	}
}

func TestProviderFailureOutcomesReturnAllowlistAndExpiry(t *testing.T) {
	provider := newDeterministicProvider(t)
	app := providerTestApp(Config{
		DataDir: t.TempDir(), SyncMode: "normal", Providers: []ProviderAdapter{provider},
		ProviderCallbackURL: "https://app.example/api/auth/provider/callback",
		ProviderReturnURLs:  []string{"https://app.example/auth/complete"}, ProviderFlowTTL: time.Minute,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	handler := app.APIHandler(db)

	unallowed := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/start",
		`{"provider":"test","returnUrl":"https://attacker.example/callback"}`, "")
	if unallowed.Code != http.StatusBadRequest || !strings.Contains(unallowed.Body.String(), ProviderOutcomeInvalidFlow) {
		t.Fatalf("unallowlisted return status=%d body=%s", unallowed.Code, unallowed.Body.String())
	}

	deniedState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	deniedCallback := httptest.NewRecorder()
	handler.ServeHTTP(deniedCallback, httptest.NewRequest(http.MethodGet,
		"/api/auth/provider/callback?state="+url.QueryEscape(deniedState)+"&error=access_denied", nil))
	var denied map[string]any
	_ = json.Unmarshal(deniedCallback.Body.Bytes(), &denied)
	deniedResult := redeemProviderResult(t, handler, fmt.Sprint(denied["resultCode"]), http.StatusOK)
	if deniedResult["outcome"] != ProviderOutcomeProviderDenied {
		t.Fatalf("provider denial outcome: %#v", deniedResult)
	}

	failingState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	failingCallback := httptest.NewRecorder()
	handler.ServeHTTP(failingCallback, httptest.NewRequest(http.MethodGet,
		"/api/auth/provider/callback?state="+url.QueryEscape(failingState)+"&code=bad-code", nil))
	var failed map[string]any
	_ = json.Unmarshal(failingCallback.Body.Bytes(), &failed)
	failedResult := redeemProviderResult(t, handler, fmt.Sprint(failed["resultCode"]), http.StatusOK)
	if failedResult["outcome"] != ProviderOutcomeExchangeFailed {
		t.Fatalf("provider exchange outcome: %#v", failedResult)
	}

	provider.mu.Lock()
	provider.ident.Subject = " "
	provider.mu.Unlock()
	blankSubjectState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	blankSubjectCode := completeProviderCallback(t, handler, blankSubjectState, "")
	blankSubject := redeemProviderResult(t, handler, blankSubjectCode, http.StatusOK)
	if blankSubject["outcome"] != ProviderOutcomeInvalidFlow {
		t.Fatalf("blank subject outcome: %#v", blankSubject)
	}
	provider.mu.Lock()
	provider.ident.Subject = "opaque-subject-1"
	provider.mu.Unlock()

	expiredState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	liveState, _ := startProviderFlow(t, handler, ProviderIntentSignIn, "", "")
	flows := db.db.GetTable(systemProviderFlowTableName)
	rows, scanErr := flows.Scan(100, 0)
	if scanErr != nil {
		t.Fatalf("scan flows: %v", scanErr)
	}
	for _, row := range rows {
		if fmt.Sprint(row["phase"]) == "started" {
			if _, err := flows.Update(fmt.Sprint(row["id"]), map[string]any{"expires_at": time.Now().Add(-time.Minute).UnixMilli()}, nil); err != nil {
				t.Fatalf("expire flow: %v", err)
			}
			break
		}
	}
	before := flows.Count()
	deleted, cleanupErr := db.authService.CleanupExpiredProviderFlows(time.Now(), 1)
	if cleanupErr != nil || deleted != 1 || flows.Count() != before-1 {
		t.Fatalf("cleanup deleted=%d before=%d after=%d err=%v", deleted, before, flows.Count(), cleanupErr)
	}
	// Exactly one of the two fresh state values was expired. The other remains
	// usable; neither can be confused with an earlier consumed flow.
	statuses := []int{}
	for _, state := range []string{expiredState, liveState} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
			"/api/auth/provider/callback?state="+url.QueryEscape(state)+"&code=good-code", nil))
		statuses = append(statuses, rec.Code)
	}
	if !((statuses[0] == http.StatusBadRequest && statuses[1] == http.StatusOK) || (statuses[1] == http.StatusBadRequest && statuses[0] == http.StatusOK)) {
		t.Fatalf("expired/live callback statuses=%v", statuses)
	}
}

func TestProviderConfigurationValidation(t *testing.T) {
	provider := newDeterministicProvider(t)
	app := providerTestApp(Config{DataDir: t.TempDir(), Providers: []ProviderAdapter{provider, provider}, ProviderCallbackURL: "https://app.example/callback"})
	if db, err := app.Open(); err == nil {
		_ = db.Close()
		t.Fatal("duplicate provider configuration unexpectedly opened")
	} else if !strings.Contains(err.Error(), "duplicate provider key") {
		t.Fatalf("unexpected duplicate provider error: %v", err)
	}
}

func TestConcurrentProviderLinkHasSingleOwner(t *testing.T) {
	provider := newDeterministicProvider(t)
	app := providerTestApp(Config{
		DataDir: t.TempDir(), SyncMode: "normal", Providers: []ProviderAdapter{provider},
		ProviderCallbackURL: "https://app.example/api/auth/provider/callback", ProviderFlowTTL: time.Minute,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	handler := app.APIHandler(db)
	tokens := []string{
		registerSecurityTestUser(t, handler, "first@example.com", "password123"),
		registerSecurityTestUser(t, handler, "second@example.com", "password123"),
	}
	confirmationCodes := make([]string, 2)
	for i, token := range tokens {
		state, _ := startProviderFlow(t, handler, ProviderIntentLink, token, "")
		resultCode := completeProviderCallback(t, handler, state, "")
		result := redeemProviderResult(t, handler, resultCode, http.StatusOK)
		confirmationCodes[i] = fmt.Sprint(result["confirmationCode"])
	}

	type response struct {
		status int
		body   string
	}
	responses := make(chan response, 2)
	var wg sync.WaitGroup
	for i := range tokens {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/link/confirm",
				fmt.Sprintf(`{"confirmationCode":%q}`, confirmationCodes[i]), tokens[i])
			responses <- response{status: rec.Code, body: rec.Body.String()}
		}()
	}
	wg.Wait()
	close(responses)
	successes, conflicts := 0, 0
	for result := range responses {
		switch result.status {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			if !strings.Contains(result.body, ProviderOutcomeIdentityConflict) {
				t.Fatalf("non-enumerating conflict missing stable code: %s", result.body)
			}
			conflicts++
		default:
			t.Fatalf("unexpected concurrent link response: status=%d body=%s", result.status, result.body)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent link results successes=%d conflicts=%d", successes, conflicts)
	}
	if got := db.db.GetTable(systemProviderIdentityTableName).Count(); got != 1 {
		t.Fatalf("provider identity owners=%d, want 1", got)
	}
}

func TestProviderSystemTableCollisionIsActionable(t *testing.T) {
	dataDir := t.TempDir()
	legacy := engine.NewDatabase(engine.DatabaseConfig{DataDir: dataDir, SyncMode: "normal"})
	err := legacy.Open(map[string]*schema.TableDef{
		systemProviderIdentityTableName: {
			Name: systemProviderIdentityTableName,
			CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
				{Name: "id", Kind: schema.KindString, Required: true, Unique: true},
				{Name: "user_data", Kind: schema.KindString, Required: false},
			}),
		},
	})
	if err != nil {
		t.Fatalf("create legacy collision: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy collision: %v", err)
	}
	app := providerTestApp(Config{DataDir: dataDir, SyncMode: "normal"})
	if db, err := app.Open(); err == nil {
		_ = db.Close()
		t.Fatal("reserved provider system table collision unexpectedly opened")
	} else if !strings.Contains(err.Error(), "conflicts with reserved system table") || !strings.Contains(err.Error(), "rename or migrate") {
		t.Fatalf("collision error is not actionable: %v", err)
	}
}

func providerTestApp(config Config) *App {
	app := New(config)
	Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.Boolean("verified").Default(false)
		s.Boolean("archived").Default(false)
		s.String("default_role").Default("user")
		s.Roles("roles")
	})
	return app
}

func startProviderFlow(t *testing.T, handler http.Handler, intent, bearer, returnURL string) (string, string) {
	t.Helper()
	body := fmt.Sprintf(`{"provider":"test","intent":%q,"returnUrl":%q}`, intent, returnURL)
	rec := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/start", body, bearer)
	if rec.Code != http.StatusOK {
		t.Fatalf("start provider flow status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		AuthorizationURL string `json:"authorizationUrl"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode provider start: %v", err)
	}
	u, err := url.Parse(out.AuthorizationURL)
	if err != nil || u.Query().Get("state") == "" {
		t.Fatalf("invalid provider authorization URL %q: %v", out.AuthorizationURL, err)
	}
	return u.Query().Get("state"), out.AuthorizationURL
}

func assertProviderStartBindings(t *testing.T, provider *deterministicProvider, authorizationURL, state string) {
	t.Helper()
	u, _ := url.Parse(authorizationURL)
	q := u.Query()
	if q.Get("state") != state || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("nonce") == "" {
		t.Fatalf("authorization URL missing flow bindings: %s", authorizationURL)
	}
	if q.Get("redirect_uri") != "https://app.example/api/auth/provider/callback" {
		t.Fatalf("callback URI = %q", q.Get("redirect_uri"))
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.auth.State != state || provider.auth.RedirectURI != q.Get("redirect_uri") {
		t.Fatalf("adapter authorization request mismatch: %#v", provider.auth)
	}
}

func assertProviderSecretsNotStoredRaw(t *testing.T, db *Database, provider *deterministicProvider) {
	t.Helper()
	rows, err := db.db.GetTable(systemProviderFlowTableName).Scan(10, 0)
	if err != nil || len(rows) == 0 {
		t.Fatalf("scan provider flows: rows=%d err=%v", len(rows), err)
	}
	provider.mu.Lock()
	auth := provider.auth
	provider.mu.Unlock()
	serialized, _ := json.Marshal(rows[len(rows)-1])
	for _, secret := range []string{auth.State, auth.Nonce, auth.CodeChallenge} {
		if secret != "" && strings.Contains(string(serialized), secret) {
			t.Fatalf("raw provider flow secret persisted: %s", serialized)
		}
	}
	adminTables, err := (&EngineAdminProvider{DB: db}).AdminTables()
	if err != nil {
		t.Fatalf("admin tables: %v", err)
	}
	for _, table := range adminTables {
		if table.Name == systemProviderFlowTableName || table.Name == systemProviderIdentityTableName {
			t.Fatalf("provider system table exposed through admin: %s", table.Name)
		}
	}
}

func assertProviderTokensNotStored(t *testing.T, db *Database) {
	t.Helper()
	for _, tableName := range []string{systemProviderFlowTableName, systemProviderIdentityTableName} {
		rows, err := db.db.GetTable(tableName).Scan(100, 0)
		if err != nil {
			t.Fatalf("scan %s: %v", tableName, err)
		}
		serialized, _ := json.Marshal(rows)
		for _, secret := range []string{"must-not-be-retained", "claim-token-must-not-be-retained"} {
			if strings.Contains(string(serialized), secret) {
				t.Fatalf("provider token persisted in %s: %s", tableName, serialized)
			}
		}
	}
}

func completeProviderCallback(t *testing.T, handler http.Handler, state, returnURL string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	path := "/api/auth/provider/callback?state=" + url.QueryEscape(state) + "&code=good-code&iss=" + url.QueryEscape("https://issuer.example")
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if returnURL != "" {
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("callback status=%d body=%s", rec.Code, rec.Body.String())
		}
		location, err := url.Parse(rec.Header().Get("Location"))
		if err != nil || location.Scheme+"://"+location.Host+location.Path != returnURL {
			t.Fatalf("callback location=%q err=%v", rec.Header().Get("Location"), err)
		}
		if strings.Contains(rec.Header().Get("Location"), "good-code") || strings.Contains(rec.Header().Get("Location"), "token") {
			t.Fatalf("callback redirect leaked credential material: %q", rec.Header().Get("Location"))
		}
		return location.Query().Get("result")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("callback status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return fmt.Sprint(out["resultCode"])
}

func redeemProviderResult(t *testing.T, handler http.Handler, code string, wantStatus int) map[string]any {
	t.Helper()
	rec := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/result", fmt.Sprintf(`{"resultCode":%q}`, code), "")
	if rec.Code != wantStatus {
		t.Fatalf("redeem result status=%d want=%d body=%s", rec.Code, wantStatus, rec.Body.String())
	}
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return out
}

func linkProviderIdentity(t *testing.T, handler http.Handler, token string) LinkedProviderIdentity {
	t.Helper()
	state, _ := startProviderFlow(t, handler, ProviderIntentLink, token, "")
	resultCode := completeProviderCallback(t, handler, state, "")
	result := redeemProviderResult(t, handler, resultCode, http.StatusOK)
	confirmationCode := fmt.Sprint(result["confirmationCode"])
	confirm := providerJSONRequest(handler, http.MethodPost, "/api/auth/provider/link/confirm", fmt.Sprintf(`{"confirmationCode":%q}`, confirmationCode), token)
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm provider link status=%d body=%s", confirm.Code, confirm.Body.String())
	}
	var out struct {
		Identity LinkedProviderIdentity `json:"identity"`
	}
	_ = json.Unmarshal(confirm.Body.Bytes(), &out)
	return out.Identity
}

func providerJSONRequest(handler http.Handler, method, path, body, bearer string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	handler.ServeHTTP(rec, req)
	return rec
}

func hAuthFromToken(t *testing.T, db *Database, token string) *AuthContext {
	t.Helper()
	auth, err := db.ValidateAccessToken(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	return auth
}
