package flop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOAuth2ProviderProtocolFixture(t *testing.T) {
	var refreshes, revocations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if r.Method != http.MethodPost {
				t.Errorf("token method=%s", r.Method)
			}
			_ = r.ParseForm()
			if r.Form.Get("client_id") != "client" || r.Form.Get("client_secret") != "secret" {
				t.Error("token endpoint did not receive client authentication")
			}
			if r.Form.Get("grant_type") == "refresh_token" {
				refreshes.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "refreshed", "refresh_token": "rotated", "token_type": "Bearer", "expires_in": 3600, "scope": "openid read"})
				return
			}
			if r.Form.Get("code_verifier") != "verifier" {
				t.Error("token exchange omitted PKCE verifier")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "refresh_token": "refresh", "token_type": "Bearer", "expires_in": 3600, "id_token": "signed.fixture.token"})
		case "/userinfo":
			if r.Header.Get("Authorization") != "Bearer access" {
				t.Errorf("userinfo authorization=%q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "subject", "name": "Person", "picture": " https://cdn.example/person.png ", "profile": map[string]any{"handle": " person ", "url": " https://social.example/person "}, "email": "person@example.com", "email_verified": true})
		case "/revoke":
			revocations.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	adapter := &OAuth2AuthProvider{Definition: OAuth2ProviderDefinition{AuthorizationEndpoint: server.URL + "/authorize", TokenEndpoint: server.URL + "/token", UserInfoEndpoint: server.URL + "/userinfo", RevocationEndpoint: server.URL + "/revoke", Issuer: "https://issuer.example", Audience: "client", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "sub", DisplayNameClaim: "name", AvatarURLClaim: "picture", ProfileHandleClaim: "profile.handle", ProfileURLClaim: "profile.url", EmailClaim: "email", EmailVerifiedClaim: "email_verified", VerifyIDToken: func(_ context.Context, raw string) (map[string]any, error) {
		if raw != "signed.fixture.token" {
			return nil, fmt.Errorf("bad token")
		}
		return map[string]any{"iss": "https://issuer.example", "sub": "subject", "aud": "client", "nonce": "nonce", "exp": time.Now().Add(time.Minute).Unix()}, nil
	}}, HTTPClient: server.Client()}
	authorization, err := adapter.AuthorizationURL(context.Background(), AuthProviderAuthorizationRequest{ClientID: "client", RedirectURI: "https://flop.example/callback", State: "state", Nonce: "nonce", CodeChallenge: "challenge", CodeChallengeMethod: "S256", Scopes: []string{"openid", "read"}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authorization)
	if parsed.Query().Get("state") != "state" || parsed.Query().Get("code_challenge") != "challenge" || parsed.Query().Get("scope") != "openid read" {
		t.Fatalf("authorization query=%s", parsed.RawQuery)
	}
	result, err := adapter.ExchangeGrant(context.Background(), AuthProviderCallbackRequest{Provider: "fixture", Code: "code", RedirectURI: "https://flop.example/callback", CodeVerifier: "verifier", Nonce: "nonce", ClientID: "client", ClientSecret: "secret", RequestedScopes: []string{"openid", "read"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Identity.Subject != "subject" || result.Identity.Issuer != "https://issuer.example" || result.Identity.AvatarURL != "https://cdn.example/person.png" || result.Identity.ProfileHandle != "person" || result.Identity.ProfileURL != "https://social.example/person" || !result.Identity.EmailVerified {
		t.Fatalf("identity=%+v", result.Identity)
	}
	if !scopeSubset([]string{"openid", "read"}, result.GrantedScopes) {
		t.Fatalf("granted scopes=%v", result.GrantedScopes)
	}
	if _, err := adapter.RefreshGrant(context.Background(), AuthProviderRefreshRequest{ClientID: "client", ClientSecret: "secret", RefreshToken: "refresh", Scopes: []string{"openid", "read"}}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RevokeGrant(context.Background(), AuthProviderRevokeRequest{ClientID: "client", ClientSecret: "secret", Token: "rotated", TokenTypeHint: "refresh_token"}); err != nil {
		t.Fatal(err)
	}
	if refreshes.Load() != 1 || revocations.Load() != 1 {
		t.Fatalf("refreshes=%d revocations=%d", refreshes.Load(), revocations.Load())
	}
}

func TestOAuth2ProviderNormalizesOnlyGoogleUserInfoScopeAliases(t *testing.T) {
	const (
		emailAlias   = "https://www.googleapis.com/auth/userinfo.email"
		profileAlias = "https://www.googleapis.com/auth/userinfo.profile"
		unknownScope = "provider.example/arbitrary"
	)
	upstreamScopes := []string{GoogleScopeOpenID, emailAlias, profileAlias, GoogleScopeYouTubeReadonly, unknownScope}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "refresh_token": "refresh", "scope": strings.Join(upstreamScopes, " ")})
		case "/userinfo":
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "subject"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tests := []struct {
		name, issuer string
		want         []string
	}{
		{name: "google", issuer: "https://accounts.google.com", want: []string{GoogleScopeOpenID, GoogleScopeEmail, GoogleScopeProfile, GoogleScopeYouTubeReadonly, unknownScope}},
		{name: "discord", issuer: "https://discord.com", want: upstreamScopes},
		{name: "twitch", issuer: "https://id.twitch.tv/oauth2", want: upstreamScopes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := &OAuth2AuthProvider{Definition: OAuth2ProviderDefinition{
				TokenEndpoint: server.URL + "/token", UserInfoEndpoint: server.URL + "/userinfo",
				Issuer: test.issuer, UserInfoSubjectClaim: "sub",
			}, HTTPClient: server.Client()}
			exchanged, err := adapter.ExchangeGrant(context.Background(), AuthProviderCallbackRequest{Provider: test.name, Code: "code"})
			if err != nil {
				t.Fatal(err)
			}
			refreshed, err := adapter.RefreshGrant(context.Background(), AuthProviderRefreshRequest{Provider: test.name, RefreshToken: "refresh"})
			if err != nil {
				t.Fatal(err)
			}
			want := canonicalStrings(test.want)
			for operation, got := range map[string][]string{"exchange": exchanged.GrantedScopes, "refresh": refreshed.Scopes} {
				if len(got) != len(want) || !scopeSubset(want, got) || !scopeSubset(got, want) {
					t.Errorf("%s scopes=%v want %v", operation, got, want)
				}
			}
		})
	}
}

func TestGoogleLiveScopeAliasesCompleteAndPersistWithoutTokenExposure(t *testing.T) {
	const (
		accessSecret  = "google-access-secret"
		refreshSecret = "google-refresh-secret"
		emailAlias    = "https://www.googleapis.com/auth/userinfo.email"
		profileAlias  = "https://www.googleapis.com/auth/userinfo.profile"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_ = r.ParseForm()
			scopes := []string{GoogleScopeOpenID, emailAlias, profileAlias}
			if r.Form.Get("code") == "extra-scope" {
				scopes = append(scopes, "provider.example/unrequested")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": accessSecret, "refresh_token": refreshSecret, "token_type": "Bearer",
				"expires_in": 3600, "scope": strings.Join(scopes, " "),
			})
		case "/userinfo":
			if r.Header.Get("Authorization") != "Bearer "+accessSecret {
				t.Errorf("userinfo authorization=%q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "google-subject", "name": "Google Person", "email": "google@example.com", "email_verified": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	definition, _ := BuiltinOAuth2ProviderDefinition("google")
	definition.AuthorizationEndpoint = server.URL + "/authorize"
	definition.TokenEndpoint = server.URL + "/token"
	definition.UserInfoEndpoint = server.URL + "/userinfo"
	definition.RevocationEndpoint = ""
	adapter := &OAuth2AuthProvider{Definition: definition, HTTPClient: server.Client()}
	app := New(Config{
		DataDir: t.TempDir(), SyncMode: "normal",
		AuthProviders: map[string]AuthProviderConfig{"google": {
			Adapter: adapter, Issuer: definition.Issuer,
			RedirectURI: "https://app.example/api/auth/provider/callback?provider=google",
			ClientID:    "google-client", ClientSecret: "google-secret",
			AllowedScopes:  []string{GoogleScopeOpenID, GoogleScopeEmail, GoogleScopeProfile},
			DefaultScopes:  []string{GoogleScopeOpenID, GoogleScopeEmail, GoogleScopeProfile},
			RequiredScopes: []string{GoogleScopeOpenID},
		}},
		ProviderSecretKeys: map[string][]byte{"v1": []byte("0123456789abcdef0123456789abcdef")}, ActiveProviderSecretKey: "v1",
	})
	defineGrantTestUsers(app)
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := app.APIHandler(db)
	token, _ := registerProviderTestUser(t, handler, "google-link@example.com")

	state, _ := startProviderFlow(t, handler, "google", "link", token, "")
	completion := callbackProviderFlow(t, handler, "google", state, "valid")
	completed := completeProviderFlow(t, handler, completion, token, true)
	if completed.Code != http.StatusOK {
		t.Fatalf("Google completion status=%d body=%s", completed.Code, completed.Body.String())
	}
	for _, secret := range []string{accessSecret, refreshSecret} {
		if strings.Contains(completed.Body.String(), secret) {
			t.Fatalf("Google completion exposed provider token %q", secret)
		}
	}
	grantView, _ := decodeProviderResponse(t, completed)["grant"].(map[string]any)
	grantID := toString(grantView["id"])
	grant, err := db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grant == nil {
		t.Fatalf("Google grant=%#v err=%v", grant, err)
	}
	wantScopes := []string{GoogleScopeOpenID, GoogleScopeEmail, GoogleScopeProfile}
	storedScopes := storedStrings(grant["granted_scopes"])
	if len(storedScopes) != len(wantScopes) || !scopeSubset(wantScopes, storedScopes) || !scopeSubset(storedScopes, wantScopes) {
		t.Fatalf("Google stored scopes=%v want %v", storedScopes, wantScopes)
	}
	for _, secret := range []string{accessSecret, refreshSecret} {
		if strings.Contains(toString(grant["token_ciphertext"]), secret) {
			t.Fatalf("Google grant stored provider token %q in plaintext", secret)
		}
	}

	state, _ = startProviderFlow(t, handler, "google", "link", token, "")
	completion = callbackProviderFlow(t, handler, "google", state, "extra-scope")
	rejected := completeProviderFlow(t, handler, completion, token, true)
	if rejected.Code != http.StatusBadRequest || decodeProviderResponse(t, rejected)["code"] != "provider_scope_invalid" {
		t.Fatalf("Google extra scope status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	if db.Table(systemAuthProviderGrantTableName).Count() != 1 {
		t.Fatal("Google extra scope persisted a grant")
	}
}

func TestOAuth2ProviderProfileExtractionIsOptionalAndNonFatal(t *testing.T) {
	tests := []struct {
		name, handleClaim, urlClaim, handle, profileURL string
		userInfo                                        map[string]any
	}{
		{name: "top-level", handleClaim: "login", urlClaim: "html_url", handle: "person", profileURL: "https://profiles.example/person", userInfo: map[string]any{"id": "subject", "login": " person ", "html_url": " https://profiles.example/person "}},
		{name: "nested", handleClaim: "profile.handle", urlClaim: "profile.url", handle: "nested", profileURL: "https://profiles.example/nested", userInfo: map[string]any{"id": "subject", "profile": map[string]any{"handle": " nested ", "url": "https://profiles.example/nested"}}},
		{name: "url-without-handle", handleClaim: "login", urlClaim: "html_url", profileURL: "https://profiles.example/person", userInfo: map[string]any{"id": "subject", "html_url": "https://profiles.example/person"}},
		{name: "missing-url-clears-handle", handleClaim: "login", urlClaim: "html_url", userInfo: map[string]any{"id": "subject", "login": "person"}},
		{name: "malformed-url-clears-handle", handleClaim: "login", urlClaim: "html_url", userInfo: map[string]any{"id": "subject", "login": "person", "html_url": "://bad"}},
		{name: "non-web-url-clears-handle", handleClaim: "login", urlClaim: "html_url", userInfo: map[string]any{"id": "subject", "login": "person", "html_url": "mailto:person@example.com"}},
		{name: "hostless-url-clears-handle", handleClaim: "login", urlClaim: "html_url", userInfo: map[string]any{"id": "subject", "login": "person", "html_url": "https:///person"}},
		{name: "non-string-url-clears-handle", handleClaim: "login", urlClaim: "html_url", userInfo: map[string]any{"id": "subject", "login": "person", "html_url": 42}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/token":
					_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access"})
				case "/userinfo":
					_ = json.NewEncoder(w).Encode(test.userInfo)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			adapter := &OAuth2AuthProvider{Definition: OAuth2ProviderDefinition{
				TokenEndpoint: server.URL + "/token", UserInfoEndpoint: server.URL + "/userinfo",
				Issuer: "https://issuer.example", UserInfoSubjectClaim: "id", ProfileHandleClaim: test.handleClaim, ProfileURLClaim: test.urlClaim,
			}, HTTPClient: server.Client()}
			result, err := adapter.ExchangeGrant(context.Background(), AuthProviderCallbackRequest{Provider: "fixture", Code: "code"})
			if err != nil {
				t.Fatalf("exchange with optional profile: %v", err)
			}
			if result.Identity.ProfileHandle != test.handle || result.Identity.ProfileURL != test.profileURL {
				t.Fatalf("profile=(%q, %q) want (%q, %q)", result.Identity.ProfileHandle, result.Identity.ProfileURL, test.handle, test.profileURL)
			}
		})
	}
}

func TestOAuth2ProviderAvatarExtractionIsOptionalAndNonFatal(t *testing.T) {
	tests := []struct {
		name, claim, avatar string
		userInfo            map[string]any
	}{
		{name: "top-level", claim: "avatar_url", avatar: "https://cdn.example/top.png", userInfo: map[string]any{"id": "subject", "avatar_url": "https://cdn.example/top.png"}},
		{name: "nested-provider-response", claim: "picture.data.url", avatar: "https://cdn.example/nested.png", userInfo: map[string]any{"data": map[string]any{"id": "subject", "picture": map[string]any{"data": map[string]any{"url": "https://cdn.example/nested.png"}}}}},
		{name: "missing", claim: "avatar_url", userInfo: map[string]any{"id": "subject"}},
		{name: "malformed", claim: "avatar_url", userInfo: map[string]any{"id": "subject", "avatar_url": "://bad"}},
		{name: "non-web-scheme", claim: "avatar_url", userInfo: map[string]any{"id": "subject", "avatar_url": "data:image/png;base64,abc"}},
		{name: "non-string", claim: "avatar_url", userInfo: map[string]any{"id": "subject", "avatar_url": 42}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/token":
					_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access"})
				case "/userinfo":
					_ = json.NewEncoder(w).Encode(test.userInfo)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			adapter := &OAuth2AuthProvider{Definition: OAuth2ProviderDefinition{
				TokenEndpoint: server.URL + "/token", UserInfoEndpoint: server.URL + "/userinfo",
				Issuer: "https://issuer.example", UserInfoSubjectClaim: "id", AvatarURLClaim: test.claim,
			}, HTTPClient: server.Client()}
			result, err := adapter.ExchangeGrant(context.Background(), AuthProviderCallbackRequest{Provider: "fixture", Code: "code"})
			if err != nil {
				t.Fatalf("exchange with optional avatar: %v", err)
			}
			if result.Identity.AvatarURL != test.avatar {
				t.Fatalf("avatar URL=%q want %q", result.Identity.AvatarURL, test.avatar)
			}
		})
	}
}

func TestSteamOpenIDProtocolFixture(t *testing.T) {
	var validationRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validationRequests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/" {
			t.Errorf("unexpected Steam request method=%s path=%s; no playtime or owned-games request is allowed", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "openid.mode=check_authentication") {
			t.Errorf("verification body=%s", body)
		}
		_, _ = io.WriteString(w, "ns:http://specs.openid.net/auth/2.0\nis_valid:true\n")
	}))
	defer server.Close()
	adapter := &SteamOpenIDProvider{HTTPClient: server.Client(), Endpoint: server.URL}
	authorization, err := adapter.AuthorizationURL(context.Background(), AuthProviderAuthorizationRequest{RedirectURI: "https://flop.example/callback?provider=steam", State: "state"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authorization)
	returnTo, _ := url.Parse(parsed.Query().Get("openid.return_to"))
	if returnTo.Query().Get("state") != "state" {
		t.Fatalf("return_to=%s", returnTo)
	}
	parametersFor := func(claimedID string) url.Values {
		return url.Values{
			"state":              {"state"},
			"openid.mode":        {"id_res"},
			"openid.op_endpoint": {server.URL},
			"openid.return_to":   {returnTo.String()},
			"openid.claimed_id":  {claimedID},
			"openid.identity":    {claimedID},
		}
	}
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			parameters := parametersFor(scheme + "://steamcommunity.com/openid/id/76561198000000000")
			identity, err := adapter.Exchange(context.Background(), AuthProviderCallbackRequest{Provider: "steam", RedirectURI: "https://flop.example/callback?provider=steam", Parameters: parameters})
			if err != nil {
				t.Fatal(err)
			}
			if identity.Subject != "76561198000000000" || identity.Issuer != "https://steamcommunity.com/openid" {
				t.Fatalf("identity=%+v", identity)
			}
		})
	}

	valid := parametersFor("https://steamcommunity.com/openid/id/76561198000000000")
	tests := []struct {
		name   string
		mutate func(url.Values)
	}{
		{name: "mismatched identity", mutate: func(values url.Values) {
			values.Set("openid.identity", "https://steamcommunity.com/openid/id/76561198000000001")
		}},
		{name: "non-Steam host", mutate: func(values url.Values) {
			values.Set("openid.claimed_id", "https://attacker.example/openid/id/76561198000000000")
			values.Set("openid.identity", values.Get("openid.claimed_id"))
		}},
		{name: "unsupported scheme", mutate: func(values url.Values) {
			values.Set("openid.claimed_id", "ftp://steamcommunity.com/openid/id/76561198000000000")
			values.Set("openid.identity", values.Get("openid.claimed_id"))
		}},
		{name: "bad endpoint", mutate: func(values url.Values) { values.Set("openid.op_endpoint", "https://attacker.example/openid") }},
		{name: "bad return-to", mutate: func(values url.Values) {
			values.Set("openid.return_to", "https://attacker.example/callback?state=state")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parameters := cloneURLValues(valid)
			test.mutate(parameters)
			if _, err := adapter.Exchange(context.Background(), AuthProviderCallbackRequest{Provider: "steam", RedirectURI: "https://flop.example/callback?provider=steam", Parameters: parameters}); err == nil {
				t.Fatal("Steam accepted an invalid assertion")
			}
		})
	}
	if got := validationRequests.Load(); got != 2 {
		t.Fatalf("Steam made %d requests, want one OpenID assertion validation request for each accepted scheme and no requests for rejected shapes", got)
	}
}

func TestSteamHTTPSClaimedIDCompletesThroughProviderCallback(t *testing.T) {
	var validationRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validationRequests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || !strings.Contains(string(body), "openid.mode=check_authentication") {
			t.Errorf("Steam verification method=%s body=%s", r.Method, body)
		}
		_, _ = io.WriteString(w, "ns:http://specs.openid.net/auth/2.0\nis_valid:true\n")
	}))
	defer server.Close()

	adapter := &SteamOpenIDProvider{HTTPClient: server.Client(), Endpoint: server.URL}
	config := fakeProviderConfig(adapter, "steam", "https://steamcommunity.com/openid")
	_, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"steam": config})
	token, principalID := registerProviderTestUser(t, handler, "steam-link@example.com")
	started := providerRequest(t, handler, http.MethodPost, "/api/auth/provider/start", `{"provider":"steam","intent":"link"}`, token)
	if started.Code != http.StatusOK {
		t.Fatalf("Steam start status=%d body=%s", started.Code, started.Body.String())
	}
	authorizationURL, _ := url.Parse(toString(decodeProviderResponse(t, started)["authorizationUrl"]))
	returnTo := authorizationURL.Query().Get("openid.return_to")
	parsedReturnTo, err := url.Parse(returnTo)
	if err != nil {
		t.Fatal(err)
	}
	claimedID := "https://steamcommunity.com/openid/id/76561198000000000"
	parameters := url.Values{
		"provider":           {"steam"},
		"state":              {parsedReturnTo.Query().Get("state")},
		"openid.mode":        {"id_res"},
		"openid.op_endpoint": {server.URL},
		"openid.return_to":   {returnTo},
		"openid.claimed_id":  {claimedID},
		"openid.identity":    {claimedID},
	}
	callback := providerRequest(t, handler, http.MethodGet, "/api/auth/provider/callback?"+parameters.Encode(), "", "")
	if callback.Code != http.StatusOK {
		t.Fatalf("Steam callback status=%d body=%s", callback.Code, callback.Body.String())
	}
	completionCode := toString(decodeProviderResponse(t, callback)["completionCode"])
	completed := completeProviderFlow(t, handler, completionCode, token, true)
	if completed.Code != http.StatusOK {
		t.Fatalf("Steam completion status=%d body=%s", completed.Code, completed.Body.String())
	}
	identity, ok := db.Table(systemAuthIdentityTableName).FindByUniqueCompositeIndex([]string{"issuer", "subject"}, "https://steamcommunity.com/openid", "76561198000000000")
	if !ok || toString(identity["principal_id"]) != principalID {
		t.Fatalf("Steam identity=%#v want principal %s", identity, principalID)
	}
	if validationRequests.Load() != 1 {
		t.Fatalf("Steam verification requests=%d want 1", validationRequests.Load())
	}
}

func TestBuiltinProviderCatalog(t *testing.T) {
	for _, provider := range []string{"discord", "twitch", "github", "google", "facebook", "x"} {
		if _, ok := BuiltinOAuth2ProviderDefinition(provider); !ok {
			t.Errorf("missing built-in provider %s", provider)
		}
	}
	if _, ok := BuiltinOAuth2ProviderDefinition("youtube"); ok {
		t.Fatal("YouTube must not be a separate provider")
	}
	if GoogleScopeYouTubeReadonly == "" {
		t.Fatal("Google YouTube scopes were not published")
	}
	wantAvatarClaims := map[string]string{"google": "picture", "github": "avatar_url", "twitch": "profile_image_url", "facebook": "picture.data.url", "x": "profile_image_url"}
	for provider, claim := range wantAvatarClaims {
		definition, _ := BuiltinOAuth2ProviderDefinition(provider)
		if definition.AvatarURLClaim != claim {
			t.Errorf("%s avatar claim=%q want %q", provider, definition.AvatarURLClaim, claim)
		}
	}
	xDefinition, _ := BuiltinOAuth2ProviderDefinition("x")
	if !strings.Contains(xDefinition.UserInfoEndpoint, "user.fields=profile_image_url") {
		t.Fatalf("X user-info endpoint does not request profile_image_url: %s", xDefinition.UserInfoEndpoint)
	}
	githubDefinition, _ := BuiltinOAuth2ProviderDefinition("github")
	if githubDefinition.ProfileHandleClaim != "login" || githubDefinition.ProfileURLClaim != "html_url" {
		t.Fatalf("GitHub profile mapping=(%q, %q)", githubDefinition.ProfileHandleClaim, githubDefinition.ProfileURLClaim)
	}
	for _, provider := range []string{"discord", "twitch", "google", "facebook", "x"} {
		definition, _ := BuiltinOAuth2ProviderDefinition(provider)
		if definition.ProfileHandleClaim != "" || definition.ProfileURLClaim != "" {
			t.Errorf("%s acquired synthetic profile mapping=(%q, %q)", provider, definition.ProfileHandleClaim, definition.ProfileURLClaim)
		}
	}
	if config, err := BuiltinAuthProviderConfig("draugiem", BuiltinAuthProviderOptions{ClientID: "app", ClientSecret: "key", RedirectURI: "https://flop.example/callback"}); err != nil || config.Issuer != "https://www.draugiem.lv" {
		t.Fatalf("Draugiem built-in config=%+v err=%v", config, err)
	} else if capabilities := config.Adapter.(AuthProviderCapabilityAdapter).ProviderCapabilities(); capabilities.Revocation {
		t.Fatal("Draugiem advertised unsupported remote revocation")
	}
}
