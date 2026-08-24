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
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "subject", "name": "Person", "picture": " https://cdn.example/person.png ", "email": "person@example.com", "email_verified": true})
		case "/revoke":
			revocations.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	adapter := &OAuth2AuthProvider{Definition: OAuth2ProviderDefinition{AuthorizationEndpoint: server.URL + "/authorize", TokenEndpoint: server.URL + "/token", UserInfoEndpoint: server.URL + "/userinfo", RevocationEndpoint: server.URL + "/revoke", Issuer: "https://issuer.example", Audience: "client", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "sub", DisplayNameClaim: "name", AvatarURLClaim: "picture", EmailClaim: "email", EmailVerifiedClaim: "email_verified", VerifyIDToken: func(_ context.Context, raw string) (map[string]any, error) {
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
	if result.Identity.Subject != "subject" || result.Identity.Issuer != "https://issuer.example" || result.Identity.AvatarURL != "https://cdn.example/person.png" || !result.Identity.EmailVerified {
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
	parameters := url.Values{
		"state":              {"state"},
		"openid.mode":        {"id_res"},
		"openid.op_endpoint": {server.URL},
		"openid.return_to":   {returnTo.String()},
		"openid.claimed_id":  {"http://steamcommunity.com/openid/id/76561198000000000"},
		"openid.identity":    {"http://steamcommunity.com/openid/id/76561198000000000"},
	}
	identity, err := adapter.Exchange(context.Background(), AuthProviderCallbackRequest{Provider: "steam", RedirectURI: "https://flop.example/callback?provider=steam", Parameters: parameters})
	if err != nil {
		t.Fatal(err)
	}
	if identity.Subject != "76561198000000000" || identity.Issuer != "https://steamcommunity.com/openid" {
		t.Fatalf("identity=%+v", identity)
	}
	badReturnTo := cloneURLValues(parameters)
	badReturnTo.Set("openid.return_to", "https://attacker.example/callback?state=state")
	if _, err := adapter.Exchange(context.Background(), AuthProviderCallbackRequest{Provider: "steam", RedirectURI: "https://flop.example/callback?provider=steam", Parameters: badReturnTo}); err == nil {
		t.Fatal("Steam accepted an assertion bound to another return URL")
	}
	badEndpoint := cloneURLValues(parameters)
	badEndpoint.Set("openid.op_endpoint", "https://attacker.example/openid")
	if _, err := adapter.Exchange(context.Background(), AuthProviderCallbackRequest{Provider: "steam", RedirectURI: "https://flop.example/callback?provider=steam", Parameters: badEndpoint}); err == nil {
		t.Fatal("Steam accepted an assertion from another OpenID endpoint")
	}
	if got := validationRequests.Load(); got != 1 {
		t.Fatalf("Steam made %d requests, want only the OpenID assertion validation request and no playtime or owned-games request", got)
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
	if config, err := BuiltinAuthProviderConfig("draugiem", BuiltinAuthProviderOptions{ClientID: "app", ClientSecret: "key", RedirectURI: "https://flop.example/callback"}); err != nil || config.Issuer != "https://www.draugiem.lv" {
		t.Fatalf("Draugiem built-in config=%+v err=%v", config, err)
	} else if capabilities := config.Adapter.(AuthProviderCapabilityAdapter).ProviderCapabilities(); capabilities.Revocation {
		t.Fatal("Draugiem advertised unsupported remote revocation")
	}
}
