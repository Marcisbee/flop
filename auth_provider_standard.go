package flop

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	GoogleScopeOpenID          = "openid"
	GoogleScopeProfile         = "profile"
	GoogleScopeEmail           = "email"
	GoogleScopeYouTubeReadonly = "https://www.googleapis.com/auth/youtube.readonly"
	GoogleScopeYouTubeForceSSL = "https://www.googleapis.com/auth/youtube.force-ssl"
)

type AuthProviderClientAuthStyle string

const (
	AuthProviderClientSecretPost  AuthProviderClientAuthStyle = "client_secret_post"
	AuthProviderClientSecretBasic AuthProviderClientAuthStyle = "client_secret_basic"
)

// OAuth2ProviderDefinition declaratively describes a standards-based
// provider. UserInfoSubjectClaim must identify an immutable provider subject.
type OAuth2ProviderDefinition struct {
	AuthorizationEndpoint    string
	TokenEndpoint            string
	UserInfoEndpoint         string
	RevocationEndpoint       string
	RevocationTokenType      string
	RevocationStyle          string
	OmitClientSecretOnRevoke bool
	Issuer                   string
	Audience                 string
	ClientAuthStyle          AuthProviderClientAuthStyle
	RefreshSupported         bool
	UserInfoSubjectClaim     string
	DisplayNameClaim         string
	// AvatarURLClaim may be a top-level claim or a dot-separated object path.
	AvatarURLClaim string
	// ProfileHandleClaim and ProfileURLClaim may be top-level claims or
	// dot-separated object paths. A profile handle is retained only when the
	// provider also supplies a valid absolute HTTP(S) profile URL.
	ProfileHandleClaim      string
	ProfileURLClaim         string
	EmailClaim              string
	EmailVerifiedClaim      string
	AuthorizationParameters map[string]string
	UserInfoHeaders         map[string]string
	// VerifyIDToken must verify the token signature and return its claims.
	// Flop then independently checks issuer, audience, expiry, nonce, and that
	// the authenticated user-info subject matches the ID-token subject.
	VerifyIDToken func(context.Context, string) (map[string]any, error)
}

type OAuth2AuthProvider struct {
	Definition OAuth2ProviderDefinition
	HTTPClient *http.Client
}

func (p *OAuth2AuthProvider) RevocationTokenType() string {
	if p.Definition.RevocationTokenType != "" {
		return p.Definition.RevocationTokenType
	}
	return "access_token"
}

func (p *OAuth2AuthProvider) ProviderCapabilities() AuthProviderCapabilities {
	return AuthProviderCapabilities{Refresh: p.Definition.RefreshSupported, Revocation: p.Definition.RevocationEndpoint != ""}
}

func (p *OAuth2AuthProvider) client() *http.Client {
	if p.HTTPClient != nil {
		return p.HTTPClient
	}
	return http.DefaultClient
}

func (p *OAuth2AuthProvider) AuthorizationURL(_ context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	u, err := url.Parse(p.Definition.AuthorizationEndpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", request.ClientID)
	q.Set("redirect_uri", request.RedirectURI)
	q.Set("state", request.State)
	if request.Nonce != "" {
		q.Set("nonce", request.Nonce)
	}
	if len(request.Scopes) > 0 {
		q.Set("scope", strings.Join(request.Scopes, " "))
	}
	if request.CodeChallenge != "" {
		q.Set("code_challenge", request.CodeChallenge)
		q.Set("code_challenge_method", request.CodeChallengeMethod)
	}
	for key, value := range p.Definition.AuthorizationParameters {
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (p *OAuth2AuthProvider) Exchange(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	result, err := p.ExchangeGrant(ctx, request)
	return result.Identity, err
}

func (p *OAuth2AuthProvider) ExchangeGrant(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderExchangeResult, error) {
	form := url.Values{"grant_type": {"authorization_code"}, "code": {request.Code}, "redirect_uri": {request.RedirectURI}}
	if request.CodeVerifier != "" {
		form.Set("code_verifier", request.CodeVerifier)
	}
	tokens, err := p.tokenRequest(ctx, form, request.ClientID, request.ClientSecret)
	if err != nil {
		return AuthProviderExchangeResult{}, err
	}
	if len(tokens.Scopes) == 0 {
		tokens.Scopes = canonicalStrings(request.RequestedScopes)
	}
	verifiedSubject := ""
	if p.Definition.VerifyIDToken != nil {
		claims, verifyErr := p.Definition.VerifyIDToken(ctx, tokens.IDToken)
		if verifyErr != nil {
			return AuthProviderExchangeResult{}, verifyErr
		}
		audience := strings.ReplaceAll(p.Definition.Audience, "{client_id}", request.ClientID)
		if audience == "" {
			audience = request.ClientID
		}
		if err := validateOIDCClaims(claims, p.Definition.Issuer, audience, request.Nonce, time.Now()); err != nil {
			return AuthProviderExchangeResult{}, err
		}
		verifiedSubject = claimString(claims["sub"])
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, p.Definition.UserInfoEndpoint, nil)
	if err != nil {
		return AuthProviderExchangeResult{}, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	for key, value := range p.Definition.UserInfoHeaders {
		httpRequest.Header.Set(key, strings.ReplaceAll(value, "{client_id}", request.ClientID))
	}
	response, err := p.client().Do(httpRequest)
	if err != nil {
		return AuthProviderExchangeResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return AuthProviderExchangeResult{}, fmt.Errorf("provider user-info status %d", response.StatusCode)
	}
	var claims map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&claims); err != nil {
		return AuthProviderExchangeResult{}, err
	}
	if claimString(claims[p.Definition.UserInfoSubjectClaim]) == "" {
		switch nested := claims["data"].(type) {
		case map[string]any:
			claims = nested
		case []any:
			if len(nested) > 0 {
				if first, ok := nested[0].(map[string]any); ok {
					claims = first
				}
			}
		}
	}
	subject := claimString(claims[p.Definition.UserInfoSubjectClaim])
	if subject == "" {
		return AuthProviderExchangeResult{}, fmt.Errorf("provider user-info omitted subject")
	}
	if verifiedSubject != "" && subject != verifiedSubject {
		return AuthProviderExchangeResult{}, fmt.Errorf("OIDC user-info subject mismatch")
	}
	profileHandle, profileURL := normalizedProfilePair(claimString(claimPath(claims, p.Definition.ProfileHandleClaim)), claimPath(claims, p.Definition.ProfileURLClaim))
	identity := AuthProviderIdentity{Provider: request.Provider, Issuer: p.Definition.Issuer, Subject: subject, DisplayName: claimString(claims[p.Definition.DisplayNameClaim]), AvatarURL: normalizedAvatarURL(claimPath(claims, p.Definition.AvatarURLClaim)), ProfileHandle: profileHandle, ProfileURL: profileURL, Email: claimString(claims[p.Definition.EmailClaim]), EmailVerified: claimBool(claims[p.Definition.EmailVerifiedClaim])}
	return AuthProviderExchangeResult{Identity: identity, Tokens: tokens, GrantedScopes: tokens.Scopes, Capabilities: p.ProviderCapabilities()}, nil
}

func (p *OAuth2AuthProvider) tokenRequest(ctx context.Context, form url.Values, clientID, clientSecret string) (AuthProviderTokenSet, error) {
	if p.Definition.ClientAuthStyle != AuthProviderClientSecretBasic {
		form.Set("client_id", clientID)
		form.Set("client_secret", clientSecret)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Definition.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return AuthProviderTokenSet{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	if p.Definition.ClientAuthStyle == AuthProviderClientSecretBasic {
		request.SetBasicAuth(clientID, clientSecret)
	}
	response, err := p.client().Do(request)
	if err != nil {
		return AuthProviderTokenSet{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var payload struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&payload)
		return AuthProviderTokenSet{}, &AuthProviderUpstreamError{Code: payload.Error, Terminal: payload.Error == "invalid_grant"}
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int64  `json:"expires_in"`
		Scope        any    `json:"scope"`
		IDToken      string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return AuthProviderTokenSet{}, err
	}
	if payload.AccessToken == "" {
		return AuthProviderTokenSet{}, fmt.Errorf("provider token response omitted access token")
	}
	scopes := []string{}
	switch value := payload.Scope.(type) {
	case string:
		scopes = strings.Fields(strings.ReplaceAll(value, ",", " "))
	case []any:
		for _, item := range value {
			scopes = append(scopes, claimString(item))
		}
	}
	tokens := AuthProviderTokenSet{AccessToken: payload.AccessToken, RefreshToken: payload.RefreshToken, TokenType: payload.TokenType, Scopes: canonicalStrings(scopes), IDToken: payload.IDToken}
	if payload.ExpiresIn > 0 {
		tokens.ExpiresAt = time.Now().Add(time.Duration(payload.ExpiresIn) * time.Second)
	}
	return tokens, nil
}

func validateOIDCClaims(claims map[string]any, issuer, audience, nonce string, now time.Time) error {
	if claimString(claims["iss"]) != issuer || claimString(claims["sub"]) == "" {
		return fmt.Errorf("OIDC issuer or subject mismatch")
	}
	audienceMatched := false
	switch value := claims["aud"].(type) {
	case string:
		audienceMatched = value == audience
	case []any:
		for _, item := range value {
			if claimString(item) == audience {
				audienceMatched = true
			}
		}
	}
	if !audienceMatched {
		return fmt.Errorf("OIDC audience mismatch")
	}
	if nonce != "" && claimString(claims["nonce"]) != nonce {
		return fmt.Errorf("OIDC nonce mismatch")
	}
	expiry := providerUnix(claims["exp"])
	if expiry <= now.Unix() {
		return fmt.Errorf("OIDC token expired")
	}
	return nil
}

func (p *OAuth2AuthProvider) RefreshGrant(ctx context.Context, request AuthProviderRefreshRequest) (AuthProviderTokenSet, error) {
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {request.RefreshToken}}
	if len(request.Scopes) > 0 {
		form.Set("scope", strings.Join(request.Scopes, " "))
	}
	return p.tokenRequest(ctx, form, request.ClientID, request.ClientSecret)
}

func (p *OAuth2AuthProvider) RevokeGrant(ctx context.Context, request AuthProviderRevokeRequest) error {
	if p.Definition.RevocationEndpoint == "" {
		return fmt.Errorf("provider does not publish a revocation endpoint")
	}
	if p.Definition.RevocationStyle == "github_grant_delete" {
		endpoint := strings.ReplaceAll(p.Definition.RevocationEndpoint, "{client_id}", url.PathEscape(request.ClientID))
		payload, _ := json.Marshal(map[string]string{"access_token": request.Token})
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Accept", "application/vnd.github+json")
		httpRequest.SetBasicAuth(request.ClientID, request.ClientSecret)
		response, err := p.client().Do(httpRequest)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("provider revocation status %d", response.StatusCode)
		}
		return nil
	}
	if p.Definition.RevocationStyle == "bearer_delete" {
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, p.Definition.RevocationEndpoint, nil)
		if err != nil {
			return err
		}
		httpRequest.Header.Set("Authorization", "Bearer "+request.Token)
		response, err := p.client().Do(httpRequest)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("provider revocation status %d", response.StatusCode)
		}
		return nil
	}
	form := url.Values{"token": {request.Token}}
	if request.TokenTypeHint != "" {
		form.Set("token_type_hint", request.TokenTypeHint)
	}
	if p.Definition.ClientAuthStyle != AuthProviderClientSecretBasic {
		form.Set("client_id", request.ClientID)
		if !p.Definition.OmitClientSecretOnRevoke {
			form.Set("client_secret", request.ClientSecret)
		}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Definition.RevocationEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if p.Definition.ClientAuthStyle == AuthProviderClientSecretBasic {
		httpRequest.SetBasicAuth(request.ClientID, request.ClientSecret)
	}
	response, err := p.client().Do(httpRequest)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("provider revocation status %d", response.StatusCode)
	}
	return nil
}

func claimString(value any) string {
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return value.String()
	case float64:
		return strconv.FormatInt(int64(value), 10)
	}
	return ""
}
func claimBool(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		parsed, _ := strconv.ParseBool(value)
		return parsed
	}
	return false
}

func claimPath(claims map[string]any, path string) any {
	if path == "" {
		return nil
	}
	var value any = claims
	for _, segment := range strings.Split(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value, ok = object[segment]
		if !ok {
			return nil
		}
	}
	return value
}

func normalizedAvatarURL(value any) string {
	raw, ok := value.(string)
	if !ok {
		return ""
	}
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || (!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return ""
	}
	return raw
}

func normalizedProfilePair(handle string, value any) (string, string) {
	raw, ok := value.(string)
	if !ok {
		return "", ""
	}
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || (!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return "", ""
	}
	return strings.TrimSpace(handle), raw
}

// BuiltinOAuth2ProviderDefinition returns Flop's reviewed standard protocol
// definition. YouTube deliberately has no entry; its scopes belong to Google.
func BuiltinOAuth2ProviderDefinition(provider string) (OAuth2ProviderDefinition, bool) {
	definitions := map[string]OAuth2ProviderDefinition{
		"discord":  {AuthorizationEndpoint: "https://discord.com/oauth2/authorize", TokenEndpoint: "https://discord.com/api/v10/oauth2/token", UserInfoEndpoint: "https://discord.com/api/v10/users/@me", RevocationEndpoint: "https://discord.com/api/v10/oauth2/token/revoke", RevocationTokenType: "refresh_token", RefreshSupported: true, Issuer: "https://discord.com", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "id", DisplayNameClaim: "username", EmailClaim: "email", EmailVerifiedClaim: "verified"},
		"twitch":   {AuthorizationEndpoint: "https://id.twitch.tv/oauth2/authorize", TokenEndpoint: "https://id.twitch.tv/oauth2/token", UserInfoEndpoint: "https://api.twitch.tv/helix/users", RevocationEndpoint: "https://id.twitch.tv/oauth2/revoke", OmitClientSecretOnRevoke: true, RefreshSupported: true, Issuer: "https://id.twitch.tv/oauth2", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "id", DisplayNameClaim: "display_name", AvatarURLClaim: "profile_image_url", EmailClaim: "email", UserInfoHeaders: map[string]string{"Client-Id": "{client_id}"}},
		"github":   {AuthorizationEndpoint: "https://github.com/login/oauth/authorize", TokenEndpoint: "https://github.com/login/oauth/access_token", UserInfoEndpoint: "https://api.github.com/user", RevocationEndpoint: "https://api.github.com/applications/{client_id}/grant", RevocationStyle: "github_grant_delete", Issuer: "https://github.com", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "id", DisplayNameClaim: "login", AvatarURLClaim: "avatar_url", ProfileHandleClaim: "login", ProfileURLClaim: "html_url", EmailClaim: "email"},
		"google":   {AuthorizationEndpoint: "https://accounts.google.com/o/oauth2/v2/auth", TokenEndpoint: "https://oauth2.googleapis.com/token", UserInfoEndpoint: "https://openidconnect.googleapis.com/v1/userinfo", RevocationEndpoint: "https://oauth2.googleapis.com/revoke", RevocationTokenType: "refresh_token", RefreshSupported: true, Issuer: "https://accounts.google.com", Audience: "{client_id}", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "sub", DisplayNameClaim: "name", AvatarURLClaim: "picture", EmailClaim: "email", EmailVerifiedClaim: "email_verified", AuthorizationParameters: map[string]string{"access_type": "offline", "include_granted_scopes": "true"}},
		"facebook": {AuthorizationEndpoint: "https://www.facebook.com/dialog/oauth", TokenEndpoint: "https://graph.facebook.com/oauth/access_token", UserInfoEndpoint: "https://graph.facebook.com/me?fields=id,name,email,picture", RevocationEndpoint: "https://graph.facebook.com/me/permissions", RevocationStyle: "bearer_delete", Issuer: "https://www.facebook.com", ClientAuthStyle: AuthProviderClientSecretPost, UserInfoSubjectClaim: "id", DisplayNameClaim: "name", AvatarURLClaim: "picture.data.url", EmailClaim: "email"},
		"x":        {AuthorizationEndpoint: "https://x.com/i/oauth2/authorize", TokenEndpoint: "https://api.x.com/2/oauth2/token", UserInfoEndpoint: "https://api.x.com/2/users/me?user.fields=profile_image_url", RevocationEndpoint: "https://api.x.com/2/oauth2/revoke", RevocationTokenType: "refresh_token", RefreshSupported: true, Issuer: "https://x.com", ClientAuthStyle: AuthProviderClientSecretBasic, UserInfoSubjectClaim: "id", DisplayNameClaim: "username", AvatarURLClaim: "profile_image_url"},
	}
	definition, ok := definitions[provider]
	return definition, ok
}

type SteamOpenIDProvider struct {
	HTTPClient *http.Client
	Endpoint   string
}

// DraugiemPassportProvider implements Draugiem.lv's reviewed Passport
// protocol. ClientID is the public application ID and ClientSecret is the
// application API key. The returned per-user API key is handled as an access
// token and never exposed to a browser.
type DraugiemPassportProvider struct {
	HTTPClient                         *http.Client
	AuthorizationEndpoint, APIEndpoint string
}

func (p *DraugiemPassportProvider) CallbackCodeOptional() bool { return true }
func (p *DraugiemPassportProvider) ProviderCapabilities() AuthProviderCapabilities {
	return AuthProviderCapabilities{Refresh: false, Revocation: false}
}
func (p *DraugiemPassportProvider) authorizationEndpoint() string {
	if p.AuthorizationEndpoint != "" {
		return p.AuthorizationEndpoint
	}
	return "https://api.draugiem.lv/authorize/"
}
func (p *DraugiemPassportProvider) apiEndpoint() string {
	if p.APIEndpoint != "" {
		return p.APIEndpoint
	}
	return "https://api.draugiem.lv/json/"
}
func (p *DraugiemPassportProvider) AuthorizationURL(_ context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	redirect, err := url.Parse(request.RedirectURI)
	if err != nil {
		return "", err
	}
	query := redirect.Query()
	query.Set("state", request.State)
	redirect.RawQuery = query.Encode()
	sum := md5.Sum([]byte(request.ClientSecret + redirect.String()))
	target, _ := url.Parse(p.authorizationEndpoint())
	values := target.Query()
	values.Set("app", request.ClientID)
	values.Set("hash", hex.EncodeToString(sum[:]))
	values.Set("redirect", redirect.String())
	target.RawQuery = values.Encode()
	return target.String(), nil
}
func (p *DraugiemPassportProvider) Exchange(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	result, err := p.ExchangeGrant(ctx, request)
	return result.Identity, err
}
func (p *DraugiemPassportProvider) ExchangeGrant(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderExchangeResult, error) {
	code := request.Code
	if code == "" {
		code = request.Parameters.Get("dr_auth_code")
	}
	if code == "" || request.Parameters.Get("dr_auth_status") == "failed" {
		return AuthProviderExchangeResult{}, fmt.Errorf("Draugiem authorization failed")
	}
	form := url.Values{"action": {"authorize"}, "app": {request.ClientSecret}, "code": {code}}
	httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.apiEndpoint(), strings.NewReader(form.Encode()))
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return AuthProviderExchangeResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return AuthProviderExchangeResult{}, fmt.Errorf("Draugiem API status %d", response.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return AuthProviderExchangeResult{}, err
	}
	profile := firstProviderProfile(payload["users"])
	subject := claimString(payload["uid"])
	if subject == "" {
		subject = claimString(profile["uid"])
	}
	apiKey := claimString(payload["apikey"])
	if subject == "" || apiKey == "" {
		return AuthProviderExchangeResult{}, fmt.Errorf("Draugiem response omitted identity or API key")
	}
	display := strings.TrimSpace(claimString(profile["name"]) + " " + claimString(profile["surname"]))
	tokens := AuthProviderTokenSet{AccessToken: apiKey, TokenType: "DraugiemAPIKey", Scopes: canonicalStrings(request.RequestedScopes)}
	return AuthProviderExchangeResult{Identity: AuthProviderIdentity{Provider: request.Provider, Issuer: "https://www.draugiem.lv", Subject: subject, DisplayName: display}, Tokens: tokens, GrantedScopes: tokens.Scopes}, nil
}
func firstProviderProfile(value any) map[string]any {
	switch value := value.(type) {
	case []any:
		if len(value) > 0 {
			if profile, ok := value[0].(map[string]any); ok {
				return profile
			}
		}
	case map[string]any:
		for _, item := range value {
			if profile, ok := item.(map[string]any); ok {
				return profile
			}
		}
	}
	return map[string]any{}
}
func (p *DraugiemPassportProvider) RefreshGrant(context.Context, AuthProviderRefreshRequest) (AuthProviderTokenSet, error) {
	return AuthProviderTokenSet{}, &AuthProviderUpstreamError{Code: "reauthorization_required", Terminal: true}
}
func (p *DraugiemPassportProvider) RevokeGrant(context.Context, AuthProviderRevokeRequest) error {
	return fmt.Errorf("Draugiem Passport does not support remote revocation")
}

func (p *SteamOpenIDProvider) endpoint() string {
	if p.Endpoint != "" {
		return p.Endpoint
	}
	return "https://steamcommunity.com/openid/login"
}
func (p *SteamOpenIDProvider) CallbackCodeOptional() bool { return true }
func (p *SteamOpenIDProvider) AuthorizationURL(_ context.Context, request AuthProviderAuthorizationRequest) (string, error) {
	u, _ := url.Parse(p.endpoint())
	q := u.Query()
	q.Set("openid.ns", "http://specs.openid.net/auth/2.0")
	q.Set("openid.mode", "checkid_setup")
	returnTo, _ := url.Parse(request.RedirectURI)
	returnQuery := returnTo.Query()
	returnQuery.Set("state", request.State)
	returnTo.RawQuery = returnQuery.Encode()
	q.Set("openid.return_to", returnTo.String())
	q.Set("openid.realm", originOf(request.RedirectURI))
	q.Set("openid.identity", "http://specs.openid.net/auth/2.0/identifier_select")
	q.Set("openid.claimed_id", "http://specs.openid.net/auth/2.0/identifier_select")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
func (p *SteamOpenIDProvider) Exchange(ctx context.Context, request AuthProviderCallbackRequest) (AuthProviderIdentity, error) {
	values := cloneURLValues(request.Parameters)
	claimed := values.Get("openid.claimed_id")
	const claimedPrefix = "http://steamcommunity.com/openid/id/"
	if !strings.HasPrefix(claimed, claimedPrefix) || values.Get("openid.identity") != claimed {
		return AuthProviderIdentity{}, fmt.Errorf("invalid Steam claimed identity")
	}
	if values.Get("openid.op_endpoint") != p.endpoint() {
		return AuthProviderIdentity{}, fmt.Errorf("invalid Steam OpenID endpoint")
	}
	expectedReturnTo, err := url.Parse(request.RedirectURI)
	if err != nil {
		return AuthProviderIdentity{}, fmt.Errorf("invalid Steam return URL")
	}
	expectedQuery := expectedReturnTo.Query()
	expectedQuery.Set("state", request.Parameters.Get("state"))
	expectedReturnTo.RawQuery = expectedQuery.Encode()
	if values.Get("openid.return_to") != expectedReturnTo.String() {
		return AuthProviderIdentity{}, fmt.Errorf("invalid Steam return URL")
	}
	values.Set("openid.mode", "check_authentication")
	values.Del("state")
	values.Del("provider")
	httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint(), strings.NewReader(values.Encode()))
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := p.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return AuthProviderIdentity{}, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode != 200 || !strings.Contains(string(body), "is_valid:true") {
		return AuthProviderIdentity{}, fmt.Errorf("Steam OpenID assertion rejected")
	}
	subject := strings.TrimPrefix(claimed, claimedPrefix)
	if _, err := strconv.ParseUint(subject, 10, 64); err != nil {
		return AuthProviderIdentity{}, fmt.Errorf("invalid Steam subject")
	}
	return AuthProviderIdentity{Provider: request.Provider, Issuer: "https://steamcommunity.com/openid", Subject: subject}, nil
}
func originOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

type BuiltinAuthProviderOptions struct {
	ClientID, ClientSecret, RedirectURI                             string
	AllowedReturnURLs, AllowedScopes, DefaultScopes, RequiredScopes []string
	HTTPClient                                                      *http.Client
}

func BuiltinAuthProviderConfig(provider string, options BuiltinAuthProviderOptions) (AuthProviderConfig, error) {
	if provider == "youtube" {
		return AuthProviderConfig{}, fmt.Errorf("YouTube is an incremental Google grant, not a provider")
	}
	if provider == "steam" {
		return AuthProviderConfig{Adapter: &SteamOpenIDProvider{HTTPClient: options.HTTPClient}, Issuer: "https://steamcommunity.com/openid", RedirectURI: options.RedirectURI, AllowedReturnURLs: options.AllowedReturnURLs, PKCEUnsupported: true}, nil
	}
	if provider == "draugiem" {
		return AuthProviderConfig{Adapter: &DraugiemPassportProvider{HTTPClient: options.HTTPClient}, Issuer: "https://www.draugiem.lv", RedirectURI: options.RedirectURI, AllowedReturnURLs: options.AllowedReturnURLs, ClientID: options.ClientID, ClientSecret: options.ClientSecret, AllowedScopes: options.AllowedScopes, DefaultScopes: options.DefaultScopes, RequiredScopes: options.RequiredScopes, PKCEUnsupported: true}, nil
	}
	definition, ok := BuiltinOAuth2ProviderDefinition(provider)
	if !ok {
		return AuthProviderConfig{}, fmt.Errorf("unknown built-in provider %q", provider)
	}
	return AuthProviderConfig{Adapter: &OAuth2AuthProvider{Definition: definition, HTTPClient: options.HTTPClient}, Issuer: definition.Issuer, RedirectURI: options.RedirectURI, AllowedReturnURLs: options.AllowedReturnURLs, ClientID: options.ClientID, ClientSecret: options.ClientSecret, AllowedScopes: options.AllowedScopes, DefaultScopes: options.DefaultScopes, RequiredScopes: options.RequiredScopes}, nil
}
