package flop

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/jsonx"
	"github.com/marcisbee/flop/internal/schema"
)

const (
	providerFlowTTL              = 10 * time.Minute
	providerCompletionTTL        = 5 * time.Minute
	providerOutstandingFlowLimit = 1024

	providerFlowPhaseAuthorizationProcessing = "authorization_processing"
	providerFlowPhaseStarted                 = "started"
	providerFlowPhaseCallbackProcessing      = "callback_processing"
	providerFlowPhaseResultReady             = "result_ready"
	providerFlowPhaseConsumed                = "consumed"
)

// AuthProviderError is a stable provider-auth failure. Cause is retained for
// server-side diagnosis but Error never exposes provider response details.
type AuthProviderError struct {
	Code    string
	Message string
	Status  int
	Cause   error
}

func (e *AuthProviderError) Error() string {
	if e == nil {
		return "provider authentication failed"
	}
	if e.Message != "" {
		return e.Message
	}
	return "provider authentication failed"
}

func (e *AuthProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func providerError(code, message string, status int, cause ...error) error {
	var wrapped error
	if len(cause) > 0 {
		wrapped = cause[0]
	}
	return &AuthProviderError{Code: code, Message: message, Status: status, Cause: wrapped}
}

type providerAuthService struct {
	db             *Database
	providers      map[string]AuthProviderConfig
	apps           map[string]AuthProviderAppConfig
	aeadMu         sync.RWMutex
	aead           cipher.AEAD
	tokenAEAD      map[string]cipher.AEAD
	activeKey      string
	initErr        error
	now            func() time.Time
	random         io.Reader
	maxFlows       int
	mu             sync.Locker
	grantLocks     sync.Map
	unusableGrants sync.Map
}

type providerFlowSecrets struct {
	Nonce        string `json:"nonce"`
	PKCEVerifier string `json:"pkceVerifier,omitempty"`
}

type providerStartResult struct {
	AuthorizationURL string
}

type providerCallbackResult struct {
	CompletionCode string
	Status         string
	ReturnTo       string
}

type providerCompleteResult struct {
	Token        string
	RefreshToken string
	Auth         *schema.AuthContext
	Linked       *providerIdentityView
	Grant        *providerGrantView
}

type providerIdentityView struct {
	ID                  string `json:"id"`
	Provider            string `json:"provider"`
	Issuer              string `json:"issuer"`
	DisplayName         string `json:"displayName,omitempty"`
	Email               string `json:"email,omitempty"`
	EmailVerified       bool   `json:"emailVerified"`
	LinkedAt            int64  `json:"linkedAt"`
	LastAuthenticatedAt int64  `json:"lastAuthenticatedAt"`
}

type providerDescriptor struct {
	Key            string                   `json:"key"`
	Issuer         string                   `json:"issuer"`
	PKCES256       bool                     `json:"pkceS256"`
	Scopes         []string                 `json:"scopes,omitempty"`
	DefaultScopes  []string                 `json:"defaultScopes,omitempty"`
	RequiredScopes []string                 `json:"requiredScopes,omitempty"`
	Capabilities   AuthProviderCapabilities `json:"capabilities"`
}

func newProviderAuthService(db *Database, providers map[string]AuthProviderConfig, apps map[string]AuthProviderAppConfig, keys map[string][]byte, activeKey, secret string) *providerAuthService {
	aead, err := newProviderFlowAEAD(secret)
	if err != nil {
		return nil
	}
	registered := make(map[string]AuthProviderConfig, len(providers))
	for provider, config := range providers {
		registered[provider] = config
	}
	normalizedApps := make(map[string]AuthProviderAppConfig, len(apps)+1)
	for appID, config := range apps {
		normalizedApps[appID] = cloneProviderAppConfig(config)
	}
	if len(providers) > 0 {
		returns := []string{}
		for _, config := range providers {
			returns = append(returns, config.AllowedReturnURLs...)
		}
		normalizedApps["legacy"] = AuthProviderAppConfig{AllowedReturnURLs: canonicalStrings(returns), Providers: registered}
	}
	tokenAEAD := make(map[string]cipher.AEAD, len(keys))
	for version, raw := range keys {
		if len(raw) != 32 {
			continue
		}
		keyBlock, keyErr := aes.NewCipher(raw)
		if keyErr != nil {
			continue
		}
		value, keyErr := cipher.NewGCM(keyBlock)
		if keyErr == nil {
			tokenAEAD[version] = value
		}
	}
	locker := &sync.Mutex{}
	service := &providerAuthService{
		db:        db,
		providers: registered,
		apps:      normalizedApps,
		aead:      aead,
		tokenAEAD: tokenAEAD,
		activeKey: activeKey,
		now:       time.Now,
		random:    rand.Reader,
		maxFlows:  providerOutstandingFlowLimit,
		mu:        locker,
	}
	if db != nil && db.authService != nil {
		db.authService.SetProviderSessionLocker(locker)
	}
	if db != nil {
		service.initErr = service.syncRegistrations()
	}
	return service
}

func newProviderFlowAEAD(secret string) (cipher.AEAD, error) {
	key := sha256.Sum256([]byte("flop/provider-flow/aead/v1\x00" + secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aead, nil
}

func (s *providerAuthService) rekeyFlowCipher(secret string) {
	aead, err := newProviderFlowAEAD(secret)
	if err != nil {
		return
	}
	s.aeadMu.Lock()
	s.aead = aead
	s.aeadMu.Unlock()
}

func cloneProviderAppConfig(in AuthProviderAppConfig) AuthProviderAppConfig {
	out := in
	out.AllowedReturnURLs = append([]string(nil), in.AllowedReturnURLs...)
	out.BackendCredentials = append([]string(nil), in.BackendCredentials...)
	out.Providers = make(map[string]AuthProviderConfig, len(in.Providers))
	for key, config := range in.Providers {
		out.Providers[key] = config
	}
	return out
}

func validateAuthProviderConfigs(providers map[string]AuthProviderConfig) error {
	issuerOwners := make(map[string]string, len(providers))
	for provider, config := range providers {
		if provider == "" || provider != strings.TrimSpace(provider) {
			return fmt.Errorf("flop: auth provider key must be non-empty and canonical")
		}
		if config.Adapter == nil {
			return fmt.Errorf("flop: auth provider %q adapter is required", provider)
		}
		if config.Issuer == "" || config.Issuer != strings.TrimSpace(config.Issuer) {
			return fmt.Errorf("flop: auth provider %q issuer must be non-empty and canonical", provider)
		}
		if existing, ok := issuerOwners[config.Issuer]; ok {
			return fmt.Errorf("flop: auth providers %q and %q use the same issuer %q", existing, provider, config.Issuer)
		}
		issuerOwners[config.Issuer] = provider
		if err := validateConfiguredURL(config.RedirectURI, false); err != nil {
			return fmt.Errorf("flop: auth provider %q redirect URI: %w", provider, err)
		}
		for _, returnTo := range config.AllowedReturnURLs {
			if err := validateConfiguredURL(returnTo, true); err != nil {
				return fmt.Errorf("flop: auth provider %q return URL: %w", provider, err)
			}
		}
	}
	return nil
}

func validateAuthProviderAppConfigs(legacy map[string]AuthProviderConfig, apps map[string]AuthProviderAppConfig, keys map[string][]byte, active string) error {
	clientOwners := map[string]string{}
	needsKey := false
	for provider, config := range legacy {
		if config.ClientID != "" {
			owner := "legacy/" + provider
			if previous, ok := clientOwners[config.ClientID]; ok {
				return fmt.Errorf("flop: provider registrations %q and %q reuse upstream client ID %q", previous, owner, config.ClientID)
			}
			clientOwners[config.ClientID] = owner
		}
		if config.ClientSecret != "" || len(config.AllowedScopes) > 0 {
			needsKey = true
		}
	}
	for appID, app := range apps {
		if appID == "" || appID != strings.TrimSpace(appID) || appID == "legacy" {
			return fmt.Errorf("flop: provider app ID must be non-empty and canonical (legacy is reserved)")
		}
		for _, raw := range app.AllowedReturnURLs {
			if err := validateConfiguredURL(raw, true); err != nil {
				return fmt.Errorf("flop: provider app %q return URL: %w", appID, err)
			}
		}
		if len(app.BackendCredentials) > 0 {
			needsKey = true
		}
		if err := validateAuthProviderConfigs(app.Providers); err != nil {
			return fmt.Errorf("flop: provider app %q: %w", appID, err)
		}
		for provider, config := range app.Providers {
			if config.ClientID != "" {
				owner := appID + "/" + provider
				if previous, ok := clientOwners[config.ClientID]; ok {
					return fmt.Errorf("flop: provider registrations %q and %q reuse upstream client ID %q", previous, owner, config.ClientID)
				}
				clientOwners[config.ClientID] = owner
			}
			if config.ClientSecret != "" || len(config.AllowedScopes) > 0 {
				needsKey = true
			}
			allowed := canonicalStrings(config.AllowedScopes)
			for _, scope := range append(append([]string{}, config.DefaultScopes...), config.RequiredScopes...) {
				if !exactStringMatch(allowed, scope) {
					return fmt.Errorf("flop: provider app %q provider %q scope %q is not allowed", appID, provider, scope)
				}
			}
		}
	}
	if needsKey {
		if active == "" || len(keys[active]) != 32 {
			return fmt.Errorf("flop: active provider secret key must select an exactly 32-byte key")
		}
	}
	for version, key := range keys {
		if version == "" || version != strings.TrimSpace(version) || len(key) != 32 {
			return fmt.Errorf("flop: provider secret key %q must have a canonical version and exactly 32 bytes", version)
		}
	}
	return nil
}

func (s *providerAuthService) providerConfig(provider string) (AuthProviderConfig, error) {
	if s == nil {
		return AuthProviderConfig{}, providerError("provider_auth_unavailable", "provider authentication unavailable", 404)
	}
	if provider == "" || provider != strings.TrimSpace(provider) {
		return AuthProviderConfig{}, providerError("invalid_provider", "invalid provider", 400)
	}
	config, ok := s.providers[provider]
	if !ok || config.Adapter == nil {
		return AuthProviderConfig{}, providerError("provider_not_configured", "provider not configured", 404)
	}
	if config.Issuer == "" || config.Issuer != strings.TrimSpace(config.Issuer) {
		return AuthProviderConfig{}, providerError("provider_misconfigured", "provider authentication unavailable", 503)
	}
	if err := validateConfiguredURL(config.RedirectURI, false); err != nil {
		return AuthProviderConfig{}, providerError("provider_misconfigured", "provider authentication unavailable", 503, err)
	}
	for _, returnTo := range config.AllowedReturnURLs {
		if err := validateConfiguredURL(returnTo, true); err != nil {
			return AuthProviderConfig{}, providerError("provider_misconfigured", "provider authentication unavailable", 503, err)
		}
	}
	return config, nil
}

func (s *providerAuthService) descriptors() []providerDescriptor {
	if s == nil {
		return []providerDescriptor{}
	}
	out := make([]providerDescriptor, 0, len(s.providers))
	for key, config := range s.providers {
		out = append(out, providerDescriptor{Key: key, Issuer: config.Issuer, PKCES256: !config.PKCEUnsupported})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (s *providerAuthService) descriptorsForApp(appID string) ([]providerDescriptor, error) {
	app, ok := s.apps[appID]
	if !ok {
		return nil, providerError("app_not_configured", "provider app not configured", 404)
	}
	out := make([]providerDescriptor, 0, len(app.Providers))
	for key, config := range app.Providers {
		_, grantCapable := config.Adapter.(AuthProviderGrantAdapter)
		capabilities := AuthProviderCapabilities{Refresh: grantCapable, Revocation: grantCapable}
		if advertised, ok := config.Adapter.(AuthProviderCapabilityAdapter); ok {
			capabilities = advertised.ProviderCapabilities()
		}
		out = append(out, providerDescriptor{Key: key, Issuer: config.Issuer, PKCES256: !config.PKCEUnsupported, Scopes: canonicalStrings(config.AllowedScopes), DefaultScopes: canonicalStrings(config.DefaultScopes), RequiredScopes: canonicalStrings(config.RequiredScopes), Capabilities: capabilities})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *providerAuthService) configured() bool {
	return s != nil && len(s.apps) > 0 && s.initErr == nil
}

func validateConfiguredURL(raw string, allowFragment bool) error {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return fmt.Errorf("empty or non-canonical URL")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return fmt.Errorf("URL must be absolute")
	}
	if !allowFragment && u.Fragment != "" {
		return fmt.Errorf("URL fragment is not allowed")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && isLoopbackHost(u.Hostname())) {
		return fmt.Errorf("URL must use HTTPS or loopback HTTP")
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func (s *providerAuthService) start(ctx context.Context, provider, intent, returnTo string, auth *AuthContext) (*providerStartResult, error) {
	return s.startForApp(ctx, "", provider, intent, returnTo, nil, "", auth)
}

func (s *providerAuthService) startForApp(ctx context.Context, appID, provider, intent, returnTo string, requestedScopes []string, incrementalGrantID string, auth *AuthContext) (*providerStartResult, error) {
	if appID == "" && len(s.apps) == 1 {
		for key := range s.apps {
			appID = key
		}
	}
	app, config, err := s.appProviderConfig(appID, provider)
	if err != nil {
		return nil, err
	}
	if intent != "sign_in" && intent != "link" && intent != "consent" {
		return nil, providerError("invalid_intent", "intent must be sign_in, link, or consent", 400)
	}
	allowedReturns := canonicalStrings(append(append([]string{}, app.AllowedReturnURLs...), config.AllowedReturnURLs...))
	if returnTo != "" && !exactStringMatch(allowedReturns, returnTo) {
		return nil, providerError("return_not_allowed", "return destination not allowed", 400)
	}
	if intent == "link" || intent == "consent" {
		if auth == nil || auth.ID == "" || auth.SessionID == "" {
			return nil, providerError("authentication_required", "authentication required", 401)
		}
		if err := s.db.authService.ValidateUserSession(auth.ID, auth.SessionID); err != nil {
			return nil, providerError("authentication_required", "authentication required", 401, err)
		}
	}
	scopes := canonicalStrings(requestedScopes)
	if len(scopes) == 0 {
		scopes = canonicalStrings(config.DefaultScopes)
	}
	scopes = canonicalStrings(append(scopes, config.RequiredScopes...))
	allowedScopes := canonicalStrings(config.AllowedScopes)
	if !scopeSubset(scopes, allowedScopes) {
		return nil, providerError("scope_not_allowed", "requested provider scope is not configured", 400)
	}
	if len(scopes) > 0 {
		if _, ok := config.Adapter.(AuthProviderGrantAdapter); !ok {
			return nil, providerError("grant_not_supported", "provider adapter does not support token grants", 400)
		}
		if s.tokenAEAD[s.activeKey] == nil {
			return nil, providerError("provider_key_unavailable", "provider token storage unavailable", 503)
		}
	}
	registration, err := s.registration(appID, provider)
	if err != nil {
		return nil, err
	}
	clientSecret, err := s.registrationClientSecret(registration, config.ClientSecret)
	if err != nil {
		return nil, providerError("provider_key_unavailable", "provider credentials unavailable", 503, err)
	}
	if intent == "consent" {
		grant, grantErr := s.db.Table(systemAuthProviderGrantTableName).Get(incrementalGrantID)
		if grantErr != nil || grant == nil || toString(grant["principal_id"]) != auth.ID || toString(grant["registration_id"]) != toString(registration["id"]) {
			return nil, providerError("grant_not_found", "provider grant not found", 404)
		}
		scopes = canonicalStrings(append(scopes, storedStrings(grant["granted_scopes"])...))
		if !scopeSubset(scopes, allowedScopes) {
			return nil, providerError("scope_not_allowed", "existing grant contains a scope that is no longer configured", 409)
		}
	}

	state, err := s.randomToken(32)
	if err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	nonce, err := s.randomToken(32)
	if err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	verifier := ""
	challenge := ""
	method := ""
	if !config.PKCEUnsupported {
		verifier, err = s.randomToken(32)
		if err != nil {
			return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
		}
		sum := sha256.Sum256([]byte(verifier))
		challenge = base64.RawURLEncoding.EncodeToString(sum[:])
		method = "S256"
	}

	now := s.now().Unix()
	stateHash := hashProviderToken(state)
	sealed := ""
	secretsKeyVersion := ""
	if s.tokenAEAD[s.activeKey] != nil {
		sealed, secretsKeyVersion, err = s.sealProviderValue("flow-secrets", appID, toString(registration["id"]), stateHash, providerFlowSecrets{Nonce: nonce, PKCEVerifier: verifier})
	} else {
		sealed, err = s.sealSecrets(providerFlowSecrets{Nonce: nonce, PKCEVerifier: verifier})
	}
	if err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	row := map[string]any{
		"state_hash":           stateHash,
		"completion_hash":      "pending:" + stateHash,
		"provider":             provider,
		"app_id":               appID,
		"registration_id":      toString(registration["id"]),
		"requested_scopes":     scopes,
		"incremental_grant_id": incrementalGrantID,
		"intent":               intent,
		"secrets_ciphertext":   sealed,
		"secrets_key_version":  secretsKeyVersion,
		"redirect_uri":         config.RedirectURI,
		"return_to":            returnTo,
		"created_at":           now,
		"expires_at":           now + int64(providerFlowTTL/time.Second),
		"phase":                providerFlowPhaseAuthorizationProcessing,
	}
	if intent == "link" || intent == "consent" {
		row["link_principal_id"] = auth.ID
		row["link_session_id"] = auth.SessionID
	}
	flows := s.db.Table(systemAuthProviderFlowTableName)
	s.mu.Lock()
	if s.maxFlows > 0 && flows.Count() >= s.maxFlows {
		if _, err := cleanupExpiredProviderFlowRows(flows, s.now()); err != nil {
			s.mu.Unlock()
			return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
		}
		if flows.Count() >= s.maxFlows {
			s.mu.Unlock()
			return nil, providerError("provider_flow_limit", "provider authentication is temporarily unavailable", 503)
		}
	}
	inserted, err := flows.Insert(row)
	s.mu.Unlock()
	if err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	flowID := toString(inserted["id"])
	authorizationURL, adapterErr := config.Adapter.AuthorizationURL(ctx, AuthProviderAuthorizationRequest{
		AppID: appID, Provider: provider, State: state, Nonce: nonce, RedirectURI: config.RedirectURI,
		CodeChallenge: challenge, CodeChallengeMethod: method, ClientID: config.ClientID, ClientSecret: clientSecret, Scopes: scopes,
	})
	var authorizationURLErr error
	if adapterErr == nil {
		authorizationURLErr = validateConfiguredURL(authorizationURL, false)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	flow, getErr := flows.Get(flowID)
	if getErr != nil || flow == nil || toString(flow["phase"]) != providerFlowPhaseAuthorizationProcessing {
		return nil, providerError("provider_flow_gone", "provider flow is invalid or expired", 410, getErr)
	}
	if providerUnix(flow["expires_at"]) <= s.now().Unix() {
		_, _ = flows.Delete(flowID)
		return nil, providerError("provider_flow_expired", "provider flow expired", 410)
	}
	if adapterErr != nil || authorizationURLErr != nil {
		_, _ = flows.Delete(flowID)
		if adapterErr != nil {
			return nil, providerError("provider_unavailable", "provider authentication unavailable", 502, adapterErr)
		}
		return nil, providerError("provider_unavailable", "provider authentication unavailable", 502, authorizationURLErr)
	}
	if _, err := flows.Update(flowID, map[string]any{"phase": providerFlowPhaseStarted}); err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	return &providerStartResult{AuthorizationURL: authorizationURL}, nil
}

func (s *providerAuthService) callback(ctx context.Context, provider, state, code, providerFailure string, params url.Values) (*providerCallbackResult, error) {
	if state == "" {
		return nil, providerError("invalid_callback", "invalid provider callback", 400)
	}
	s.mu.Lock()
	flows := s.db.Table(systemAuthProviderFlowTableName)
	flow, ok := flows.FindByUniqueIndex("state_hash", hashProviderToken(state))
	if !ok {
		s.mu.Unlock()
		return nil, providerError("provider_flow_gone", "provider flow is invalid or expired", 410)
	}
	now := s.now().Unix()
	if toString(flow["phase"]) != providerFlowPhaseStarted || providerUnix(flow["callback_consumed_at"]) > 0 {
		s.mu.Unlock()
		return nil, providerError("provider_flow_consumed", "provider flow already consumed", 410)
	}
	if providerUnix(flow["expires_at"]) <= now {
		s.mu.Unlock()
		return nil, providerError("provider_flow_expired", "provider flow expired", 410)
	}
	flowProvider := toString(flow["provider"])
	if provider != "" && flowProvider != provider {
		s.mu.Unlock()
		return nil, providerError("provider_mismatch", "provider callback does not match flow", 400)
	}
	provider = flowProvider
	appID := toString(flow["app_id"])
	_, config, err := s.appProviderConfig(appID, provider)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	registration, registrationErr := s.registration(appID, provider)
	if registrationErr != nil || toString(registration["id"]) != toString(flow["registration_id"]) {
		s.mu.Unlock()
		return nil, providerError("provider_flow_invalid", "provider flow is invalid", 400, registrationErr)
	}
	clientSecret, secretErr := s.registrationClientSecret(registration, config.ClientSecret)
	if secretErr != nil {
		s.mu.Unlock()
		return nil, providerError("provider_key_unavailable", "provider credentials unavailable", 503, secretErr)
	}
	if toString(flow["redirect_uri"]) != config.RedirectURI {
		s.mu.Unlock()
		return nil, providerError("provider_flow_invalid", "provider flow is invalid", 400)
	}
	var secrets providerFlowSecrets
	if version := toString(flow["secrets_key_version"]); version != "" {
		err = s.openProviderValue("flow-secrets", appID, toString(flow["registration_id"]), toString(flow["state_hash"]), toString(flow["secrets_ciphertext"]), version, &secrets)
	} else {
		secrets, err = s.openSecrets(toString(flow["secrets_ciphertext"]))
	}
	if err != nil {
		s.mu.Unlock()
		return nil, providerError("provider_flow_invalid", "provider flow is invalid", 400, err)
	}
	flowID := toString(flow["id"])
	if _, err := flows.Update(flowID, map[string]any{
		"phase":                     providerFlowPhaseCallbackProcessing,
		"callback_consumed_at":      now,
		"callback_claim_expires_at": now + int64(providerFlowTTL/time.Second),
	}); err != nil {
		s.mu.Unlock()
		return nil, providerError("provider_flow_failed", "provider callback could not be completed", 500, err)
	}
	if providerFailure != "" {
		result, finalizeErr := s.finalizeCallbackLocked(flows, flowID, "provider_denied", nil, nil)
		s.mu.Unlock()
		return result, finalizeErr
	}
	codeOptional, _ := config.Adapter.(AuthProviderCodelessAdapter)
	if code == "" && (codeOptional == nil || !codeOptional.CallbackCodeOptional()) {
		result, finalizeErr := s.finalizeCallbackLocked(flows, flowID, "invalid_callback", nil, nil)
		s.mu.Unlock()
		return result, finalizeErr
	}
	exchangeRequest := AuthProviderCallbackRequest{
		AppID: appID, Provider: provider, Code: code, RedirectURI: config.RedirectURI,
		CodeVerifier: secrets.PKCEVerifier, Nonce: secrets.Nonce, Parameters: cloneURLValues(params),
		ClientID: config.ClientID, ClientSecret: clientSecret, RequestedScopes: storedStrings(flow["requested_scopes"]),
	}
	s.mu.Unlock()

	var exchange AuthProviderExchangeResult
	var exchangeErr error
	if adapter, ok := config.Adapter.(AuthProviderGrantAdapter); ok {
		exchange, exchangeErr = adapter.ExchangeGrant(ctx, exchangeRequest)
	} else {
		exchange.Identity, exchangeErr = config.Adapter.Exchange(ctx, exchangeRequest)
	}
	identity := exchange.Identity
	s.mu.Lock()
	defer s.mu.Unlock()
	if exchangeErr != nil {
		return s.finalizeCallbackLocked(flows, flowID, "provider_exchange_failed", nil, nil)
	}
	if identity.Provider != provider || identity.Issuer != config.Issuer || identity.Subject == "" || identity.Subject != strings.TrimSpace(identity.Subject) {
		return s.finalizeCallbackLocked(flows, flowID, "provider_identity_invalid", nil, nil)
	}
	requested := storedStrings(flow["requested_scopes"])
	exchange.GrantedScopes = canonicalStrings(append(exchange.GrantedScopes, exchange.Tokens.Scopes...))
	if len(requested) > 0 && exchange.Tokens.AccessToken == "" {
		return s.finalizeCallbackLocked(flows, flowID, "provider_token_invalid", nil, nil)
	}
	if !scopeSubset(exchange.GrantedScopes, config.AllowedScopes) {
		return s.finalizeCallbackLocked(flows, flowID, "provider_scope_invalid", nil, nil)
	}
	if !scopeSubset(exchange.GrantedScopes, requested) {
		return s.finalizeCallbackLocked(flows, flowID, "provider_scope_invalid", nil, nil)
	}
	if len(requested) > 0 && !scopeSubset(config.RequiredScopes, exchange.GrantedScopes) {
		return s.finalizeCallbackLocked(flows, flowID, "required_scope_denied", nil, nil)
	}
	return s.finalizeCallbackLocked(flows, flowID, "", &identity, &exchange)
}

func (s *providerAuthService) finalizeCallbackLocked(flows *TableInstance, flowID, failureCode string, identity *AuthProviderIdentity, exchange *AuthProviderExchangeResult) (*providerCallbackResult, error) {
	flow, err := flows.Get(flowID)
	if err != nil || flow == nil || toString(flow["phase"]) != providerFlowPhaseCallbackProcessing {
		return nil, providerError("provider_flow_gone", "provider flow is invalid or expired", 410, err)
	}
	now := s.now().Unix()
	if providerUnix(flow["callback_claim_expires_at"]) <= now {
		_, _ = flows.Delete(flowID)
		return nil, providerError("provider_flow_expired", "provider flow expired", 410)
	}
	completionCode, err := s.randomToken(32)
	if err != nil {
		return nil, providerError("provider_flow_failed", "provider callback could not be completed", 500, err)
	}
	updates := map[string]any{
		"phase": providerFlowPhaseResultReady, "completion_hash": hashProviderToken(completionCode),
		"completion_expires_at": now + int64(providerCompletionTTL/time.Second),
		"secrets_ciphertext":    "completed", "result_error_code": failureCode,
	}
	status := "success"
	if failureCode != "" {
		status = "failure"
	} else if identity != nil {
		updates["result_provider"] = identity.Provider
		updates["result_issuer"] = identity.Issuer
		updates["result_subject"] = identity.Subject
		updates["result_display_name"] = identity.DisplayName
		updates["result_email"] = identity.Email
		updates["result_email_verified"] = identity.EmailVerified
		if exchange != nil && exchange.Tokens.AccessToken != "" {
			exchange.Tokens.Scopes = exchange.GrantedScopes
			ciphertext, version, sealErr := s.sealProviderValue("flow", toString(flow["app_id"]), toString(flow["registration_id"]), flowID, exchange.Tokens)
			if sealErr != nil {
				return nil, providerError("provider_flow_failed", "provider callback could not be completed", 500, sealErr)
			}
			updates["pending_tokens_ciphertext"] = ciphertext
			updates["pending_tokens_key_version"] = version
		}
		if toString(flow["intent"]) == "sign_in" {
			if linked, ok := s.db.Table(systemAuthIdentityTableName).FindByUniqueCompositeIndex([]string{"issuer", "subject"}, identity.Issuer, identity.Subject); ok {
				updates["resolved_principal_id"] = toString(linked["principal_id"])
			}
		}
	}
	if _, err := flows.Update(flowID, updates); err != nil {
		return nil, providerError("provider_flow_failed", "provider callback could not be completed", 500, err)
	}
	return &providerCallbackResult{CompletionCode: completionCode, Status: status, ReturnTo: toString(flow["return_to"])}, nil
}

func (s *providerAuthService) complete(code string, confirm bool, auth *AuthContext) (*providerCompleteResult, error) {
	if code == "" {
		return nil, providerError("completion_code_required", "completion code required", 400)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	flows := s.db.Table(systemAuthProviderFlowTableName)
	flow, ok := flows.FindByUniqueIndex("completion_hash", hashProviderToken(code))
	if !ok {
		return nil, providerError("completion_gone", "completion code is invalid or expired", 410)
	}
	now := s.now().Unix()
	if toString(flow["phase"]) != providerFlowPhaseResultReady {
		return nil, providerError("completion_consumed", "completion code already consumed", 410)
	}
	if providerUnix(flow["completion_consumed_at"]) > 0 {
		return nil, providerError("completion_consumed", "completion code already consumed", 410)
	}
	if providerUnix(flow["completion_expires_at"]) <= now {
		return nil, providerError("completion_expired", "completion code expired", 410)
	}
	if failureCode := toString(flow["result_error_code"]); failureCode != "" {
		_, _ = flows.Update(toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now})
		switch failureCode {
		case "provider_denied":
			return nil, providerError(failureCode, "provider authorization was denied", 401)
		case "provider_exchange_failed":
			return nil, providerError(failureCode, "provider authentication failed", 502)
		default:
			return nil, providerError(failureCode, "provider authentication failed", 400)
		}
	}
	identity := AuthProviderIdentity{
		Provider: toString(flow["result_provider"]), Issuer: toString(flow["result_issuer"]), Subject: toString(flow["result_subject"]),
		DisplayName: toString(flow["result_display_name"]), Email: toString(flow["result_email"]),
		EmailVerified: providerBool(flow["result_email_verified"]),
	}
	if identity.Provider == "" || identity.Issuer == "" || identity.Subject == "" {
		return nil, providerError("provider_identity_invalid", "provider authentication failed", 400)
	}
	intent := toString(flow["intent"])
	if intent == "link" {
		return s.completeLink(flow, identity, confirm, auth, now)
	}
	if intent == "consent" {
		return s.completeConsent(flow, identity, confirm, auth, now)
	}
	if intent != "sign_in" {
		return nil, providerError("provider_flow_invalid", "provider flow is invalid", 400)
	}
	return s.completeSignIn(flow, identity, now)
}

func (s *providerAuthService) completeSignIn(flow map[string]any, identity AuthProviderIdentity, now int64) (*providerCompleteResult, error) {
	identities := s.db.Table(systemAuthIdentityTableName)
	expectedPrincipalID := toString(flow["resolved_principal_id"])
	linked, ok := identities.FindByUniqueCompositeIndex([]string{"issuer", "subject"}, identity.Issuer, identity.Subject)
	if expectedPrincipalID == "" {
		_, _ = s.db.Table(systemAuthProviderFlowTableName).Update(toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now})
		return nil, providerError("link_required", "provider identity must be linked to an authenticated account", 409)
	}
	if !ok || toString(linked["principal_id"]) != expectedPrincipalID {
		_, _ = s.db.Table(systemAuthProviderFlowTableName).Update(toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now})
		return nil, providerError("principal_unavailable", "account is unavailable", 401)
	}
	tx := newProviderTxn(s.db)
	if err := tx.update(s.db.Table(systemAuthProviderFlowTableName), toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now}); err != nil {
		tx.abort()
		return nil, providerError("provider_flow_failed", "provider authentication failed", 500, err)
	}
	grant, err := s.materializeGrant(tx, flow, linked)
	if err != nil {
		tx.abort()
		return nil, providerError("provider_grant_failed", "provider grant could not be saved", 500, err)
	}
	if err := tx.update(identities, toString(linked["id"]), map[string]any{
		"provider": identity.Provider, "display_name": identity.DisplayName, "email": identity.Email,
		"email_verified": identity.EmailVerified, "last_authenticated_at": now,
	}); err != nil {
		tx.abort()
		return nil, providerError("provider_flow_failed", "provider authentication failed", 500, err)
	}
	token, refreshToken, appAuth, sessionID, err := s.db.authService.CreateProviderSession(toString(linked["principal_id"]), toString(linked["id"]), tx.txBuf)
	if err != nil {
		tx.abort()
		return nil, providerError("principal_unavailable", "account is unavailable", 401, err)
	}
	tx.addInserted(s.db.Table(systemAuthSessionTableName), sessionID)
	if err := tx.commit(); err != nil {
		return nil, providerError("provider_flow_failed", "provider authentication failed", 500, err)
	}
	return &providerCompleteResult{Token: token, RefreshToken: refreshToken, Auth: appAuth, Grant: grant}, nil
}

func (s *providerAuthService) completeLink(flow map[string]any, identity AuthProviderIdentity, confirm bool, auth *AuthContext, now int64) (*providerCompleteResult, error) {
	if !confirm {
		return nil, providerError("confirmation_required", "explicit confirmation required", 400)
	}
	principalID := toString(flow["link_principal_id"])
	sessionID := toString(flow["link_session_id"])
	if auth == nil || auth.ID != principalID || auth.SessionID != sessionID {
		return nil, providerError("link_session_changed", "the original authenticated session is required", 401)
	}
	if err := s.db.authService.ValidateUserSession(principalID, sessionID); err != nil {
		return nil, providerError("link_session_changed", "the original authenticated session is required", 401, err)
	}
	identities := s.db.Table(systemAuthIdentityTableName)
	row, exists := identities.FindByUniqueCompositeIndex([]string{"issuer", "subject"}, identity.Issuer, identity.Subject)
	if exists && toString(row["principal_id"]) != principalID {
		_, _ = s.db.Table(systemAuthProviderFlowTableName).Update(toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now})
		return nil, providerError("identity_already_linked", "provider identity is already linked", 409)
	}
	tx := newProviderTxn(s.db)
	if err := tx.update(s.db.Table(systemAuthProviderFlowTableName), toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now}); err != nil {
		tx.abort()
		return nil, providerError("provider_flow_failed", "provider identity could not be linked", 500, err)
	}
	if !exists {
		var err error
		row, err = tx.insert(identities, map[string]any{
			"principal_id": principalID, "provider": identity.Provider, "issuer": identity.Issuer, "subject": identity.Subject,
			"display_name": identity.DisplayName, "email": identity.Email, "email_verified": identity.EmailVerified,
			"linked_at": now, "last_authenticated_at": now,
		})
		if err != nil {
			tx.abort()
			if strings.Contains(strings.ToLower(err.Error()), "duplicate unique") {
				return nil, providerError("identity_already_linked", "provider identity is already linked", 409)
			}
			return nil, providerError("provider_flow_failed", "provider identity could not be linked", 500, err)
		}
	}
	grant, err := s.materializeGrant(tx, flow, row)
	if err != nil {
		tx.abort()
		return nil, providerError("provider_grant_failed", "provider grant could not be saved", 500, err)
	}
	if err := tx.commit(); err != nil {
		return nil, providerError("provider_flow_failed", "provider identity could not be linked", 500, err)
	}
	view := providerIdentityViewFromRow(row)
	return &providerCompleteResult{Linked: &view, Grant: grant}, nil
}

func (s *providerAuthService) completeConsent(flow map[string]any, identity AuthProviderIdentity, confirm bool, auth *AuthContext, now int64) (*providerCompleteResult, error) {
	if !confirm {
		return nil, providerError("confirmation_required", "explicit confirmation required", 400)
	}
	principalID, sessionID := toString(flow["link_principal_id"]), toString(flow["link_session_id"])
	if auth == nil || auth.ID != principalID || auth.SessionID != sessionID {
		return nil, providerError("link_session_changed", "the original authenticated session is required", 401)
	}
	grantID := toString(flow["incremental_grant_id"])
	grantRow, err := s.db.Table(systemAuthProviderGrantTableName).Get(grantID)
	if err != nil || grantRow == nil || toString(grantRow["principal_id"]) != principalID {
		return nil, providerError("grant_not_found", "provider grant not found", 404)
	}
	identityRow, err := s.db.Table(systemAuthIdentityTableName).Get(toString(grantRow["identity_id"]))
	if err != nil || identityRow == nil || toString(identityRow["issuer"]) != identity.Issuer || toString(identityRow["subject"]) != identity.Subject {
		return nil, providerError("provider_identity_invalid", "provider identity does not match grant", 409)
	}
	tx := newProviderTxn(s.db)
	if err := tx.update(s.db.Table(systemAuthProviderFlowTableName), toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now}); err != nil {
		tx.abort()
		return nil, err
	}
	grant, err := s.materializeGrant(tx, flow, identityRow)
	if err != nil {
		tx.abort()
		return nil, providerError("provider_grant_failed", "provider grant could not be saved", 500, err)
	}
	if err := tx.commit(); err != nil {
		return nil, providerError("provider_grant_failed", "provider grant could not be saved", 500, err)
	}
	return &providerCompleteResult{Grant: grant}, nil
}

func (s *providerAuthService) listIdentities(principalID string) ([]providerIdentityView, error) {
	if principalID == "" {
		return nil, providerError("authentication_required", "authentication required", 401)
	}
	rows, err := s.db.Table(systemAuthIdentityTableName).FindByIndex("principal_id", principalID)
	if err != nil {
		return nil, providerError("provider_identity_failed", "linked identities unavailable", 500, err)
	}
	out := make([]providerIdentityView, 0, len(rows))
	for _, row := range rows {
		out = append(out, providerIdentityViewFromRow(row))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LinkedAt != out[j].LinkedAt {
			return out[i].LinkedAt < out[j].LinkedAt
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *providerAuthService) unlink(principalID, identityID string) error {
	if principalID == "" {
		return providerError("authentication_required", "authentication required", 401)
	}
	if identityID == "" {
		return providerError("identity_required", "identity required", 400)
	}
	s.mu.Lock()
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}()
	identities := s.db.Table(systemAuthIdentityTableName)
	identity, err := identities.Get(identityID)
	if err != nil || identity == nil || toString(identity["principal_id"]) != principalID {
		return providerError("identity_not_found", "linked identity not found", 404)
	}
	linkedRows, err := identities.FindByIndex("principal_id", principalID)
	if err != nil {
		return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
	}
	otherMethods := 0
	for _, linked := range linkedRows {
		if toString(linked["id"]) == identityID {
			continue
		}
		provider := toString(linked["provider"])
		usable := false
		for _, app := range s.apps {
			config, ok := app.Providers[provider]
			if ok && config.Adapter != nil && config.Issuer == toString(linked["issuer"]) {
				usable = true
				break
			}
		}
		if usable {
			otherMethods++
		}
	}
	if otherMethods <= 0 && !s.db.authService.HasUsablePassword(principalID) {
		return providerError("last_sign_in_method", "cannot remove the last usable sign-in method", 409)
	}

	tx := newProviderTxn(s.db)
	grants, _ := s.db.Table(systemAuthProviderGrantTableName).FindByIndex("identity_id", identityID)
	retryIDs := make([]string, 0, len(grants))
	for _, grant := range grants {
		tx.lockGrant(s, toString(grant["id"]))
		grant, err = s.db.Table(systemAuthProviderGrantTableName).Get(toString(grant["id"]))
		if err != nil || grant == nil || toString(grant["identity_id"]) != identityID || toString(grant["principal_id"]) != principalID {
			tx.abort()
			return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
		}
		if toString(grant["state"]) == "revoked" && toString(grant["token_ciphertext"]) == "" {
			continue
		}
		retryID, err := s.stageGrantRevocation(tx, grant)
		if err != nil {
			tx.abort()
			return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
		}
		retryIDs = append(retryIDs, retryID)
	}
	if err := tx.delete(identities, identityID); err != nil {
		tx.abort()
		return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
	}
	sessions := s.db.Table(systemAuthSessionTableName)
	derived, err := sessions.FindByIndex("auth_identity_id", identityID)
	if err != nil {
		tx.abort()
		return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
	}
	now := s.now().Unix()
	for _, session := range derived {
		if providerUnix(session["revoked_at"]) > 0 {
			continue
		}
		if err := tx.update(sessions, toString(session["id"]), map[string]any{"revoked_at": now, "reason": "provider_identity_unlinked"}); err != nil {
			tx.abort()
			return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
		}
	}
	if err := tx.commit(); err != nil {
		return providerError("provider_identity_failed", "linked identity could not be removed", 500, err)
	}
	s.mu.Unlock()
	locked = false
	for _, retryID := range retryIDs {
		_ = s.attemptRevocation(context.Background(), retryID)
	}
	return nil
}

func providerIdentityViewFromRow(row map[string]any) providerIdentityView {
	return providerIdentityView{
		ID: toString(row["id"]), Provider: toString(row["provider"]), Issuer: toString(row["issuer"]),
		DisplayName: toString(row["display_name"]), Email: toString(row["email"]), EmailVerified: providerBool(row["email_verified"]),
		LinkedAt: providerUnix(row["linked_at"]), LastAuthenticatedAt: providerUnix(row["last_authenticated_at"]),
	}
}

func (s *providerAuthService) randomToken(size int) (string, error) {
	b := make([]byte, size)
	if _, err := io.ReadFull(s.random, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (s *providerAuthService) sealSecrets(secrets providerFlowSecrets) (string, error) {
	plain, err := jsonx.Marshal(secrets)
	if err != nil {
		return "", err
	}
	s.aeadMu.RLock()
	defer s.aeadMu.RUnlock()
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(s.random, nonce); err != nil {
		return "", err
	}
	sealed := s.aead.Seal(nil, nonce, plain, []byte("flop-provider-flow-v1"))
	return base64.RawURLEncoding.EncodeToString(append(nonce, sealed...)), nil
}

func (s *providerAuthService) openSecrets(encoded string) (providerFlowSecrets, error) {
	var secrets providerFlowSecrets
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return secrets, errors.New("invalid provider flow ciphertext")
	}
	s.aeadMu.RLock()
	defer s.aeadMu.RUnlock()
	if len(raw) <= s.aead.NonceSize() {
		return secrets, errors.New("invalid provider flow ciphertext")
	}
	plain, err := s.aead.Open(nil, raw[:s.aead.NonceSize()], raw[s.aead.NonceSize():], []byte("flop-provider-flow-v1"))
	if err != nil {
		return secrets, err
	}
	if err := jsonx.Unmarshal(plain, &secrets); err != nil {
		return secrets, err
	}
	if secrets.Nonce == "" {
		return secrets, errors.New("provider flow nonce missing")
	}
	return secrets, nil
}

func hashProviderToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func exactStringMatch(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneURLValues(values url.Values) url.Values {
	out := make(url.Values, len(values))
	for key, entries := range values {
		out[key] = append([]string(nil), entries...)
	}
	return out
}

func providerUnix(value any) int64 {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case jsonx.Number:
		n, _ := strconv.ParseInt(string(value), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(value, 10, 64)
		return n
	default:
		return 0
	}
}

func providerBool(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		parsed, _ := strconv.ParseBool(value)
		return parsed
	default:
		return false
	}
}

type providerTxn struct {
	db     *Database
	txBuf  map[string]*engine.WalBufEntry
	undo   []func()
	locked map[string]struct{}
	unlock []func()
	closed bool
}

func newProviderTxn(db *Database) *providerTxn {
	return &providerTxn{db: db, txBuf: make(map[string]*engine.WalBufEntry), locked: make(map[string]struct{})}
}

func (tx *providerTxn) lockGrant(service *providerAuthService, grantID string) {
	if tx == nil || service == nil || grantID == "" {
		return
	}
	if _, ok := tx.locked[grantID]; ok {
		return
	}
	lockValue, _ := service.grantLocks.LoadOrStore(grantID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	tx.locked[grantID] = struct{}{}
	tx.unlock = append(tx.unlock, lock.Unlock)
}

func (tx *providerTxn) releaseLocks() {
	for i := len(tx.unlock) - 1; i >= 0; i-- {
		tx.unlock[i]()
	}
	tx.unlock = nil
}

func (tx *providerTxn) insert(table *TableInstance, data map[string]any) (map[string]any, error) {
	row, err := table.ti.Insert(data, tx.txBuf)
	if err != nil {
		return nil, err
	}
	tx.addInserted(table, toString(row[table.primaryKeyField()]))
	return row, nil
}

func (tx *providerTxn) addInserted(table *TableInstance, id string) {
	tx.undo = append(tx.undo, func() { _ = table.rollbackInserted(id) })
}

func (tx *providerTxn) update(table *TableInstance, id string, updates map[string]any) error {
	raw, err := table.rawRow(id)
	if err != nil || len(raw) == 0 {
		if err == nil {
			err = fmt.Errorf("row not found")
		}
		return err
	}
	row, err := table.ti.Update(id, updates, tx.txBuf)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("row not found")
	}
	tx.undo = append(tx.undo, func() { _ = table.rollbackRawRow(raw) })
	return nil
}

func (tx *providerTxn) delete(table *TableInstance, id string) error {
	raw, err := table.rawRow(id)
	if err != nil || len(raw) == 0 {
		if err == nil {
			err = fmt.Errorf("row not found")
		}
		return err
	}
	ok, err := table.ti.Delete(id, tx.txBuf)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("row not found")
	}
	tx.undo = append(tx.undo, func() { _ = table.rollbackRawRow(raw) })
	return nil
}

func (tx *providerTxn) commit() error {
	if tx == nil || tx.closed {
		return fmt.Errorf("provider transaction closed")
	}
	tx.closed = true
	defer tx.releaseLocks()
	if err := tx.db.db.EnqueueCommit(tx.txBuf); err != nil {
		tx.rollback()
		return err
	}
	return nil
}

func (tx *providerTxn) abort() {
	if tx == nil || tx.closed {
		return
	}
	tx.closed = true
	defer tx.releaseLocks()
	tx.rollback()
}

func (tx *providerTxn) rollback() {
	for i := len(tx.undo) - 1; i >= 0; i-- {
		tx.undo[i]()
	}
	tx.db.db.DiscardTransaction(tx.txBuf)
}

func (d *Database) cleanupExpiredProviderFlows(now time.Time) (int, error) {
	if d == nil || d.db == nil {
		return 0, nil
	}
	flows := d.Table(systemAuthProviderFlowTableName)
	if flows == nil {
		return 0, nil
	}
	providerAuth := d.providerAuth
	if providerAuth != nil {
		providerAuth.mu.Lock()
		defer providerAuth.mu.Unlock()
	}
	return cleanupExpiredProviderFlowRows(flows, now)
}

func cleanupExpiredProviderFlowRows(flows *TableInstance, now time.Time) (int, error) {
	total := flows.Count()
	rows, err := flows.Scan(total, 0)
	if err != nil {
		return 0, err
	}
	nowUnix := now.Unix()
	ids := make([]string, 0)
	for _, row := range rows {
		completionConsumed := providerUnix(row["completion_consumed_at"])
		completionExpires := providerUnix(row["completion_expires_at"])
		callbackConsumed := providerUnix(row["callback_consumed_at"])
		callbackClaimExpires := providerUnix(row["callback_claim_expires_at"])
		if callbackConsumed > 0 && callbackClaimExpires == 0 {
			callbackClaimExpires = callbackConsumed + int64(providerFlowTTL/time.Second)
		}
		flowExpires := providerUnix(row["expires_at"])
		claimExpired := callbackConsumed > 0 && callbackClaimExpires > 0 && callbackClaimExpires <= nowUnix
		unclaimedExpired := callbackConsumed == 0 && flowExpires <= nowUnix
		if completionConsumed > 0 || (completionExpires > 0 && completionExpires <= nowUnix) || (completionExpires == 0 && (claimExpired || unclaimedExpired)) {
			ids = append(ids, toString(row["id"]))
		}
	}
	deleted := 0
	for _, id := range ids {
		ok, err := flows.Delete(id)
		if err != nil {
			return deleted, err
		}
		if ok {
			deleted++
		}
	}
	return deleted, nil
}
