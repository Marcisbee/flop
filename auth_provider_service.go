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
	db        *Database
	providers map[string]AuthProviderConfig
	aead      cipher.AEAD
	now       func() time.Time
	random    io.Reader
	maxFlows  int
	mu        sync.Locker
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

func newProviderAuthService(db *Database, providers map[string]AuthProviderConfig, secret string) *providerAuthService {
	key := sha256.Sum256([]byte("flop/provider-flow/aead/v1\x00" + secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil
	}
	registered := make(map[string]AuthProviderConfig, len(providers))
	for provider, config := range providers {
		registered[provider] = config
	}
	locker := &sync.Mutex{}
	service := &providerAuthService{
		db:        db,
		providers: registered,
		aead:      aead,
		now:       time.Now,
		random:    rand.Reader,
		maxFlows:  providerOutstandingFlowLimit,
		mu:        locker,
	}
	if db != nil && db.authService != nil {
		db.authService.SetProviderSessionLocker(locker)
	}
	return service
}

func validateAuthProviderConfigs(providers map[string]AuthProviderConfig) error {
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
	config, err := s.providerConfig(provider)
	if err != nil {
		return nil, err
	}
	if intent != "sign_in" && intent != "link" {
		return nil, providerError("invalid_intent", "intent must be sign_in or link", 400)
	}
	if returnTo != "" && !exactStringMatch(config.AllowedReturnURLs, returnTo) {
		return nil, providerError("return_not_allowed", "return destination not allowed", 400)
	}
	if intent == "link" {
		if auth == nil || auth.ID == "" || auth.SessionID == "" {
			return nil, providerError("authentication_required", "authentication required", 401)
		}
		if err := s.db.authService.ValidateUserSession(auth.ID, auth.SessionID); err != nil {
			return nil, providerError("authentication_required", "authentication required", 401, err)
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

	sealed, err := s.sealSecrets(providerFlowSecrets{Nonce: nonce, PKCEVerifier: verifier})
	if err != nil {
		return nil, providerError("provider_flow_failed", "could not start provider authentication", 500, err)
	}
	now := s.now().Unix()
	stateHash := hashProviderToken(state)
	row := map[string]any{
		"state_hash":         stateHash,
		"completion_hash":    "pending:" + stateHash,
		"provider":           provider,
		"intent":             intent,
		"secrets_ciphertext": sealed,
		"redirect_uri":       config.RedirectURI,
		"return_to":          returnTo,
		"created_at":         now,
		"expires_at":         now + int64(providerFlowTTL/time.Second),
		"phase":              providerFlowPhaseAuthorizationProcessing,
	}
	if intent == "link" {
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
		Provider: provider, State: state, Nonce: nonce, RedirectURI: config.RedirectURI,
		CodeChallenge: challenge, CodeChallengeMethod: method,
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
	if provider == "" || state == "" {
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
	if toString(flow["provider"]) != provider {
		s.mu.Unlock()
		return nil, providerError("provider_mismatch", "provider callback does not match flow", 400)
	}
	config, err := s.providerConfig(provider)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if toString(flow["redirect_uri"]) != config.RedirectURI {
		s.mu.Unlock()
		return nil, providerError("provider_flow_invalid", "provider flow is invalid", 400)
	}
	secrets, err := s.openSecrets(toString(flow["secrets_ciphertext"]))
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
		result, finalizeErr := s.finalizeCallbackLocked(flows, flowID, "provider_denied", nil)
		s.mu.Unlock()
		return result, finalizeErr
	}
	if code == "" {
		result, finalizeErr := s.finalizeCallbackLocked(flows, flowID, "invalid_callback", nil)
		s.mu.Unlock()
		return result, finalizeErr
	}
	exchangeRequest := AuthProviderCallbackRequest{
		Provider: provider, Code: code, RedirectURI: config.RedirectURI,
		CodeVerifier: secrets.PKCEVerifier, Nonce: secrets.Nonce, Parameters: cloneURLValues(params),
	}
	s.mu.Unlock()

	identity, exchangeErr := config.Adapter.Exchange(ctx, exchangeRequest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if exchangeErr != nil {
		return s.finalizeCallbackLocked(flows, flowID, "provider_exchange_failed", nil)
	}
	if identity.Provider != provider || identity.Issuer != config.Issuer || identity.Subject == "" || identity.Subject != strings.TrimSpace(identity.Subject) {
		return s.finalizeCallbackLocked(flows, flowID, "provider_identity_invalid", nil)
	}
	return s.finalizeCallbackLocked(flows, flowID, "", &identity)
}

func (s *providerAuthService) finalizeCallbackLocked(flows *TableInstance, flowID, failureCode string, identity *AuthProviderIdentity) (*providerCallbackResult, error) {
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
	return &providerCompleteResult{Token: token, RefreshToken: refreshToken, Auth: appAuth}, nil
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
	if _, exists := identities.FindByUniqueCompositeIndex([]string{"issuer", "subject"}, identity.Issuer, identity.Subject); exists {
		_, _ = s.db.Table(systemAuthProviderFlowTableName).Update(toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now})
		return nil, providerError("identity_already_linked", "provider identity is already linked", 409)
	}
	tx := newProviderTxn(s.db)
	if err := tx.update(s.db.Table(systemAuthProviderFlowTableName), toString(flow["id"]), map[string]any{"phase": providerFlowPhaseConsumed, "completion_consumed_at": now}); err != nil {
		tx.abort()
		return nil, providerError("provider_flow_failed", "provider identity could not be linked", 500, err)
	}
	row, err := tx.insert(identities, map[string]any{
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
	if err := tx.commit(); err != nil {
		return nil, providerError("provider_flow_failed", "provider identity could not be linked", 500, err)
	}
	view := providerIdentityViewFromRow(row)
	return &providerCompleteResult{Linked: &view}, nil
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
	defer s.mu.Unlock()
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
		config, ok := s.providers[provider]
		if ok && config.Adapter != nil && config.Issuer == toString(linked["issuer"]) {
			otherMethods++
		}
	}
	if otherMethods <= 0 && !s.db.authService.HasUsablePassword(principalID) {
		return providerError("last_sign_in_method", "cannot remove the last usable sign-in method", 409)
	}

	tx := newProviderTxn(s.db)
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
	if err != nil || len(raw) <= s.aead.NonceSize() {
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
	closed bool
}

func newProviderTxn(db *Database) *providerTxn {
	return &providerTxn{db: db, txBuf: make(map[string]*engine.WalBufEntry)}
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
	if d.providerAuth != nil {
		d.providerAuth.mu.Lock()
		defer d.providerAuth.mu.Unlock()
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
