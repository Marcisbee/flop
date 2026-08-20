package server

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
)

const (
	ProviderIntentSignIn = "sign_in"
	ProviderIntentLink   = "link"

	ProviderOutcomeSignInReady          = "sign_in_ready"
	ProviderOutcomeLinkRequired         = "link_required"
	ProviderOutcomeLinkPending          = "link_pending"
	ProviderOutcomeProviderDenied       = "provider_denied"
	ProviderOutcomeExchangeFailed       = "provider_exchange_failed"
	ProviderOutcomeInvalidFlow          = "provider_flow_invalid"
	ProviderOutcomeIdentityConflict     = "provider_identity_conflict"
	ProviderOutcomeLastMethod           = "last_sign_in_method"
	ProviderOutcomePrincipalUnavailable = "principal_unavailable"
)

// ProviderCapabilities describes the security extensions implemented by an
// authorization-code provider. Flop always supplies a transaction-specific
// state value, and supplies PKCE S256 and OIDC nonce inputs when requested.
type ProviderCapabilities struct {
	PKCES256  bool `json:"pkceS256"`
	OIDCNonce bool `json:"oidcNonce"`
}

// ProviderAuthorizationRequest contains the values an adapter must bind into
// its authorization URL.
type ProviderAuthorizationRequest struct {
	RedirectURI         string
	State               string
	CodeChallenge       string
	CodeChallengeMethod string
	Nonce               string
}

// ProviderCodeExchangeRequest contains the one-time values needed to exchange
// an authorization code and verify the returned identity.
type ProviderCodeExchangeRequest struct {
	Code          string
	RedirectURI   string
	CodeVerifier  string
	ExpectedNonce string
}

// ProviderIdentity is the verified, provider-independent identity result.
// Issuer and Subject are ownership keys. Email and other claims are display
// metadata only and are never used to select or link a Flop account.
type ProviderIdentity struct {
	Issuer        string            `json:"issuer"`
	Subject       string            `json:"subject"`
	DisplayName   string            `json:"displayName,omitempty"`
	Email         string            `json:"email,omitempty"`
	EmailVerified bool              `json:"emailVerified,omitempty"`
	Claims        map[string]string `json:"claims,omitempty"`
}

// ProviderAdapter implements one external authorization-code provider.
// ExchangeCode must validate the provider response, including OIDC nonce when
// OIDCNonce is declared, before returning ProviderIdentity.
type ProviderAdapter interface {
	Key() string
	Issuer() string
	Capabilities() ProviderCapabilities
	AuthorizationURL(context.Context, ProviderAuthorizationRequest) (string, error)
	ExchangeCode(context.Context, ProviderCodeExchangeRequest) (ProviderIdentity, error)
}

type ProviderDescriptor struct {
	Key       string `json:"key"`
	Issuer    string `json:"issuer"`
	PKCES256  bool   `json:"pkceS256"`
	OIDCNonce bool   `json:"oidcNonce"`
}

type ProviderStart struct {
	AuthorizationURL string `json:"authorizationUrl"`
}

type ProviderCallbackInput struct {
	State       string
	Code        string
	Error       string
	Issuer      string
	ErrorDetail string
}

type ProviderCallbackResult struct {
	ResultCode string
	ReturnURL  string
	Outcome    string
}

type ProviderAuthResult struct {
	Outcome          string
	Provider         string
	Token            string
	RefreshToken     string
	Auth             *schema.AuthContext
	ConfirmationCode string
}

type LinkedProviderIdentity struct {
	ID            string            `json:"id"`
	Provider      string            `json:"provider"`
	Issuer        string            `json:"issuer"`
	Subject       string            `json:"subject"`
	DisplayName   string            `json:"displayName,omitempty"`
	Email         string            `json:"email,omitempty"`
	EmailVerified bool              `json:"emailVerified,omitempty"`
	Claims        map[string]string `json:"claims,omitempty"`
	CreatedAt     int64             `json:"createdAt"`
}

type ProviderAuthError struct {
	Code string
}

func (e *ProviderAuthError) Error() string {
	if e == nil || e.Code == "" {
		return ProviderOutcomeInvalidFlow
	}
	return e.Code
}

func ProviderAuthErrorCode(err error) string {
	var providerErr *ProviderAuthError
	if errors.As(err, &providerErr) && providerErr.Code != "" {
		return providerErr.Code
	}
	return "provider_internal_error"
}

type providerRuntime struct {
	adapter      ProviderAdapter
	key          string
	issuer       string
	capabilities ProviderCapabilities
}

type providerAuthRuntime struct {
	mu          sync.Mutex
	identities  *engine.TableInstance
	flows       *engine.TableInstance
	providers   map[string]providerRuntime
	callbackURL string
	returnURLs  map[string]struct{}
	flowTTL     time.Duration
	aead        cipher.AEAD
	digestKey   []byte
	now         func() time.Time
}

func ValidateProviderConfiguration(providers []ProviderAdapter, callbackURL string, returnURLs []string) error {
	if len(providers) == 0 {
		return nil
	}
	if err := validateAbsoluteHTTPURL(callbackURL, "provider callback URL"); err != nil {
		return err
	}
	seenKeys := make(map[string]struct{}, len(providers))
	seenIssuers := make(map[string]struct{}, len(providers))
	for i, adapter := range providers {
		if adapter == nil {
			return fmt.Errorf("provider adapter %d is nil", i)
		}
		key := adapter.Key()
		issuer := adapter.Issuer()
		if key == "" || strings.TrimSpace(key) != key {
			return fmt.Errorf("provider adapter %d has a blank or non-canonical key", i)
		}
		if issuer == "" || strings.TrimSpace(issuer) != issuer {
			return fmt.Errorf("provider %q has a blank or non-canonical issuer", key)
		}
		if _, exists := seenKeys[key]; exists {
			return fmt.Errorf("duplicate provider key %q", key)
		}
		if _, exists := seenIssuers[issuer]; exists {
			return fmt.Errorf("duplicate provider issuer %q", issuer)
		}
		seenKeys[key] = struct{}{}
		seenIssuers[issuer] = struct{}{}
	}
	seenReturns := make(map[string]struct{}, len(returnURLs))
	for _, target := range returnURLs {
		if strings.TrimSpace(target) != target || target == "" {
			return fmt.Errorf("provider return URL must be non-blank and canonical")
		}
		parsed, err := url.Parse(target)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return fmt.Errorf("provider return URL %q must be an absolute HTTP(S) URL", target)
		}
		if parsed.Fragment != "" {
			return fmt.Errorf("provider return URL %q must not contain a fragment", target)
		}
		if _, exists := seenReturns[target]; exists {
			return fmt.Errorf("duplicate provider return URL %q", target)
		}
		seenReturns[target] = struct{}{}
	}
	return nil
}

func validateAbsoluteHTTPURL(raw, label string) error {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return fmt.Errorf("%s is required and must be canonical", label)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return fmt.Errorf("%s must be an absolute HTTP(S) URL", label)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%s must not contain a query or fragment", label)
	}
	return nil
}

func (as *AuthService) ConfigureProviderAuth(identityTable, flowTable *engine.TableInstance, providers []ProviderAdapter, callbackURL string, returnURLs []string, ttl time.Duration) error {
	if err := ValidateProviderConfiguration(providers, callbackURL, returnURLs); err != nil {
		return err
	}
	if len(providers) == 0 {
		as.providerAuth = nil
		return nil
	}
	if ttl > 0 && ttl < time.Second {
		return fmt.Errorf("provider flow TTL must be at least one second")
	}
	if identityTable == nil || flowTable == nil {
		return fmt.Errorf("provider authentication system tables are unavailable")
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	digestKey := deriveProviderKey(as.secret, "digest")
	block, err := aes.NewCipher(deriveProviderKey(as.secret, "aead"))
	if err != nil {
		return fmt.Errorf("initialize provider flow encryption: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("initialize provider flow encryption: %w", err)
	}
	runtime := &providerAuthRuntime{
		identities:  identityTable,
		flows:       flowTable,
		providers:   make(map[string]providerRuntime, len(providers)),
		callbackURL: callbackURL,
		returnURLs:  make(map[string]struct{}, len(returnURLs)),
		flowTTL:     ttl,
		aead:        aead,
		digestKey:   digestKey,
		now:         time.Now,
	}
	for _, adapter := range providers {
		key := adapter.Key()
		runtime.providers[key] = providerRuntime{
			adapter:      adapter,
			key:          key,
			issuer:       adapter.Issuer(),
			capabilities: adapter.Capabilities(),
		}
	}
	for _, target := range returnURLs {
		runtime.returnURLs[target] = struct{}{}
	}
	as.providerAuth = runtime
	return nil
}

func deriveProviderKey(secret, purpose string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("flop/provider-auth/v1/" + purpose))
	return mac.Sum(nil)
}

func (as *AuthService) ProviderDescriptors() []ProviderDescriptor {
	pa := as.providerAuth
	if pa == nil {
		return []ProviderDescriptor{}
	}
	out := make([]ProviderDescriptor, 0, len(pa.providers))
	for _, provider := range pa.providers {
		out = append(out, ProviderDescriptor{
			Key: provider.key, Issuer: provider.issuer,
			PKCES256: provider.capabilities.PKCES256, OIDCNonce: provider.capabilities.OIDCNonce,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (as *AuthService) HasProviderAuth() bool {
	return as != nil && as.providerAuth != nil && len(as.providerAuth.providers) > 0
}

func (as *AuthService) BeginProviderFlow(ctx context.Context, providerKey, intent, returnURL string, principal *schema.AuthContext) (*ProviderStart, error) {
	pa := as.providerAuth
	if pa == nil {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	provider, ok := pa.providers[providerKey]
	if !ok {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	if intent == "" {
		intent = ProviderIntentSignIn
	}
	if intent != ProviderIntentSignIn && intent != ProviderIntentLink {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	if returnURL != "" {
		if _, allowed := pa.returnURLs[returnURL]; !allowed {
			return nil, providerError(ProviderOutcomeInvalidFlow)
		}
	}
	principalID, sessionID := "", ""
	if intent == ProviderIntentLink {
		if principal == nil || principal.PrincipalType != principalTypeUser || principal.ID == "" || principal.SessionID == "" {
			return nil, providerError("authentication_required")
		}
		if _, err := as.requireActiveSession(principal.SessionID, principalTypeUser, principal.ID); err != nil {
			return nil, providerError("authentication_required")
		}
		user, err := as.authTable.Get(principal.ID)
		if err != nil || validatePrincipalRow(user, principalTypeUser) != nil {
			return nil, providerError(ProviderOutcomePrincipalUnavailable)
		}
		principalID, sessionID = principal.ID, principal.SessionID
	}

	pa.mu.Lock()
	defer pa.mu.Unlock()
	flowID, err := randomOpaque(24)
	if err != nil {
		return nil, err
	}
	state, err := randomOpaque(32)
	if err != nil {
		return nil, err
	}
	verifier, challenge := "", ""
	if provider.capabilities.PKCES256 {
		verifier, err = randomOpaque(32)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(verifier))
		challenge = base64.RawURLEncoding.EncodeToString(sum[:])
	}
	nonce := ""
	if provider.capabilities.OIDCNonce {
		nonce, err = randomOpaque(32)
		if err != nil {
			return nil, err
		}
	}
	verifierCipher, err := pa.encrypt(flowID+"|verifier", verifier)
	if err != nil {
		return nil, err
	}
	nonceCipher, err := pa.encrypt(flowID+"|nonce", nonce)
	if err != nil {
		return nil, err
	}
	now := pa.now().UnixMilli()
	_, err = pa.flows.Insert(map[string]interface{}{
		"id": flowID, "intent": intent, "provider_key": provider.key,
		"expected_issuer": provider.issuer, "initiating_principal_id": principalID,
		"initiating_session_id": sessionID, "return_url": returnURL,
		"callback_uri": pa.callbackURL,
		"state_digest": pa.digest(state), "verifier_ciphertext": verifierCipher,
		"nonce_ciphertext": nonceCipher, "expires_at": now + pa.flowTTL.Milliseconds(),
		"phase": "started", "created_at": now,
	}, nil)
	if err != nil {
		return nil, err
	}
	authorizationURL, err := provider.adapter.AuthorizationURL(ctx, ProviderAuthorizationRequest{
		RedirectURI: pa.callbackURL, State: state, CodeChallenge: challenge,
		CodeChallengeMethod: func() string {
			if challenge != "" {
				return "S256"
			}
			return ""
		}(), Nonce: nonce,
	})
	if err != nil {
		_, _ = pa.flows.Delete(flowID, nil)
		return nil, providerError("provider_authorization_failed")
	}
	if err := validateAbsoluteHTTPURLWithQuery(authorizationURL); err != nil {
		_, _ = pa.flows.Delete(flowID, nil)
		return nil, providerError("provider_authorization_failed")
	}
	return &ProviderStart{AuthorizationURL: authorizationURL}, nil
}

func validateAbsoluteHTTPURLWithQuery(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.Fragment != "" {
		return fmt.Errorf("invalid authorization URL")
	}
	return nil
}

func (as *AuthService) HandleProviderCallback(ctx context.Context, in ProviderCallbackInput) (*ProviderCallbackResult, error) {
	pa := as.providerAuth
	if pa == nil || in.State == "" {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	flow := pa.findFlowByDigest("state_digest", pa.digest(in.State))
	if !pa.validFlow(flow, "started") {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	flowID := toString(flow["id"])
	now := pa.now().UnixMilli()
	if _, err := pa.flows.Update(flowID, map[string]interface{}{
		"phase": "callback_processing", "callback_consumed_at": now,
	}, nil); err != nil {
		return nil, err
	}
	provider, ok := pa.providers[toString(flow["provider_key"])]
	callbackURI := toString(flow["callback_uri"])
	if !ok || provider.issuer != toString(flow["expected_issuer"]) || callbackURI == "" || callbackURI != pa.callbackURL {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	if in.Issuer != "" && in.Issuer != provider.issuer {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	if in.Error != "" {
		return pa.prepareResult(flow, ProviderOutcomeProviderDenied, ProviderIdentity{}, "")
	}
	if strings.TrimSpace(in.Code) == "" {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	verifier, err := pa.decrypt(flowID+"|verifier", toString(flow["verifier_ciphertext"]))
	if err != nil {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	nonce, err := pa.decrypt(flowID+"|nonce", toString(flow["nonce_ciphertext"]))
	if err != nil {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	identity, err := provider.adapter.ExchangeCode(ctx, ProviderCodeExchangeRequest{
		Code: in.Code, RedirectURI: callbackURI, CodeVerifier: verifier, ExpectedNonce: nonce,
	})
	if err != nil {
		return pa.prepareResult(flow, ProviderOutcomeExchangeFailed, ProviderIdentity{}, "")
	}
	if identity.Issuer != provider.issuer || identity.Subject == "" || strings.TrimSpace(identity.Subject) == "" {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	identity.Claims = sanitizeProviderDisplayClaims(identity.Claims)

	if toString(flow["intent"]) == ProviderIntentSignIn {
		linked := pa.findIdentity(identity.Issuer, identity.Subject)
		if linked == nil {
			return pa.prepareResult(flow, ProviderOutcomeLinkRequired, identity, "")
		}
		return pa.prepareResult(flow, ProviderOutcomeSignInReady, identity, toString(linked["principal_id"]))
	}
	principalID := toString(flow["initiating_principal_id"])
	sessionID := toString(flow["initiating_session_id"])
	if principalID == "" || sessionID == "" {
		return pa.prepareResult(flow, ProviderOutcomeInvalidFlow, ProviderIdentity{}, "")
	}
	if _, err := as.requireActiveSession(sessionID, principalTypeUser, principalID); err != nil {
		return pa.prepareResult(flow, ProviderOutcomePrincipalUnavailable, ProviderIdentity{}, "")
	}
	user, err := as.authTable.Get(principalID)
	if err != nil || validatePrincipalRow(user, principalTypeUser) != nil {
		return pa.prepareResult(flow, ProviderOutcomePrincipalUnavailable, ProviderIdentity{}, "")
	}
	return pa.prepareResult(flow, ProviderOutcomeLinkPending, identity, principalID)
}

func (pa *providerAuthRuntime) prepareResult(flow map[string]interface{}, outcome string, identity ProviderIdentity, principalID string) (*ProviderCallbackResult, error) {
	resultCode, err := randomOpaque(32)
	if err != nil {
		return nil, err
	}
	claims, _ := json.Marshal(identity.Claims)
	updates := map[string]interface{}{
		"phase": "result_ready", "outcome": outcome, "result_digest": pa.digest(resultCode),
		"resolved_principal_id": principalID, "identity_issuer": identity.Issuer,
		"identity_subject": identity.Subject, "identity_display_name": identity.DisplayName,
		"identity_email": identity.Email, "identity_email_verified": identity.EmailVerified,
		"identity_claims":     string(claims),
		"verifier_ciphertext": "", "nonce_ciphertext": "", "state_digest": "",
	}
	if _, err := pa.flows.Update(toString(flow["id"]), updates, nil); err != nil {
		return nil, err
	}
	returnURL := toString(flow["return_url"])
	if returnURL != "" {
		if _, allowed := pa.returnURLs[returnURL]; !allowed {
			returnURL = ""
		}
	}
	return &ProviderCallbackResult{ResultCode: resultCode, ReturnURL: returnURL, Outcome: outcome}, nil
}

func (as *AuthService) RedeemProviderResult(resultCode string) (*ProviderAuthResult, error) {
	pa := as.providerAuth
	if pa == nil || resultCode == "" {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	flow := pa.findFlowByDigest("result_digest", pa.digest(resultCode))
	if !pa.validFlow(flow, "result_ready") {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	flowID := toString(flow["id"])
	outcome := toString(flow["outcome"])
	result := &ProviderAuthResult{Outcome: outcome, Provider: toString(flow["provider_key"])}
	if outcome == ProviderOutcomeLinkPending {
		confirmationCode, err := randomOpaque(32)
		if err != nil {
			return nil, err
		}
		if _, err := pa.flows.Update(flowID, map[string]interface{}{
			"phase": "link_confirm", "result_digest": pa.digest(confirmationCode),
			"result_consumed_at": pa.now().UnixMilli(),
		}, nil); err != nil {
			return nil, err
		}
		result.ConfirmationCode = confirmationCode
		return result, nil
	}
	if _, err := pa.flows.Update(flowID, map[string]interface{}{
		"phase": "consumed", "result_consumed_at": pa.now().UnixMilli(), "result_digest": "",
	}, nil); err != nil {
		return nil, err
	}
	if outcome != ProviderOutcomeSignInReady {
		return result, nil
	}
	principalID := toString(flow["resolved_principal_id"])
	user, err := as.authTable.Get(principalID)
	if err != nil || user == nil {
		result.Outcome = ProviderOutcomePrincipalUnavailable
		return result, nil
	}
	token, refresh, auth, err := as.createUserSession(user, "provider_login")
	if err != nil {
		result.Outcome = ProviderOutcomePrincipalUnavailable
		return result, nil
	}
	result.Token, result.RefreshToken, result.Auth = token, refresh, auth
	return result, nil
}

func (as *AuthService) ConfirmProviderLink(confirmationCode string, principal *schema.AuthContext) (*LinkedProviderIdentity, error) {
	pa := as.providerAuth
	if pa == nil || confirmationCode == "" || principal == nil || principal.ID == "" || principal.SessionID == "" {
		return nil, providerError("authentication_required")
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	as.mutationMu.Lock()
	defer as.mutationMu.Unlock()
	flow := pa.findFlowByDigest("result_digest", pa.digest(confirmationCode))
	if !pa.validFlow(flow, "link_confirm") {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	if toString(flow["initiating_principal_id"]) != principal.ID || toString(flow["initiating_session_id"]) != principal.SessionID {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	if _, err := as.requireActiveSession(principal.SessionID, principalTypeUser, principal.ID); err != nil {
		return nil, providerError("authentication_required")
	}
	user, err := as.authTable.Get(principal.ID)
	if err != nil || validatePrincipalRow(user, principalTypeUser) != nil {
		return nil, providerError(ProviderOutcomePrincipalUnavailable)
	}
	identity := providerIdentityFromFlow(flow)
	if identity.Issuer == "" || strings.TrimSpace(identity.Subject) == "" {
		return nil, providerError(ProviderOutcomeInvalidFlow)
	}
	existing := pa.findIdentity(identity.Issuer, identity.Subject)
	if existing != nil && toString(existing["principal_id"]) != principal.ID {
		_ = pa.consumeFlow(flow)
		return nil, providerError(ProviderOutcomeIdentityConflict)
	}
	var linked map[string]interface{}
	if existing != nil {
		linked = existing
	} else {
		claims, _ := json.Marshal(identity.Claims)
		identityID, randomErr := randomOpaque(24)
		if randomErr != nil {
			return nil, randomErr
		}
		now := pa.now().UnixMilli()
		linked, err = pa.identities.Insert(map[string]interface{}{
			"id": identityID, "provider_key": toString(flow["provider_key"]),
			"issuer": identity.Issuer, "subject": identity.Subject, "principal_id": principal.ID,
			"display_name": identity.DisplayName, "email": identity.Email,
			"email_verified": identity.EmailVerified, "claims": string(claims),
			"created_at": now, "updated_at": now,
		}, nil)
		if err != nil {
			existing = pa.findIdentity(identity.Issuer, identity.Subject)
			if existing == nil || toString(existing["principal_id"]) != principal.ID {
				_ = pa.consumeFlow(flow)
				return nil, providerError(ProviderOutcomeIdentityConflict)
			}
			linked = existing
		}
	}
	if err := pa.consumeFlow(flow); err != nil {
		return nil, err
	}
	out := linkedIdentityFromRow(linked)
	return &out, nil
}

func (as *AuthService) ListProviderIdentities(principal *schema.AuthContext) ([]LinkedProviderIdentity, error) {
	pa := as.providerAuth
	if pa == nil || principal == nil || principal.ID == "" || principal.SessionID == "" {
		return nil, providerError("authentication_required")
	}
	if _, err := as.requireActiveSession(principal.SessionID, principalTypeUser, principal.ID); err != nil {
		return nil, providerError("authentication_required")
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	rows := pa.findIdentitiesForPrincipal(principal.ID)
	out := make([]LinkedProviderIdentity, 0, len(rows))
	for _, row := range rows {
		out = append(out, linkedIdentityFromRow(row))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (as *AuthService) UnlinkProviderIdentity(identityID string, principal *schema.AuthContext) error {
	pa := as.providerAuth
	if pa == nil || identityID == "" || principal == nil || principal.ID == "" || principal.SessionID == "" {
		return providerError("authentication_required")
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	as.mutationMu.Lock()
	defer as.mutationMu.Unlock()
	if _, err := as.requireActiveSession(principal.SessionID, principalTypeUser, principal.ID); err != nil {
		return providerError("authentication_required")
	}
	user, err := as.authTable.Get(principal.ID)
	if err != nil || validatePrincipalRow(user, principalTypeUser) != nil {
		return providerError(ProviderOutcomePrincipalUnavailable)
	}
	identity, err := pa.identities.Get(identityID)
	if err != nil || identity == nil || toString(identity["principal_id"]) != principal.ID {
		return providerError("provider_identity_not_found")
	}
	identities := pa.findIdentitiesForPrincipal(principal.ID)
	if len(identities) <= 1 && !HasUsablePasswordHash(toString(user["password"])) {
		return providerError(ProviderOutcomeLastMethod)
	}
	deleted, err := pa.identities.Delete(identityID, nil)
	if err != nil {
		return err
	}
	if !deleted {
		return providerError("provider_identity_not_found")
	}
	return nil
}

func HasUsablePasswordHash(hash string) bool {
	if hash == "" {
		return false
	}
	passwordVerifiersMu.RLock()
	defer passwordVerifiersMu.RUnlock()
	for _, verifier := range passwordVerifiers {
		if strings.HasPrefix(hash, verifier.Prefix()) {
			return true
		}
	}
	return false
}

func (as *AuthService) CleanupExpiredProviderFlows(now time.Time, limit int) (int, error) {
	pa := as.providerAuth
	if pa == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 512
	}
	pa.mu.Lock()
	defer pa.mu.Unlock()
	const scanChunk = 512
	staleIDs := make([]string, 0, limit)
	total := pa.flows.Count()
	for offset := 0; offset < total && len(staleIDs) < limit; offset += scanChunk {
		rows, err := pa.flows.Scan(scanChunk, offset)
		if err != nil {
			return 0, err
		}
		for _, row := range rows {
			if int64(authNumber(row["expires_at"])) >= now.UnixMilli() {
				continue
			}
			staleIDs = append(staleIDs, toString(row["id"]))
			if len(staleIDs) >= limit {
				break
			}
		}
	}
	deleted := 0
	for _, id := range staleIDs {
		ok, deleteErr := pa.flows.Delete(id, nil)
		if deleteErr != nil {
			return deleted, deleteErr
		}
		if ok {
			deleted++
		}
	}
	return deleted, nil
}

func (pa *providerAuthRuntime) validFlow(flow map[string]interface{}, phase string) bool {
	return flow != nil && toString(flow["phase"]) == phase && int64(authNumber(flow["expires_at"])) >= pa.now().UnixMilli()
}

func (pa *providerAuthRuntime) findFlowByDigest(field, digest string) map[string]interface{} {
	ptrs := pa.flows.FindAllByIndex([]string{field}, digest)
	if len(ptrs) != 1 {
		return nil
	}
	row, err := pa.flows.GetByPointer(ptrs[0])
	if err != nil {
		return nil
	}
	return row
}

func (pa *providerAuthRuntime) findIdentity(issuer, subject string) map[string]interface{} {
	ptr, ok := pa.identities.FindByIndex([]string{"issuer", "subject"}, []interface{}{issuer, subject})
	if !ok {
		return nil
	}
	row, err := pa.identities.GetByPointer(ptr)
	if err != nil {
		return nil
	}
	return row
}

func (pa *providerAuthRuntime) findIdentitiesForPrincipal(principalID string) []map[string]interface{} {
	ptrs := pa.identities.FindAllByIndex([]string{"principal_id"}, principalID)
	rows := make([]map[string]interface{}, 0, len(ptrs))
	for _, ptr := range ptrs {
		row, err := pa.identities.GetByPointer(ptr)
		if err == nil && row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}

func (pa *providerAuthRuntime) consumeFlow(flow map[string]interface{}) error {
	_, err := pa.flows.Update(toString(flow["id"]), map[string]interface{}{
		"phase": "consumed", "result_consumed_at": pa.now().UnixMilli(), "result_digest": "",
		"identity_subject": "", "identity_claims": "",
	}, nil)
	return err
}

func (pa *providerAuthRuntime) digest(value string) string {
	mac := hmac.New(sha256.New, pa.digestKey)
	_, _ = mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (pa *providerAuthRuntime) encrypt(aad, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	nonce := make([]byte, pa.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := pa.aead.Seal(nil, nonce, []byte(plaintext), []byte(aad))
	return base64.RawURLEncoding.EncodeToString(append(nonce, sealed...)), nil
}

func (pa *providerAuthRuntime) decrypt(aad, encoded string) (string, error) {
	if encoded == "" {
		return "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(data) < pa.aead.NonceSize() {
		return "", fmt.Errorf("invalid encrypted provider flow value")
	}
	plaintext, err := pa.aead.Open(nil, data[:pa.aead.NonceSize()], data[pa.aead.NonceSize():], []byte(aad))
	if err != nil {
		return "", fmt.Errorf("invalid encrypted provider flow value")
	}
	return string(plaintext), nil
}

func randomOpaque(size int) (string, error) {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func providerError(code string) error {
	return &ProviderAuthError{Code: code}
}

func providerIdentityFromFlow(flow map[string]interface{}) ProviderIdentity {
	identity := ProviderIdentity{
		Issuer: toString(flow["identity_issuer"]), Subject: toString(flow["identity_subject"]),
		DisplayName: toString(flow["identity_display_name"]), Email: toString(flow["identity_email"]),
		EmailVerified: isTruthy(flow["identity_email_verified"]),
	}
	_ = json.Unmarshal([]byte(toString(flow["identity_claims"])), &identity.Claims)
	return identity
}

func linkedIdentityFromRow(row map[string]interface{}) LinkedProviderIdentity {
	out := LinkedProviderIdentity{
		ID: toString(row["id"]), Provider: toString(row["provider_key"]),
		Issuer: toString(row["issuer"]), Subject: toString(row["subject"]),
		DisplayName: toString(row["display_name"]), Email: toString(row["email"]),
		EmailVerified: isTruthy(row["email_verified"]), CreatedAt: int64(authNumber(row["created_at"])),
	}
	_ = json.Unmarshal([]byte(toString(row["claims"])), &out.Claims)
	return out
}

func sanitizeProviderDisplayClaims(claims map[string]string) map[string]string {
	if len(claims) == 0 {
		return nil
	}
	keys := make([]string, 0, len(claims))
	for key := range claims {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if len(out) >= 32 || key == "" || len(key) > 64 {
			continue
		}
		lower := strings.ToLower(key)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") || lower == "code" || lower == "state" || strings.Contains(lower, "verifier") {
			continue
		}
		value := claims[key]
		if len(value) > 2048 {
			value = value[:2048]
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
