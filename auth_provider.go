package flop

import (
	"context"
	"net/url"
	"time"
)

// AuthProviderAdapter implements one external authentication provider. Flop
// owns state, nonce, PKCE, callback consumption, identity linking, and Flop
// session creation; adapters only construct the provider URL and validate the
// provider response.
type AuthProviderAdapter interface {
	AuthorizationURL(context.Context, AuthProviderAuthorizationRequest) (string, error)
	Exchange(context.Context, AuthProviderCallbackRequest) (AuthProviderIdentity, error)
}

// AuthProviderGrantAdapter is the token-capable extension to
// AuthProviderAdapter. Existing identity-only adapters remain source
// compatible, but cannot create or service provider grants.
type AuthProviderGrantAdapter interface {
	AuthProviderAdapter
	ExchangeGrant(context.Context, AuthProviderCallbackRequest) (AuthProviderExchangeResult, error)
	RefreshGrant(context.Context, AuthProviderRefreshRequest) (AuthProviderTokenSet, error)
	RevokeGrant(context.Context, AuthProviderRevokeRequest) error
}

// AuthProviderCodelessAdapter marks reviewed non-OAuth protocols, such as
// Steam OpenID 2.0, whose authenticated callback does not contain a code.
type AuthProviderCodelessAdapter interface{ CallbackCodeOptional() bool }
type AuthProviderRevocationPreference interface{ RevocationTokenType() string }
type AuthProviderCapabilityAdapter interface {
	ProviderCapabilities() AuthProviderCapabilities
}

// AuthProviderAppConfig owns the registrations and backend trust boundary for
// one client application. BackendCredentials are accepted as configuration
// input only; Flop persists salted hashes, never their plaintext values.
type AuthProviderAppConfig struct {
	AllowedReturnURLs  []string
	BackendCredentials []string
	Providers          map[string]AuthProviderConfig
}

// AuthProviderConfig registers an adapter under the map key used in Config.
type AuthProviderConfig struct {
	Adapter AuthProviderAdapter

	// Issuer is the exact canonical issuer this registration accepts. It is
	// required and is compared byte-for-byte with the verified identity.
	Issuer string

	// RedirectURI is the exact callback URI registered with the provider.
	RedirectURI string

	// AllowedReturnURLs is an exact allowlist of client destinations to which
	// Flop may return a one-time completion code after the provider callback.
	AllowedReturnURLs []string

	// ClientID and ClientSecret identify this app's upstream registration.
	// Reusing a non-empty ClientID across apps is rejected.
	ClientID     string
	ClientSecret string

	AllowedScopes     []string
	DefaultScopes     []string
	RequiredScopes    []string
	CredentialVersion string

	// PKCEUnsupported may only be set for an adapter whose provider has been
	// reviewed and cannot support PKCE. Flop otherwise always uses S256.
	PKCEUnsupported bool
}

// AuthProviderAuthorizationRequest contains server-generated, server-bound
// authorization parameters. CodeChallengeMethod is either "S256" or empty
// when PKCEUnsupported was explicitly configured.
type AuthProviderAuthorizationRequest struct {
	AppID               string
	Provider            string
	State               string
	Nonce               string
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string
	ClientID            string
	ClientSecret        string
	Scopes              []string
}

// AuthProviderCallbackRequest contains transient callback material. Adapters
// must validate the provider response before returning an identity. OIDC
// adapters must validate signature, exact issuer, audience, expiry, and nonce.
// OAuth/non-standard adapters must resolve Subject through an authenticated
// provider API rather than profile labels or email.
type AuthProviderCallbackRequest struct {
	AppID           string
	Provider        string
	Code            string
	RedirectURI     string
	CodeVerifier    string
	Nonce           string
	Parameters      url.Values
	ClientID        string
	ClientSecret    string
	RequestedScopes []string
}

// AuthProviderIdentity is the normalized, verified provider identity.
// Authentication uses only Issuer and Subject. All other fields are display
// metadata and are never used to select or automatically link a Flop user.
type AuthProviderIdentity struct {
	Provider      string
	Issuer        string
	Subject       string
	DisplayName   string
	Email         string
	EmailVerified bool
}

// AuthProviderTokenSet is normalized token material. It is encrypted before
// persistence and refresh tokens are never returned by Flop's public APIs.
type AuthProviderTokenSet struct {
	AccessToken  string
	TokenType    string
	ExpiresAt    time.Time
	RefreshToken string
	Scopes       []string
	IDToken      string `json:"-"`
}

// AuthProviderExchangeResult is returned by token-capable adapters.
type AuthProviderExchangeResult struct {
	Identity      AuthProviderIdentity
	Tokens        AuthProviderTokenSet
	GrantedScopes []string
	Capabilities  AuthProviderCapabilities
}

type AuthProviderCapabilities struct {
	Refresh    bool `json:"refresh"`
	Revocation bool `json:"revocation"`
}

type AuthProviderRefreshRequest struct {
	AppID, Provider, ClientID, ClientSecret, RefreshToken string
	Scopes                                                []string
}

type AuthProviderRevokeRequest struct {
	AppID, Provider, ClientID, ClientSecret, Token, TokenTypeHint string
}

// AuthProviderUpstreamError carries a safe protocol error code. Terminal is
// true when reconnecting is required (for example OAuth invalid_grant).
type AuthProviderUpstreamError struct {
	Code     string
	Terminal bool
	Cause    error
}

func (e *AuthProviderUpstreamError) Error() string {
	if e == nil || e.Code == "" {
		return "provider request failed"
	}
	return "provider request failed: " + e.Code
}
func (e *AuthProviderUpstreamError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ProviderTokenLease is the server-only token retrieval result.
type ProviderTokenLease struct {
	AccessToken string    `json:"accessToken"`
	TokenType   string    `json:"tokenType"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Scopes      []string  `json:"scopes"`
}

// ProviderIdentityGrant is the verified provider identity resolved for one
// app-isolated grant. It is returned only to an authenticated app backend.
type ProviderIdentityGrant struct {
	GrantID  string `json:"grantId"`
	AppID    string `json:"appID"`
	Provider string `json:"provider"`
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
}
