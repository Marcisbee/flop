package flop

import (
	"context"
	"net/url"
)

// AuthProviderAdapter implements one external authentication provider. Flop
// owns state, nonce, PKCE, callback consumption, identity linking, and Flop
// session creation; adapters only construct the provider URL and validate the
// provider response.
type AuthProviderAdapter interface {
	AuthorizationURL(context.Context, AuthProviderAuthorizationRequest) (string, error)
	Exchange(context.Context, AuthProviderCallbackRequest) (AuthProviderIdentity, error)
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

	// PKCEUnsupported may only be set for an adapter whose provider has been
	// reviewed and cannot support PKCE. Flop otherwise always uses S256.
	PKCEUnsupported bool
}

// AuthProviderAuthorizationRequest contains server-generated, server-bound
// authorization parameters. CodeChallengeMethod is either "S256" or empty
// when PKCEUnsupported was explicitly configured.
type AuthProviderAuthorizationRequest struct {
	Provider            string
	State               string
	Nonce               string
	RedirectURI         string
	CodeChallenge       string
	CodeChallengeMethod string
}

// AuthProviderCallbackRequest contains transient callback material. Adapters
// must validate the provider response before returning an identity. OIDC
// adapters must validate signature, exact issuer, audience, expiry, and nonce.
// OAuth/non-standard adapters must resolve Subject through an authenticated
// provider API rather than profile labels or email.
type AuthProviderCallbackRequest struct {
	Provider     string
	Code         string
	RedirectURI  string
	CodeVerifier string
	Nonce        string
	Parameters   url.Values
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
