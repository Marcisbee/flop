package flop

import "github.com/marcisbee/flop/internal/server"

// ProviderAdapter implements an external authorization-code identity provider.
type ProviderAdapter = server.ProviderAdapter

// ProviderCapabilities declares optional authorization security extensions.
type ProviderCapabilities = server.ProviderCapabilities

// ProviderAuthorizationRequest contains values that must be bound into the
// provider authorization URL.
type ProviderAuthorizationRequest = server.ProviderAuthorizationRequest

// ProviderCodeExchangeRequest contains the callback values an adapter must
// exchange and verify.
type ProviderCodeExchangeRequest = server.ProviderCodeExchangeRequest

// ProviderIdentity is a verified provider identity. Issuer and Subject are the
// ownership keys; the remaining claims are display metadata only.
type ProviderIdentity = server.ProviderIdentity

// LinkedProviderIdentity is safe account-linking metadata returned to its owner.
type LinkedProviderIdentity = server.LinkedProviderIdentity

const (
	ProviderIntentSignIn = server.ProviderIntentSignIn
	ProviderIntentLink   = server.ProviderIntentLink

	ProviderOutcomeSignInReady          = server.ProviderOutcomeSignInReady
	ProviderOutcomeLinkRequired         = server.ProviderOutcomeLinkRequired
	ProviderOutcomeLinkPending          = server.ProviderOutcomeLinkPending
	ProviderOutcomeProviderDenied       = server.ProviderOutcomeProviderDenied
	ProviderOutcomeExchangeFailed       = server.ProviderOutcomeExchangeFailed
	ProviderOutcomeInvalidFlow          = server.ProviderOutcomeInvalidFlow
	ProviderOutcomeIdentityConflict     = server.ProviderOutcomeIdentityConflict
	ProviderOutcomeLastMethod           = server.ProviderOutcomeLastMethod
	ProviderOutcomePrincipalUnavailable = server.ProviderOutcomePrincipalUnavailable
)
