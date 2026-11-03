package flop

import "fmt"

// CreateSession creates a standard user session for an existing account.
// This is a trusted backend API: the caller must authenticate the account first.
// Never pass an unverified client-supplied account ID. Flop validates the current
// account state and issues its normal access and refresh tokens.
func (d *Database) CreateSession(accountID string) (token, refreshToken string, err error) {
	if d == nil || d.authService == nil {
		return "", "", fmt.Errorf("auth not configured")
	}
	return d.authService.CreateSession(accountID)
}

// RefreshSession rotates a user session and returns replacement credentials.
// The previous access token becomes invalid. Retrying an unexpired refresh token
// recovers the same immediate replacement while that session is active; it does
// not extend the replacement refresh token's expiry or follow later rotations.
// Persist the returned pair together. Recovery survives database reopening.
func (d *Database) RefreshSession(refreshToken string) (token, nextRefreshToken string, err error) {
	if d == nil || d.authService == nil {
		return "", "", fmt.Errorf("auth not configured")
	}
	return d.authService.Refresh(refreshToken)
}

// RevokeSession revokes the specified user session, invalidating its access and
// refresh tokens. Obtain accountID and sessionID from a validated AuthContext.
// This trusted backend API checks ownership and accepts repeated revocation.
// After refresh, use the replacement session's ID. Other sessions are unaffected.
func (d *Database) RevokeSession(accountID, sessionID string) error {
	if d == nil || d.authService == nil {
		return fmt.Errorf("auth not configured")
	}
	return d.authService.RevokeSession(accountID, sessionID)
}
