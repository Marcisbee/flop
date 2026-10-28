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

// RefreshSession consumes a user refresh token and returns replacement access
// and refresh tokens. Rotation invalidates both tokens of the previous session.
// Persist the returned pair together; refresh tokens are single-use.
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
