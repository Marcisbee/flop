package flop

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const providerRefreshWindow = time.Minute

type providerGrantView struct {
	ID         string   `json:"id"`
	AppID      string   `json:"appID"`
	Provider   string   `json:"provider"`
	IdentityID string   `json:"identityId"`
	Scopes     []string `json:"scopes"`
	State      string   `json:"state"`
	ExpiresAt  int64    `json:"expiresAt,omitempty"`
}

func canonicalStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func scopeSubset(required, granted []string) bool {
	for _, scope := range canonicalStrings(required) {
		if !exactStringMatch(granted, scope) {
			return false
		}
	}
	return true
}

func (s *providerAuthService) appProviderConfig(appID, provider string) (AuthProviderAppConfig, AuthProviderConfig, error) {
	if s == nil || s.initErr != nil {
		return AuthProviderAppConfig{}, AuthProviderConfig{}, providerError("provider_auth_unavailable", "provider authentication unavailable", 503, s.initErr)
	}
	if appID == "" && len(s.apps) == 1 {
		for key := range s.apps {
			appID = key
		}
	}
	app, ok := s.apps[appID]
	if !ok {
		return AuthProviderAppConfig{}, AuthProviderConfig{}, providerError("app_not_configured", "provider app not configured", 404)
	}
	config, ok := app.Providers[provider]
	if !ok || config.Adapter == nil {
		return AuthProviderAppConfig{}, AuthProviderConfig{}, providerError("provider_not_configured", "provider not configured", 404)
	}
	return app, config, nil
}

func (s *providerAuthService) registration(appID, provider string) (map[string]any, error) {
	row, ok := s.db.Table(systemAuthProviderRegistrationTableName).FindByUniqueCompositeIndex([]string{"app_id", "provider"}, appID, provider)
	if !ok || !providerBool(row["enabled"]) {
		return nil, providerError("provider_not_configured", "provider not configured", 404)
	}
	return row, nil
}

func (s *providerAuthService) registrationClientSecret(registration map[string]any, fallback string) (string, error) {
	encoded := toString(registration["credential_ciphertext"])
	if encoded == "" {
		return fallback, nil
	}
	var credentials map[string]string
	if err := s.openProviderValue("credential", toString(registration["app_id"]), toString(registration["id"]), toString(registration["id"]), encoded, toString(registration["credential_key_version"]), &credentials); err != nil {
		return "", err
	}
	return credentials["clientSecret"], nil
}

func providerAAD(kind, appID, registrationID, recordID, version string) []byte {
	return []byte("flop/provider/" + kind + "/v1\x00" + appID + "\x00" + registrationID + "\x00" + recordID + "\x00" + version)
}

func (s *providerAuthService) sealProviderValue(kind, appID, registrationID, recordID string, value any) (string, string, error) {
	version := s.activeKey
	aead := s.tokenAEAD[version]
	if aead == nil {
		return "", "", fmt.Errorf("provider secret key unavailable")
	}
	plain, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(s.random, nonce); err != nil {
		return "", "", err
	}
	sealed := aead.Seal(nonce, nonce, plain, providerAAD(kind, appID, registrationID, recordID, version))
	return base64.RawURLEncoding.EncodeToString(sealed), version, nil
}

func (s *providerAuthService) openProviderValue(kind, appID, registrationID, recordID, encoded, version string, out any) error {
	aead := s.tokenAEAD[version]
	if aead == nil {
		return fmt.Errorf("provider secret key version unavailable")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(sealed) < aead.NonceSize() {
		return fmt.Errorf("invalid provider ciphertext")
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], providerAAD(kind, appID, registrationID, recordID, version))
	if err != nil {
		return fmt.Errorf("provider ciphertext authentication failed")
	}
	return json.Unmarshal(plain, out)
}

func hashBackendCredential(credential string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	digest := sha256.Sum256(append(append([]byte{}, salt...), []byte(credential)...))
	return hex.EncodeToString(salt) + ":" + hex.EncodeToString(digest[:]), nil
}

func checkBackendCredential(encoded, credential string) bool {
	parts := strings.Split(encoded, ":")
	if len(parts) != 2 {
		return false
	}
	salt, err1 := hex.DecodeString(parts[0])
	expected, err2 := hex.DecodeString(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	digest := sha256.Sum256(append(append([]byte{}, salt...), []byte(credential)...))
	return len(expected) == len(digest) && subtle.ConstantTimeCompare(expected, digest[:]) == 1
}

func storedStrings(value any) []string {
	switch value := value.(type) {
	case []string:
		return canonicalStrings(value)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return canonicalStrings(out)
	case string:
		var out []string
		if json.Unmarshal([]byte(value), &out) == nil {
			return canonicalStrings(out)
		}
	}
	return []string{}
}

func (s *providerAuthService) syncRegistrations() error {
	if s.db == nil {
		return nil
	}
	for _, encrypted := range []struct{ table, ciphertext, version string }{
		{systemAuthProviderRegistrationTableName, "credential_ciphertext", "credential_key_version"},
		{systemAuthProviderGrantTableName, "token_ciphertext", "token_key_version"},
		{systemAuthProviderRevocationTableName, "token_ciphertext", "token_key_version"},
		{systemAuthProviderFlowTableName, "pending_tokens_ciphertext", "pending_tokens_key_version"},
		{systemAuthProviderFlowTableName, "secrets_ciphertext", "secrets_key_version"},
	} {
		for _, row := range providerAllRows(s.db.Table(encrypted.table)) {
			if toString(row[encrypted.ciphertext]) != "" && toString(row[encrypted.version]) != "" && s.tokenAEAD[toString(row[encrypted.version])] == nil {
				return fmt.Errorf("flop: encrypted provider state requires unavailable key version %q", toString(row[encrypted.version]))
			}
		}
	}
	grants := s.db.Table(systemAuthProviderGrantTableName)
	now := time.Now().Unix()
	for _, row := range providerAllRows(s.db.Table(systemAuthProviderAppTableName)) {
		if _, configured := s.apps[toString(row["app_id"])]; !configured {
			_, _ = s.db.Table(systemAuthProviderAppTableName).Update(toString(row["id"]), map[string]any{"enabled": false, "backend_credential_hashes": []string{}, "updated_at": now})
		}
	}
	for _, row := range providerAllRows(s.db.Table(systemAuthProviderRegistrationTableName)) {
		app, appConfigured := s.apps[toString(row["app_id"])]
		_, providerConfigured := app.Providers[toString(row["provider"])]
		if !appConfigured || !providerConfigured {
			_, _ = s.db.Table(systemAuthProviderRegistrationTableName).Update(toString(row["id"]), map[string]any{"enabled": false, "updated_at": now})
		}
	}
	for appID, app := range s.apps {
		hashes := make([]string, 0, len(app.BackendCredentials))
		for _, credential := range app.BackendCredentials {
			if credential == "" {
				return fmt.Errorf("flop: provider app %q has an empty backend credential", appID)
			}
			hash, err := hashBackendCredential(credential)
			if err != nil {
				return err
			}
			hashes = append(hashes, hash)
		}
		appsTable := s.db.Table(systemAuthProviderAppTableName)
		if row, ok := appsTable.FindByUniqueIndex("app_id", appID); ok {
			if _, err := appsTable.Update(toString(row["id"]), map[string]any{"backend_credential_hashes": hashes, "enabled": true, "updated_at": now}); err != nil {
				return err
			}
		} else if _, err := appsTable.Insert(map[string]any{"app_id": appID, "backend_credential_hashes": hashes, "enabled": true, "updated_at": now}); err != nil {
			return err
		}
		for provider, config := range app.Providers {
			fingerprintInput, _ := json.Marshal([]any{appID, provider, config.Issuer, config.ClientID, config.RedirectURI, canonicalStrings(config.AllowedScopes), canonicalStrings(config.DefaultScopes), canonicalStrings(config.RequiredScopes), config.CredentialVersion})
			fingerprint := sha256.Sum256(fingerprintInput)
			registrations := s.db.Table(systemAuthProviderRegistrationTableName)
			row, exists := registrations.FindByUniqueCompositeIndex([]string{"app_id", "provider"}, appID, provider)
			oldClientID := ""
			oldIssuer := ""
			if exists {
				oldClientID = toString(row["client_id"])
				oldIssuer = toString(row["issuer"])
			}
			updates := map[string]any{"issuer": config.Issuer, "client_id": config.ClientID, "credential_version": config.CredentialVersion, "config_fingerprint": hex.EncodeToString(fingerprint[:]), "enabled": true, "updated_at": now}
			if !exists {
				created, err := registrations.Insert(map[string]any{"app_id": appID, "provider": provider, "issuer": config.Issuer, "client_id": config.ClientID, "credential_version": config.CredentialVersion, "config_fingerprint": hex.EncodeToString(fingerprint[:]), "enabled": true, "updated_at": now})
				if err != nil {
					return err
				}
				row = created
			} else if _, err := registrations.Update(toString(row["id"]), updates); err != nil {
				return err
			}
			registrationID := toString(row["id"])
			if config.ClientSecret != "" {
				ciphertext, version, err := s.sealProviderValue("credential", appID, registrationID, registrationID, map[string]string{"clientSecret": config.ClientSecret})
				if err != nil {
					return err
				}
				if _, err := registrations.Update(registrationID, map[string]any{"credential_ciphertext": ciphertext, "credential_key_version": version}); err != nil {
					return err
				}
			} else if toString(row["credential_ciphertext"]) != "" {
				if _, err := registrations.Update(registrationID, map[string]any{"credential_ciphertext": "", "credential_key_version": ""}); err != nil {
					return err
				}
			}
			if exists && (oldClientID != config.ClientID || oldIssuer != config.Issuer) {
				rows, _ := grants.FindByIndex("app_id", appID)
				for _, grant := range rows {
					if toString(grant["registration_id"]) == registrationID {
						_, _ = grants.Update(toString(grant["id"]), map[string]any{"state": "reconnect_required"})
					}
				}
			}
		}
	}
	return nil
}

func providerAllRows(table *TableInstance) []map[string]any {
	if table == nil || table.ti == nil {
		return nil
	}
	out := []map[string]any{}
	offset := 0
	for {
		rows, err := table.ti.Scan(256, offset)
		if err != nil || len(rows) == 0 {
			break
		}
		out = append(out, rows...)
		offset += len(rows)
	}
	return out
}

func providerGrantViewFromRow(row map[string]any) providerGrantView {
	return providerGrantView{ID: toString(row["id"]), AppID: toString(row["app_id"]), Provider: toString(row["provider"]), IdentityID: toString(row["identity_id"]), Scopes: storedStrings(row["granted_scopes"]), State: toString(row["state"]), ExpiresAt: providerUnix(row["access_expires_at"])}
}

func (s *providerAuthService) materializeGrant(tx *providerTxn, flow, identity map[string]any) (*providerGrantView, error) {
	encoded := toString(flow["pending_tokens_ciphertext"])
	if encoded == "" {
		return nil, nil
	}
	appID, registrationID, flowID := toString(flow["app_id"]), toString(flow["registration_id"]), toString(flow["id"])
	var tokens AuthProviderTokenSet
	if err := s.openProviderValue("flow", appID, registrationID, flowID, encoded, toString(flow["pending_tokens_key_version"]), &tokens); err != nil {
		return nil, err
	}
	tokens.Scopes = canonicalStrings(tokens.Scopes)
	grants := s.db.Table(systemAuthProviderGrantTableName)
	existing, exists := grants.FindByUniqueCompositeIndex([]string{"registration_id", "identity_id"}, registrationID, toString(identity["id"]))
	grantID := ""
	var err error
	if exists {
		grantID = toString(existing["id"])
	} else {
		grantID, err = s.randomToken(18)
		if err != nil {
			return nil, err
		}
	}
	ciphertext, version, err := s.sealProviderValue("grant", appID, registrationID, grantID, tokens)
	if err != nil {
		return nil, err
	}
	updates := map[string]any{"granted_scopes": tokens.Scopes, "token_ciphertext": ciphertext, "token_key_version": version, "state": "active", "consented_at": s.now().Unix()}
	if !tokens.ExpiresAt.IsZero() {
		updates["access_expires_at"] = tokens.ExpiresAt.Unix()
	}
	if exists {
		if err := tx.update(grants, grantID, updates); err != nil {
			return nil, err
		}
	} else {
		created := map[string]any{"id": grantID, "principal_id": toString(identity["principal_id"]), "identity_id": toString(identity["id"]), "registration_id": registrationID, "app_id": appID, "provider": toString(flow["provider"]), "consented_at": s.now().Unix()}
		for key, value := range updates {
			created[key] = value
		}
		inserted, insertErr := tx.insert(grants, created)
		if insertErr != nil {
			return nil, insertErr
		}
		existing = inserted
	}
	row := make(map[string]any, len(existing)+len(updates))
	for key, value := range existing {
		row[key] = value
	}
	for key, value := range updates {
		row[key] = value
	}
	view := providerGrantViewFromRow(row)
	return &view, nil
}

func (s *providerAuthService) listGrants(principalID, appID string) ([]providerGrantView, error) {
	rows, err := s.db.Table(systemAuthProviderGrantTableName).FindByIndex("principal_id", principalID)
	if err != nil {
		return nil, err
	}
	out := []providerGrantView{}
	for _, row := range rows {
		if appID == "" || toString(row["app_id"]) == appID {
			out = append(out, providerGrantViewFromRow(row))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *providerAuthService) authenticateBackend(appID, credential string) bool {
	row, ok := s.db.Table(systemAuthProviderAppTableName).FindByUniqueIndex("app_id", appID)
	if !ok || !providerBool(row["enabled"]) {
		return false
	}
	matched := 0
	for _, encoded := range storedStrings(row["backend_credential_hashes"]) {
		if checkBackendCredential(encoded, credential) {
			matched = 1
		}
	}
	return matched == 1
}

func (s *providerAuthService) tokenLease(ctx context.Context, appID, backendCredential, grantID string, requiredScopes []string) (*ProviderTokenLease, error) {
	if !s.authenticateBackend(appID, backendCredential) {
		return nil, providerError("backend_unauthorized", "backend authentication failed", 401)
	}
	lockValue, _ := s.grantLocks.LoadOrStore(grantID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	grants := s.db.Table(systemAuthProviderGrantTableName)
	if _, unusable := s.unusableGrants.Load(grantID); unusable {
		return nil, providerError("reconnect_required", "provider grant requires reconnection", 409)
	}
	row, err := grants.Get(grantID)
	if err != nil || row == nil || toString(row["app_id"]) != appID {
		return nil, providerError("grant_not_found", "provider grant not found", 404)
	}
	if toString(row["state"]) != "active" {
		return nil, providerError("reconnect_required", "provider grant requires reconnection", 409)
	}
	granted := storedStrings(row["granted_scopes"])
	if !scopeSubset(requiredScopes, granted) {
		return nil, providerError("insufficient_scope", "provider grant lacks required scope", 403)
	}
	var tokens AuthProviderTokenSet
	if err := s.openProviderValue("grant", appID, toString(row["registration_id"]), grantID, toString(row["token_ciphertext"]), toString(row["token_key_version"]), &tokens); err != nil {
		return nil, providerError("reconnect_required", "provider grant requires reconnection", 409, err)
	}
	if !tokens.ExpiresAt.IsZero() && time.Until(tokens.ExpiresAt) <= providerRefreshWindow {
		_, config, configErr := s.appProviderConfig(appID, toString(row["provider"]))
		registration, registrationErr := s.db.Table(systemAuthProviderRegistrationTableName).Get(toString(row["registration_id"]))
		if configErr == nil && registrationErr != nil {
			configErr = registrationErr
		}
		clientSecret := config.ClientSecret
		if configErr == nil {
			clientSecret, configErr = s.registrationClientSecret(registration, config.ClientSecret)
		}
		adapter, ok := config.Adapter.(AuthProviderGrantAdapter)
		if advertised, hasCapabilities := config.Adapter.(AuthProviderCapabilityAdapter); hasCapabilities && !advertised.ProviderCapabilities().Refresh {
			ok = false
		}
		if configErr != nil || !ok || tokens.RefreshToken == "" {
			_, _ = grants.Update(grantID, map[string]any{"state": "reconnect_required"})
			return nil, providerError("reconnect_required", "provider grant requires reconnection", 409, configErr)
		}
		refreshed, refreshErr := adapter.RefreshGrant(ctx, AuthProviderRefreshRequest{AppID: appID, Provider: toString(row["provider"]), ClientID: config.ClientID, ClientSecret: clientSecret, RefreshToken: tokens.RefreshToken, Scopes: granted})
		if refreshErr != nil {
			var upstream *AuthProviderUpstreamError
			if errors.As(refreshErr, &upstream) && upstream.Terminal {
				s.unusableGrants.Store(grantID, true)
				_, _ = grants.Update(grantID, map[string]any{"state": "reconnect_required"})
				return nil, providerError("reconnect_required", "provider grant requires reconnection", 409, refreshErr)
			}
			return nil, providerError("refresh_failed", "provider token refresh failed", 502, refreshErr)
		}
		if refreshed.RefreshToken == "" {
			refreshed.RefreshToken = tokens.RefreshToken
		}
		if refreshed.AccessToken == "" {
			s.unusableGrants.Store(grantID, true)
			_, _ = grants.Update(grantID, map[string]any{"state": "reconnect_required"})
			return nil, providerError("reconnect_required", "provider grant requires reconnection", 409)
		}
		refreshed.Scopes = canonicalStrings(refreshed.Scopes)
		if len(refreshed.Scopes) == 0 {
			refreshed.Scopes = granted
		}
		ciphertext, version, sealErr := s.sealProviderValue("grant", appID, toString(row["registration_id"]), grantID, refreshed)
		if sealErr != nil {
			s.unusableGrants.Store(grantID, true)
			_, _ = grants.Update(grantID, map[string]any{"state": "reconnect_required"})
			return nil, providerError("reconnect_required", "provider grant requires reconnection", 409, sealErr)
		}
		if _, persistErr := grants.Update(grantID, map[string]any{"token_ciphertext": ciphertext, "token_key_version": version, "access_expires_at": refreshed.ExpiresAt.Unix(), "granted_scopes": refreshed.Scopes, "refreshed_at": s.now().Unix()}); persistErr != nil {
			s.unusableGrants.Store(grantID, true)
			_, _ = grants.Update(grantID, map[string]any{"state": "reconnect_required"})
			return nil, providerError("reconnect_required", "provider grant requires reconnection", 409, persistErr)
		}
		tokens = refreshed
		granted = refreshed.Scopes
	}
	return &ProviderTokenLease{AccessToken: tokens.AccessToken, TokenType: tokens.TokenType, ExpiresAt: tokens.ExpiresAt, Scopes: granted}, nil
}

func (s *providerAuthService) revokeGrant(ctx context.Context, principalID, grantID string) error {
	lockValue, _ := s.grantLocks.LoadOrStore(grantID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	grants := s.db.Table(systemAuthProviderGrantTableName)
	row, err := grants.Get(grantID)
	if err != nil || row == nil || toString(row["principal_id"]) != principalID {
		return providerError("grant_not_found", "provider grant not found", 404)
	}
	if toString(row["state"]) == "revoked" {
		return nil
	}
	tx := newProviderTxn(s.db)
	retryID, err := s.stageGrantRevocation(tx, row)
	if err != nil {
		tx.abort()
		return providerError("provider_grant_failed", "provider grant could not be revoked", 500, err)
	}
	if err := tx.commit(); err != nil {
		return providerError("provider_grant_failed", "provider grant could not be revoked", 500, err)
	}
	if err := s.attemptRevocation(ctx, retryID); err != nil {
		return providerError("revocation_pending", "provider access is disabled; remote revocation is pending", 202, err)
	}
	return nil
}

func (s *providerAuthService) stageGrantRevocation(tx *providerTxn, grant map[string]any) (string, error) {
	grantID, appID, registrationID := toString(grant["id"]), toString(grant["app_id"]), toString(grant["registration_id"])
	if existing, ok := s.db.Table(systemAuthProviderRevocationTableName).FindByUniqueIndex("grant_id", grantID); ok {
		return toString(existing["id"]), nil
	}
	var tokens AuthProviderTokenSet
	if err := s.openProviderValue("grant", appID, registrationID, grantID, toString(grant["token_ciphertext"]), toString(grant["token_key_version"]), &tokens); err != nil {
		return "", err
	}
	retryID, err := s.randomToken(18)
	if err != nil {
		return "", err
	}
	ciphertext, version, err := s.sealProviderValue("revocation", appID, registrationID, retryID, tokens)
	if err != nil {
		return "", err
	}
	if err := tx.update(s.db.Table(systemAuthProviderGrantTableName), grantID, map[string]any{"state": "revoking", "revoked_at": s.now().Unix()}); err != nil {
		return "", err
	}
	_, err = tx.insert(s.db.Table(systemAuthProviderRevocationTableName), map[string]any{"id": retryID, "grant_id": grantID, "app_id": appID, "registration_id": registrationID, "token_ciphertext": ciphertext, "token_key_version": version, "attempts": 0, "next_attempt_at": s.now().Unix()})
	return retryID, err
}

func (s *providerAuthService) attemptRevocation(ctx context.Context, retryID string) error {
	retries := s.db.Table(systemAuthProviderRevocationTableName)
	retry, err := retries.Get(retryID)
	if err != nil || retry == nil {
		return err
	}
	grants := s.db.Table(systemAuthProviderGrantTableName)
	grant, err := grants.Get(toString(retry["grant_id"]))
	if err != nil || grant == nil {
		return err
	}
	appID, registrationID := toString(retry["app_id"]), toString(retry["registration_id"])
	var tokens AuthProviderTokenSet
	if err := s.openProviderValue("revocation", appID, registrationID, retryID, toString(retry["token_ciphertext"]), toString(retry["token_key_version"]), &tokens); err != nil {
		return err
	}
	_, config, configErr := s.appProviderConfig(appID, toString(grant["provider"]))
	adapter, ok := config.Adapter.(AuthProviderGrantAdapter)
	remoteErr := configErr
	registration, registrationErr := s.db.Table(systemAuthProviderRegistrationTableName).Get(registrationID)
	if remoteErr == nil && registrationErr != nil {
		remoteErr = registrationErr
	}
	clientSecret := config.ClientSecret
	if remoteErr == nil {
		clientSecret, remoteErr = s.registrationClientSecret(registration, config.ClientSecret)
	}
	if remoteErr == nil && ok {
		hint := "access_token"
		if preference, preferred := config.Adapter.(AuthProviderRevocationPreference); preferred {
			hint = preference.RevocationTokenType()
		}
		token := tokens.AccessToken
		if hint == "refresh_token" && tokens.RefreshToken != "" {
			token = tokens.RefreshToken
		} else {
			hint = "access_token"
		}
		remoteErr = adapter.RevokeGrant(ctx, AuthProviderRevokeRequest{AppID: appID, Provider: toString(grant["provider"]), ClientID: config.ClientID, ClientSecret: clientSecret, Token: token, TokenTypeHint: hint})
	}
	if !ok && remoteErr == nil {
		remoteErr = fmt.Errorf("provider adapter does not support revocation")
	}
	if remoteErr != nil {
		attempts := providerUnix(retry["attempts"]) + 1
		_, _ = retries.Update(retryID, map[string]any{"attempts": attempts, "next_attempt_at": s.now().Add(time.Duration(attempts) * time.Minute).Unix(), "last_error_code": "provider_unavailable"})
		_, _ = grants.Update(toString(grant["id"]), map[string]any{"state": "revoked"})
		return remoteErr
	}
	finish := newProviderTxn(s.db)
	if err := finish.update(grants, toString(grant["id"]), map[string]any{"state": "revoked", "token_ciphertext": "", "token_key_version": ""}); err != nil {
		finish.abort()
		return err
	}
	if err := finish.delete(retries, retryID); err != nil {
		finish.abort()
		return err
	}
	return finish.commit()
}

// RetryProviderRevocations retries due upstream cleanup while local access
// remains disabled. It returns the number of terminally completed attempts.
func (d *Database) RetryProviderRevocations(ctx context.Context) (int, error) {
	if d == nil || d.providerAuth == nil {
		return 0, nil
	}
	completed := 0
	for _, row := range providerAllRows(d.Table(systemAuthProviderRevocationTableName)) {
		if providerUnix(row["next_attempt_at"]) > time.Now().Unix() {
			continue
		}
		if err := d.providerAuth.attemptRevocation(ctx, toString(row["id"])); err != nil {
			continue
		}
		completed++
	}
	return completed, nil
}

// ProviderToken retrieves a provider access token for a trusted backend. The
// credential is checked against the app registration and refresh tokens never
// leave the engine.
func (d *Database) ProviderToken(ctx context.Context, appID, backendCredential, grantID string, requiredScopes ...string) (*ProviderTokenLease, error) {
	if d == nil || d.providerAuth == nil {
		return nil, providerError("provider_auth_unavailable", "provider authentication unavailable", 404)
	}
	return d.providerAuth.tokenLease(ctx, appID, backendCredential, grantID, requiredScopes)
}
