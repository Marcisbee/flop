package flop

import (
	"net/mail"
	"strings"
)

// requestSignupEmail persists a hashed challenge before SMTP delivery. It never
// creates accounts, changes provider claims, or extends the completion lifetime.
func (s *providerAuthService) requestSignupEmail(code, email, browserBinding string) error {
	email = strings.TrimSpace(email)
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || strings.ContainsAny(email, "\r\n") || len(email) > 254 {
		return providerError("invalid_email", "a valid email address is required", 400)
	}
	s.mu.Lock()
	flow, err := s.completionFlowLocked(code, browserBinding)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if toString(flow["intent"]) != "sign_up" || toString(flow["result_error_code"]) != "" || toString(flow["result_subject"]) == "" {
		s.mu.Unlock()
		return providerError("invalid_intent", "email verification requires a successful signup authorization", 400)
	}
	if providerBool(flow["result_email_verified"]) && strings.TrimSpace(toString(flow["result_email"])) != "" {
		s.mu.Unlock()
		return providerError("email_verification_not_required", "provider email is already verified", 400)
	}
	now := s.now().Unix()
	if sent := providerUnix(flow["signup_email_sent_at"]); sent > 0 && now-sent < 60 {
		s.mu.Unlock()
		return providerError("email_verification_rate_limited", "wait before requesting another email", 429)
	}
	token, err := s.randomToken(24)
	if err != nil {
		s.mu.Unlock()
		return providerError("provider_flow_failed", "email verification unavailable", 500, err)
	}
	hash := hashProviderToken(token)
	id := toString(flow["id"])
	flows := s.db.Table(systemAuthProviderFlowTableName)
	_, err = flows.Update(id, map[string]any{"signup_email": email, "signup_email_token_hash": hash, "signup_email_sent_at": now})
	s.mu.Unlock()
	if err != nil {
		return providerError("provider_flow_failed", "email verification unavailable", 500, err)
	}
	// SMTP must not hold the global provider/session lock.
	if err := s.db.sendAuthTemplateEmail("provider-signup", email, email, token); err != nil {
		s.mu.Lock()
		current, readErr := flows.Get(id)
		if readErr == nil && current != nil && toString(current["signup_email_token_hash"]) == hash {
			_, _ = flows.Update(id, map[string]any{"signup_email_token_hash": ""})
		}
		s.mu.Unlock()
		return providerError("email_delivery_failed", "verification email could not be sent; retry later", 502, err)
	}
	return nil
}
