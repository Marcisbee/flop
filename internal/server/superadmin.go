package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
)

// SuperadminService manages built-in admin accounts stored in the _superadmin table.
type SuperadminService struct {
	table           *engine.TableInstance
	sessionTable    *engine.TableInstance
	secret          string
	instanceID      string
	accessTokenTTL  int64
	refreshTokenTTL int64
}

func NewSuperadminService(table, sessionTable *engine.TableInstance, secret, instanceID string) *SuperadminService {
	return &SuperadminService{
		table:           table,
		sessionTable:    sessionTable,
		secret:          secret,
		instanceID:      instanceID,
		accessTokenTTL:  900,
		refreshTokenTTL: 604800,
	}
}

func (s *SuperadminService) Login(email, password string) (token, refreshToken string, auth *schema.AuthContext, err error) {
	user := s.findByEmail(email)
	if user == nil {
		return "", "", nil, fmt.Errorf("invalid credentials")
	}
	if !VerifyPassword(password, toString(user["password"])) {
		return "", "", nil, fmt.Errorf("invalid credentials")
	}
	if err := validatePrincipalRow(user, principalTypeSuperadmin); err != nil {
		return "", "", nil, err
	}
	pk := s.getPK(user)
	name := toString(user["name"])
	sessionID, err := s.createSession(pk, "login")
	if err != nil {
		return "", "", nil, err
	}
	token = s.issueToken(pk, toString(user["email"]), name, sessionID)
	refreshToken = s.issueRefreshToken(pk, sessionID)
	return token, refreshToken, &schema.AuthContext{
		ID:            pk,
		Email:         toString(user["email"]),
		Roles:         []string{"superadmin"},
		PrincipalType: principalTypeSuperadmin,
		SessionID:     sessionID,
		InstanceID:    s.instanceID,
	}, nil
}

func (s *SuperadminService) Refresh(refreshToken string) (string, string, error) {
	payload := VerifyJWT(refreshToken, s.secret)
	if payload == nil {
		return "", "", fmt.Errorf("invalid refresh token")
	}
	if payload.InstanceID != s.instanceID || payload.PrincipalType != principalTypeSuperadmin || payload.SessionID == "" {
		return "", "", fmt.Errorf("invalid refresh token")
	}
	session, err := s.requireActiveSession(payload.SessionID, payload.Sub)
	if err != nil {
		return "", "", err
	}
	user, err := s.table.Get(payload.Sub)
	if err != nil || user == nil {
		_ = s.revokeSession(payload.SessionID, "principal_missing")
		return "", "", fmt.Errorf("superadmin not found")
	}
	if err := validatePrincipalRow(user, principalTypeSuperadmin); err != nil {
		_ = s.revokeSession(payload.SessionID, "principal_blocked")
		return "", "", err
	}
	newSessionID, err := s.rotateSession(session, payload.Sub)
	if err != nil {
		return "", "", err
	}
	return s.issueToken(payload.Sub, toString(user["email"]), toString(user["name"]), newSessionID), s.issueRefreshToken(payload.Sub, newSessionID), nil
}

func (s *SuperadminService) HasSuperadmin() bool {
	if s == nil || s.table == nil {
		return false
	}
	return s.table.Count() > 0
}

func (s *SuperadminService) Register(email, password, name string) (string, *schema.AuthContext, error) {
	if s == nil || s.table == nil {
		return "", nil, fmt.Errorf("superadmin table not configured")
	}
	email = normalizeEmail(email)
	if email == "" {
		return "", nil, fmt.Errorf("email is required")
	}
	if s.findByEmail(email) != nil {
		return "", nil, fmt.Errorf("email already registered")
	}
	hashedPassword, err := HashPassword(password)
	if err != nil {
		return "", nil, err
	}
	row, err := s.table.Insert(map[string]interface{}{
		"email":    email,
		"password": hashedPassword,
		"name":     name,
		"createdAt": time.Now().UnixMilli(),
	}, nil)
	if err != nil {
		return "", nil, err
	}
	pk := s.getPK(row)
	token := s.issueToken(pk, email, toString(row["name"]), "")
	return token, &schema.AuthContext{
		ID:    pk,
		Email: email,
		Roles: []string{"superadmin"},
	}, nil
}

func (s *SuperadminService) findByEmail(email string) map[string]interface{} {
	if s == nil || s.table == nil {
		return nil
	}
	email = normalizeEmail(email)
	pointer, ok := s.table.FindByIndex([]string{"email"}, email)
	if !ok {
		return nil
	}
	row, err := s.table.GetByPointer(pointer)
	if err != nil {
		return nil
	}
	return row
}

func (s *SuperadminService) getPK(row map[string]interface{}) string {
	def := s.table.GetDef()
	if len(def.CompiledSchema.Fields) == 0 {
		return ""
	}
	return toString(row[def.CompiledSchema.Fields[0].Name])
}

func (s *SuperadminService) ValidateAccessToken(token string) (*schema.AuthContext, error) {
	payload := VerifyJWT(token, s.secret)
	if payload == nil {
		return nil, fmt.Errorf("invalid or expired token")
	}
	if payload.InstanceID != s.instanceID || payload.PrincipalType != principalTypeSuperadmin || payload.SessionID == "" {
		return nil, fmt.Errorf("invalid or expired token")
	}
	if _, err := s.requireActiveSession(payload.SessionID, payload.Sub); err != nil {
		return nil, err
	}
	user, err := s.table.Get(payload.Sub)
	if err != nil || user == nil {
		_ = s.revokeSession(payload.SessionID, "principal_missing")
		return nil, fmt.Errorf("superadmin not found")
	}
	if err := validatePrincipalRow(user, principalTypeSuperadmin); err != nil {
		_ = s.revokeSession(payload.SessionID, "principal_blocked")
		return nil, err
	}
	return &schema.AuthContext{
		ID:            payload.Sub,
		Email:         toString(user["email"]),
		Roles:         []string{"superadmin"},
		PrincipalType: principalTypeSuperadmin,
		SessionID:     payload.SessionID,
		InstanceID:    s.instanceID,
	}, nil
}

func (s *SuperadminService) issueToken(id, email, name, sessionID string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:           id,
		Email:         email,
		Name:          name,
		Roles:         []string{"superadmin"},
		PrincipalType: principalTypeSuperadmin,
		SessionID:     sessionID,
		InstanceID:    s.instanceID,
		Iat:           now,
		Exp:           now + s.accessTokenTTL,
	}, s.secret)
}

func (s *SuperadminService) issueRefreshToken(id, sessionID string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:           id,
		PrincipalType: principalTypeSuperadmin,
		SessionID:     sessionID,
		InstanceID:    s.instanceID,
		Iat:           now,
		Exp:           now + s.refreshTokenTTL,
	}, s.secret)
}

func (s *SuperadminService) createSession(principalID, reason string) (string, error) {
	if s.sessionTable == nil {
		return "", fmt.Errorf("auth sessions not configured")
	}
	now := time.Now().Unix()
	row, err := s.sessionTable.Insert(map[string]interface{}{
		"principal_type": principalTypeSuperadmin,
		"principal_id":   principalID,
		"instance_id":    s.instanceID,
		"created_at":     now,
		"last_used_at":   now,
		"expires_at":     now + s.refreshTokenTTL,
		"reason":         reason,
	}, nil)
	if err != nil {
		return "", err
	}
	return toString(row["id"]), nil
}

func (s *SuperadminService) requireActiveSession(sessionID, principalID string) (map[string]interface{}, error) {
	if s.sessionTable == nil {
		return nil, fmt.Errorf("auth sessions not configured")
	}
	row, err := s.sessionTable.Get(sessionID)
	if err != nil || row == nil {
		return nil, fmt.Errorf("session not found")
	}
	if toString(row["principal_type"]) != principalTypeSuperadmin || toString(row["principal_id"]) != principalID || toString(row["instance_id"]) != s.instanceID {
		return nil, fmt.Errorf("invalid session")
	}
	if authNumber(row["revoked_at"]) > 0 {
		return nil, fmt.Errorf("session revoked")
	}
	if exp := int64(authNumber(row["expires_at"])); exp > 0 && exp < time.Now().Unix() {
		return nil, fmt.Errorf("session expired")
	}
	return row, nil
}

func (s *SuperadminService) rotateSession(session map[string]interface{}, principalID string) (string, error) {
	newID, err := s.createSession(principalID, "refresh")
	if err != nil {
		return "", err
	}
	_, err = s.sessionTable.Update(toString(session["id"]), map[string]interface{}{
		"revoked_at":             time.Now().Unix(),
		"replaced_by_session_id": newID,
	}, nil)
	if err != nil {
		return "", err
	}
	return newID, nil
}

func (s *SuperadminService) revokeSession(sessionID, reason string) error {
	if s.sessionTable == nil || sessionID == "" {
		return nil
	}
	_, err := s.sessionTable.Update(sessionID, map[string]interface{}{
		"revoked_at": time.Now().Unix(),
		"reason":     reason,
	}, nil)
	return err
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
