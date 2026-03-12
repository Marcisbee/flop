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
	secret          string
	accessTokenTTL  int64
	refreshTokenTTL int64
}

func NewSuperadminService(table *engine.TableInstance, secret string) *SuperadminService {
	return &SuperadminService{
		table:           table,
		secret:          secret,
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
	pk := s.getPK(user)
	name := toString(user["name"])
	token = s.issueToken(pk, toString(user["email"]), name)
	refreshToken = s.issueRefreshToken(pk)
	return token, refreshToken, &schema.AuthContext{
		ID:    pk,
		Email: toString(user["email"]),
		Roles: []string{"superadmin"},
	}, nil
}

func (s *SuperadminService) Refresh(refreshToken string) (string, error) {
	payload := VerifyJWT(refreshToken, s.secret)
	if payload == nil {
		return "", fmt.Errorf("invalid refresh token")
	}
	user, err := s.table.Get(payload.Sub)
	if err != nil || user == nil {
		return "", fmt.Errorf("superadmin not found")
	}
	return s.issueToken(payload.Sub, toString(user["email"]), toString(user["name"])), nil
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
	token := s.issueToken(pk, email, toString(row["name"]))
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

func (s *SuperadminService) issueToken(id, email, name string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:   id,
		Email: email,
		Name:  name,
		Roles: []string{"superadmin"},
		Iat:   now,
		Exp:   now + s.accessTokenTTL,
	}, s.secret)
}

func (s *SuperadminService) issueRefreshToken(id string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub: id,
		Iat: now,
		Exp: now + s.refreshTokenTTL,
	}, s.secret)
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
