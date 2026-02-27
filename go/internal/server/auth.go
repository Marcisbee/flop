package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	"golang.org/x/crypto/pbkdf2"
)

// JWTPayload is the JWT claims structure.
type JWTPayload struct {
	Sub   string   `json:"sub"`
	Email string   `json:"email"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
	Iat   int64    `json:"iat"`
	Exp   int64    `json:"exp"`
}

// --- JWT ---

func base64urlEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func base64urlDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func hmacSign(data, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(data))
	return base64urlEncode(h.Sum(nil))
}

// CreateJWT creates a signed JWT token.
func CreateJWT(payload *JWTPayload, secret string) string {
	header := base64urlEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyJSON, _ := json.Marshal(payload)
	body := base64urlEncode(bodyJSON)
	signature := hmacSign(header+"."+body, secret)
	return header + "." + body + "." + signature
}

// JWT verification cache
var (
	jwtCacheMu sync.RWMutex
	jwtCache   = make(map[string]*jwtCacheEntry)
)

type jwtCacheEntry struct {
	payload  *JWTPayload
	expireAt int64
}

const jwtCacheMax = 10000

// VerifyJWT verifies and decodes a JWT token.
func VerifyJWT(token, secret string) *JWTPayload {
	// Check cache
	jwtCacheMu.RLock()
	if entry, ok := jwtCache[token]; ok {
		if entry.expireAt > time.Now().UnixMilli() {
			jwtCacheMu.RUnlock()
			return entry.payload
		}
	}
	jwtCacheMu.RUnlock()

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}

	expected := hmacSign(parts[0]+"."+parts[1], secret)
	if !hmac.Equal([]byte(parts[2]), []byte(expected)) {
		return nil
	}

	bodyBytes, err := base64urlDecode(parts[1])
	if err != nil {
		return nil
	}

	var payload JWTPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		return nil
	}

	// Check expiration
	now := time.Now().Unix()
	if payload.Exp > 0 && payload.Exp < now {
		return nil
	}

	// Cache the result
	expireAt := int64(0)
	if payload.Exp > 0 {
		expireAt = payload.Exp*1000 - 60000
	} else {
		expireAt = time.Now().UnixMilli() + 300000
	}

	jwtCacheMu.Lock()
	if len(jwtCache) >= jwtCacheMax {
		// Evict first entry
		for k := range jwtCache {
			delete(jwtCache, k)
			break
		}
	}
	jwtCache[token] = &jwtCacheEntry{payload: &payload, expireAt: expireAt}
	jwtCacheMu.Unlock()

	return &payload
}

// JWTToAuthContext converts a JWT payload to an AuthContext.
func JWTToAuthContext(payload *JWTPayload) *schema.AuthContext {
	return &schema.AuthContext{
		ID:    payload.Sub,
		Email: payload.Email,
		Roles: payload.Roles,
	}
}

// ExtractBearerToken extracts token from Authorization header or query param.
func ExtractBearerToken(authHeader, queryToken string) string {
	if strings.HasPrefix(authHeader, "Bearer ") {
		return authHeader[7:]
	}
	return queryToken
}

// --- Password Hashing (PBKDF2) ---

// HashPassword hashes a password using PBKDF2-SHA256.
func HashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	derived := pbkdf2.Key([]byte(password), salt, 10000, 32, sha256.New)
	saltHex := hex.EncodeToString(salt)
	hashHex := hex.EncodeToString(derived)
	return fmt.Sprintf("$pbkdf2$%s$%s", saltHex, hashHex), nil
}

// VerifyPassword checks a password against a PBKDF2 hash.
func VerifyPassword(password, hash string) bool {
	parts := strings.Split(hash, "$")
	if len(parts) != 4 || parts[1] != "pbkdf2" {
		return false
	}

	salt, err := hex.DecodeString(parts[2])
	if err != nil {
		return false
	}

	derived := pbkdf2.Key([]byte(password), salt, 10000, 32, sha256.New)
	hashHex := hex.EncodeToString(derived)
	return hmac.Equal([]byte(hashHex), []byte(parts[3]))
}

// --- AuthService ---

// AuthService handles user registration, login, and token management.
type AuthService struct {
	authTable       *engine.TableInstance
	secret          string
	accessTokenTTL  int64 // seconds
	refreshTokenTTL int64 // seconds
}

// NewAuthService creates a new AuthService.
func NewAuthService(authTable *engine.TableInstance, secret string) *AuthService {
	return &AuthService{
		authTable:       authTable,
		secret:          secret,
		accessTokenTTL:  900,    // 15 min
		refreshTokenTTL: 604800, // 7 days
	}
}

// Register creates a new user account.
func (as *AuthService) Register(email, password, name string) (token string, auth *schema.AuthContext, err error) {
	existing := as.findByEmail(email)
	if existing != nil {
		return "", nil, fmt.Errorf("email already registered")
	}

	hashedPassword, err := HashPassword(password)
	if err != nil {
		return "", nil, err
	}

	row, err := as.authTable.Insert(map[string]interface{}{
		"email":    email,
		"password": hashedPassword,
		"name":     name,
		"roles":    []interface{}{"user"},
		"verified": false,
	}, nil)
	if err != nil {
		return "", nil, err
	}

	pk := as.getPK(row)
	roles := toStringSlice(row["roles"])
	tok := as.issueToken(pk, email, name, roles)
	return tok, &schema.AuthContext{ID: pk, Email: email, Roles: roles}, nil
}

// Login authenticates a user.
func (as *AuthService) Login(email, password string) (token, refreshToken string, auth *schema.AuthContext, err error) {
	user := as.findByEmail(email)
	if user == nil {
		return "", "", nil, fmt.Errorf("invalid credentials")
	}

	if !VerifyPassword(password, toString(user["password"])) {
		return "", "", nil, fmt.Errorf("invalid credentials")
	}

	pk := as.getPK(user)
	roles := toStringSlice(user["roles"])
	tok := as.issueToken(pk, toString(user["email"]), toString(user["name"]), roles)
	refresh := as.issueRefreshToken(pk)

	return tok, refresh, &schema.AuthContext{
		ID:    pk,
		Email: toString(user["email"]),
		Roles: roles,
	}, nil
}

// Refresh issues a new access token from a refresh token.
func (as *AuthService) Refresh(refreshToken string) (string, error) {
	payload := VerifyJWT(refreshToken, as.secret)
	if payload == nil {
		return "", fmt.Errorf("invalid refresh token")
	}

	user, err := as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		return "", fmt.Errorf("user not found")
	}

	roles := toStringSlice(user["roles"])
	return as.issueToken(payload.Sub, toString(user["email"]), toString(user["name"]), roles), nil
}

// HasSuperadmin checks if any user has the superadmin role.
func (as *AuthService) HasSuperadmin() bool {
	rows, err := as.authTable.Scan(10000, 0)
	if err != nil {
		return false
	}
	for _, row := range rows {
		roles := toStringSlice(row["roles"])
		for _, r := range roles {
			if r == "superadmin" {
				return true
			}
		}
	}
	return false
}

// RegisterSuperadmin creates a superadmin account.
// extraFields are merged into the insert data for app-specific required fields.
func (as *AuthService) RegisterSuperadmin(email, password string, extraFields map[string]interface{}) (string, *schema.AuthContext, error) {
	existing := as.findByEmail(email)
	if existing != nil {
		return "", nil, fmt.Errorf("email already registered")
	}

	hashedPassword, err := HashPassword(password)
	if err != nil {
		return "", nil, err
	}

	data := map[string]interface{}{
		"email":    email,
		"password": hashedPassword,
		"roles":    []interface{}{"superadmin"},
		"verified": true,
	}
	for k, v := range extraFields {
		data[k] = v
	}

	row, err := as.authTable.Insert(data, nil)
	if err != nil {
		return "", nil, err
	}

	pk := as.getPK(row)
	name := toString(row["name"])
	roles := toStringSlice(row["roles"])
	tok := as.issueToken(pk, email, name, roles)
	return tok, &schema.AuthContext{ID: pk, Email: email, Roles: roles}, nil
}

// SetRoles updates a user's roles.
func (as *AuthService) SetRoles(userID string, roles []string) error {
	iRoles := make([]interface{}, len(roles))
	for i, r := range roles {
		iRoles[i] = r
	}
	_, err := as.authTable.Update(userID, map[string]interface{}{"roles": iRoles}, nil)
	return err
}

// AuthSchemaFields returns the compiled fields for the auth table.
func (as *AuthService) AuthSchemaFields() []schema.CompiledField {
	def := as.authTable.GetDef()
	return def.CompiledSchema.Fields
}

func (as *AuthService) findByEmail(email string) map[string]interface{} {
	pointer, ok := as.authTable.FindByIndex([]string{"email"}, email)
	if !ok {
		return nil
	}
	row, err := as.authTable.GetByPointer(pointer)
	if err != nil {
		return nil
	}
	return row
}

func (as *AuthService) getPK(row map[string]interface{}) string {
	def := as.authTable.GetDef()
	for _, f := range def.CompiledSchema.Fields {
		if f.AutoGenPattern != "" {
			return toString(row[f.Name])
		}
	}
	if len(def.CompiledSchema.Fields) > 0 {
		return toString(row[def.CompiledSchema.Fields[0].Name])
	}
	return ""
}

func (as *AuthService) issueToken(id, email, name string, roles []string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:   id,
		Email: email,
		Name:  name,
		Roles: roles,
		Iat:   now,
		Exp:   now + as.accessTokenTTL,
	}, as.secret)
}

func (as *AuthService) issueRefreshToken(id string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:   id,
		Email: "",
		Name:  "",
		Roles: nil,
		Iat:   now,
		Exp:   now + as.refreshTokenTTL,
	}, as.secret)
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		result := make([]string, len(val))
		for i, item := range val {
			result[i] = toString(item)
		}
		return result
	default:
		return nil
	}
}
