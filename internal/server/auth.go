package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/marcisbee/flop/internal/jsonx"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/pbkdf2"
)

// JWTPayload is the JWT claims structure.
type JWTPayload struct {
	Sub           string   `json:"sub"`
	Email         string   `json:"email"`
	Name          string   `json:"name"`
	Roles         []string `json:"roles"`
	PrincipalType string   `json:"principalType,omitempty"`
	SessionID     string   `json:"sessionId,omitempty"`
	InstanceID    string   `json:"instanceId,omitempty"`
	Iat           int64    `json:"iat"`
	Exp           int64    `json:"exp"`
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
	bodyJSON, _ := jsonx.Marshal(payload)
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
	if err := jsonx.Unmarshal(bodyBytes, &payload); err != nil {
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
		ID:            payload.Sub,
		Email:         payload.Email,
		Roles:         payload.Roles,
		PrincipalType: payload.PrincipalType,
		SessionID:     payload.SessionID,
		InstanceID:    payload.InstanceID,
	}
}

// ExtractBearerToken extracts token from Authorization header or query param.
func ExtractBearerToken(authHeader, queryToken string) string {
	if strings.HasPrefix(authHeader, "Bearer ") {
		return authHeader[7:]
	}
	return queryToken
}

// --- Password Hashing (Multi-Format) ---

// PasswordVerifier checks if a plaintext password matches a stored hash.
type PasswordVerifier interface {
	// Prefix returns the hash prefix this verifier handles (e.g. "$pbkdf2$", "$2a$").
	Prefix() string
	// Verify checks the password against the hash. Returns true if valid.
	Verify(password, hash string) bool
}

var (
	passwordVerifiersMu sync.RWMutex
	passwordVerifiers   = []PasswordVerifier{&pbkdf2Verifier{}, &bcryptVerifier{}}
)

// RegisterPasswordVerifier adds a custom password verifier for a specific hash format.
func RegisterPasswordVerifier(v PasswordVerifier) {
	passwordVerifiersMu.Lock()
	passwordVerifiers = append(passwordVerifiers, v)
	passwordVerifiersMu.Unlock()
}

// pbkdf2Verifier handles $pbkdf2$salt$hash format.
type pbkdf2Verifier struct{}

func (v *pbkdf2Verifier) Prefix() string { return "$pbkdf2$" }
func (v *pbkdf2Verifier) Verify(password, hash string) bool {
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

// bcryptVerifier handles $2a$, $2b$, $2y$ bcrypt formats.
type bcryptVerifier struct{}

func (v *bcryptVerifier) Prefix() string { return "$2" }
func (v *bcryptVerifier) Verify(password, hash string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

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

// VerifyPassword checks a password against a hash using registered verifiers.
// Supports PBKDF2 and bcrypt by default. Register custom verifiers with RegisterPasswordVerifier.
func VerifyPassword(password, hash string) bool {
	passwordVerifiersMu.RLock()
	verifiers := passwordVerifiers
	passwordVerifiersMu.RUnlock()

	for _, v := range verifiers {
		if strings.HasPrefix(hash, v.Prefix()) {
			return v.Verify(password, hash)
		}
	}
	return false
}

// --- Purpose JWTs (verification, email change, password reset) ---

// PurposePayload is used for single-use verification tokens.
type PurposePayload struct {
	Sub      string `json:"sub"`
	Email    string `json:"email,omitempty"`
	NewEmail string `json:"newEmail,omitempty"`
	Purpose  string `json:"purpose"`
	Iat      int64  `json:"iat"`
	Exp      int64  `json:"exp"`
}

const (
	PasswordResetTTL int64 = 3600  // 1 hour
	VerificationTTL  int64 = 86400 // 24 hours
	EmailChangeTTL   int64 = 86400 // 24 hours
)

// CreatePurposeJWT creates a signed JWT for verification/reset purposes.
func CreatePurposeJWT(payload *PurposePayload, secret string) string {
	header := base64urlEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyJSON, _ := jsonx.Marshal(payload)
	body := base64urlEncode(bodyJSON)
	signature := hmacSign(header+"."+body, secret)
	return header + "." + body + "." + signature
}

// VerifyPurposeJWT verifies and decodes a purpose JWT token.
// Returns nil if invalid or expired. Does not use the JWT cache.
func VerifyPurposeJWT(token, secret string) *PurposePayload {
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

	var payload PurposePayload
	if err := jsonx.Unmarshal(bodyBytes, &payload); err != nil {
		return nil
	}

	now := time.Now().Unix()
	if payload.Exp > 0 && payload.Exp < now {
		return nil
	}

	return &payload
}

// --- AuthService ---

// AuthService handles user registration, login, and token management.
type AuthService struct {
	authTable       *engine.TableInstance
	sessionTable    *engine.TableInstance
	secret          string
	instanceID      string
	accessTokenTTL  int64 // seconds
	refreshTokenTTL int64 // seconds
}

// NewAuthService creates a new AuthService.
func NewAuthService(authTable, sessionTable *engine.TableInstance, secret, instanceID string) *AuthService {
	return &AuthService{
		authTable:       authTable,
		sessionTable:    sessionTable,
		secret:          secret,
		instanceID:      instanceID,
		accessTokenTTL:  900,     // 15 min
		refreshTokenTTL: 2592000, // 30 days
	}
}

// Register creates a new user account.
func (as *AuthService) Register(email, password, name string, extraFields map[string]interface{}) (token, refreshToken string, auth *schema.AuthContext, err error) {
	existing := as.findByEmail(email)
	if existing != nil {
		return "", "", nil, fmt.Errorf("email already registered")
	}

	hashedPassword, err := HashPassword(password)
	if err != nil {
		return "", "", nil, err
	}

	data := map[string]interface{}{
		"email":    email,
		"password": hashedPassword,
	}
	if as.authFieldExists("name") && strings.TrimSpace(name) != "" {
		data["name"] = name
	}
	if as.authFieldExists("display_name") && strings.TrimSpace(name) != "" {
		if extraFields == nil {
			extraFields = map[string]interface{}{}
		}
		if _, ok := extraFields["display_name"]; !ok {
			extraFields["display_name"] = name
		}
	}
	defaultRole := "user"
	if value, ok := as.authFieldDefaultString("default_role"); ok && strings.TrimSpace(value) != "" {
		defaultRole = strings.TrimSpace(value)
	}
	if as.authFieldExists("default_role") {
		data["default_role"] = defaultRole
	}
	if as.authFieldExists("roles") {
		data["roles"] = []interface{}{defaultRole}
	}
	if as.authFieldExists("verified") {
		data["verified"] = false
	}
	for key, value := range sanitizeRegisterExtraFields(extraFields) {
		if as.authFieldExists(key) {
			data[key] = value
		}
	}
	row, err := as.authTable.Insert(data, nil)
	if err != nil {
		return "", "", nil, err
	}

	pk := as.getPK(row)
	roles := toStringSlice(row["roles"])
	sessionID, err := as.createSession(principalTypeUser, pk, as.refreshTokenTTL, "register")
	if err != nil {
		return "", "", nil, err
	}
	tok := as.issueAccessToken(pk, email, name, roles, sessionID)
	refresh := as.issueRefreshToken(pk, sessionID)
	return tok, refresh, &schema.AuthContext{
		ID:            pk,
		Email:         email,
		Roles:         roles,
		PrincipalType: principalTypeUser,
		SessionID:     sessionID,
		InstanceID:    as.instanceID,
	}, nil
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

	user, err = as.normalizeVerifiedUser(user)
	if err != nil {
		return "", "", nil, err
	}

	pk := as.getPK(user)
	if err := validatePrincipalRow(user, principalTypeUser); err != nil {
		return "", "", nil, err
	}
	roles := toStringSlice(user["roles"])
	sessionID, err := as.createSession(principalTypeUser, pk, as.refreshTokenTTL, "login")
	if err != nil {
		return "", "", nil, err
	}
	tok := as.issueAccessToken(pk, toString(user["email"]), toString(user["name"]), roles, sessionID)
	refresh := as.issueRefreshToken(pk, sessionID)

	return tok, refresh, &schema.AuthContext{
		ID:            pk,
		Email:         toString(user["email"]),
		Roles:         roles,
		PrincipalType: principalTypeUser,
		SessionID:     sessionID,
		InstanceID:    as.instanceID,
	}, nil
}

// Refresh issues a new access token from a refresh token.
func (as *AuthService) Refresh(refreshToken string) (string, string, error) {
	payload := VerifyJWT(refreshToken, as.secret)
	if payload == nil {
		return "", "", fmt.Errorf("invalid refresh token")
	}
	if payload.InstanceID != as.instanceID || payload.PrincipalType != principalTypeUser || payload.SessionID == "" {
		return "", "", fmt.Errorf("invalid refresh token")
	}
	session, err := as.requireActiveSession(payload.SessionID, principalTypeUser, payload.Sub)
	if err != nil {
		return "", "", err
	}
	user, err := as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		_ = as.revokeSession(payload.SessionID, "principal_missing")
		return "", "", fmt.Errorf("user not found")
	}
	user, err = as.normalizeVerifiedUser(user)
	if err != nil {
		_ = as.revokeSession(payload.SessionID, "principal_invalid")
		return "", "", err
	}
	if err := validatePrincipalRow(user, principalTypeUser); err != nil {
		_ = as.revokeSession(payload.SessionID, "principal_blocked")
		return "", "", err
	}
	newSessionID, err := as.rotateSession(session, principalTypeUser, payload.Sub)
	if err != nil {
		return "", "", err
	}
	roles := toStringSlice(user["roles"])
	return as.issueAccessToken(payload.Sub, toString(user["email"]), toString(user["name"]), roles, newSessionID), as.issueRefreshToken(payload.Sub, newSessionID), nil
}

func (as *AuthService) ValidateAccessToken(token string) (*schema.AuthContext, error) {
	payload := VerifyJWT(token, as.secret)
	if payload == nil {
		return nil, fmt.Errorf("invalid or expired token")
	}
	if payload.InstanceID != as.instanceID || payload.PrincipalType != principalTypeUser || payload.SessionID == "" {
		return nil, fmt.Errorf("invalid or expired token")
	}
	if _, err := as.requireActiveSession(payload.SessionID, principalTypeUser, payload.Sub); err != nil {
		return nil, err
	}
	user, err := as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		_ = as.revokeSession(payload.SessionID, "principal_missing")
		return nil, fmt.Errorf("user not found")
	}
	user, err = as.normalizeVerifiedUser(user)
	if err != nil {
		_ = as.revokeSession(payload.SessionID, "principal_invalid")
		return nil, err
	}
	if err := validatePrincipalRow(user, principalTypeUser); err != nil {
		_ = as.revokeSession(payload.SessionID, "principal_blocked")
		return nil, err
	}
	return &schema.AuthContext{
		ID:            payload.Sub,
		Email:         toString(user["email"]),
		Roles:         toStringSlice(user["roles"]),
		PrincipalType: principalTypeUser,
		SessionID:     payload.SessionID,
		InstanceID:    as.instanceID,
	}, nil
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
	tok := as.issueAccessToken(pk, email, name, roles, "")
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

// ChangePassword verifies the old password and sets a new one.
// The new password is always hashed with PBKDF2, migrating any legacy hash format.
func (as *AuthService) ChangePassword(userID, oldPassword, newPassword string) error {
	user, err := as.authTable.Get(userID)
	if err != nil || user == nil {
		return fmt.Errorf("user not found")
	}
	if !VerifyPassword(oldPassword, toString(user["password"])) {
		return fmt.Errorf("invalid current password")
	}
	hashed, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	_, err = as.authTable.Update(userID, map[string]interface{}{"password": hashed}, nil)
	return err
}

// RequestEmailChange generates a token to confirm an email change.
// The caller's password must be verified for security.
func (as *AuthService) RequestEmailChange(userID, newEmail, password string) (string, error) {
	user, err := as.authTable.Get(userID)
	if err != nil || user == nil {
		return "", fmt.Errorf("user not found")
	}
	if !VerifyPassword(password, toString(user["password"])) {
		return "", fmt.Errorf("invalid password")
	}
	existing := as.findByEmail(newEmail)
	if existing != nil {
		return "", fmt.Errorf("email already in use")
	}
	token := CreatePurposeJWT(&PurposePayload{
		Sub:      userID,
		Email:    toString(user["email"]),
		NewEmail: newEmail,
		Purpose:  "email-change",
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Unix() + EmailChangeTTL,
	}, as.secret)
	return token, nil
}

// ConfirmEmailChange verifies the token and updates the user's email.
// Returns a new authenticated session with the updated email.
func (as *AuthService) ConfirmEmailChange(token string) (string, string, *schema.AuthContext, error) {
	payload := VerifyPurposeJWT(token, as.secret)
	if payload == nil || payload.Purpose != "email-change" {
		return "", "", nil, fmt.Errorf("invalid or expired token")
	}
	existing := as.findByEmail(payload.NewEmail)
	if existing != nil {
		return "", "", nil, fmt.Errorf("email already in use")
	}
	_, err := as.authTable.Update(payload.Sub, map[string]interface{}{
		"email": payload.NewEmail,
	}, nil)
	if err != nil {
		return "", "", nil, err
	}
	user, err := as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		return "", "", nil, fmt.Errorf("user not found")
	}
	return as.createUserSession(user, "email_change_confirm")
}

// RequestVerification generates a token to confirm a user's email address.
func (as *AuthService) RequestVerification(userID string) (string, error) {
	user, err := as.authTable.Get(userID)
	if err != nil || user == nil {
		return "", fmt.Errorf("user not found")
	}
	token := CreatePurposeJWT(&PurposePayload{
		Sub:     userID,
		Email:   toString(user["email"]),
		Purpose: "verification",
		Iat:     time.Now().Unix(),
		Exp:     time.Now().Unix() + VerificationTTL,
	}, as.secret)
	return token, nil
}

// ConfirmVerification verifies the token, promotes the user if needed,
// and returns a fresh authenticated session.
func (as *AuthService) ConfirmVerification(token string) (string, string, *schema.AuthContext, error) {
	payload := VerifyPurposeJWT(token, as.secret)
	if payload == nil || payload.Purpose != "verification" {
		return "", "", nil, fmt.Errorf("invalid or expired token")
	}
	user, err := as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		return "", "", nil, fmt.Errorf("user not found")
	}

	updates := map[string]interface{}{
		"verified": true,
	}

	if as.authFieldExists("default_role") && strings.EqualFold(strings.TrimSpace(toString(user["default_role"])), "unverified") {
		updates["default_role"] = "user"
	}

	if as.authFieldExists("roles") {
		roles := promoteVerifiedRoles(toStringSlice(user["roles"]))
		iRoles := make([]interface{}, len(roles))
		for i, role := range roles {
			iRoles[i] = role
		}
		updates["roles"] = iRoles
	}

	if _, err := as.authTable.Update(payload.Sub, updates, nil); err != nil {
		return "", "", nil, err
	}

	user, err = as.authTable.Get(payload.Sub)
	if err != nil || user == nil {
		return "", "", nil, fmt.Errorf("user not found")
	}
	return as.createUserSession(user, "verification_confirm")
}

// RequestPasswordReset generates a token for resetting a user's password.
// Returns empty string (no error) if email not found to prevent user enumeration.
func (as *AuthService) RequestPasswordReset(email string) (string, error) {
	user := as.findByEmail(email)
	if user == nil {
		return "", nil
	}
	pk := as.getPK(user)
	token := CreatePurposeJWT(&PurposePayload{
		Sub:     pk,
		Email:   email,
		Purpose: "password-reset",
		Iat:     time.Now().Unix(),
		Exp:     time.Now().Unix() + PasswordResetTTL,
	}, as.secret)
	return token, nil
}

// ConfirmPasswordReset verifies the token and sets a new password.
func (as *AuthService) ConfirmPasswordReset(token, newPassword string) error {
	payload := VerifyPurposeJWT(token, as.secret)
	if payload == nil || payload.Purpose != "password-reset" {
		return fmt.Errorf("invalid or expired token")
	}
	hashed, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	_, err = as.authTable.Update(payload.Sub, map[string]interface{}{
		"password": hashed,
	}, nil)
	return err
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

func (as *AuthService) authFieldExists(name string) bool {
	_, ok := as.authField(name)
	return ok
}

func (as *AuthService) authField(name string) (*schema.CompiledField, bool) {
	if as == nil || as.authTable == nil {
		return nil, false
	}
	for i, field := range as.authTable.GetDef().CompiledSchema.Fields {
		if field.Name == name {
			return &as.authTable.GetDef().CompiledSchema.Fields[i], true
		}
	}
	return nil, false
}

func (as *AuthService) authFieldDefaultString(name string) (string, bool) {
	field, ok := as.authField(name)
	if !ok || field == nil {
		return "", false
	}
	value, ok := field.DefaultValue.(string)
	if !ok {
		return "", false
	}
	return value, true
}

func sanitizeRegisterExtraFields(extraFields map[string]interface{}) map[string]interface{} {
	if len(extraFields) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(extraFields))
	for key, value := range extraFields {
		switch key {
		case "", "id", "email", "password", "roles", "verified", "default_role", "createdAt", "updatedAt":
			continue
		default:
			out[key] = value
		}
	}
	return out
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

func (as *AuthService) issueAccessToken(id, email, name string, roles []string, sessionID string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:           id,
		Email:         email,
		Name:          name,
		Roles:         roles,
		PrincipalType: principalTypeUser,
		SessionID:     sessionID,
		InstanceID:    as.instanceID,
		Iat:           now,
		Exp:           now + as.accessTokenTTL,
	}, as.secret)
}

func (as *AuthService) issueRefreshToken(id, sessionID string) string {
	now := time.Now().Unix()
	return CreateJWT(&JWTPayload{
		Sub:           id,
		Email:         "",
		Name:          "",
		Roles:         nil,
		PrincipalType: principalTypeUser,
		SessionID:     sessionID,
		InstanceID:    as.instanceID,
		Iat:           now,
		Exp:           now + as.refreshTokenTTL,
	}, as.secret)
}

func (as *AuthService) createUserSession(user map[string]interface{}, reason string) (string, string, *schema.AuthContext, error) {
	pk := as.getPK(user)
	user, err := as.normalizeVerifiedUser(user)
	if err != nil {
		return "", "", nil, err
	}
	if err := validatePrincipalRow(user, principalTypeUser); err != nil {
		return "", "", nil, err
	}
	roles := toStringSlice(user["roles"])
	sessionID, err := as.createSession(principalTypeUser, pk, as.refreshTokenTTL, reason)
	if err != nil {
		return "", "", nil, err
	}
	token := as.issueAccessToken(pk, toString(user["email"]), toString(user["name"]), roles, sessionID)
	refreshToken := as.issueRefreshToken(pk, sessionID)
	return token, refreshToken, &schema.AuthContext{
		ID:            pk,
		Email:         toString(user["email"]),
		Roles:         roles,
		PrincipalType: principalTypeUser,
		SessionID:     sessionID,
		InstanceID:    as.instanceID,
	}, nil
}

func (as *AuthService) normalizeVerifiedUser(user map[string]interface{}) (map[string]interface{}, error) {
	if as == nil || user == nil {
		return user, nil
	}
	updates := map[string]interface{}{}
	defaultRole := strings.TrimSpace(toString(user["default_role"]))
	verified := as.authFieldExists("verified") && isTruthy(user["verified"])
	currentRoles := toStringSlice(user["roles"])

	if as.authFieldExists("roles") && len(currentRoles) == 0 && defaultRole != "" {
		iRoles := []interface{}{defaultRole}
		updates["roles"] = iRoles
		currentRoles = []string{defaultRole}
	}

	if as.authFieldExists("verified") && !verified && defaultRole != "" && !strings.EqualFold(defaultRole, "unverified") {
		updates["verified"] = true
		verified = true
	}

	if verified && as.authFieldExists("default_role") && strings.EqualFold(defaultRole, "unverified") {
		updates["default_role"] = "user"
		defaultRole = "user"
	}

	if verified && as.authFieldExists("roles") {
		nextRoles := promoteVerifiedRoles(currentRoles)
		if !sameStringSet(currentRoles, nextRoles) {
			iRoles := make([]interface{}, len(nextRoles))
			for i, role := range nextRoles {
				iRoles[i] = role
			}
			updates["roles"] = iRoles
		}
	}
	if len(updates) == 0 {
		return user, nil
	}

	updated, err := as.authTable.Update(as.getPK(user), updates, nil)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.TrimSpace(a[i]) != strings.TrimSpace(b[i]) {
			return false
		}
	}
	return true
}

func promoteVerifiedRoles(roles []string) []string {
	if len(roles) == 0 {
		return []string{"user"}
	}

	out := make([]string, 0, len(roles)+1)
	hadUnverified := false
	hadUser := false
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		if role == "unverified" {
			hadUnverified = true
			continue
		}
		if role == "user" {
			hadUser = true
		}
		out = append(out, role)
	}
	if hadUnverified && !hadUser {
		out = append([]string{"user"}, out...)
	}
	if len(out) == 0 {
		return []string{"user"}
	}
	return out
}

const (
	principalTypeUser       = "user"
	principalTypeSuperadmin = "superadmin"
)

func (as *AuthService) createSession(principalType, principalID string, ttl int64, reason string) (string, error) {
	if as.sessionTable == nil {
		return "", fmt.Errorf("auth sessions not configured")
	}
	now := time.Now().Unix()
	row, err := as.sessionTable.Insert(map[string]interface{}{
		"principal_type": principalType,
		"principal_id":   principalID,
		"instance_id":    as.instanceID,
		"created_at":     now,
		"last_used_at":   now,
		"expires_at":     now + ttl,
		"reason":         reason,
	}, nil)
	if err != nil {
		return "", err
	}
	return toString(row["id"]), nil
}

func (as *AuthService) requireActiveSession(sessionID, principalType, principalID string) (map[string]interface{}, error) {
	if as.sessionTable == nil {
		return nil, fmt.Errorf("auth sessions not configured")
	}
	row, err := as.sessionTable.Get(sessionID)
	if err != nil || row == nil {
		return nil, fmt.Errorf("session not found")
	}
	if toString(row["principal_type"]) != principalType || toString(row["principal_id"]) != principalID || toString(row["instance_id"]) != as.instanceID {
		return nil, fmt.Errorf("invalid session")
	}
	if ts := int64(authNumber(row["revoked_at"])); ts > 0 {
		return nil, fmt.Errorf("session revoked")
	}
	now := time.Now().Unix()
	if exp := int64(authNumber(row["expires_at"])); exp > 0 && exp < now {
		return nil, fmt.Errorf("session expired")
	}
	return row, nil
}

func (as *AuthService) rotateSession(session map[string]interface{}, principalType, principalID string) (string, error) {
	newID, err := as.createSession(principalType, principalID, as.refreshTokenTTL, "refresh")
	if err != nil {
		return "", err
	}
	_, err = as.sessionTable.Update(toString(session["id"]), map[string]interface{}{
		"revoked_at":             time.Now().Unix(),
		"replaced_by_session_id": newID,
	}, nil)
	if err != nil {
		return "", err
	}
	return newID, nil
}

func (as *AuthService) revokeSession(sessionID, reason string) error {
	if as.sessionTable == nil || sessionID == "" {
		return nil
	}
	_, err := as.sessionTable.Update(sessionID, map[string]interface{}{
		"revoked_at": time.Now().Unix(),
		"reason":     reason,
	}, nil)
	return err
}

func validatePrincipalRow(row map[string]interface{}, principalType string) error {
	if row == nil {
		return fmt.Errorf("%s not found", principalType)
	}
	if isTruthy(row["banned"]) {
		return fmt.Errorf("%s is banned", principalType)
	}
	if isTruthy(row["deleted"]) || isTruthy(row["archived"]) || isTruthy(row["disabled"]) || isTruthy(row["suspended"]) {
		return fmt.Errorf("%s is unavailable", principalType)
	}
	if status := strings.ToLower(strings.TrimSpace(toString(row["status"]))); status != "" {
		switch status {
		case "banned", "archived", "deleted", "disabled", "inactive", "suspended":
			return fmt.Errorf("%s is unavailable", principalType)
		}
	}
	if value, ok := row["active"].(bool); ok && !value {
		return fmt.Errorf("%s is inactive", principalType)
	}
	if authNumber(row["deleted_at"]) > 0 || authNumber(row["archived_at"]) > 0 || authNumber(row["disabled_at"]) > 0 || authNumber(row["banned_at"]) > 0 {
		return fmt.Errorf("%s is unavailable", principalType)
	}
	return nil
}

func isTruthy(v interface{}) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		value = strings.TrimSpace(strings.ToLower(value))
		return value == "1" || value == "true" || value == "yes"
	case int:
		return value != 0
	case int64:
		return value != 0
	case float64:
		return value != 0
	default:
		return false
	}
}

func authNumber(v interface{}) float64 {
	switch value := v.(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return 0
	}
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
