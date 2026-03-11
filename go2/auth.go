package flop

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

// PasswordHasher is the interface for password hashing strategies.
type PasswordHasher interface {
	Hash(password string) (string, error)
	Verify(password, encoded string) bool
}

// Argon2idHasher hashes passwords using Argon2id.
type Argon2idHasher struct {
	Time    uint32
	Memory  uint32
	Threads uint8
	KeyLen  uint32
}

func (h *Argon2idHasher) Hash(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, h.Time, h.Memory, h.Threads, h.KeyLen)
	return fmt.Sprintf("$argon2id$%s$%s",
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

func (h *Argon2idHasher) Verify(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[1] != "argon2id" {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}
	hash := argon2.IDKey([]byte(password), salt, h.Time, h.Memory, h.Threads, h.KeyLen)
	return subtle.ConstantTimeCompare(hash, expectedHash) == 1
}

// BcryptHasher hashes passwords using bcrypt.
type BcryptHasher struct {
	Cost int // bcrypt cost factor (default 10)
}

func (h *BcryptHasher) Hash(password string) (string, error) {
	cost := h.Cost
	if cost == 0 {
		cost = 10
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func (h *BcryptHasher) Verify(password, encoded string) bool {
	return bcrypt.CompareHashAndPassword([]byte(encoded), []byte(password)) == nil
}

// MultiHasher tries multiple hashers for verification (useful for migration).
// Hashing always uses the first hasher.
type MultiHasher struct {
	Hashers []PasswordHasher
}

func (h *MultiHasher) Hash(password string) (string, error) {
	if len(h.Hashers) == 0 {
		return "", fmt.Errorf("no hashers configured")
	}
	return h.Hashers[0].Hash(password)
}

func (h *MultiHasher) Verify(password, encoded string) bool {
	for _, hasher := range h.Hashers {
		if hasher.Verify(password, encoded) {
			return true
		}
	}
	return false
}

// AuthConfig configures the built-in authentication system.
type AuthConfig struct {
	TableName     string
	EmailField    string
	PasswordField string
	SessionExpiry time.Duration
	RefreshExpiry time.Duration
	Hasher        PasswordHasher // password hashing strategy (default: Argon2id)
	// Deprecated: use Hasher instead. Kept for backward compat if Hasher is nil.
	Argon2Time    uint32
	Argon2Memory  uint32
	Argon2Threads uint8
	Argon2KeyLen  uint32
}

func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		TableName:     "users",
		EmailField:    "email",
		PasswordField: "password",
		SessionExpiry: 1 * time.Hour,
		RefreshExpiry: 30 * 24 * time.Hour,
		Hasher:        &Argon2idHasher{Time: 1, Memory: 64 * 1024, Threads: 4, KeyLen: 32},
		Argon2Time:    1,
		Argon2Memory:  64 * 1024,
		Argon2Threads: 4,
		Argon2KeyLen:  32,
	}
}

// Internal schema for the _sessions table.
var sessionSchema = &Schema{
	Name: "_sessions",
	Fields: []Field{
		{Name: "token", Type: FieldString, Unique: true},
		{Name: "user_id", Type: FieldInt, Indexed: true},
		{Name: "kind", Type: FieldString},       // "access", "refresh", "admin", "admin_refresh"
		{Name: "expires_at", Type: FieldFloat},   // unix timestamp
		{Name: "created_at", Type: FieldFloat},
	},
}

// Session represents an active user session.
type Session struct {
	Token        string
	RefreshToken string
	UserID       uint64
	ExpiresAt    time.Time
}

// AuthManager handles user authentication with persistent sessions.
type AuthManager struct {
	db       *DB
	config   *AuthConfig
	mu       sync.RWMutex
}

func NewAuthManager(db *DB, config *AuthConfig) *AuthManager {
	// Ensure _sessions table exists
	db.CreateTable(sessionSchema)

	// If no hasher configured, fall back to Argon2id with config params
	if config.Hasher == nil {
		config.Hasher = &Argon2idHasher{
			Time:    config.Argon2Time,
			Memory:  config.Argon2Memory,
			Threads: config.Argon2Threads,
			KeyLen:  config.Argon2KeyLen,
		}
	}

	return &AuthManager{
		db:     db,
		config: config,
	}
}

// HashPassword hashes a password using the configured hasher.
func (am *AuthManager) HashPassword(password string) (string, error) {
	return am.config.Hasher.Hash(password)
}

// VerifyPassword checks a password against a hash using the configured hasher.
func (am *AuthManager) VerifyPassword(password, encoded string) bool {
	return am.config.Hasher.Verify(password, encoded)
}

// CreateSession creates a session for a user ID without password verification.
// Use this when the app handles its own authentication logic.
func (am *AuthManager) CreateSession(userID uint64) (*Session, error) {
	now := time.Now()

	token := generateToken()
	am.db.Insert("_sessions", map[string]any{
		"token":      token,
		"user_id":    userID,
		"kind":       "access",
		"expires_at": float64(now.Add(am.config.SessionExpiry).Unix()),
		"created_at": float64(now.Unix()),
	})

	refreshToken := generateToken()
	am.db.Insert("_sessions", map[string]any{
		"token":      refreshToken,
		"user_id":    userID,
		"kind":       "refresh",
		"expires_at": float64(now.Add(am.config.RefreshExpiry).Unix()),
		"created_at": float64(now.Unix()),
	})

	return &Session{
		Token:        token,
		RefreshToken: refreshToken,
		UserID:       userID,
		ExpiresAt:    now.Add(am.config.SessionExpiry),
	}, nil
}

// ValidateToken validates an access token and returns the user ID.
// Returns 0 if the token is invalid or expired.
func (am *AuthManager) ValidateToken(token string) uint64 {
	sess := am.findSession(token, "access")
	if sess == nil {
		return 0
	}
	return sess.userID
}

// Register creates a new user account.
func (am *AuthManager) Register(email, password string, extra map[string]any) (*Row, error) {
	hash, err := am.HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	data := make(map[string]any)
	for k, v := range extra {
		data[k] = v
	}
	data[am.config.EmailField] = email
	data[am.config.PasswordField] = hash

	return am.db.Insert(am.config.TableName, data)
}

// Login authenticates a user and creates a persistent session.
// Returns a Session with both access and refresh tokens.
func (am *AuthManager) Login(email, password string) (*Session, error) {
	table := am.db.Table(am.config.TableName)
	if table == nil {
		return nil, fmt.Errorf("auth table not found")
	}

	var user *Row
	table.ScanByField(am.config.EmailField, email, func(row *Row) bool {
		user = row
		return false
	})
	if user == nil {
		// Fallback to full scan if no index
		table.Scan(func(row *Row) bool {
			if e, ok := row.Data[am.config.EmailField].(string); ok && e == email {
				user = row
				return false
			}
			return true
		})
	}

	if user == nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	hash, ok := user.Data[am.config.PasswordField].(string)
	if !ok || !am.VerifyPassword(password, hash) {
		return nil, fmt.Errorf("invalid credentials")
	}

	now := time.Now()

	// Create access token
	token := generateToken()
	am.db.Insert("_sessions", map[string]any{
		"token":      token,
		"user_id":    user.ID,
		"kind":       "access",
		"expires_at": float64(now.Add(am.config.SessionExpiry).Unix()),
		"created_at": float64(now.Unix()),
	})

	// Create refresh token
	refreshToken := generateToken()
	am.db.Insert("_sessions", map[string]any{
		"token":      refreshToken,
		"user_id":    user.ID,
		"kind":       "refresh",
		"expires_at": float64(now.Add(am.config.RefreshExpiry).Unix()),
		"created_at": float64(now.Unix()),
	})

	return &Session{
		Token:        token,
		RefreshToken: refreshToken,
		UserID:       user.ID,
		ExpiresAt:    now.Add(am.config.SessionExpiry),
	}, nil
}

// Refresh validates a refresh token and returns a new access token.
func (am *AuthManager) Refresh(refreshToken string) (*Session, error) {
	sess := am.findSession(refreshToken, "refresh")
	if sess == nil {
		return nil, fmt.Errorf("invalid refresh token")
	}

	now := time.Now()
	token := generateToken()
	am.db.Insert("_sessions", map[string]any{
		"token":      token,
		"user_id":    sess.userID,
		"kind":       "access",
		"expires_at": float64(now.Add(am.config.SessionExpiry).Unix()),
		"created_at": float64(now.Unix()),
	})

	return &Session{
		Token:     token,
		UserID:    sess.userID,
		ExpiresAt: now.Add(am.config.SessionExpiry),
	}, nil
}

// Authenticate extracts and validates a session from an HTTP request.
func (am *AuthManager) Authenticate(r *http.Request) (any, error) {
	token := r.Header.Get("Authorization")
	token = strings.TrimPrefix(token, "Bearer ")

	if token == "" {
		// Try cookie
		if cookie, err := r.Cookie("session"); err == nil {
			token = cookie.Value
		}
	}

	if token == "" {
		return nil, nil // no auth (anonymous)
	}

	sess := am.findSession(token, "access")
	if sess == nil {
		return nil, fmt.Errorf("session expired")
	}

	table := am.db.Table(am.config.TableName)
	if table == nil {
		return nil, fmt.Errorf("auth table not found")
	}

	user, err := table.Get(sess.userID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	// Strip password from returned data
	delete(user.Data, am.config.PasswordField)
	return user, nil
}

// Logout removes a session.
func (am *AuthManager) Logout(token string) {
	sessTable := am.db.Table("_sessions")
	if sessTable == nil {
		return
	}
	sessTable.ScanByField("token", token, func(row *Row) bool {
		am.db.Delete("_sessions", row.ID)
		return false
	})
}

// sessionRow is an internal helper for session lookups.
type sessionRow struct {
	rowID     uint64
	userID    uint64
	expiresAt float64
}

// findSession looks up a valid session by token and kind.
func (am *AuthManager) findSession(token, kind string) *sessionRow {
	sessTable := am.db.Table("_sessions")
	if sessTable == nil {
		return nil
	}

	var found *sessionRow
	sessTable.ScanByField("token", token, func(row *Row) bool {
		k, _ := row.Data["kind"].(string)
		if k != kind {
			return true
		}
		expiresAt, _ := row.Data["expires_at"].(float64)
		if time.Now().Unix() > int64(expiresAt) {
			// Expired — clean up
			am.db.Delete("_sessions", row.ID)
			return false
		}
		uid := toUint64(row.Data["user_id"])
		found = &sessionRow{rowID: row.ID, userID: uid, expiresAt: expiresAt}
		return false
	})

	return found
}

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}
