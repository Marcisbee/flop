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
)

// AuthConfig configures the built-in authentication system.
type AuthConfig struct {
	TableName     string
	EmailField    string
	PasswordField string
	SessionExpiry time.Duration
	RefreshExpiry time.Duration
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

	return &AuthManager{
		db:     db,
		config: config,
	}
}

// HashPassword hashes a password using Argon2id.
func (am *AuthManager) HashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	hash := argon2.IDKey(
		[]byte(password),
		salt,
		am.config.Argon2Time,
		am.config.Argon2Memory,
		am.config.Argon2Threads,
		am.config.Argon2KeyLen,
	)

	// Format: $argon2id$salt$hash
	return fmt.Sprintf("$argon2id$%s$%s",
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

// VerifyPassword checks a password against a hash.
func (am *AuthManager) VerifyPassword(password, encoded string) bool {
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

	hash := argon2.IDKey(
		[]byte(password),
		salt,
		am.config.Argon2Time,
		am.config.Argon2Memory,
		am.config.Argon2Threads,
		am.config.Argon2KeyLen,
	)

	return subtle.ConstantTimeCompare(hash, expectedHash) == 1
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
