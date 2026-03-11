package flop

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
)

// orderedMap is a JSON-serializable map that preserves key insertion order.
// Used to ensure "id" is always the first key in schema JSON, which the admin UI requires.
type orderedMap struct {
	keys []string
	vals map[string]any
}

func newOrderedMap() *orderedMap {
	return &orderedMap{vals: make(map[string]any)}
}

func (o *orderedMap) Set(key string, val any) {
	if _, exists := o.vals[key]; !exists {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = val
}

func (o *orderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, _ := json.Marshal(k)
		val, _ := json.Marshal(o.vals[k])
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// Internal schema for the _superadmins table.
var superadminSchema = &Schema{
	Name: "_superadmins",
	Fields: []Field{
		{Name: "email", Type: FieldString, Required: true, Unique: true},
		{Name: "password", Type: FieldString, Required: true},
	},
}

// AdminConfig configures the admin panel.
type AdminConfig struct {
	// SetupToken is populated when no superadmin exists.
	// Read after MountAdmin to display the setup URL.
	SetupToken string
}

// adminAuth manages authentication for the admin panel using the _superadmins table
// and persistent sessions in the _sessions table.
type adminAuth struct {
	db *DB
}

func newAdminAuth(db *DB) *adminAuth {
	// Ensure _sessions table exists (shared with app auth)
	db.CreateTable(sessionSchema)
	return &adminAuth{db: db}
}

func (a *adminAuth) hasSuperadmin() bool {
	table := a.db.Table("_superadmins")
	if table == nil {
		return false
	}
	count, _ := table.Count()
	return count > 0
}

func (a *adminAuth) register(email, password string) error {
	hash, err := hashAdminPassword(password)
	if err != nil {
		return err
	}
	_, err = a.db.Insert("_superadmins", map[string]any{
		"email":    email,
		"password": hash,
	})
	return err
}

func (a *adminAuth) login(email, password string) (token string, refreshToken string, err error) {
	table := a.db.Table("_superadmins")
	if table == nil {
		return "", "", fmt.Errorf("admin table not found")
	}

	var user *Row
	table.Scan(func(row *Row) bool {
		if e, ok := row.Data["email"].(string); ok && e == email {
			user = row
			return false
		}
		return true
	})
	if user == nil {
		return "", "", fmt.Errorf("invalid credentials")
	}

	hash, ok := user.Data["password"].(string)
	if !ok || !verifyAdminPassword(password, hash) {
		return "", "", fmt.Errorf("invalid credentials")
	}

	now := time.Now()

	token = generateToken()
	a.db.Insert("_sessions", map[string]any{
		"token":      token,
		"user_id":    user.ID,
		"kind":       "admin",
		"expires_at": float64(now.Add(1 * time.Hour).Unix()),
		"created_at": float64(now.Unix()),
	})

	refreshToken = generateToken()
	a.db.Insert("_sessions", map[string]any{
		"token":      refreshToken,
		"user_id":    user.ID,
		"kind":       "admin_refresh",
		"expires_at": float64(now.Add(7 * 24 * time.Hour).Unix()),
		"created_at": float64(now.Unix()),
	})

	return token, refreshToken, nil
}

func (a *adminAuth) refresh(refreshToken string) (string, error) {
	sess := a.findSession(refreshToken, "admin_refresh")
	if sess == nil {
		return "", fmt.Errorf("invalid refresh token")
	}

	now := time.Now()
	token := generateToken()
	a.db.Insert("_sessions", map[string]any{
		"token":      token,
		"user_id":    sess.userID,
		"kind":       "admin",
		"expires_at": float64(now.Add(1 * time.Hour).Unix()),
		"created_at": float64(now.Unix()),
	})

	return token, nil
}

func (a *adminAuth) isAuthorized(token string) bool {
	return a.findSession(token, "admin") != nil
}

func (a *adminAuth) findSession(token, kind string) *sessionRow {
	sessTable := a.db.Table("_sessions")
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
			a.db.Delete("_sessions", row.ID)
			return false
		}
		uid := toUint64(row.Data["user_id"])
		found = &sessionRow{rowID: row.ID, userID: uid, expiresAt: expiresAt}
		return false
	})

	return found
}

func hashAdminPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32)
	return fmt.Sprintf("$argon2id$%s$%s",
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

func verifyAdminPassword(password, encoded string) bool {
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
	hash := argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32)
	return subtle.ConstantTimeCompare(hash, expectedHash) == 1
}

// adminSSEHub broadcasts change events to admin panel SSE clients.
type adminSSEHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]bool
}

func newAdminSSEHub() *adminSSEHub {
	return &adminSSEHub{clients: make(map[chan []byte]bool)}
}

func (h *adminSSEHub) subscribe() chan []byte {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.clients[ch] = true
	h.mu.Unlock()
	return ch
}

func (h *adminSSEHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

func (h *adminSSEHub) broadcast(tableName, op string) {
	data, _ := json.Marshal(map[string]string{"table": tableName, "op": op})
	msg := []byte(fmt.Sprintf("event: change\ndata: %s\n\n", data))
	h.mu.RLock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
	h.mu.RUnlock()
}

// MountAdmin mounts the admin panel at /_/ on the server's internal mux.
// It creates a dedicated _superadmins table, independent of the app's user schema.
// If no superadmin exists, a one-time setup token is generated.
func (s *Server) MountAdmin(_ *AuthManager, cfg *AdminConfig) *AdminConfig {
	return s.MountAdminOn(s.mux, cfg)
}

// MountAdminOn mounts the admin panel at /_/ on the given mux.
// Use this when the app manages its own http.ServeMux.
func (s *Server) MountAdminOn(mux *http.ServeMux, cfg *AdminConfig) *AdminConfig {
	if cfg == nil {
		cfg = &AdminConfig{}
	}

	// Create the _superadmins table
	s.db.CreateTable(superadminSchema)

	auth := newAdminAuth(s.db)

	var setupMu sync.Mutex
	if !auth.hasSuperadmin() && cfg.SetupToken == "" {
		b := make([]byte, 16)
		rand.Read(b)
		cfg.SetupToken = hex.EncodeToString(b)
	}

	sseHub := newAdminSSEHub()
	handler := s.adminHandler(auth, cfg, &setupMu, sseHub)
	mux.Handle("/_/", handler)
	mux.Handle("/_", handler)
	s.adminCfg = cfg
	return cfg
}

func (s *Server) adminHandler(auth *adminAuth, cfg *AdminConfig, setupMu *sync.Mutex, sseHub *adminSSEHub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Login page
		if path == "/_/login" || path == "/_/login/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.Write([]byte(adminLoginHTML))
			return
		}

		// Setup page
		if path == "/_/setup" || path == "/_/setup/" {
			setupMu.Lock()
			tok := cfg.SetupToken
			setupMu.Unlock()
			if tok == "" || auth.hasSuperadmin() {
				http.Redirect(w, r, "/_/", http.StatusFound)
				return
			}
			if r.URL.Query().Get("token") != tok {
				adminJSON(w, http.StatusForbidden, map[string]any{"error": "invalid setup token"})
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte(adminSetupHTML))
			return
		}

		// Setup schema API (returns empty fields for go2 — no extra fields needed)
		if path == "/_/api/setup-schema" && r.Method == http.MethodGet {
			adminJSON(w, http.StatusOK, map[string]any{"fields": []any{}})
			return
		}

		// Setup API
		if path == "/_/api/setup" && r.Method == http.MethodPost {
			setupMu.Lock()
			tok := cfg.SetupToken
			setupMu.Unlock()
			if tok == "" {
				adminJSON(w, http.StatusBadRequest, map[string]any{"error": "setup not available"})
				return
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			bodyToken, _ := body["token"].(string)
			email, _ := body["email"].(string)
			password, _ := body["password"].(string)
			if bodyToken != tok {
				adminJSON(w, http.StatusForbidden, map[string]any{"error": "invalid setup token"})
				return
			}
			if email == "" || password == "" {
				adminJSON(w, http.StatusBadRequest, map[string]any{"error": "email and password required"})
				return
			}
			if auth.hasSuperadmin() {
				setupMu.Lock()
				cfg.SetupToken = ""
				setupMu.Unlock()
				adminJSON(w, http.StatusBadRequest, map[string]any{"error": "superadmin already exists"})
				return
			}
			if err := auth.register(email, password); err != nil {
				adminJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			setupMu.Lock()
			cfg.SetupToken = ""
			setupMu.Unlock()
			adminJSON(w, http.StatusOK, map[string]any{"ok": true})
			return
		}

		// Login API
		if path == "/_/api/login" && r.Method == http.MethodPost {
			var body struct {
				Email    string `json:"email"`
				Password string `json:"password"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			token, refreshToken, err := auth.login(body.Email, body.Password)
			if err != nil {
				adminJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
				return
			}
			adminJSON(w, http.StatusOK, map[string]any{"token": token, "refreshToken": refreshToken})
			return
		}

		// Refresh token API
		if path == "/_/api/refresh" && r.Method == http.MethodPost {
			var body struct {
				RefreshToken string `json:"refreshToken"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			token, err := auth.refresh(body.RefreshToken)
			if err != nil {
				adminJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
				return
			}
			adminJSON(w, http.StatusOK, map[string]any{"token": token})
			return
		}

		// Auth check for remaining API routes (except events which uses query param)
		if strings.HasPrefix(path, "/_/api/") {
			token := extractAdminToken(r)
			if !auth.isAuthorized(token) {
				adminJSON(w, http.StatusUnauthorized, map[string]any{"error": "authentication required"})
				return
			}
		}

		// Analytics stubs — return not-enabled so the admin UI gracefully hides analytics
		if path == "/_/api/analytics/config" {
			adminJSON(w, http.StatusOK, map[string]any{"enabled": false})
			return
		}
		if path == "/_/api/analytics/indexes" {
			adminJSON(w, http.StatusOK, map[string]any{"data": nil})
			return
		}

		// SSE events endpoint
		if path == "/_/api/events" {
			flusher, ok := w.(http.Flusher)
			if !ok {
				adminJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming not supported"})
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")

			// Send initial snapshot with table counts
			s.db.mu.RLock()
			counts := make(map[string]int)
			for name, table := range s.db.tables {
				c, _ := table.Count()
				counts[name] = c
			}
			s.db.mu.RUnlock()
			snapData, _ := json.Marshal(map[string]any{"tableCounts": counts})
			fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", snapData)
			flusher.Flush()

			// Subscribe to change events
			ch := sseHub.subscribe()
			defer sseHub.unsubscribe(ch)

			// Keep connection alive with periodic pings and forward change events
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			ctx := r.Context()
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-ch:
					w.Write(msg)
					flusher.Flush()
				case <-ticker.C:
					fmt.Fprintf(w, ": ping\n\n")
					flusher.Flush()
				}
			}
		}

		// Tables list
		if path == "/_/api/tables" && r.Method == http.MethodGet {
			s.db.mu.RLock()
			tables := make([]map[string]any, 0)
			for name, table := range s.db.tables {
				count, _ := table.Count()
				schemaDef := s.db.schemas[name]
				// Build schema as object map: { fieldName: { type, required, ... } }
				// This is the format the flop-go admin UI expects
				schemaMap := newOrderedMap()
				// First field must be the auto-ID field (admin UI uses Object.keys(schema)[0] as PK)
				schemaMap.Set("id", map[string]any{
					"type": "integer",
				})
				for _, f := range schemaDef.Fields {
					fieldInfo := map[string]any{
						"type":     fieldTypeName(f.Type),
						"required": f.Required,
					}
					if f.Unique {
						fieldInfo["unique"] = true
					}
					if f.Indexed {
						fieldInfo["indexed"] = true
					}
					if f.Searchable {
						fieldInfo["searchable"] = true
					}
					if f.Type == FieldRef && f.RefTable != "" {
						fieldInfo["refTable"] = f.RefTable
					}
					// Mark password fields as bcrypt so admin UI shows them as redacted/read-only
					if f.Name == "password" && strings.HasPrefix(name, "_") {
						fieldInfo["type"] = "bcrypt"
					}
					schemaMap.Set(f.Name, fieldInfo)
				}
				tables = append(tables, map[string]any{
					"name":     name,
					"rowCount": count,
					"schema":   schemaMap,
				})
			}
			s.db.mu.RUnlock()
			adminJSON(w, http.StatusOK, map[string]any{"tables": tables})
			return
		}

		// Table rows
		if strings.HasPrefix(path, "/_/api/tables/") {
			rest := strings.TrimPrefix(path, "/_/api/tables/")
			parts := strings.Split(strings.Trim(rest, "/"), "/")

			if len(parts) == 2 && parts[1] == "rows" {
				tableName := parts[0]
				table := s.db.Table(tableName)
				if table == nil {
					adminJSON(w, http.StatusNotFound, map[string]any{"error": "table not found"})
					return
				}
				isInternal := strings.HasPrefix(tableName, "_")

				switch r.Method {
				case http.MethodGet:
					limit := clampAdminInt(parseAdminInt(r.URL.Query().Get("limit"), 100), 1, 1000)
					page := parseAdminInt(r.URL.Query().Get("page"), 1)
					offset := (page - 1) * limit

					total, _ := table.Count()
					var rows []map[string]any
					skipped := 0
					table.Scan(func(row *Row) bool {
						if skipped < offset {
							skipped++
							return true
						}
						if len(rows) >= limit {
							return false // stop early
						}
						data := make(map[string]any, len(row.Data)+1)
						data["id"] = row.ID
						for k, v := range row.Data {
							// Redact password fields on internal tables
							if k == "password" && isInternal {
								data[k] = "[REDACTED]"
							} else {
								data[k] = v
							}
						}
						rows = append(rows, data)
						return true
					})

					if rows == nil {
						rows = []map[string]any{}
					}
					pages := 0
					if total > 0 {
						pages = (total + limit - 1) / limit
					}
					adminJSON(w, http.StatusOK, map[string]any{
						"rows":  rows,
						"total": total,
						"page":  page,
						"pages": pages,
						"limit": limit,
					})

				case http.MethodPost:
					var data map[string]any
					json.NewDecoder(r.Body).Decode(&data)
					row, err := s.db.Insert(tableName, data)
					if err != nil {
						adminJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
						return
					}
					result := make(map[string]any, len(row.Data)+1)
					result["id"] = row.ID
					for k, v := range row.Data {
						result[k] = v
					}
					sseHub.broadcast(tableName, "insert")
					adminJSON(w, http.StatusOK, map[string]any{"ok": true, "row": result})
				}
				return
			}

			// Single row operations: /_/api/tables/{table}/rows/{id}
			if len(parts) == 3 && parts[1] == "rows" {
				tableName := parts[0]
				id, err := strconv.ParseUint(parts[2], 10, 64)
				if err != nil {
					adminJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid id"})
					return
				}

				switch r.Method {
				case http.MethodPut:
					var fields map[string]any
					json.NewDecoder(r.Body).Decode(&fields)
					_, err := s.db.Update(tableName, id, fields)
					if err != nil {
						adminJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
						return
					}
					sseHub.broadcast(tableName, "update")
					adminJSON(w, http.StatusOK, map[string]any{"ok": true})

				case http.MethodDelete:
					if err := s.db.Delete(tableName, id); err != nil {
						adminJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
						return
					}
					sseHub.broadcast(tableName, "delete")
					adminJSON(w, http.StatusOK, map[string]any{"ok": true})

				default:
					adminJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
				}
				return
			}
		}

		// Backup download
		if path == "/_/api/backup" && r.Method == http.MethodGet {
			adminJSON(w, http.StatusNotImplemented, map[string]any{"error": "backup not yet supported in go2"})
			return
		}

		// Admin SPA
		if path == "/_" || path == "/_/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.Write([]byte(adminPageHTML))
			return
		}

		adminJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
	})
}

func extractAdminToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
		return h
	}
	return r.URL.Query().Get("_token")
}

func fieldTypeName(ft FieldType) string {
	switch ft {
	case FieldString:
		return "string"
	case FieldInt:
		return "int"
	case FieldFloat:
		return "float"
	case FieldBool:
		return "bool"
	case FieldTime:
		return "time"
	case FieldRef:
		return "ref"
	default:
		return "unknown"
	}
}

func adminJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func parseAdminInt(s string, fallback int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return n
}

func clampAdminInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

// adminPageHTML, adminLoginHTML, adminSetupHTML are loaded via go:embed in admin_embed.go
