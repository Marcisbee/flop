package flop

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/marcisbee/flop/internal/server"
)

type AdminTable struct {
	Name     string `json:"name"`
	RowCount int    `json:"rowCount"`
}

type AdminRowsPage struct {
	Table  string           `json:"table"`
	Rows   []map[string]any `json:"rows"`
	Total  int              `json:"total"`
	Offset int              `json:"offset"`
	Limit  int              `json:"limit"`
}

type AdminProvider interface {
	AdminTables() ([]AdminTable, error)
	AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error)
}

type AdminAuthProvider interface {
	AdminLogin(email, password string) (token, refreshToken string, err error)
	AdminRefresh(refreshToken string) (token string, err error)
	AdminIsAuthorized(token string) bool
}

func MountDefaultAdmin(mux *http.ServeMux, provider AdminProvider) {
	if mux == nil {
		panic("flop: admin mux is nil")
	}
	h := DefaultAdminHandler(provider)
	mux.Handle("/_", h)
	mux.Handle("/_/", h)
}

func DefaultAdminHandler(provider AdminProvider) http.Handler {
	authProvider, authEnabled := provider.(AdminAuthProvider)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if authEnabled {
			switch path {
			case "/_/login", "/_/login/":
				if r.Method != http.MethodGet {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write([]byte(server.AdminLoginHTML))
				return
			case "/_/api/login":
				if r.Method != http.MethodPost {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				var body struct {
					Email    string `json:"email"`
					Password string `json:"password"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					adminJSONError(w, "invalid json", http.StatusBadRequest)
					return
				}
				token, refresh, err := authProvider.AdminLogin(body.Email, body.Password)
				if err != nil {
					adminJSONError(w, err.Error(), http.StatusUnauthorized)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{
					"token":        token,
					"refreshToken": refresh,
				})
				return
			case "/_/api/refresh":
				if r.Method != http.MethodPost {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				var body struct {
					RefreshToken string `json:"refreshToken"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					adminJSONError(w, "invalid json", http.StatusBadRequest)
					return
				}
				token, err := authProvider.AdminRefresh(body.RefreshToken)
				if err != nil {
					adminJSONError(w, err.Error(), http.StatusUnauthorized)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{"token": token})
				return
			}
		}

		if path == "/_" || path == "/_/" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			_, _ = w.Write([]byte(server.AdminPageHTML))
			return
		}

		if path == "/_/api/tables" {
			if r.Method != http.MethodGet {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			tables, err := provider.AdminTables()
			if err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			adminJSONResp(w, http.StatusOK, map[string]any{
				"tables": tables,
			})
			return
		}

		if strings.HasPrefix(path, "/_/api/tables/") {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			if r.Method != http.MethodGet {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			tableName, ok := parseRowsPath(path)
			if !ok {
				adminJSONError(w, "not found", http.StatusNotFound)
				return
			}
			limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 100), 1, 1000)
			page := parseIntOr(r.URL.Query().Get("page"), 1)
			if page < 1 {
				page = 1
			}
			offset := (page - 1) * limit
			result, found, err := provider.AdminRows(tableName, limit, offset)
			if err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !found {
				adminJSONError(w, "not found", http.StatusNotFound)
				return
			}
			pages := (result.Total + limit - 1) / limit
			adminJSONResp(w, http.StatusOK, map[string]any{
				"rows":  result.Rows,
				"total": result.Total,
				"page":  page,
				"pages": pages,
				"limit": limit,
			})
			return
		}

		adminJSONError(w, "not found", http.StatusNotFound)
	})
}

func isAuthorizedRequest(r *http.Request, authProvider AdminAuthProvider) bool {
	token := extractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		return false
	}
	return authProvider.AdminIsAuthorized(token)
}

func extractBearerToken(headerValue, queryToken string) string {
	h := strings.TrimSpace(headerValue)
	if h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
		return h
	}
	return strings.TrimSpace(queryToken)
}

func parseRowsPath(path string) (string, bool) {
	rest := strings.TrimPrefix(path, "/_/api/tables/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 2 || parts[1] != "rows" || parts[0] == "" {
		return "", false
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		return "", false
	}
	return name, true
}

func parseIntOr(raw string, fallback int) int {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func adminJSONResp(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func adminJSONError(w http.ResponseWriter, message string, status int) {
	adminJSONResp(w, status, map[string]any{"error": message})
}
