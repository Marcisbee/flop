package flop

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/marcisbee/flop/internal/jsonx"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/server"
)

type AdminTable struct {
	Name                 string           `json:"name"`
	Schema               jsonx.RawMessage `json:"schema,omitempty"`
	RowCount             int              `json:"rowCount"`
	Archive              bool             `json:"archive,omitempty"`
	SourceTable          string           `json:"sourceTable,omitempty"`
	ReadOnly             bool             `json:"readOnly,omitempty"`
	Materialized         bool             `json:"materialized,omitempty"`
	LastRefreshUnixMilli int64            `json:"lastRefreshUnixMilli,omitempty"`
	RefreshError         string           `json:"refreshError,omitempty"`
}

type AdminRowsPage struct {
	Table   string           `json:"table"`
	Archive bool             `json:"archive,omitempty"`
	Rows    []map[string]any `json:"rows"`
	Total   int              `json:"total"`
	Offset  int              `json:"offset"`
	Limit   int              `json:"limit"`
}

type AdminMediaUsage struct {
	TableName string `json:"tableName"`
	RowID     string `json:"rowId"`
	FieldName string `json:"fieldName"`
	Multi     bool   `json:"multi"`
}

type AdminMediaRow struct {
	Path       string   `json:"path"`
	Name       string   `json:"name"`
	URL        string   `json:"url"`
	Mime       string   `json:"mime"`
	RefSize    int64    `json:"refSize"`
	DiskSize   int64    `json:"diskSize"`
	ThumbCount int      `json:"thumbCount"`
	ThumbBytes int64    `json:"thumbBytes"`
	Width      int      `json:"width,omitempty"`
	Height     int      `json:"height,omitempty"`
	Orphaned   bool     `json:"orphaned"`
	TableName  string   `json:"tableName,omitempty"`
	RowID      string   `json:"rowId,omitempty"`
	FieldName  string   `json:"fieldName,omitempty"`
	Thumbs     []string `json:"thumbs,omitempty"`
}

type AdminProvider interface {
	AdminTables() ([]AdminTable, error)
	AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error)
}

// AdminFilterProvider scans rows with a predicate and server-side pagination.
// It returns the page of matching rows, the total count of all matches, and whether the table was found.
// indexField/indexValue are optional hints: when non-empty, the implementation may use
// an index lookup instead of a full table scan (for simple field="value" filters).
type AdminFilterProvider interface {
	AdminFilterRows(table string, match func(map[string]any) bool, limit, offset int, indexField, indexValue string) (rows []map[string]any, total int, found bool, err error)
}

type AdminMediaProvider interface {
	AdminMediaRows(limit, offset int, search string, orphansOnly bool) ([]AdminMediaRow, int, error)
}

type AdminWriteProvider interface {
	AdminCreateRow(table string, data map[string]any) (map[string]any, error)
	AdminUpdateRow(table, pk string, fields map[string]any) error
	AdminDeleteRow(table, pk string) error
	AdminArchiveRow(table, pk string) error
}

type AdminArchiveProvider interface {
	AdminArchiveTables() ([]AdminTable, error)
	AdminArchiveRows(table string, limit, offset int) (AdminRowsPage, bool, error)
	AdminRestoreRow(table, archiveID string) error
	AdminDeleteArchivedRow(table, archiveID string) error
}

type AdminAuthProvider interface {
	AdminLogin(email, password string) (token, refreshToken string, err error)
	AdminRefresh(refreshToken string) (token, nextRefreshToken string, err error)
	AdminIsAuthorized(token string) bool
}

// AdminSetupProvider enables the one-time superadmin setup flow.
// When implemented, if AdminHasSuperadmin returns false the handler
// generates a one-time setup token and serves the setup page.
type AdminSetupProvider interface {
	AdminAuthProvider
	AdminHasSuperadmin() bool
	AdminRegisterSuperadmin(email, password string, extraFields map[string]any) error
}

// SetupField describes an extra field required by the auth table during setup.
type SetupField struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Required   bool     `json:"required"`
	EnumValues []string `json:"enumValues,omitempty"`
}

// AdminSetupSchemaProvider exposes the auth table's extra required fields
// so the setup form can render them dynamically.
type AdminSetupSchemaProvider interface {
	AdminSetupExtraFields() []SetupField
}

// AdminSSEProvider enables server-sent events for real-time admin updates.
type AdminSSEProvider interface {
	// AdminSSE starts an SSE stream. It should block until ctx is done.
	// The writer sends events in SSE format (data: ...\n\n).
	AdminSSE(w http.ResponseWriter, r *http.Request)
}

// AdminAnalyticsProvider enables built-in request analytics APIs in admin.
type AdminAnalyticsProvider interface {
	AdminAnalytics() *server.RequestAnalytics
}

// AdminIndexStatsProvider exposes per-table index statistics for observability.
type AdminIndexStatsProvider interface {
	AdminIndexStats() any
}

type AdminMaterializedProvider interface {
	AdminRefreshMaterialized(table string) error
}

// AdminPprofProvider exposes whether profiling routes should be enabled.
type AdminPprofProvider interface {
	AdminEnablePprof() bool
}

// AdminConfig configures the default admin handler.
type AdminConfig struct {
	// SetupToken is populated automatically when AdminSetupProvider is
	// implemented and no superadmin exists. Read it after calling
	// MountDefaultAdmin to display the setup URL in the server banner.
	SetupToken string
}

func MountDefaultAdmin(mux *http.ServeMux, provider AdminProvider) *AdminConfig {
	return MountDefaultAdminWithConfig(mux, provider, nil)
}

func MountDefaultAdminWithConfig(mux *http.ServeMux, provider AdminProvider, cfg *AdminConfig) *AdminConfig {
	if mux == nil {
		panic("flop: admin mux is nil")
	}
	if cfg == nil {
		cfg = &AdminConfig{}
	}
	h := defaultAdminHandler(provider, cfg)
	mux.Handle("/_", h)
	mux.Handle("/_/", h)
	return cfg
}

func defaultAdminHandler(provider AdminProvider, cfg *AdminConfig) http.Handler {
	authProvider, authEnabled := provider.(AdminAuthProvider)
	writeProvider, writeEnabled := provider.(AdminWriteProvider)
	archiveProvider, archiveEnabled := provider.(AdminArchiveProvider)
	sseProvider, sseEnabled := provider.(AdminSSEProvider)
	filterProvider, filterEnabled := provider.(AdminFilterProvider)
	mediaProvider, mediaEnabled := provider.(AdminMediaProvider)
	analyticsProvider, analyticsCapable := provider.(AdminAnalyticsProvider)
	indexStatsProvider, indexStatsCapable := provider.(AdminIndexStatsProvider)
	pprofProvider, pprofCapable := provider.(AdminPprofProvider)

	// Setup provider — generates a one-time token when no superadmin exists.
	setupProvider, setupCapable := provider.(AdminSetupProvider)
	var setupMu sync.Mutex
	if setupCapable && !setupProvider.AdminHasSuperadmin() {
		if cfg.SetupToken == "" {
			b := make([]byte, 16)
			_, _ = rand.Read(b)
			cfg.SetupToken = hex.EncodeToString(b)
		}
	}

	getAnalytics := func() *server.RequestAnalytics {
		if !analyticsCapable {
			return nil
		}
		return analyticsProvider.AdminAnalytics()
	}

	// When auth is disabled, inject a script that pre-sets tokens in
	// localStorage so the SPA skips the login redirect entirely.
	noAuthAdminHTML := server.AdminPageHTML
	if !authEnabled {
		const inject = `<script>localStorage.setItem('flop_admin_token','no-auth');localStorage.setItem('flop_admin_refresh','no-auth');</script>`
		noAuthAdminHTML = strings.Replace(server.AdminPageHTML, "<script type=\"module\">", inject+"\n<script type=\"module\">", 1)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		w.Header().Set("Cache-Control", "no-store")

		// Login page
		if path == "/_/login" || path == "/_/login/" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if !authEnabled {
				// No auth configured — skip login, go straight to admin.
				http.Redirect(w, r, "/_", http.StatusFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(server.AdminLoginHTML))
			return
		}

		// Login API
		if path == "/_/api/login" && r.Method == http.MethodPost {
			if !authEnabled {
				adminJSONResp(w, http.StatusOK, map[string]any{
					"token":        "no-auth",
					"refreshToken": "no-auth",
				})
				return
			}
			var body struct {
				Email    string `json:"email"`
				Password string `json:"password"`
			}
			if err := jsonx.NewDecoder(r.Body).Decode(&body); err != nil {
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
		}

		// Refresh API
		if path == "/_/api/refresh" && r.Method == http.MethodPost {
			if !authEnabled {
				adminJSONResp(w, http.StatusOK, map[string]any{"token": "no-auth"})
				return
			}
			var body struct {
				RefreshToken string `json:"refreshToken"`
			}
			if err := jsonx.NewDecoder(r.Body).Decode(&body); err != nil {
				adminJSONError(w, "invalid json", http.StatusBadRequest)
				return
			}
			token, nextRefreshToken, err := authProvider.AdminRefresh(body.RefreshToken)
			if err != nil {
				adminJSONError(w, err.Error(), http.StatusUnauthorized)
				return
			}
			adminJSONResp(w, http.StatusOK, map[string]any{"token": token, "refreshToken": nextRefreshToken})
			return
		}

		// Setup page
		if path == "/_/setup" || path == "/_/setup/" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			setupMu.Lock()
			tok := cfg.SetupToken
			setupMu.Unlock()
			if !setupCapable || tok == "" {
				http.Redirect(w, r, "/_/login", http.StatusFound)
				return
			}
			if setupProvider.AdminHasSuperadmin() {
				setupMu.Lock()
				cfg.SetupToken = ""
				setupMu.Unlock()
				http.Redirect(w, r, "/_/login", http.StatusFound)
				return
			}
			if r.URL.Query().Get("token") != tok {
				adminJSONError(w, "invalid setup token", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(server.AdminSetupHTML))
			return
		}

		// Setup schema API — returns extra required fields for the setup form
		if path == "/_/api/setup-schema" && r.Method == http.MethodGet {
			setupMu.Lock()
			tok := cfg.SetupToken
			setupMu.Unlock()
			if !setupCapable || tok == "" {
				adminJSONError(w, "setup not available", http.StatusBadRequest)
				return
			}
			var fields []SetupField
			if sp, ok := provider.(AdminSetupSchemaProvider); ok {
				fields = sp.AdminSetupExtraFields()
			}
			if fields == nil {
				fields = []SetupField{}
			}
			adminJSONResp(w, http.StatusOK, map[string]any{"fields": fields})
			return
		}

		// Setup API
		if path == "/_/api/setup" && r.Method == http.MethodPost {
			setupMu.Lock()
			tok := cfg.SetupToken
			setupMu.Unlock()
			if !setupCapable || tok == "" {
				adminJSONError(w, "setup not available", http.StatusBadRequest)
				return
			}
			var body map[string]any
			if err := jsonx.NewDecoder(r.Body).Decode(&body); err != nil {
				adminJSONError(w, "invalid json", http.StatusBadRequest)
				return
			}
			bodyToken, _ := body["token"].(string)
			email, _ := body["email"].(string)
			password, _ := body["password"].(string)
			if bodyToken != tok {
				adminJSONError(w, "invalid setup token", http.StatusForbidden)
				return
			}
			if setupProvider.AdminHasSuperadmin() {
				setupMu.Lock()
				cfg.SetupToken = ""
				setupMu.Unlock()
				adminJSONError(w, "superadmin already exists", http.StatusBadRequest)
				return
			}
			if email == "" || password == "" {
				adminJSONError(w, "email and password required", http.StatusBadRequest)
				return
			}
			// Collect extra fields (everything except the standard ones)
			extraFields := make(map[string]any)
			for k, v := range body {
				switch k {
				case "token", "email", "password":
					continue
				default:
					extraFields[k] = v
				}
			}
			if err := setupProvider.AdminRegisterSuperadmin(email, password, extraFields); err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			setupMu.Lock()
			cfg.SetupToken = ""
			setupMu.Unlock()
			adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
			return
		}

		// SSE events
		if path == "/_/api/events" && r.Method == http.MethodGet {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			if sseEnabled {
				sseProvider.AdminSSE(w, r)
			} else {
				// Minimal SSE: send initial snapshot and keep connection open
				flusher, ok := w.(http.Flusher)
				if !ok {
					adminJSONError(w, "SSE not supported", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("Connection", "keep-alive")
				fmt.Fprintf(w, "event: snapshot\ndata: {}\n\n")
				flusher.Flush()
				// Keep alive until client disconnects
				<-r.Context().Done()
			}
			return
		}

		// Go pprof diagnostics (superadmin only when auth is enabled)
		if strings.HasPrefix(path, "/_/debug/pprof") {
			if !pprofCapable || !pprofProvider.AdminEnablePprof() {
				adminJSONError(w, "pprof disabled", http.StatusNotFound)
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			server.ServePrefixedPprof("/_/debug/pprof", w, r)
			return
		}

		// Request analytics APIs
		if strings.HasPrefix(path, "/_/api/analytics/") {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			analytics := getAnalytics()
			if analytics == nil {
				adminJSONError(w, "analytics unavailable", http.StatusNotImplemented)
				return
			}

			switch path {
			case "/_/api/analytics/config":
				if r.Method == http.MethodGet {
					adminJSONResp(w, http.StatusOK, map[string]any{
						"ok":             true,
						"enabled":        true,
						"retentionHours": analytics.Retention().Hours(),
						"droppedEvents":  analytics.DroppedEvents(),
						"routeTypes":     analytics.RouteTypes(),
					})
					return
				}
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return

			case "/_/api/analytics/logs":
				if r.Method != http.MethodGet {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				page := parseIntOr(r.URL.Query().Get("page"), 1)
				limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 50), 1, 500)
				rows, total, err := analytics.QueryLogs(page, limit, r.URL.Query().Get("search"), r.URL.Query().Get("filter"), r.URL.Query().Get("routeType"))
				if err != nil {
					adminJSONError(w, "Invalid filter: "+err.Error(), http.StatusBadRequest)
					return
				}
				pages := 0
				if total > 0 {
					pages = (total + limit - 1) / limit
				}
				adminJSONResp(w, http.StatusOK, map[string]any{
					"rows":           rows,
					"total":          total,
					"page":           page,
					"pages":          pages,
					"limit":          limit,
					"retentionHours": analytics.Retention().Hours(),
					"droppedEvents":  analytics.DroppedEvents(),
				})
				return

			case "/_/api/analytics/timeseries":
				if r.Method != http.MethodGet {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				window := parseAdminDuration(r.URL.Query().Get("window"), 24*time.Hour)
				if window < time.Minute {
					window = time.Minute
				}
				data := analytics.QuerySeries(window, strings.TrimSpace(r.URL.Query().Get("routeType")), strings.TrimSpace(r.URL.Query().Get("routeName")))
				adminJSONResp(w, http.StatusOK, map[string]any{
					"ok":     true,
					"window": window.String(),
					"data":   data,
				})
				return

			case "/_/api/analytics/runtime":
				if r.Method != http.MethodGet {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{
					"ok":   true,
					"data": server.RuntimeStatsSnapshot(),
				})
				return

			case "/_/api/analytics/indexes":
				if r.Method != http.MethodGet {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				if !indexStatsCapable {
					adminJSONError(w, "index stats unavailable", http.StatusNotImplemented)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{
					"ok":   true,
					"data": indexStatsProvider.AdminIndexStats(),
				})
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
			if authEnabled {
				_, _ = w.Write([]byte(server.AdminPageHTML))
			} else {
				_, _ = w.Write([]byte(noAuthAdminHTML))
			}
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

		if path == "/_/api/archive/tables" {
			if r.Method != http.MethodGet {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if !archiveEnabled {
				adminJSONResp(w, http.StatusOK, map[string]any{"tables": []AdminTable{}})
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			tables, err := archiveProvider.AdminArchiveTables()
			if err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			adminJSONResp(w, http.StatusOK, map[string]any{"tables": tables})
			return
		}

		if path == "/_/api/media" {
			if r.Method != http.MethodGet {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			if !mediaEnabled {
				adminJSONResp(w, http.StatusOK, map[string]any{"rows": []AdminMediaRow{}, "total": 0, "page": 1, "pages": 0, "limit": 50})
				return
			}
			limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 50), 1, 200)
			page := parseIntOr(r.URL.Query().Get("page"), 1)
			if page < 1 {
				page = 1
			}
			offset := (page - 1) * limit
			search := strings.TrimSpace(r.URL.Query().Get("search"))
			orphansOnly := r.URL.Query().Get("orphans") == "1"
			rows, total, err := mediaProvider.AdminMediaRows(limit, offset, search, orphansOnly)
			if err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			pages := (total + limit - 1) / limit
			adminJSONResp(w, http.StatusOK, map[string]any{"rows": rows, "total": total, "page": page, "pages": pages, "limit": limit})
			return
		}

		if strings.HasPrefix(path, "/_/api/materialized/") && strings.HasSuffix(path, "/refresh") {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			materializedProvider, ok := provider.(AdminMaterializedProvider)
			if !ok {
				adminJSONError(w, "materialized refresh unsupported", http.StatusNotFound)
				return
			}
			if r.Method != http.MethodPost {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			tableName := strings.TrimPrefix(path, "/_/api/materialized/")
			tableName = strings.TrimSuffix(tableName, "/refresh")
			tableName = strings.TrimSuffix(tableName, "/")
			if tableName == "" {
				adminJSONError(w, "table required", http.StatusBadRequest)
				return
			}
			if err := materializedProvider.AdminRefreshMaterialized(tableName); err != nil {
				adminJSONError(w, err.Error(), http.StatusBadRequest)
				return
			}
			adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
			return
		}

		if strings.HasPrefix(path, "/_/api/tables/") {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}

			// /_/api/tables/{table}/rows/{pk} — single row operations
			if tableName, pk, ok := parseRowPath(path); ok {
				if !writeEnabled {
					adminJSONError(w, "write operations not supported", http.StatusMethodNotAllowed)
					return
				}
				switch r.Method {
				case http.MethodPut:
					var fields map[string]any
					if err := jsonx.NewDecoder(r.Body).Decode(&fields); err != nil {
						adminJSONError(w, "invalid json", http.StatusBadRequest)
						return
					}
					if err := writeProvider.AdminUpdateRow(tableName, pk, fields); err != nil {
						adminJSONError(w, err.Error(), http.StatusBadRequest)
						return
					}
					adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
				case http.MethodDelete:
					if err := writeProvider.AdminDeleteRow(tableName, pk); err != nil {
						adminJSONError(w, err.Error(), http.StatusBadRequest)
						return
					}
					adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
				default:
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				}
				return
			}

			if tableName, pk, ok := parseRowArchivePath(path); ok {
				if !writeEnabled {
					adminJSONError(w, "write operations not supported", http.StatusMethodNotAllowed)
					return
				}
				if r.Method != http.MethodPost {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				if err := writeProvider.AdminArchiveRow(tableName, pk); err != nil {
					adminJSONError(w, err.Error(), http.StatusBadRequest)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
				return
			}

			// /_/api/tables/{table}/rows — list or create rows
			tableName, ok := parseRowsPath(path)
			if !ok {
				adminJSONError(w, "not found", http.StatusNotFound)
				return
			}
			if r.Method == http.MethodPost {
				if !writeEnabled {
					adminJSONError(w, "write operations not supported", http.StatusMethodNotAllowed)
					return
				}
				var data map[string]any
				if err := jsonx.NewDecoder(r.Body).Decode(&data); err != nil {
					adminJSONError(w, "invalid json", http.StatusBadRequest)
					return
				}
				row, err := writeProvider.AdminCreateRow(tableName, data)
				if err != nil {
					adminJSONError(w, err.Error(), http.StatusBadRequest)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{"ok": true, "row": row})
				return
			}
			if r.Method != http.MethodGet {
				adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 100), 1, 1000)
			page := parseIntOr(r.URL.Query().Get("page"), 1)
			if page < 1 {
				page = 1
			}

			filterExpr := r.URL.Query().Get("filter")
			searchExpr := r.URL.Query().Get("search")

			if filterExpr != "" || searchExpr != "" {
				// Build predicate and optional index hint
				var matchFn func(map[string]any) bool
				var indexField, indexValue string
				if filterExpr != "" {
					groups, fn, err := server.ParseFilter(filterExpr)
					if err != nil {
						adminJSONError(w, "Invalid filter: "+err.Error(), http.StatusBadRequest)
						return
					}
					matchFn = fn
					indexField, indexValue, _ = server.ExtractSingleEquality(groups)
				} else {
					lower := strings.ToLower(searchExpr)
					matchFn = func(row map[string]any) bool {
						for _, v := range row {
							if s, ok := v.(string); ok && strings.Contains(strings.ToLower(s), lower) {
								return true
							}
						}
						return false
					}
				}

				offset := (page - 1) * limit
				var pageRows []map[string]any
				var total int
				if filterEnabled {
					matched, matchTotal, found, err := filterProvider.AdminFilterRows(tableName, matchFn, limit, offset, indexField, indexValue)
					if err != nil {
						adminJSONError(w, err.Error(), http.StatusBadRequest)
						return
					}
					if !found {
						adminJSONError(w, "not found", http.StatusNotFound)
						return
					}
					pageRows = matched
					total = matchTotal
				} else {
					result, found, err := provider.AdminRows(tableName, 1000000, 0)
					if err != nil {
						adminJSONError(w, err.Error(), http.StatusBadRequest)
						return
					}
					if !found {
						adminJSONError(w, "not found", http.StatusNotFound)
						return
					}
					for _, row := range result.Rows {
						if matchFn(row) {
							total++
							if total > offset && len(pageRows) < limit {
								pageRows = append(pageRows, row)
							}
						}
					}
				}

				if pageRows == nil {
					pageRows = []map[string]any{}
				}
				pages := (total + limit - 1) / limit
				adminJSONResp(w, http.StatusOK, map[string]any{
					"rows":  pageRows,
					"total": total,
					"page":  page,
					"pages": pages,
					"limit": limit,
				})
				return
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

		if strings.HasPrefix(path, "/_/api/archive/tables/") {
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				adminJSONError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			if !archiveEnabled {
				adminJSONError(w, "archive unavailable", http.StatusNotFound)
				return
			}
			rest := strings.TrimPrefix(path, "/_/api/archive/tables/")
			parts := strings.Split(rest, "/")
			if len(parts) >= 4 && parts[1] == "rows" && parts[3] == "restore" {
				if r.Method != http.MethodPost {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				if err := archiveProvider.AdminRestoreRow(parts[0], parts[2]); err != nil {
					adminJSONError(w, err.Error(), http.StatusBadRequest)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
				return
			}
			if len(parts) >= 3 && parts[1] == "rows" && r.Method == http.MethodDelete {
				if err := archiveProvider.AdminDeleteArchivedRow(parts[0], parts[2]); err != nil {
					adminJSONError(w, err.Error(), http.StatusBadRequest)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{"ok": true})
				return
			}
			if len(parts) >= 2 && parts[1] == "rows" {
				if r.Method != http.MethodGet {
					adminJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 50), 1, 1000)
				offset := parseIntOr(r.URL.Query().Get("offset"), 0)
				if offset < 0 {
					offset = 0
				}
				result, found, err := archiveProvider.AdminArchiveRows(parts[0], limit, offset)
				if err != nil {
					adminJSONError(w, err.Error(), http.StatusBadRequest)
					return
				}
				if !found {
					adminJSONError(w, "not found", http.StatusNotFound)
					return
				}
				adminJSONResp(w, http.StatusOK, map[string]any{
					"rows":    result.Rows,
					"total":   result.Total,
					"table":   result.Table,
					"limit":   result.Limit,
					"offset":  result.Offset,
					"pages":   (result.Total + result.Limit - 1) / result.Limit,
					"archive": true,
				})
				return
			}
			adminJSONError(w, "not found", http.StatusNotFound)
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

// parseRowPath matches /_/api/tables/{table}/rows/{pk}
func parseRowPath(path string) (table, pk string, ok bool) {
	rest := strings.TrimPrefix(path, "/_/api/tables/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 3 || parts[1] != "rows" || parts[0] == "" || parts[2] == "" {
		return "", "", false
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		return "", "", false
	}
	id, err := url.PathUnescape(parts[2])
	if err != nil || id == "" {
		return "", "", false
	}
	return name, id, true
}

// parseRowArchivePath matches /_/api/tables/{table}/rows/{pk}/archive
func parseRowArchivePath(path string) (table, pk string, ok bool) {
	rest := strings.TrimPrefix(path, "/_/api/tables/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 4 || parts[1] != "rows" || parts[3] != "archive" || parts[0] == "" || parts[2] == "" {
		return "", "", false
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		return "", "", false
	}
	id, err := url.PathUnescape(parts[2])
	if err != nil || id == "" {
		return "", "", false
	}
	return name, id, true
}

// parseRowsPath matches /_/api/tables/{table}/rows
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

func parseAdminDuration(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(raw, "d"))
		if err != nil || days <= 0 {
			return fallback
		}
		return time.Duration(days) * 24 * time.Hour
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
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
	_ = jsonx.NewEncoder(w).Encode(payload)
}

func adminJSONError(w http.ResponseWriter, message string, status int) {
	adminJSONResp(w, status, map[string]any{"error": message})
}

// JWTPayload contains the claims for a JWT token.
type JWTPayload = server.JWTPayload

// CreateJWT creates a signed HS256 JWT token.
func CreateJWT(payload *JWTPayload, secret string) string {
	return server.CreateJWT(payload, secret)
}

// VerifyJWT verifies and decodes a JWT token. Returns nil if invalid or expired.
func VerifyJWT(token, secret string) *JWTPayload {
	return server.VerifyJWT(token, secret)
}

// HashPassword hashes a password using PBKDF2-SHA256.
func HashPassword(password string) (string, error) {
	return server.HashPassword(password)
}

// VerifyPassword checks a password against a hash using registered verifiers.
// Supports PBKDF2 and bcrypt by default.
func VerifyPassword(password, hash string) bool {
	return server.VerifyPassword(password, hash)
}

// PasswordVerifier checks if a plaintext password matches a stored hash.
type PasswordVerifier = server.PasswordVerifier

// RegisterPasswordVerifier adds a custom password verifier for a specific hash format.
func RegisterPasswordVerifier(v PasswordVerifier) {
	server.RegisterPasswordVerifier(v)
}

// PurposePayload is used for single-use verification tokens.
type PurposePayload = server.PurposePayload

// CreatePurposeJWT creates a signed JWT for verification/reset purposes.
func CreatePurposeJWT(payload *PurposePayload, secret string) string {
	return server.CreatePurposeJWT(payload, secret)
}

// VerifyPurposeJWT verifies and decodes a purpose JWT token.
func VerifyPurposeJWT(token, secret string) *PurposePayload {
	return server.VerifyPurposeJWT(token, secret)
}

// SMTPConfig holds SMTP server settings for sending emails.
type SMTPConfig = server.SMTPConfig
