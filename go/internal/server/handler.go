package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/runtime"
	"github.com/marcisbee/flop/internal/schema"
)

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port      int
	JWTSecret string
	StaticDir string
}

// HandlerCaller calls JS view/reducer handlers.
type HandlerCaller interface {
	CallHandler(handlerType, name, paramsJSON, authJSON string) (string, error)
}

// Handler manages all HTTP request routing.
type Handler struct {
	db          *engine.Database
	caller      HandlerCaller
	routes      []runtime.RouteInfo
	pageRoutes  []runtime.FlatRoute
	authService *AuthService
	config      ServerConfig
	setupToken  string

	clientJS  []byte
	clientCSS []byte
}

const sseEventBufferSize = 4096
const sseChangeBatchSize = 128
const sseChangeFlushInterval = 25 * time.Millisecond

// NewHandler creates the main HTTP handler.
func NewHandler(
	db *engine.Database,
	caller HandlerCaller,
	routes []runtime.RouteInfo,
	pageRoutes []runtime.FlatRoute,
	authService *AuthService,
	config ServerConfig,
	setupToken string,
	clientJS, clientCSS []byte,
) *Handler {
	return &Handler{
		db:          db,
		caller:      caller,
		routes:      routes,
		pageRoutes:  pageRoutes,
		authService: authService,
		config:      config,
		setupToken:  setupToken,
		clientJS:    clientJS,
		clientCSS:   clientCSS,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}

	path := r.URL.Path

	defer func() {
		if rec := recover(); rec != nil {
			jsonError(w, fmt.Sprintf("%v", rec), 500)
		}
	}()

	// Admin panel routes
	if strings.HasPrefix(path, "/_") {
		h.handleAdmin(w, r, path)
		return
	}

	// API routes
	if strings.HasPrefix(path, "/api/") {
		h.handleAPI(w, r, path)
		return
	}

	// Bundled client assets
	if path == "/assets/client.js" && len(h.clientJS) > 0 && r.Method == "GET" {
		w.Header().Set("Content-Type", "application/javascript")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Write(h.clientJS)
		return
	}
	if path == "/assets/client.css" && len(h.clientCSS) > 0 && r.Method == "GET" {
		w.Header().Set("Content-Type", "text/css")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Write(h.clientCSS)
		return
	}

	// Static file serving
	if strings.HasPrefix(path, "/assets/") && h.config.StaticDir != "" && r.Method == "GET" {
		if h.serveStatic(w, r, path) {
			return
		}
		jsonError(w, "Not found", 404)
		return
	}

	// Page route matching
	if len(h.pageRoutes) > 0 && r.Method == "GET" {
		if h.handlePageRoute(w, r, path) {
			return
		}
	}

	jsonError(w, "Not found", 404)
}

func (h *Handler) handleAPI(w http.ResponseWriter, r *http.Request, path string) {
	// File serving
	if strings.HasPrefix(path, "/api/files/") {
		filePath := filepath.Join(h.db.GetDataDir(), strings.Replace(path, "/api/files/", "/_files/", 1))
		http.ServeFile(w, r, filePath)
		return
	}

	// Schema endpoint
	if path == "/api/schema" && r.Method == "GET" {
		h.handleSchema(w)
		return
	}

	// Auth endpoints
	if strings.HasPrefix(path, "/api/auth/") && h.authService != nil {
		h.handleAuthEndpoint(w, r, path)
		return
	}

	// SSE multiplexed
	if path == "/api/sse" && strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.handleMultiplexedSSE(w, r)
		return
	}

	// Route matching (views + reducers)
	route := h.findRoute(path)
	if route == nil {
		jsonError(w, "Not found", 404)
		return
	}

	// Method check
	if route.Type == "reducer" && r.Method != "POST" {
		jsonError(w, "Method not allowed. Use POST.", 405)
		return
	}

	// Permission enforcement
	auth, denied := h.enforceAccess(w, r, route)
	if denied {
		return
	}

	// SSE for views
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") && route.Type == "view" {
		h.handleSSE(w, r, route, auth)
		return
	}

	// Normal HTTP handler
	if route.Type == "reducer" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			jsonError(w, "Failed to read body", 400)
			return
		}
		h.callHandler(w, "reducer", route.Name, string(body), auth)
		return
	}

	if route.Type == "view" {
		params := make(map[string]string)
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
		paramsJSON, _ := json.Marshal(params)
		h.callHandler(w, "view", route.Name, string(paramsJSON), auth)
		return
	}

	jsonError(w, "Not found", 404)
}

func (h *Handler) callHandler(w http.ResponseWriter, handlerType, name, paramsJSON string, auth *schema.AuthContext) {
	authJSON := "null"
	if auth != nil {
		b, _ := json.Marshal(auth)
		authJSON = string(b)
	}

	result, err := h.caller.CallHandler(handlerType, name, paramsJSON, authJSON)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"data":%s}`, result)
}

func (h *Handler) handleSchema(w http.ResponseWriter) {
	endpoints := make([]map[string]interface{}, 0, len(h.routes))
	for _, r := range h.routes {
		e := map[string]interface{}{
			"name":   r.Name,
			"method": r.Method,
			"path":   r.Path,
			"type":   r.Type,
			"access": r.Access.Type,
			"params": r.ParamDefs,
		}
		endpoints = append(endpoints, e)
	}
	jsonResponse(w, map[string]interface{}{"endpoints": endpoints})
}

func (h *Handler) handleAuthEndpoint(w http.ResponseWriter, r *http.Request, path string) {
	var body map[string]interface{}
	if r.Method == "POST" {
		defer r.Body.Close()
		json.NewDecoder(r.Body).Decode(&body)
	}

	switch path {
	case "/api/auth/register":
		email, _ := body["email"].(string)
		password, _ := body["password"].(string)
		name, _ := body["name"].(string)
		if email == "" || password == "" {
			jsonError(w, "Email and password required", 400)
			return
		}
		token, user, err := h.authService.Register(email, password, name)
		if err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		jsonResponse(w, map[string]interface{}{"token": token, "user": user})

	case "/api/auth/password":
		email, _ := body["email"].(string)
		password, _ := body["password"].(string)
		if email == "" || password == "" {
			jsonError(w, "Email and password required", 400)
			return
		}
		token, refresh, user, err := h.authService.Login(email, password)
		if err != nil {
			jsonError(w, err.Error(), 401)
			return
		}
		jsonResponse(w, map[string]interface{}{"token": token, "refreshToken": refresh, "user": user})

	case "/api/auth/refresh":
		refreshToken, _ := body["refreshToken"].(string)
		if refreshToken == "" {
			jsonError(w, "Refresh token required", 400)
			return
		}
		token, err := h.authService.Refresh(refreshToken)
		if err != nil {
			jsonError(w, err.Error(), 401)
			return
		}
		jsonResponse(w, map[string]interface{}{"token": token})

	default:
		jsonError(w, "Unknown auth endpoint", 404)
	}
}

func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, route *runtime.RouteInfo, auth *schema.AuthContext) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "SSE not supported", 400)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	writeEvent := func(eventType, payload string) {
		sseEvent(w, eventType, payload)
		flusher.Flush()
	}
	writeHeartbeat := func() {
		fmt.Fprint(w, ": heartbeat\n\n")
		flusher.Flush()
	}

	// Send initial snapshot
	params := make(map[string]string)
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	paramsJSON, _ := json.Marshal(params)
	authJSON := "null"
	if auth != nil {
		b, _ := json.Marshal(auth)
		authJSON = string(b)
	}

	result, err := h.caller.CallHandler("view", route.Name, string(paramsJSON), authJSON)
	if err != nil {
		writeEvent("error", fmt.Sprintf(`{"error":%q}`, err.Error()))
	} else {
		writeEvent("snapshot", result)
	}

	// Subscribe only to tables this view depends on.
	tables := append([]string(nil), route.DependentTables...)
	if len(tables) == 0 {
		for name := range h.db.Tables {
			tables = append(tables, name)
		}
	}

	done := r.Context().Done()
	changeCh := make(chan engine.ChangeEvent, sseEventBufferSize)
	unsubscribe := h.db.GetPubSub().Subscribe(tables, func(event engine.ChangeEvent) {
		select {
		case changeCh <- event:
		default:
			// Drop if subscriber cannot keep up; avoid slowing write path.
		}
	})
	defer unsubscribe()

	// Keep alive
	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()
	flushTicker := time.NewTicker(sseChangeFlushInterval)
	defer flushTicker.Stop()
	pendingChangeFlush := false

	for {
		select {
		case <-done:
			return
		case event := <-changeCh:
			batchCount := 0
		drainChanges:
			for {
				data, _ := json.Marshal(event)
				sseEvent(w, "change", string(data))
				batchCount++
				pendingChangeFlush = true
				if batchCount >= sseChangeBatchSize {
					flusher.Flush()
					pendingChangeFlush = false
					break
				}
				select {
				case event = <-changeCh:
				default:
					break drainChanges
				}
			}
		case <-flushTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
		case <-heartbeatTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
			writeHeartbeat()
		}
	}
}

func (h *Handler) handleMultiplexedSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "SSE not supported", 400)
		return
	}

	viewNames := strings.Split(r.URL.Query().Get("views"), ",")
	if len(viewNames) == 0 || viewNames[0] == "" {
		jsonError(w, "No views specified. Use ?views=name1,name2", 400)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	writeEvent := func(eventType, payload string) {
		sseEvent(w, eventType, payload)
		flusher.Flush()
	}
	writeHeartbeat := func() {
		fmt.Fprint(w, ": heartbeat\n\n")
		flusher.Flush()
	}

	// Get auth context
	token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	var auth *schema.AuthContext
	if token != "" {
		payload := VerifyJWT(token, h.config.JWTSecret)
		if payload != nil {
			auth = JWTToAuthContext(payload)
		}
	}

	authJSON := "null"
	if auth != nil {
		b, _ := json.Marshal(auth)
		authJSON = string(b)
	}

	// Send snapshots for each view
	type snapshotResult struct {
		name   string
		result string
		err    error
	}

	results := make(chan snapshotResult, len(viewNames))
	scheduled := 0

	for _, rawName := range viewNames {
		name := strings.TrimSpace(rawName)
		route := h.findRoute(fmt.Sprintf("/api/view/%s", name))
		if route == nil {
			writeEvent(fmt.Sprintf("error:%s", name), fmt.Sprintf(`{"error":"View not found: %s"}`, name))
			continue
		}

		// Extract per-view params
		params := make(map[string]string)
		prefix := name + "."
		for k, v := range r.URL.Query() {
			if strings.HasPrefix(k, prefix) {
				params[k[len(prefix):]] = v[0]
			}
		}
		paramsJSON, _ := json.Marshal(params)
		scheduled++

		go func(viewName, pJSON string) {
			res, err := h.caller.CallHandler("view", viewName, pJSON, authJSON)
			select {
			case results <- snapshotResult{name: viewName, result: res, err: err}:
			case <-r.Context().Done():
			}
		}(name, string(paramsJSON))
	}

	for i := 0; i < scheduled; i++ {
		select {
		case <-r.Context().Done():
			return
		case out := <-results:
			if out.err != nil {
				writeEvent(fmt.Sprintf("error:%s", out.name), fmt.Sprintf(`{"error":%q}`, out.err.Error()))
			} else {
				writeEvent(fmt.Sprintf("snapshot:%s", out.name), out.result)
			}
		}
	}

	// Subscribe only to the union of tables required by selected views.
	tableSet := make(map[string]struct{})
	for _, rawName := range viewNames {
		name := strings.TrimSpace(rawName)
		route := h.findRoute(fmt.Sprintf("/api/view/%s", name))
		if route == nil || len(route.DependentTables) == 0 {
			continue
		}
		for _, t := range route.DependentTables {
			tableSet[t] = struct{}{}
		}
	}
	tables := make([]string, 0, len(tableSet))
	for name := range tableSet {
		tables = append(tables, name)
	}
	if len(tables) == 0 {
		for name := range h.db.Tables {
			tables = append(tables, name)
		}
	}

	done := r.Context().Done()
	changeCh := make(chan engine.ChangeEvent, sseEventBufferSize)
	unsubscribe := h.db.GetPubSub().Subscribe(tables, func(event engine.ChangeEvent) {
		select {
		case changeCh <- event:
		default:
			// Drop if subscriber cannot keep up; avoid slowing write path.
		}
	})
	defer unsubscribe()

	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()
	flushTicker := time.NewTicker(sseChangeFlushInterval)
	defer flushTicker.Stop()
	pendingChangeFlush := false

	for {
		select {
		case <-done:
			return
		case event := <-changeCh:
			batchCount := 0
		drainChanges:
			for {
				data, _ := json.Marshal(event)
				sseEvent(w, "change", string(data))
				batchCount++
				pendingChangeFlush = true
				if batchCount >= sseChangeBatchSize {
					flusher.Flush()
					pendingChangeFlush = false
					break
				}
				select {
				case event = <-changeCh:
				default:
					break drainChanges
				}
			}
		case <-flushTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
		case <-heartbeatTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
			writeHeartbeat()
		}
	}
}

func (h *Handler) handlePageRoute(w http.ResponseWriter, r *http.Request, path string) bool {
	for _, route := range h.pageRoutes {
		re, err := regexp.Compile("^" + patternToRegex(route.Pattern) + "$")
		if err != nil {
			continue
		}
		match := re.FindStringSubmatch(path)
		if match == nil {
			continue
		}

		// Extract params
		paramNames := extractParamNames(route.Pattern)
		params := make(map[string]string)
		for i, name := range paramNames {
			if i+1 < len(match) {
				params[name] = match[i+1]
			}
		}

		routeData, _ := json.Marshal(map[string]interface{}{
			"pattern": route.Pattern,
			"params":  params,
		})

		html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
</head>
<body>
<div id="root"></div>
<script>window.__FLOP_ROUTE__=%s</script>
<script type="module" src="/assets/client.js"></script>
</body>
</html>`, string(routeData))

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
		return true
	}
	return false
}

func (h *Handler) handleAdmin(w http.ResponseWriter, r *http.Request, path string) {
	// Setup page
	if (path == "/_/setup" || path == "/_/setup/") && r.Method == "GET" {
		if h.authService == nil || h.setupToken == "" {
			http.Redirect(w, r, "/_/login", 302)
			return
		}
		if h.authService.HasSuperadmin() {
			h.setupToken = ""
			http.Redirect(w, r, "/_/login", 302)
			return
		}
		token := r.URL.Query().Get("token")
		if token != h.setupToken {
			jsonError(w, "Invalid setup token", 403)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(adminSetupHTML))
		return
	}

	// Setup API
	if path == "/_/api/setup" && r.Method == "POST" {
		if h.authService == nil || h.setupToken == "" {
			jsonError(w, "Setup not available", 400)
			return
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		token, _ := body["token"].(string)
		if token != h.setupToken {
			jsonError(w, "Invalid setup token", 403)
			return
		}
		if h.authService.HasSuperadmin() {
			h.setupToken = ""
			jsonError(w, "Superadmin already exists", 400)
			return
		}
		email, _ := body["email"].(string)
		password, _ := body["password"].(string)
		name, _ := body["name"].(string)
		if email == "" || password == "" {
			jsonError(w, "Email and password required", 400)
			return
		}
		_, _, err := h.authService.RegisterSuperadmin(email, password, name)
		if err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		h.setupToken = ""
		jsonResponse(w, map[string]interface{}{"ok": true})
		return
	}

	// Login page
	if path == "/_/login" || path == "/_/login/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(adminLoginHTML))
		return
	}

	// Login API
	if path == "/_/api/login" && r.Method == "POST" {
		if h.authService == nil {
			jsonError(w, "Auth not configured", 400)
			return
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		email, _ := body["email"].(string)
		password, _ := body["password"].(string)
		token, refresh, user, err := h.authService.Login(email, password)
		if err != nil {
			jsonError(w, err.Error(), 401)
			return
		}
		hasSuperadmin := false
		for _, r := range user.Roles {
			if r == "superadmin" {
				hasSuperadmin = true
				break
			}
		}
		if !hasSuperadmin {
			jsonError(w, "Insufficient privileges. Requires superadmin role.", 403)
			return
		}
		jsonResponse(w, map[string]interface{}{"token": token, "refreshToken": refresh})
		return
	}

	// Refresh token
	if path == "/_/api/refresh" && r.Method == "POST" {
		if h.authService == nil {
			jsonError(w, "Auth not configured", 400)
			return
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		refreshToken, _ := body["refreshToken"].(string)
		if refreshToken == "" {
			jsonError(w, "Refresh token required", 400)
			return
		}
		token, err := h.authService.Refresh(refreshToken)
		if err != nil {
			jsonError(w, err.Error(), 401)
			return
		}
		jsonResponse(w, map[string]interface{}{"token": token})
		return
	}

	// Admin SPA
	if path == "/_" || path == "/_/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write([]byte(adminPageHTML))
		return
	}

	// Admin API routes - require superadmin
	token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		jsonError(w, "Authentication required", 401)
		return
	}
	payload := VerifyJWT(token, h.config.JWTSecret)
	if payload == nil {
		jsonError(w, "Invalid token", 401)
		return
	}
	hasSuperadmin := false
	for _, r := range payload.Roles {
		if r == "superadmin" {
			hasSuperadmin = true
			break
		}
	}
	if !hasSuperadmin {
		jsonError(w, "Requires superadmin role", 403)
		return
	}

	// Table listing
	if path == "/_/api/tables" && r.Method == "GET" {
		h.handleListTables(w)
		return
	}

	// SSE events
	if path == "/_/api/events" && r.Method == "GET" {
		h.handleAdminSSE(w, r)
		return
	}

	// Table rows
	if re := regexp.MustCompile(`^/_/api/tables/([^/]+)/rows$`); true {
		match := re.FindStringSubmatch(path)
		if match != nil {
			if r.Method == "GET" {
				h.handleListRows(w, r, match[1])
				return
			}
			if r.Method == "POST" {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				h.handleCreateRow(w, match[1], body)
				return
			}
		}
	}

	// Single row
	if re := regexp.MustCompile(`^/_/api/tables/([^/]+)/rows/([^/]+)$`); true {
		match := re.FindStringSubmatch(path)
		if match != nil {
			switch r.Method {
			case "GET":
				h.handleGetRow(w, match[1], match[2])
				return
			case "PUT":
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				h.handleUpdateRow(w, match[1], match[2], body)
				return
			case "DELETE":
				h.handleDeleteRow(w, match[1], match[2])
				return
			}
		}
	}

	// Backup
	if path == "/_/api/backup" {
		if r.Method == "GET" {
			// TODO: implement backup download
			jsonError(w, "Not implemented yet", 501)
			return
		}
	}

	jsonError(w, "Not found", 404)
}

func (h *Handler) handleListTables(w http.ResponseWriter) {
	tables := make([]map[string]interface{}, 0)
	for name, table := range h.db.Tables {
		def := table.GetDef()
		schemaMap := make(map[string]interface{})
		for _, f := range def.CompiledSchema.Fields {
			entry := map[string]interface{}{
				"type":     string(f.Kind),
				"required": f.Required,
				"unique":   f.Unique,
			}
			if f.RefTableName != "" {
				entry["refTable"] = f.RefTableName
			}
			if f.RefField != "" {
				entry["refField"] = f.RefField
			}
			if len(f.EnumValues) > 0 {
				entry["enumValues"] = f.EnumValues
			}
			if len(f.MimeTypes) > 0 {
				entry["mimeTypes"] = f.MimeTypes
			}
			schemaMap[f.Name] = entry
		}
		tables = append(tables, map[string]interface{}{
			"name":     name,
			"schema":   schemaMap,
			"rowCount": table.Count(),
		})
	}
	jsonResponse(w, map[string]interface{}{"tables": tables})
}

func (h *Handler) handleListRows(w http.ResponseWriter, r *http.Request, tableName string) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}

	q := r.URL.Query()
	page := intParam(q.Get("page"), 1)
	limit := intParam(q.Get("limit"), 50)
	search := q.Get("search")
	offset := (page - 1) * limit

	rows, err := table.Scan(10000, 0)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"rows": []interface{}{}, "total": 0, "page": page, "pages": 0, "limit": limit})
		return
	}

	// Search filter
	if search != "" {
		lower := strings.ToLower(search)
		var filtered []map[string]interface{}
		for _, row := range rows {
			for _, v := range row {
				if s, ok := v.(string); ok && strings.Contains(strings.ToLower(s), lower) {
					filtered = append(filtered, row)
					break
				}
			}
		}
		rows = filtered
	}

	total := len(rows)
	pages := (total + limit - 1) / limit
	end := offset + limit
	if end > total {
		end = total
	}
	if offset > total {
		offset = total
	}
	pageRows := rows[offset:end]

	// Redact password fields
	def := table.GetDef()
	redacted := make([]map[string]interface{}, len(pageRows))
	for i, row := range pageRows {
		r := make(map[string]interface{}, len(row))
		for k, v := range row {
			r[k] = v
		}
		for _, f := range def.CompiledSchema.Fields {
			if f.Kind == "bcrypt" && r[f.Name] != nil {
				r[f.Name] = "[REDACTED]"
			}
		}
		redacted[i] = r
	}

	jsonResponse(w, map[string]interface{}{
		"rows":  redacted,
		"total": total,
		"page":  page,
		"pages": pages,
		"limit": limit,
	})
}

func (h *Handler) handleGetRow(w http.ResponseWriter, tableName, id string) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	row, err := table.Get(id)
	if err != nil || row == nil {
		jsonError(w, "Row not found", 404)
		return
	}
	jsonResponse(w, map[string]interface{}{"row": row})
}

func (h *Handler) handleCreateRow(w http.ResponseWriter, tableName string, data map[string]interface{}) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	row, err := table.Insert(data, nil)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}
	w.WriteHeader(201)
	jsonResponse(w, map[string]interface{}{"ok": true, "row": row})
}

func (h *Handler) handleUpdateRow(w http.ResponseWriter, tableName, id string, updates map[string]interface{}) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	row, err := table.Update(id, updates, nil)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}
	if row == nil {
		jsonError(w, "Row not found", 404)
		return
	}
	jsonResponse(w, map[string]interface{}{"ok": true, "row": row})
}

func (h *Handler) handleDeleteRow(w http.ResponseWriter, tableName, id string) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	deleted, err := table.Delete(id, nil)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}
	if !deleted {
		jsonError(w, "Row not found", 404)
		return
	}
	jsonResponse(w, map[string]interface{}{"ok": true, "deleted": id})
}

func (h *Handler) handleAdminSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "SSE not supported", 400)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Send initial table counts
	tableCounts := make(map[string]int)
	for name, table := range h.db.Tables {
		tableCounts[name] = table.Count()
	}
	data, _ := json.Marshal(map[string]interface{}{"tableCounts": tableCounts})
	sseEvent(w, "snapshot", string(data))
	flusher.Flush()

	done := r.Context().Done()
	changeCh := make(chan engine.ChangeEvent, sseEventBufferSize)
	unsubscribe := h.db.GetPubSub().SubscribeAll(func(event engine.ChangeEvent) {
		select {
		case changeCh <- event:
		default:
			// Drop if subscriber cannot keep up; avoid slowing write path.
		}
	})
	defer unsubscribe()

	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()
	flushTicker := time.NewTicker(sseChangeFlushInterval)
	defer flushTicker.Stop()
	pendingChangeFlush := false

	for {
		select {
		case <-done:
			return
		case event := <-changeCh:
			batchCount := 0
		drainChanges:
			for {
				data, _ := json.Marshal(event)
				sseEvent(w, "change", string(data))
				batchCount++
				pendingChangeFlush = true
				if batchCount >= sseChangeBatchSize {
					flusher.Flush()
					pendingChangeFlush = false
					break
				}
				select {
				case event = <-changeCh:
				default:
					break drainChanges
				}
			}
		case <-flushTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
		case <-heartbeatTicker.C:
			if pendingChangeFlush {
				flusher.Flush()
				pendingChangeFlush = false
			}
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (h *Handler) findRoute(path string) *runtime.RouteInfo {
	for i := range h.routes {
		if h.routes[i].Path == path {
			return &h.routes[i]
		}
	}
	return nil
}

func (h *Handler) enforceAccess(w http.ResponseWriter, r *http.Request, route *runtime.RouteInfo) (*schema.AuthContext, bool) {
	policy := route.Access

	token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))

	if policy.Type == "public" {
		if token != "" {
			payload := VerifyJWT(token, h.config.JWTSecret)
			if payload != nil {
				return JWTToAuthContext(payload), false
			}
		}
		return nil, false
	}

	if token == "" {
		jsonError(w, "Authentication required", 401)
		return nil, true
	}

	payload := VerifyJWT(token, h.config.JWTSecret)
	if payload == nil {
		jsonError(w, "Invalid or expired token", 401)
		return nil, true
	}

	auth := JWTToAuthContext(payload)

	if policy.Type == "roles" {
		hasAccess := false
		for _, r := range auth.Roles {
			if r == "superadmin" {
				hasAccess = true
				break
			}
			for _, required := range policy.Roles {
				if r == required {
					hasAccess = true
					break
				}
			}
		}
		if !hasAccess {
			jsonError(w, "Forbidden", 403)
			return auth, true
		}
	}

	return auth, false
}

func (h *Handler) serveStatic(w http.ResponseWriter, r *http.Request, path string) bool {
	if strings.Contains(path, "..") {
		return false
	}
	relative := strings.TrimPrefix(path, "/assets")
	filePath := filepath.Join(h.config.StaticDir, relative)

	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		return false
	}

	http.ServeFile(w, r, filePath)
	return true
}

// --- helpers ---

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func sseEvent(w http.ResponseWriter, event, data string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}

func intParam(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	if n == 0 {
		return defaultVal
	}
	return n
}

func patternToRegex(pattern string) string {
	parts := strings.Split(pattern, "/")
	var regexParts []string
	for _, part := range parts {
		if strings.HasPrefix(part, ":") {
			regexParts = append(regexParts, "([^/]+)")
		} else if part == "*" {
			regexParts = append(regexParts, "(.*)")
		} else {
			regexParts = append(regexParts, regexp.QuoteMeta(part))
		}
	}
	return strings.Join(regexParts, "/")
}

func extractParamNames(pattern string) []string {
	var names []string
	for _, part := range strings.Split(pattern, "/") {
		if strings.HasPrefix(part, ":") {
			names = append(names, part[1:])
		} else if part == "*" {
			names = append(names, "*")
		}
	}
	return names
}

// Minimal admin HTML templates (inline, no external deps)
const adminLoginHTML = `<!DOCTYPE html><html><head><title>Flop Admin - Login</title><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;min-height:100vh}.card{background:#1e293b;border-radius:12px;padding:2rem;width:100%;max-width:400px}h1{margin-bottom:1.5rem;text-align:center;font-size:1.5rem}input{width:100%;padding:.75rem;border:1px solid #334155;border-radius:6px;background:#0f172a;color:#e2e8f0;margin-bottom:1rem;font-size:1rem}button{width:100%;padding:.75rem;border:none;border-radius:6px;background:#3b82f6;color:white;font-size:1rem;cursor:pointer}button:hover{background:#2563eb}.error{color:#f87171;text-align:center;margin-bottom:1rem;display:none}</style></head><body><div class="card"><h1>Flop Admin</h1><div class="error" id="error"></div><form id="form"><input name="email" type="email" placeholder="Email" required><input name="password" type="password" placeholder="Password" required><button type="submit">Sign in</button></form></div><script>document.getElementById('form').onsubmit=async e=>{e.preventDefault();const d=new FormData(e.target);try{const r=await fetch('/_/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:d.get('email'),password:d.get('password')})});const j=await r.json();if(j.error){document.getElementById('error').textContent=j.error;document.getElementById('error').style.display='block';return}localStorage.setItem('flop_token',j.token);localStorage.setItem('flop_refresh',j.refreshToken);location.href='/_'}catch(err){document.getElementById('error').textContent=err.message;document.getElementById('error').style.display='block'}}</script></body></html>`

const adminSetupHTML = `<!DOCTYPE html><html><head><title>Flop - Setup</title><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;min-height:100vh}.card{background:#1e293b;border-radius:12px;padding:2rem;width:100%;max-width:400px}h1{margin-bottom:.5rem;text-align:center;font-size:1.5rem}p{margin-bottom:1.5rem;text-align:center;color:#94a3b8;font-size:.875rem}input{width:100%;padding:.75rem;border:1px solid #334155;border-radius:6px;background:#0f172a;color:#e2e8f0;margin-bottom:1rem;font-size:1rem}button{width:100%;padding:.75rem;border:none;border-radius:6px;background:#3b82f6;color:white;font-size:1rem;cursor:pointer}button:hover{background:#2563eb}.error{color:#f87171;text-align:center;margin-bottom:1rem;display:none}</style></head><body><div class="card"><h1>Create Admin Account</h1><p>Set up your superadmin account</p><div class="error" id="error"></div><form id="form"><input name="name" placeholder="Name"><input name="email" type="email" placeholder="Email" required><input name="password" type="password" placeholder="Password" required><button type="submit">Create Account</button></form></div><script>document.getElementById('form').onsubmit=async e=>{e.preventDefault();const d=new FormData(e.target);const t=new URLSearchParams(location.search).get('token');try{const r=await fetch('/_/api/setup',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:t,email:d.get('email'),password:d.get('password'),name:d.get('name')})});const j=await r.json();if(j.error){document.getElementById('error').textContent=j.error;document.getElementById('error').style.display='block';return}location.href='/_/login'}catch(err){document.getElementById('error').textContent=err.message;document.getElementById('error').style.display='block'}}</script></body></html>`

const adminPageHTML = `<!DOCTYPE html><html><head><title>Flop Admin</title><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0}nav{background:#1e293b;padding:1rem 2rem;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #334155}nav h1{font-size:1.25rem}nav button{background:#334155;color:#e2e8f0;border:none;padding:.5rem 1rem;border-radius:6px;cursor:pointer}.container{max-width:1200px;margin:2rem auto;padding:0 2rem}.tables{display:grid;gap:1rem}.table-card{background:#1e293b;border-radius:8px;padding:1.5rem;cursor:pointer;transition:background .2s}.table-card:hover{background:#263548}.table-card h2{font-size:1.125rem;margin-bottom:.5rem}.table-card .count{color:#94a3b8;font-size:.875rem}#content{margin-top:2rem}table{width:100%;border-collapse:collapse;margin-top:1rem}th,td{padding:.75rem;text-align:left;border-bottom:1px solid #334155;font-size:.875rem}th{color:#94a3b8;font-weight:600}tr:hover{background:#1e293b}</style></head><body><nav><h1>Flop Admin</h1><button onclick="logout()">Logout</button></nav><div class="container"><div id="app">Loading...</div></div><script>const token=localStorage.getItem('flop_token');if(!token)location.href='/_/login';async function api(path,opts){const r=await fetch(path,{...opts,headers:{...opts?.headers,Authorization:'Bearer '+token}});if(r.status===401){location.href='/_/login';return null}return r.json()}function logout(){localStorage.removeItem('flop_token');localStorage.removeItem('flop_refresh');location.href='/_/login'}async function init(){const data=await api('/_/api/tables');if(!data)return;const app=document.getElementById('app');app.innerHTML='<div class="tables">'+data.tables.map(t=>'<div class="table-card" onclick="showTable(\''+t.name+'\')"><h2>'+t.name+'</h2><div class="count">'+t.rowCount+' rows</div></div>').join('')+'</div><div id="content"></div>'}window.showTable=async function(name){const data=await api('/_/api/tables/'+name+'/rows');if(!data)return;const fields=Object.keys(data.rows[0]||{});document.getElementById('content').innerHTML='<h2>'+name+' ('+data.total+' rows)</h2><table><tr>'+fields.map(f=>'<th>'+f+'</th>').join('')+'</tr>'+data.rows.map(r=>'<tr>'+fields.map(f=>'<td>'+esc(r[f])+'</td>').join('')+'</tr>').join('')+'</table>'}function esc(v){if(v===null||v===undefined)return'<span style="color:#64748b">null</span>';if(typeof v==='object')return JSON.stringify(v).slice(0,100);return String(v).replace(/</g,'&lt;')}init()</script></body></html>`
