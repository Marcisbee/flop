package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/images"
	"github.com/marcisbee/flop/internal/jsonstd"
	"github.com/marcisbee/flop/internal/jsonx"
	"github.com/marcisbee/flop/internal/reqtrace"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port                int
	JWTSecret           string
	StaticDir           string
	RequestLogRetention time.Duration
	EnablePprof         bool
}

// HandlerCaller calls JS view/reducer handlers.
type HandlerCaller interface {
	CallHandler(handlerType, name, paramsJSON, authJSON string) (string, error)
}

// Handler manages all HTTP request routing.
type Handler struct {
	db          *engine.Database
	caller      HandlerCaller
	routes      []RouteInfo
	pageRoutes  []FlatRoute
	authService *AuthService
	mailer      *Mailer
	config      ServerConfig
	setupToken  string

	clientJS  []byte
	clientCSS []byte

	analytics *RequestAnalytics
}

const sseEventBufferSize = 4096
const sseChangeBatchSize = 128
const sseChangeFlushInterval = 25 * time.Millisecond

type sseErrorPayload struct {
	Error string `json:"error"`
}

type sseAdminSnapshotPayload struct {
	TableCounts map[string]int `json:"tableCounts"`
}

// NewHandler creates the main HTTP handler.
func NewHandler(
	db *engine.Database,
	caller HandlerCaller,
	routes []RouteInfo,
	pageRoutes []FlatRoute,
	authService *AuthService,
	mailer *Mailer,
	config ServerConfig,
	setupToken string,
	clientJS, clientCSS []byte,
) *Handler {
	retention := config.RequestLogRetention
	if retention <= 0 {
		retention = DefaultRequestLogRetention
	}
	analyticsPath := filepath.Join(db.GetDataDir(), "_system", "request_logs.ndjson")
	return &Handler{
		db:          db,
		caller:      caller,
		routes:      routes,
		pageRoutes:  pageRoutes,
		authService: authService,
		mailer:      mailer,
		config:      config,
		setupToken:  setupToken,
		clientJS:    clientJS,
		clientCSS:   clientCSS,
		analytics:   NewRequestAnalyticsWithStorage(retention, analyticsPath),
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
		h.handleFileServing(w, r, path)
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
		h.callHandler(w, r, route, string(body), auth, "http")
		return
	}

	if route.Type == "view" {
		params := make(map[string]string)
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
		paramsJSON, _ := jsonx.Marshal(params)
		h.callHandler(w, r, route, string(paramsJSON), auth, "http")
		return
	}

	jsonError(w, "Not found", 404)
}

// handleFileServing serves file assets with optional on-demand thumbnail generation.
// URL: /api/files/{table}/{rowID}/{field}/{filename}?thumb=WxH
func (h *Handler) handleFileServing(w http.ResponseWriter, r *http.Request, path string) {
	// Strip /api/files/ prefix and parse parts
	rel := strings.TrimPrefix(path, "/api/files/")
	parts := strings.SplitN(rel, "/", 4)

	thumbParam := r.URL.Query().Get("thumb")

	// No thumbnail requested — serve original file
	if thumbParam == "" || len(parts) < 4 {
		filePath := filepath.Join(h.db.GetDataDir(), "_files", rel)
		http.ServeFile(w, r, filePath)
		return
	}

	tableName, rowID, fieldName, filename := parts[0], parts[1], parts[2], parts[3]

	// Find the table and field to check allowed thumb sizes
	ti := h.db.GetTable(tableName)
	if ti == nil {
		jsonError(w, "table not found", 404)
		return
	}
	def := ti.GetDef()
	cf := def.CompiledSchema.FieldMap[fieldName]
	if cf == nil || len(cf.ThumbSizes) == 0 {
		jsonError(w, "thumbnails not configured for this field", 400)
		return
	}
	if !images.IsThumbAllowed(thumbParam, cf.ThumbSizes) {
		jsonError(w, "thumb size not allowed: "+thumbParam, 400)
		return
	}

	size, err := images.ParseThumbSize(thumbParam)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}

	thumbPath := images.ThumbPath(h.db.GetDataDir(), tableName, rowID, fieldName, filename, size)

	// Serve cached thumbnail if it exists
	if _, statErr := os.Stat(thumbPath); statErr == nil {
		http.ServeFile(w, r, thumbPath)
		return
	}

	// Generate thumbnail from original
	srcPath := filepath.Join(h.db.GetDataDir(), "_files", tableName, rowID, fieldName, filename)
	if _, statErr := os.Stat(srcPath); os.IsNotExist(statErr) {
		jsonError(w, "file not found", 404)
		return
	}

	if err := images.GenerateThumb(srcPath, thumbPath, size); err != nil {
		jsonError(w, "thumbnail generation failed: "+err.Error(), 500)
		return
	}

	http.ServeFile(w, r, thumbPath)
}

func (h *Handler) callHandler(w http.ResponseWriter, r *http.Request, route *RouteInfo, paramsJSON string, auth *schema.AuthContext, transport string) {
	method := ""
	path := ""
	handlerType := ""
	name := ""
	if route != nil {
		method = route.Method
		path = route.Path
		handlerType = route.Type
		name = route.Name
	}
	if method == "" && r != nil {
		method = r.Method
	}
	if handlerType == "" {
		handlerType = "view"
	}
	if path == "" {
		path = "/api/" + handlerType + "/" + name
	}

	result, err := h.executeHandler(handlerType, name, paramsJSON, auth, method, path, transport, r)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"data":%s}`, result)
}

func (h *Handler) executeHandler(
	handlerType, name, paramsJSON string,
	auth *schema.AuthContext,
	method, path, transport string,
	req *http.Request,
) (string, error) {
	authJSON := "null"
	if auth != nil {
		b, _ := jsonx.Marshal(auth)
		authJSON = string(b)
	}

	start := time.Now()
	traceCollector := reqtrace.Start()
	defer traceCollector.End()
	result, err := h.caller.CallHandler(handlerType, name, paramsJSON, authJSON)
	if h.analytics != nil {
		details := map[string]interface{}{
			"transport":  transport,
			"paramBytes": len(paramsJSON),
			"hasAuth":    auth != nil,
		}
		if req != nil {
			details["queryBytes"] = len(req.URL.RawQuery)
		}
		if spans := traceCollector.Spans(); len(spans) > 0 {
			details["trace"] = spans
			details["traceSpans"] = len(spans)
		}
		h.analytics.Record(AnalyticsEvent{
			Timestamp:    time.Now(),
			RouteType:    handlerType,
			RouteName:    name,
			Method:       method,
			Path:         path,
			Transport:    transport,
			Duration:     time.Since(start),
			OK:           err == nil,
			StatusCode:   ternaryStatus(err == nil, 200, 400),
			ErrorMessage: ternaryError(err),
			UserID:       ternaryUserID(auth),
			Details:      details,
		})
	}
	return result, err
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
		jsonx.NewDecoder(r.Body).Decode(&body)
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

	case "/api/auth/change-password":
		token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
		payload := VerifyJWT(token, h.config.JWTSecret)
		if payload == nil {
			jsonError(w, "Authentication required", 401)
			return
		}
		oldPassword, _ := body["oldPassword"].(string)
		newPassword, _ := body["newPassword"].(string)
		if oldPassword == "" || newPassword == "" {
			jsonError(w, "Old and new password required", 400)
			return
		}
		if err := h.authService.ChangePassword(payload.Sub, oldPassword, newPassword); err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		jsonResponse(w, map[string]interface{}{"ok": true})

	case "/api/auth/request-email-change":
		token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
		payload := VerifyJWT(token, h.config.JWTSecret)
		if payload == nil {
			jsonError(w, "Authentication required", 401)
			return
		}
		newEmail, _ := body["newEmail"].(string)
		password, _ := body["password"].(string)
		if newEmail == "" || password == "" {
			jsonError(w, "New email and password required", 400)
			return
		}
		changeToken, err := h.authService.RequestEmailChange(payload.Sub, newEmail, password)
		if err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		if h.mailer != nil {
			htmlBody, _ := h.mailer.RenderTemplate("email-change", EmailTemplateData{Email: newEmail, Token: changeToken})
			go h.mailer.Send(newEmail, "Confirm email change", htmlBody)
		}
		jsonResponse(w, map[string]interface{}{"ok": true, "token": changeToken})

	case "/api/auth/confirm-email-change":
		changeToken, _ := body["token"].(string)
		if changeToken == "" {
			jsonError(w, "Token required", 400)
			return
		}
		newAuthToken, err := h.authService.ConfirmEmailChange(changeToken)
		if err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		jsonResponse(w, map[string]interface{}{"ok": true, "token": newAuthToken})

	case "/api/auth/request-verification":
		token := ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
		payload := VerifyJWT(token, h.config.JWTSecret)
		if payload == nil {
			jsonError(w, "Authentication required", 401)
			return
		}
		verifyToken, err := h.authService.RequestVerification(payload.Sub)
		if err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		if h.mailer != nil {
			htmlBody, _ := h.mailer.RenderTemplate("verification", EmailTemplateData{Email: payload.Email, Token: verifyToken})
			go h.mailer.Send(payload.Email, "Verify your email", htmlBody)
		}
		jsonResponse(w, map[string]interface{}{"ok": true, "token": verifyToken})

	case "/api/auth/confirm-verification":
		verifyToken, _ := body["token"].(string)
		if verifyToken == "" {
			jsonError(w, "Token required", 400)
			return
		}
		if err := h.authService.ConfirmVerification(verifyToken); err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		jsonResponse(w, map[string]interface{}{"ok": true})

	case "/api/auth/request-password-reset":
		email, _ := body["email"].(string)
		if email == "" {
			jsonError(w, "Email required", 400)
			return
		}
		resetToken, _ := h.authService.RequestPasswordReset(email)
		if resetToken != "" && h.mailer != nil {
			htmlBody, _ := h.mailer.RenderTemplate("password-reset", EmailTemplateData{Email: email, Token: resetToken})
			go h.mailer.Send(email, "Reset your password", htmlBody)
		}
		// Always return ok to prevent user enumeration
		resp := map[string]interface{}{"ok": true}
		if resetToken != "" {
			resp["token"] = resetToken
		}
		jsonResponse(w, resp)

	case "/api/auth/confirm-password-reset":
		resetToken, _ := body["token"].(string)
		newPassword, _ := body["password"].(string)
		if resetToken == "" || newPassword == "" {
			jsonError(w, "Token and password required", 400)
			return
		}
		if err := h.authService.ConfirmPasswordReset(resetToken, newPassword); err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
		jsonResponse(w, map[string]interface{}{"ok": true})

	default:
		jsonError(w, "Unknown auth endpoint", 404)
	}
}

func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, route *RouteInfo, auth *schema.AuthContext) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "SSE not supported", 400)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	writeEvent := func(eventType, payload string) {
		sseEventString(w, eventType, payload)
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
	paramsJSON, _ := jsonx.Marshal(params)

	result, err := h.executeHandler("view", route.Name, string(paramsJSON), auth, "GET", route.Path, "sse", r)
	if err != nil {
		writeEvent("error", marshalSSEError(err))
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
				data, _ := jsonstd.Marshal(event)
				sseEventBytes(w, "change", data)
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
		sseEventString(w, eventType, payload)
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
		paramsJSON, _ := jsonx.Marshal(params)
		scheduled++

		routePath := route.Path
		go func(viewName, pJSON, p string) {
			res, err := h.executeHandler("view", viewName, pJSON, auth, "GET", p, "sse-multiplex", r)
			select {
			case results <- snapshotResult{name: viewName, result: res, err: err}:
			case <-r.Context().Done():
			}
		}(name, string(paramsJSON), routePath)
	}

	for i := 0; i < scheduled; i++ {
		select {
		case <-r.Context().Done():
			return
		case out := <-results:
			if out.err != nil {
				writeEvent(fmt.Sprintf("error:%s", out.name), marshalSSEError(out.err))
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
				data, _ := jsonstd.Marshal(event)
				sseEventBytes(w, "change", data)
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

		routeData, _ := jsonx.Marshal(map[string]interface{}{
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
		w.Write([]byte(AdminSetupHTML))
		return
	}

	// Setup API
	if path == "/_/api/setup" && r.Method == "POST" {
		if h.authService == nil || h.setupToken == "" {
			jsonError(w, "Setup not available", 400)
			return
		}
		var body map[string]interface{}
		jsonx.NewDecoder(r.Body).Decode(&body)
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
		if email == "" || password == "" {
			jsonError(w, "Email and password required", 400)
			return
		}
		_, _, err := h.authService.RegisterSuperadmin(email, password, nil)
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
		w.Write([]byte(AdminLoginHTML))
		return
	}

	// Login API
	if path == "/_/api/login" && r.Method == "POST" {
		if h.authService == nil {
			jsonError(w, "Auth not configured", 400)
			return
		}
		var body map[string]interface{}
		jsonx.NewDecoder(r.Body).Decode(&body)
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
		jsonx.NewDecoder(r.Body).Decode(&body)
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
		w.Write([]byte(AdminPageHTML))
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

	// pprof endpoints (superadmin only)
	if strings.HasPrefix(path, "/_/debug/pprof") {
		if !h.config.EnablePprof {
			jsonError(w, "Not found", 404)
			return
		}
		ServePrefixedPprof("/_/debug/pprof", w, r)
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

	// Analytics config
	if path == "/_/api/analytics/config" {
		if r.Method == "GET" {
			h.handleAnalyticsConfig(w)
			return
		}
		jsonError(w, "Method not allowed", 405)
		return
	}

	// Analytics logs
	if path == "/_/api/analytics/logs" && r.Method == "GET" {
		h.handleAnalyticsLogs(w, r)
		return
	}

	// Analytics timeseries
	if path == "/_/api/analytics/timeseries" && r.Method == "GET" {
		h.handleAnalyticsTimeseries(w, r)
		return
	}

	// Runtime stats
	if path == "/_/api/analytics/runtime" && r.Method == "GET" {
		h.handleAnalyticsRuntime(w)
		return
	}

	// Index stats
	if path == "/_/api/analytics/indexes" && r.Method == "GET" {
		h.handleAnalyticsIndexes(w)
		return
	}

	if path == "/_/api/media" && r.Method == "GET" {
		h.handleListMedia(w, r)
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
				jsonx.NewDecoder(r.Body).Decode(&body)
				h.handleCreateRow(w, match[1], body)
				return
			}
		}
	}

	// File upload/delete: /_/api/tables/{table}/rows/{id}/files/{field}
	if re := regexp.MustCompile(`^/_/api/tables/([^/]+)/rows/([^/]+)/files/([^/]+)$`); true {
		match := re.FindStringSubmatch(path)
		if match != nil {
			switch r.Method {
			case "POST":
				h.handleFileUpload(w, r, match[1], match[2], match[3])
				return
			case "DELETE":
				h.handleFileDelete(w, r, match[1], match[2], match[3])
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
				jsonx.NewDecoder(r.Body).Decode(&body)
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
			// Preserve API shape with explicit message until restore/download is wired for pure-go runtime.
			jsonError(w, "Not implemented yet", 501)
			return
		}
		if r.Method == "POST" {
			jsonError(w, "Not implemented yet", 501)
			return
		}
	}

	jsonError(w, "Not found", 404)
}

func (h *Handler) handleListTables(w http.ResponseWriter) {
	type tableMeta struct {
		Name     string           `json:"name"`
		Schema   jsonx.RawMessage `json:"schema"`
		RowCount int              `json:"rowCount"`
	}

	names := make([]string, 0, len(h.db.Tables))
	for name := range h.db.Tables {
		names = append(names, name)
	}
	sort.Strings(names)

	tables := make([]tableMeta, 0, len(names))
	for _, name := range names {
		table := h.db.Tables[name]
		def := table.GetDef()
		orderedSchema, err := marshalOrderedSchema(def.CompiledSchema)
		if err != nil {
			jsonError(w, err.Error(), 500)
			return
		}
		tables = append(tables, tableMeta{
			Name:     name,
			Schema:   orderedSchema,
			RowCount: table.Count(),
		})
	}
	jsonResponse(w, map[string]interface{}{"tables": tables})
}

type adminMediaUsage struct {
	TableName string `json:"tableName"`
	RowID     string `json:"rowId"`
	FieldName string `json:"fieldName"`
	Multi     bool   `json:"multi"`
}

type adminMediaItem struct {
	Path       string            `json:"path"`
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	Mime       string            `json:"mime"`
	RefSize    int64             `json:"refSize"`
	DiskSize   int64             `json:"diskSize"`
	ThumbCount int               `json:"thumbCount"`
	ThumbBytes int64             `json:"thumbBytes"`
	Width      int               `json:"width,omitempty"`
	Height     int               `json:"height,omitempty"`
	Orphaned   bool              `json:"orphaned"`
	Usages     []adminMediaUsage `json:"usages"`
}

func (h *Handler) handleListMedia(w http.ResponseWriter, r *http.Request) {
	items, err := h.buildMediaInventory()
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	orphanOnly := r.URL.Query().Get("orphans") == "1"
	if search != "" || orphanOnly {
		filtered := items[:0]
		for _, item := range items {
			if orphanOnly && !item.Orphaned {
				continue
			}
			if search != "" {
				haystack := strings.ToLower(strings.Join([]string{
					item.Path,
					item.Name,
					item.URL,
					item.Mime,
				}, "\n"))
				for _, usage := range item.Usages {
					haystack += "\n" + strings.ToLower(usage.TableName) + "\n" + strings.ToLower(usage.RowID) + "\n" + strings.ToLower(usage.FieldName)
				}
				if !strings.Contains(haystack, search) {
					continue
				}
			}
			filtered = append(filtered, item)
		}
		items = filtered
	}

	jsonResponse(w, map[string]interface{}{"items": items})
}

func (h *Handler) buildMediaInventory() ([]*adminMediaItem, error) {
	dataDir := h.db.GetDataDir()
	itemsByPath := map[string]*adminMediaItem{}

	tableNames := make([]string, 0, len(h.db.Tables))
	for name := range h.db.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		table := h.db.Tables[tableName]
		if table == nil {
			continue
		}
		def := table.GetDef()
		fileFields := make([]schema.CompiledField, 0, len(def.CompiledSchema.Fields))
		for _, field := range def.CompiledSchema.Fields {
			if field.Kind == schema.KindFileSingle || field.Kind == schema.KindFileMulti {
				fileFields = append(fileFields, field)
			}
		}
		if len(fileFields) == 0 {
			continue
		}

		rows, err := table.Scan(1_000_000, 0)
		if err != nil {
			return nil, fmt.Errorf("scan media rows for %s: %w", tableName, err)
		}
		pkField := def.CompiledSchema.Fields[0].Name
		for _, row := range rows {
			rowID := fmt.Sprint(row[pkField])
			for _, field := range fileFields {
				refs := mediaRefsFromValue(row[field.Name], field.Kind)
				for _, ref := range refs {
					item := ensureMediaItem(itemsByPath, dataDir, ref)
					if item == nil {
						continue
					}
					item.Usages = append(item.Usages, adminMediaUsage{
						TableName: tableName,
						RowID:     rowID,
						FieldName: field.Name,
						Multi:     field.Kind == schema.KindFileMulti,
					})
				}
			}
		}
	}

	scanMediaFilesOnDisk(dataDir, itemsByPath)

	items := make([]*adminMediaItem, 0, len(itemsByPath))
	for _, item := range itemsByPath {
		if len(item.Usages) == 0 {
			item.Orphaned = true
		}
		sort.Slice(item.Usages, func(i, j int) bool {
			if item.Usages[i].TableName != item.Usages[j].TableName {
				return item.Usages[i].TableName < item.Usages[j].TableName
			}
			if item.Usages[i].RowID != item.Usages[j].RowID {
				return item.Usages[i].RowID < item.Usages[j].RowID
			}
			return item.Usages[i].FieldName < item.Usages[j].FieldName
		})
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Orphaned != items[j].Orphaned {
			return !items[i].Orphaned && items[j].Orphaned
		}
		return items[i].Path < items[j].Path
	})
	return items, nil
}

func scanMediaFilesOnDisk(dataDir string, itemsByPath map[string]*adminMediaItem) {
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, "_files/") && !strings.Contains(rel, "/_files/") {
			return nil
		}
		if idx := strings.Index(rel, "_files/"); idx > 0 {
			rel = rel[idx:]
		}
		if _, exists := itemsByPath[rel]; exists {
			return nil
		}
		ref := schema.FileRef{
			Path: rel,
			Name: filepath.Base(path),
			URL:  "/api/files/" + strings.TrimPrefix(filepath.ToSlash(rel), "_files/"),
			Size: info.Size(),
			Mime: storage.MimeFromExtension(path),
		}
		item := ensureMediaItem(itemsByPath, dataDir, ref)
		if item != nil {
			item.Orphaned = true
		}
		return nil
	})
}

func ensureMediaItem(items map[string]*adminMediaItem, dataDir string, ref schema.FileRef) *adminMediaItem {
	ref.Path = strings.TrimSpace(ref.Path)
	if ref.Path == "" {
		return nil
	}
	if item := items[ref.Path]; item != nil {
		if item.Name == "" {
			item.Name = ref.Name
		}
		if item.URL == "" {
			item.URL = ref.URL
		}
		if item.Mime == "" {
			item.Mime = ref.Mime
		}
		if item.RefSize == 0 {
			item.RefSize = ref.Size
		}
		return item
	}

	item := &adminMediaItem{
		Path:    ref.Path,
		Name:    ref.Name,
		URL:     ref.URL,
		Mime:    ref.Mime,
		RefSize: ref.Size,
	}
	fillMediaItemDetails(item, dataDir)
	items[ref.Path] = item
	return item
}

func fillMediaItemDetails(item *adminMediaItem, dataDir string) {
	fullPath := filepath.Join(dataDir, filepath.FromSlash(item.Path))
	if stat, err := os.Stat(fullPath); err == nil {
		item.DiskSize = stat.Size()
	}
	if strings.HasPrefix(item.Mime, "image/") || looksLikeImagePath(item.Path) {
		if w, h, err := images.ReadDimensions(fullPath); err == nil {
			item.Width = w
			item.Height = h
		}
	}

	parts := strings.Split(strings.TrimPrefix(item.Path, "_files/"), "/")
	if len(parts) != 4 {
		return
	}
	thumbDir := filepath.Join(dataDir, "_thumbs", parts[0], parts[1], parts[2])
	matches, err := filepath.Glob(filepath.Join(thumbDir, "*_"+parts[3]))
	if err != nil {
		return
	}
	item.ThumbCount = len(matches)
	for _, match := range matches {
		if stat, err := os.Stat(match); err == nil {
			item.ThumbBytes += stat.Size()
		}
	}
}

func mediaRefsFromValue(value interface{}, kind schema.FieldKind) []schema.FileRef {
	switch kind {
	case schema.KindFileSingle:
		if ref, ok := mediaRefFromAny(value); ok {
			return []schema.FileRef{ref}
		}
	case schema.KindFileMulti:
		rv := reflect.ValueOf(value)
		if !rv.IsValid() || rv.Kind() != reflect.Slice {
			return nil
		}
		refs := make([]schema.FileRef, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			item := rv.Index(i).Interface()
			if ref, ok := mediaRefFromAny(item); ok {
				refs = append(refs, ref)
			}
		}
		return refs
	}
	return nil
}

func mediaRefFromAny(value interface{}) (schema.FileRef, bool) {
	switch v := value.(type) {
	case map[string]interface{}:
		ref := schema.FileRef{
			Path: stringValue(v["path"]),
			Name: stringValue(v["name"]),
			URL:  stringValue(v["url"]),
			Mime: stringValue(v["mime"]),
			Size: int64Value(v["size"]),
		}
		if ref.Path == "" {
			return schema.FileRef{}, false
		}
		return ref, true
	case map[string]string:
		ref := schema.FileRef{
			Path: v["path"],
			Name: v["name"],
			URL:  v["url"],
			Mime: v["mime"],
		}
		if ref.Path == "" {
			return schema.FileRef{}, false
		}
		return ref, true
	case schema.FileRef:
		if strings.TrimSpace(v.Path) == "" {
			return schema.FileRef{}, false
		}
		return v, true
	case *schema.FileRef:
		if v == nil || strings.TrimSpace(v.Path) == "" {
			return schema.FileRef{}, false
		}
		return *v, true
	case string:
		path := strings.TrimSpace(v)
		if path == "" || !strings.HasPrefix(path, "_files/") {
			return schema.FileRef{}, false
		}
		return schema.FileRef{
			Path: path,
			Name: filepath.Base(path),
			URL:  "/api/files/" + strings.TrimPrefix(path, "_files/"),
			Mime: storage.MimeFromExtension(path),
		}, true
	default:
		return schema.FileRef{}, false
	}
}

func stringValue(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func int64Value(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func looksLikeImagePath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp":
		return true
	default:
		return false
	}
}

func marshalOrderedSchema(cs *schema.CompiledSchema) (jsonx.RawMessage, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range cs.Fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := jsonx.Marshal(f.Name)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')

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
		if f.MaxUploadBytes > 0 {
			entry["maxUploadBytes"] = f.MaxUploadBytes
		}
		if f.ImageMaxSize != "" {
			entry["imageMaxSize"] = f.ImageMaxSize
		}
		if f.ImageFit != "" {
			entry["imageFit"] = f.ImageFit
		}
		if f.DiscardOriginal {
			entry["discardOriginal"] = true
		}
		val, err := jsonx.Marshal(entry)
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}
	buf.WriteByte('}')
	return jsonx.RawMessage(buf.Bytes()), nil
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
	filter := q.Get("filter")
	offset := (page - 1) * limit

	rows, err := table.Scan(10000, 0)
	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"rows":  []interface{}{},
			"total": 0,
			"page":  page,
			"pages": 0,
			"limit": limit,
			"busy":  true,
		})
		return
	}

	// Stable ordering by primary key to match TS admin behavior.
	if len(rows) > 0 {
		pkField := table.GetDef().CompiledSchema.Fields[0]
		pk := pkField.Name
		sort.Slice(rows, func(i, j int) bool {
			return adminValueLess(rows[i][pk], rows[j][pk], pkField.Kind)
		})
	}

	// Filter or search
	if filter != "" {
		matchFn, err := ParseAndEvalFilter(filter)
		if err != nil {
			jsonError(w, "Invalid filter: "+err.Error(), 400)
			return
		}
		var filtered []map[string]interface{}
		for _, row := range rows {
			if matchFn(row) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	} else if search != "" {
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

func (h *Handler) handleFileUpload(w http.ResponseWriter, r *http.Request, tableName, rowID, fieldName string) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	field := table.GetDef().CompiledSchema.FieldMap[fieldName]
	if field == nil || (field.Kind != schema.KindFileSingle && field.Kind != schema.KindFileMulti) {
		jsonError(w, "Field is not a file field", 400)
		return
	}

	row, err := table.Get(rowID)
	if err != nil || row == nil {
		jsonError(w, "Row not found", 404)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		jsonError(w, "Expected multipart/form-data", 400)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		jsonError(w, "No file provided", 400)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		jsonError(w, "Failed to read file", 400)
		return
	}

	mime := header.Header.Get("Content-Type")
	if mime == "" {
		mime = storage.MimeFromExtension(header.Filename)
	}
	ref, err := storage.StoreFileWithField(h.db.GetDataDir(), tableName, rowID, fieldName, header.Filename, data, mime, field)
	if err != nil {
		jsonError(w, err.Error(), 400)
		return
	}

	refMap := map[string]interface{}{
		"path": ref.Path,
		"name": ref.Name,
		"size": ref.Size,
		"mime": ref.Mime,
		"url":  ref.URL,
	}
	if field.Kind == schema.KindFileSingle {
		if _, err := table.Update(rowID, map[string]interface{}{fieldName: refMap}, nil); err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
	} else {
		var existing []interface{}
		if cur, ok := row[fieldName].([]interface{}); ok {
			existing = append(existing, cur...)
		}
		existing = append(existing, refMap)
		if _, err := table.Update(rowID, map[string]interface{}{fieldName: existing}, nil); err != nil {
			jsonError(w, err.Error(), 400)
			return
		}
	}

	jsonResponse(w, map[string]interface{}{"ok": true, "file": refMap})
}

func (h *Handler) handleFileDelete(w http.ResponseWriter, r *http.Request, tableName, rowID, fieldName string) {
	table := h.db.GetTable(tableName)
	if table == nil {
		jsonError(w, "Table not found", 404)
		return
	}
	field := table.GetDef().CompiledSchema.FieldMap[fieldName]
	if field == nil || (field.Kind != schema.KindFileSingle && field.Kind != schema.KindFileMulti) {
		jsonError(w, "Field is not a file field", 400)
		return
	}
	row, err := table.Get(rowID)
	if err != nil || row == nil {
		jsonError(w, "Row not found", 404)
		return
	}

	if field.Kind == schema.KindFileMulti {
		targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
		if targetPath != "" {
			items, _ := row[fieldName].([]interface{})
			next := make([]interface{}, 0, len(items))
			removed := false
			for _, item := range items {
				refMap, ok := item.(map[string]interface{})
				if !ok {
					next = append(next, item)
					continue
				}
				path, _ := refMap["path"].(string)
				if !removed && path == targetPath {
					removed = true
					_ = storage.DeleteFileRef(h.db.GetDataDir(), &schema.FileRef{Path: path})
					continue
				}
				next = append(next, item)
			}
			if !removed {
				jsonError(w, "File not found", 404)
				return
			}
			if _, err := table.Update(rowID, map[string]interface{}{fieldName: next}, nil); err != nil {
				jsonError(w, err.Error(), 400)
				return
			}
			jsonResponse(w, map[string]interface{}{"ok": true})
			return
		}
	}

	// Best effort cleanup for the field directory and any precomputed thumbs.
	_ = os.RemoveAll(filepath.Join(h.db.GetDataDir(), "_files", tableName, rowID, fieldName))
	_ = os.RemoveAll(filepath.Join(h.db.GetDataDir(), "_thumbs", tableName, rowID, fieldName))

	update := map[string]interface{}{fieldName: nil}
	if field.Kind == schema.KindFileMulti {
		update[fieldName] = []interface{}{}
	}
	if _, err := table.Update(rowID, update, nil); err != nil {
		jsonError(w, err.Error(), 400)
		return
	}

	jsonResponse(w, map[string]interface{}{"ok": true})
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
	data, _ := jsonstd.Marshal(sseAdminSnapshotPayload{TableCounts: tableCounts})
	sseEventBytes(w, "snapshot", data)
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
				data, _ := jsonstd.Marshal(event)
				sseEventBytes(w, "change", data)
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

func (h *Handler) handleAnalyticsConfig(w http.ResponseWriter) {
	if h.analytics == nil {
		jsonError(w, "Analytics unavailable", 501)
		return
	}
	retention := h.analytics.Retention()
	jsonResponse(w, map[string]interface{}{
		"ok":             true,
		"enabled":        true,
		"retentionHours": retention.Hours(),
		"droppedEvents":  h.analytics.DroppedEvents(),
	})
}

func (h *Handler) handleAnalyticsLogs(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		jsonError(w, "Analytics unavailable", 501)
		return
	}

	q := r.URL.Query()
	page := intParam(q.Get("page"), 1)
	limit := intParam(q.Get("limit"), 50)
	if limit < 1 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	rows, total, err := h.analytics.QueryLogs(page, limit, q.Get("search"), q.Get("filter"))
	if err != nil {
		jsonError(w, "Invalid filter: "+err.Error(), 400)
		return
	}
	pages := 0
	if total > 0 {
		pages = (total + limit - 1) / limit
	}

	jsonResponse(w, map[string]interface{}{
		"rows":           rows,
		"total":          total,
		"page":           page,
		"pages":          pages,
		"limit":          limit,
		"retentionHours": h.analytics.Retention().Hours(),
		"droppedEvents":  h.analytics.DroppedEvents(),
	})
}

func (h *Handler) handleAnalyticsTimeseries(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		jsonError(w, "Analytics unavailable", 501)
		return
	}
	q := r.URL.Query()
	window := parseWindowDuration(q.Get("window"), 24*time.Hour)
	if window < time.Minute {
		window = time.Minute
	}
	series := h.analytics.QuerySeries(window, strings.TrimSpace(q.Get("routeType")), strings.TrimSpace(q.Get("routeName")))
	jsonResponse(w, map[string]interface{}{
		"ok":     true,
		"window": window.String(),
		"data":   series,
	})
}

func (h *Handler) handleAnalyticsRuntime(w http.ResponseWriter) {
	jsonResponse(w, map[string]interface{}{
		"ok":   true,
		"data": RuntimeStatsSnapshot(),
	})
}

func (h *Handler) handleAnalyticsIndexes(w http.ResponseWriter) {
	jsonResponse(w, map[string]interface{}{
		"ok":   true,
		"data": h.db.IndexStatsReport(),
	})
}

func (h *Handler) findRoute(path string) *RouteInfo {
	for i := range h.routes {
		if h.routes[i].Path == path {
			return &h.routes[i]
		}
	}
	return nil
}

func (h *Handler) enforceAccess(w http.ResponseWriter, r *http.Request, route *RouteInfo) (*schema.AuthContext, bool) {
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
	jsonx.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	jsonx.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func marshalSSEError(err error) string {
	if err == nil {
		return `{"error":"unknown"}`
	}
	b, mErr := jsonstd.Marshal(sseErrorPayload{Error: err.Error()})
	if mErr != nil {
		return `{"error":"marshal failed"}`
	}
	return string(b)
}

func sseEventString(w http.ResponseWriter, event, data string) {
	io.WriteString(w, "event: ")
	io.WriteString(w, event)
	io.WriteString(w, "\ndata: ")
	io.WriteString(w, data)
	io.WriteString(w, "\n\n")
}

func sseEventBytes(w http.ResponseWriter, event string, data []byte) {
	io.WriteString(w, "event: ")
	io.WriteString(w, event)
	io.WriteString(w, "\ndata: ")
	w.Write(data)
	io.WriteString(w, "\n\n")
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

func adminValueLess(a, b interface{}, kind schema.FieldKind) bool {
	switch kind {
	case schema.KindNumber, schema.KindInteger, schema.KindTimestamp:
		an, aok := adminToFloat(a)
		bn, bok := adminToFloat(b)
		if aok && bok {
			if an == bn {
				return fmt.Sprint(a) < fmt.Sprint(b)
			}
			return an < bn
		}
		if aok {
			return true
		}
		if bok {
			return false
		}
	}
	return fmt.Sprint(a) < fmt.Sprint(b)
}

func adminToFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	}
	return 0, false
}

func parseWindowDuration(raw string, fallback time.Duration) time.Duration {
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

func ternaryStatus(ok bool, success, failure int) int {
	if ok {
		return success
	}
	return failure
}

func ternaryError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func ternaryUserID(auth *schema.AuthContext) string {
	if auth == nil {
		return ""
	}
	return auth.ID
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
