package flop

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Server is the HTTP server that exposes views as GET and reducers as POST.
type Server struct {
	db         *DB
	views      map[string]*ViewDef
	reducers   map[string]*ReducerDef
	sseHub     *SSEHub
	mux        *http.ServeMux
	authFunc   func(r *http.Request) (any, error)
	authMgr    *AuthManager
	middleware []func(http.Handler) http.Handler
	adminCfg   *AdminConfig
}

// SSEHub manages Server-Sent Events subscriptions.
type SSEHub struct {
	mu          sync.RWMutex
	subscribers map[string]map[*SSEClient]bool // viewName -> clients
}

type SSEClient struct {
	ch     chan []byte
	done   chan struct{}
	view   string
	params map[string]string
	auth   any
}

func NewSSEHub() *SSEHub {
	return &SSEHub{
		subscribers: make(map[string]map[*SSEClient]bool),
	}
}

func (h *SSEHub) Subscribe(viewName string, client *SSEClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers[viewName] == nil {
		h.subscribers[viewName] = make(map[*SSEClient]bool)
	}
	h.subscribers[viewName][client] = true
}

func (h *SSEHub) Unsubscribe(viewName string, client *SSEClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.subscribers[viewName]; ok {
		delete(subs, client)
		if len(subs) == 0 {
			delete(h.subscribers, viewName)
		}
	}
}

// Notify sends data to all subscribers of a view.
func (h *SSEHub) Notify(viewName string, data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if subs, ok := h.subscribers[viewName]; ok {
		for client := range subs {
			select {
			case client.ch <- data:
			default:
				// Client too slow, skip
			}
		}
	}
}

// NotifyAll notifies all view subscribers (used after writes).
func (h *SSEHub) NotifyAll(db *DB, affectedTable string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for viewName, subs := range h.subscribers {
		if len(subs) == 0 {
			continue
		}
		// Check if any subscriber's view uses this table
		for client := range subs {
			_ = client // In a real implementation, we'd re-run the view
			// and compute diffs per subscriber
			break
		}
		_ = viewName
	}
}

func NewServer(db *DB) *Server {
	s := &Server{
		db:       db,
		views:    make(map[string]*ViewDef),
		reducers: make(map[string]*ReducerDef),
		sseHub:   NewSSEHub(),
		mux:      http.NewServeMux(),
	}
	// Auto-register batch view endpoint
	s.RegisterBatchEndpoint()
	// Admin panel is auto-mounted on ListenAndServe (after auth is configured)
	return s
}

// SetAuth configures the authentication function.
func (s *Server) SetAuth(fn func(r *http.Request) (any, error)) {
	s.authFunc = fn
}

// SetAuthManager configures both auth function and stores the manager for admin panel.
func (s *Server) SetAuthManager(am *AuthManager) {
	s.authMgr = am
	s.authFunc = am.Authenticate
}

// RegisterAuthRoutes registers built-in /api/auth/* routes (login, register, refresh, logout).
// Call this after SetAuthManager if you want the default auth endpoints.
// Skip this if you register custom auth routes on the server mux.
func (s *Server) RegisterAuthRoutes() {
	if s.authMgr == nil {
		return
	}
	s.registerAuthRoutes(s.authMgr)
}

// registerAuthRoutes sets up the built-in auth endpoints.
func (s *Server) registerAuthRoutes(am *AuthManager) {
	s.mux.HandleFunc("POST /api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSONError(w, "invalid request body", http.StatusBadRequest)
			return
		}

		session, err := am.Login(input.Email, input.Password)
		if err != nil {
			writeJSONError(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		user, _ := s.db.Table(am.config.TableName).Get(session.UserID)
		var userData any
		if user != nil {
			delete(user.Data, am.config.PasswordField)
			userData = user
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"user":         userData,
			"token":        session.Token,
			"refreshToken": session.RefreshToken,
		})
	})

	s.mux.HandleFunc("POST /api/auth/register", func(w http.ResponseWriter, r *http.Request) {
		var input map[string]any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSONError(w, "invalid request body", http.StatusBadRequest)
			return
		}

		email, _ := input[am.config.EmailField].(string)
		password, _ := input[am.config.PasswordField].(string)
		if email == "" || password == "" {
			writeJSONError(w, "email and password required", http.StatusBadRequest)
			return
		}

		// Extra fields (everything except email/password)
		extra := make(map[string]any)
		for k, v := range input {
			if k != am.config.EmailField && k != am.config.PasswordField {
				extra[k] = v
			}
		}

		user, err := am.Register(email, password, extra)
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusConflict)
			return
		}

		session, err := am.Login(email, password)
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		delete(user.Data, am.config.PasswordField)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"user":         user,
			"token":        session.Token,
			"refreshToken": session.RefreshToken,
		})
	})

	s.mux.HandleFunc("POST /api/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSONError(w, "invalid request body", http.StatusBadRequest)
			return
		}

		session, err := am.Refresh(input.RefreshToken)
		if err != nil {
			writeJSONError(w, "invalid refresh token", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"token": session.Token,
		})
	})

	s.mux.HandleFunc("POST /api/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		token = strings.TrimPrefix(token, "Bearer ")
		if token != "" {
			am.Logout(token)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
}

func writeJSONError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

// Use adds middleware.
func (s *Server) Use(mw func(http.Handler) http.Handler) {
	s.middleware = append(s.middleware, mw)
}

// RegisterView registers a view as a GET endpoint.
func (s *Server) RegisterView(path string, v *ViewDef) {
	s.views[v.Name] = v

	s.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		// Clone view def with request params
		vCopy := *v

		// Parse query params into filters
		for _, f := range v.Filters {
			if qv := r.URL.Query().Get(f.Field); qv != "" {
				// Override filter value from query
				for i := range vCopy.Filters {
					if vCopy.Filters[i].Field == f.Field {
						vCopy.Filters[i].Value = parseQueryValue(qv)
					}
				}
			}
		}

		// Handle URL path params
		for _, f := range v.Filters {
			if pv := r.PathValue(f.Field); pv != "" {
				for i := range vCopy.Filters {
					if vCopy.Filters[i].Field == f.Field {
						vCopy.Filters[i].Value = parseQueryValue(pv)
					}
				}
			}
		}

		// Limit/offset from query
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil {
				vCopy.Limit = n
			}
		}
		if o := r.URL.Query().Get("offset"); o != "" {
			if n, err := strconv.Atoi(o); err == nil {
				vCopy.Offset = n
			}
		}
		if q := r.URL.Query().Get("q"); q != "" {
			vCopy.SearchQ = q
		}

		// Auth
		var auth any
		if s.authFunc != nil {
			var err error
			auth, err = s.authFunc(r)
			if err != nil {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		if vCopy.PermFilter != nil {
			origFilter := vCopy.PermFilter
			vCopy.PermFilter = func(a any, row *Row) bool {
				return origFilter(auth, row)
			}
		}

		result, err := s.db.ExecuteView(&vCopy)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data":  result.Rows,
			"total": result.Total,
		})
	})

	// SSE endpoint (same path + /live)
	ssePath := strings.TrimSuffix(path, "/") + "/live"
	s.mux.HandleFunc("GET "+ssePath, func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		var auth any
		if s.authFunc != nil {
			var err error
			auth, err = s.authFunc(r)
			if err != nil {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		client := &SSEClient{
			ch:   make(chan []byte, 64),
			done: make(chan struct{}),
			view: v.Name,
			auth: auth,
		}

		s.sseHub.Subscribe(v.Name, client)
		defer s.sseHub.Unsubscribe(v.Name, client)

		// Send initial data
		vCopy := *v
		if vCopy.PermFilter != nil {
			origFilter := vCopy.PermFilter
			vCopy.PermFilter = func(a any, row *Row) bool {
				return origFilter(auth, row)
			}
		}
		result, err := s.db.ExecuteView(&vCopy)
		if err == nil {
			data, _ := json.Marshal(map[string]any{
				"type":  "snapshot",
				"data":  result.Rows,
				"total": result.Total,
			})
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-client.ch:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush()
			}
		}
	})
}

// RegisterReducer registers a reducer as a POST endpoint.
func (s *Server) RegisterReducer(path string, r *ReducerDef) {
	s.reducers[r.Name] = r

	s.mux.HandleFunc("POST "+path, func(w http.ResponseWriter, req *http.Request) {
		var data map[string]any
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		// Merge path params
		for _, f := range s.db.schemas[r.Table].Fields {
			if pv := req.PathValue(f.Name); pv != "" {
				data[f.Name] = parseQueryValue(pv)
			}
		}

		// Auth
		var auth any
		if s.authFunc != nil {
			var err error
			auth, err = s.authFunc(req)
			if err != nil {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		result, err := s.db.ExecuteReducer(r, data, auth)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "permission denied") {
				status = http.StatusForbidden
			} else if strings.Contains(err.Error(), "validation") {
				status = http.StatusBadRequest
			} else if strings.Contains(err.Error(), "unique constraint") {
				status = http.StatusConflict
			} else if strings.Contains(err.Error(), "not found") {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), status)
			return
		}

		// Notify SSE subscribers
		go func() {
			for viewName := range s.views {
				vDef := s.views[viewName]
				if vDef.Table == r.Table {
					// Re-execute view and push to subscribers
					viewResult, err := s.db.ExecuteView(vDef)
					if err == nil {
						data, _ := json.Marshal(map[string]any{
							"type":  "update",
							"data":  viewResult.Rows,
							"total": viewResult.Total,
						})
						s.sseHub.Notify(viewName, data)
					}
				}
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}

// RegisterBatchEndpoint registers a POST /api/view/_batch endpoint
// that executes multiple views in a single request.
func (s *Server) RegisterBatchEndpoint() {
	s.mux.HandleFunc("POST /api/view/_batch", func(w http.ResponseWriter, r *http.Request) {
		type batchCall struct {
			ID     string         `json:"id"`
			Name   string         `json:"name"`
			Params map[string]any `json:"params"`
		}
		type batchReq struct {
			Calls []batchCall `json:"calls"`
		}
		type batchResult struct {
			ID    string `json:"id"`
			Data  any    `json:"data,omitempty"`
			Total int    `json:"total,omitempty"`
			Error string `json:"error,omitempty"`
		}

		var req batchReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}

		results := make([]batchResult, 0, len(req.Calls))
		for _, call := range req.Calls {
			v, ok := s.views[call.Name]
			if !ok {
				results = append(results, batchResult{ID: call.ID, Error: "view not found: " + call.Name})
				continue
			}

			vCopy := *v
			// Apply params
			if l, ok := call.Params["limit"]; ok {
				if n, ok := l.(float64); ok {
					vCopy.Limit = int(n)
				}
			}
			if o, ok := call.Params["offset"]; ok {
				if n, ok := o.(float64); ok {
					vCopy.Offset = int(n)
				}
			}
			if q, ok := call.Params["q"]; ok {
				if s, ok := q.(string); ok {
					vCopy.SearchQ = s
				}
			}
			// Apply filter params
			for i := range vCopy.Filters {
				if pv, ok := call.Params[vCopy.Filters[i].Field]; ok {
					vCopy.Filters[i].Value = pv
				}
			}

			result, err := s.db.ExecuteView(&vCopy)
			if err != nil {
				results = append(results, batchResult{ID: call.ID, Error: err.Error()})
				continue
			}
			results = append(results, batchResult{ID: call.ID, Data: result.Rows, Total: result.Total})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "results": results})
	})
}

// Mux returns the underlying http.ServeMux for direct route registration.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// HandleFunc registers a handler function on the server's mux.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, handler)
}

// Handle registers a handler on the server's mux.
func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

// ListenAndServe starts the HTTP server.
// Admin panel is auto-mounted at /_/ before starting.
func (s *Server) ListenAndServe(addr string) error {
	// Auto-mount admin panel
	if s.adminCfg == nil {
		s.adminCfg = s.MountAdmin(s.authMgr, nil)
		if s.adminCfg.SetupToken != "" {
			log.Printf("Admin setup URL: http://localhost%s/_/setup?token=%s", addr, s.adminCfg.SetupToken)
		}
	}

	var handler http.Handler = s.mux
	for i := len(s.middleware) - 1; i >= 0; i-- {
		handler = s.middleware[i](handler)
	}

	log.Printf("flop server listening on %s", addr)
	return http.ListenAndServe(addr, handler)
}

// Shutdown gracefully shuts down the server and flushes the database.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.db.Close()
}

func parseQueryValue(s string) any {
	if n, err := strconv.ParseUint(s, 10, 64); err == nil {
		return n
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return n
	}
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	return s
}
