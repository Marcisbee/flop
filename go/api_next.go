package flop

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/reqtrace"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/server"
)

type viewRuntime struct {
	name        string
	access      AccessPolicy
	decodeJSON  func([]byte) (any, error)
	decodeQuery func(url.Values) (any, error)
	run         func(*ViewCtx, any) (any, error)
}

type reducerRuntime struct {
	name       string
	access     AccessPolicy
	decodeJSON func([]byte) (any, error)
	run        func(*ReducerCtx, any) (any, error)
}

type builtinViewRuntime struct {
	name        string
	access      AccessPolicy
	decodeJSON  func([]byte) (any, error)
	decodeQuery func(url.Values) (any, error)
	run         func(*AuthContext) (any, []string, error)
}

func buildViewRuntime[In, Out any](
	name string,
	access AccessPolicy,
	handler func(*ViewCtx, In) (Out, error),
) viewRuntime {
	return viewRuntime{
		name:   name,
		access: access,
		decodeJSON: func(raw []byte) (any, error) {
			var in In
			if err := decodeStrictJSON(raw, &in); err != nil {
				return nil, err
			}
			return in, nil
		},
		decodeQuery: func(values url.Values) (any, error) {
			var in In
			if err := decodeQueryValues(values, &in); err != nil {
				return nil, err
			}
			return in, nil
		},
		run: func(ctx *ViewCtx, in any) (any, error) {
			typed, ok := in.(In)
			if !ok {
				return nil, fmt.Errorf("invalid view params type for %s", name)
			}
			return handler(ctx, typed)
		},
	}
}

func buildReducerRuntime[In, Out any](
	name string,
	access AccessPolicy,
	handler func(*ReducerCtx, In) (Out, error),
) reducerRuntime {
	return reducerRuntime{
		name:   name,
		access: access,
		decodeJSON: func(raw []byte) (any, error) {
			var in In
			if err := decodeStrictJSON(raw, &in); err != nil {
				return nil, err
			}
			return in, nil
		},
		run: func(ctx *ReducerCtx, in any) (any, error) {
			typed, ok := in.(In)
			if !ok {
				return nil, fmt.Errorf("invalid reducer params type for %s", name)
			}
			return handler(ctx, typed)
		},
	}
}

func decodeStrictJSON(raw []byte, out any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		trimmed = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("invalid json payload")
	}
	return nil
}

func decodeQueryValues(values url.Values, out any) error {
	target := reflect.ValueOf(out)
	if target.Kind() != reflect.Ptr || target.IsNil() {
		return fmt.Errorf("query decode target must be non-nil pointer")
	}
	target = target.Elem()
	typ := target.Type()

	switch typ.Kind() {
	case reflect.Struct:
		return decodeQueryStruct(values, target, typ)
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			return fmt.Errorf("query decode map key must be string")
		}
		target.Set(reflect.MakeMap(typ))
		for key, raw := range values {
			if key == "_token" {
				continue
			}
			if len(raw) == 0 {
				continue
			}
			val := reflect.New(typ.Elem()).Elem()
			if err := setFieldFromStrings(val, raw); err != nil {
				return fmt.Errorf("invalid query param %q: %w", key, err)
			}
			target.SetMapIndex(reflect.ValueOf(key), val)
		}
		return nil
	default:
		return fmt.Errorf("query params must decode into struct or map")
	}
}

func decodeQueryStruct(values url.Values, target reflect.Value, typ reflect.Type) error {
	type fieldMeta struct {
		index int
		name  string
	}
	fields := make(map[string]fieldMeta)
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		if sf.PkgPath != "" && !sf.Anonymous {
			continue
		}
		jsonName, _, skip := parseJSONTag(sf)
		if skip {
			continue
		}
		if jsonName == "" {
			jsonName = lowerCamel(sf.Name)
		}
		fields[jsonName] = fieldMeta{index: i, name: sf.Name}
	}

	for key := range values {
		if key == "_token" {
			continue
		}
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("unknown query parameter %q", key)
		}
	}

	for key, meta := range fields {
		raw := values[key]
		if len(raw) == 0 {
			continue
		}
		field := target.Field(meta.index)
		if !field.CanSet() {
			continue
		}
		if err := setFieldFromStrings(field, raw); err != nil {
			return fmt.Errorf("invalid query parameter %q: %w", key, err)
		}
	}

	return nil
}

func setFieldFromStrings(v reflect.Value, raw []string) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if len(raw) == 0 {
		return nil
	}
	first := raw[0]

	switch v.Kind() {
	case reflect.String:
		v.SetString(first)
		return nil
	case reflect.Bool:
		b, err := strconv.ParseBool(first)
		if err != nil {
			return err
		}
		v.SetBool(b)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(first, 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(first, 10, 64)
		if err != nil {
			return err
		}
		v.SetUint(n)
		return nil
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(first, 64)
		if err != nil {
			return err
		}
		v.SetFloat(f)
		return nil
	case reflect.Slice:
		values := raw
		if len(values) == 1 && strings.Contains(values[0], ",") {
			values = strings.Split(values[0], ",")
		}
		slice := reflect.MakeSlice(v.Type(), 0, len(values))
		for _, item := range values {
			ev := reflect.New(v.Type().Elem()).Elem()
			if err := setFieldFromStrings(ev, []string{strings.TrimSpace(item)}); err != nil {
				return err
			}
			slice = reflect.Append(slice, ev)
		}
		v.Set(slice)
		return nil
	default:
		return fmt.Errorf("unsupported field type %s", v.Type())
	}
}

type tableAccessTracker struct {
	tableNames []string
	readMask   []uint64
	writeMask  []uint64
}

func newTableAccessTracker(tableNames []string) *tableAccessTracker {
	words := (len(tableNames) + 63) / 64
	if words <= 0 {
		words = 1
	}
	return &tableAccessTracker{
		tableNames: tableNames,
		readMask:   make([]uint64, words),
		writeMask:  make([]uint64, words),
	}
}

func (t *tableAccessTracker) markRead(id int) {
	if t == nil || id < 0 {
		return
	}
	word := id / 64
	bit := uint(id % 64)
	if word >= len(t.readMask) {
		return
	}
	t.readMask[word] |= 1 << bit
}

func (t *tableAccessTracker) markWrite(id int) {
	if t == nil || id < 0 {
		return
	}
	word := id / 64
	bit := uint(id % 64)
	if word >= len(t.writeMask) {
		return
	}
	t.writeMask[word] |= 1 << bit
}

func (t *tableAccessTracker) reads() []string {
	return t.tablesFromMask(t.readMask)
}

func (t *tableAccessTracker) writes() []string {
	return t.tablesFromMask(t.writeMask)
}

func (t *tableAccessTracker) tablesFromMask(mask []uint64) []string {
	if t == nil {
		return nil
	}
	out := make([]string, 0)
	for i := range t.tableNames {
		word := i / 64
		bit := uint(i % 64)
		if word >= len(mask) {
			break
		}
		if mask[word]&(1<<bit) != 0 {
			out = append(out, t.tableNames[i])
		}
	}
	sort.Strings(out)
	return out
}

type DBAccessor struct {
	db            *Database
	tracker       *tableAccessTracker
	auth          *AuthContext
	enforcePolicy bool
}

func (a *DBAccessor) Table(name string) *TableInstance {
	if a == nil || a.db == nil {
		return nil
	}
	ti := a.db.db.GetTable(name)
	if ti == nil {
		return nil
	}
	tableID, ok := a.db.tableNameToID[name]
	if !ok {
		tableID = -1
	}
	return &TableInstance{
		ti:            ti,
		db:            a.db,
		name:          name,
		tableID:       tableID,
		tracker:       a.tracker,
		auth:          a.auth,
		spec:          a.db.tableSpecs[name],
		policy:        a.db.tablePolicy[name],
		enforcePolicy: a.enforcePolicy,
	}
}

type APIHandler struct {
	app *App
	db  *Database
}

const (
	apiSSEEventBufferSize = 1024
	apiSSEHeartbeat       = 15 * time.Second
)

func (a *App) APIHandler(db *Database) *APIHandler {
	if a == nil {
		panic("flop: app is nil")
	}
	if db == nil {
		panic("flop: database is nil")
	}
	return &APIHandler{app: a, db: db}
}

func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	switch {
	case path == "/api/schema" || path == "/_schema":
		if r.Method != http.MethodGet {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h.handleSchema(w)
		return
	case path == "/api/sse":
		h.handleSSE(w, r)
		return
	case strings.HasPrefix(path, "/api/auth/"):
		h.handleAuth(w, r)
		return
	case path == "/api/view/_batch":
		h.handleViewBatch(w, r)
		return
	case strings.HasPrefix(path, "/api/view/"):
		h.handleView(w, r)
		return
	case strings.HasPrefix(path, "/api/reduce/"):
		h.handleReducer(w, r)
		return
	default:
		jsonError(w, "not found", http.StatusNotFound)
		return
	}
}

func (h *APIHandler) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "sse not supported", http.StatusBadRequest)
		return
	}

	tables := parseCSVQuery(r.URL.Query().Get("tables"))
	if len(tables) == 0 {
		tables = append([]string(nil), h.db.tableNames...)
	}
	auth := h.authFromRequest(r)
	accessor := h.db.trackedAccessor(nil, auth)
	rowFilterTables := make(map[string]bool, len(tables))
	for _, tableName := range tables {
		ti := accessor.Table(tableName)
		if ti == nil {
			continue
		}
		rowFilterTables[tableName] = ti.requiresRowReadFiltering()
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	done := r.Context().Done()
	changeCh := make(chan engine.ChangeEvent, apiSSEEventBufferSize)
	unsubscribe := h.db.db.GetPubSub().Subscribe(tables, func(event engine.ChangeEvent) {
		select {
		case changeCh <- event:
		default:
			// Drop if subscriber cannot keep up; never block writes.
		}
	})
	defer unsubscribe()

	heartbeat := time.NewTicker(apiSSEHeartbeat)
	defer heartbeat.Stop()

	type changePayload struct {
		Table string `json:"table"`
		Op    string `json:"op"`
		RowID string `json:"rowId"`
	}
	visibleRows := make(map[string]map[string]struct{})
	isReadable := func(event engine.ChangeEvent) bool {
		ti := accessor.Table(event.Table)
		if ti == nil {
			return false
		}
		if !rowFilterTables[event.Table] {
			return true
		}
		return ti.allowRead(event.Data)
	}
	wasVisible := func(event engine.ChangeEvent) bool {
		rows := visibleRows[event.Table]
		if rows == nil {
			return false
		}
		_, ok := rows[event.RowID]
		return ok
	}
	markVisible := func(event engine.ChangeEvent) {
		rows := visibleRows[event.Table]
		if rows == nil {
			rows = make(map[string]struct{})
			visibleRows[event.Table] = rows
		}
		rows[event.RowID] = struct{}{}
	}
	markHidden := func(event engine.ChangeEvent) {
		rows := visibleRows[event.Table]
		if rows == nil {
			return
		}
		delete(rows, event.RowID)
	}
	shouldEmit := func(event engine.ChangeEvent) (bool, bool) {
		if !rowFilterTables[event.Table] {
			return true, false
		}
		readable := isReadable(event)
		visibleBefore := wasVisible(event)
		switch event.Op {
		case "insert":
			if readable {
				markVisible(event)
				return true, false
			}
			return false, false
		case "delete":
			if readable || visibleBefore {
				markHidden(event)
				return true, false
			}
			return false, false
		default: // update and unknown future ops
			if readable {
				markVisible(event)
				return true, false
			}
			if visibleBefore {
				markHidden(event)
				return true, false
			}
			// Row may have been visible in initial snapshot; emit table touch without row details.
			return true, true
		}
	}

	for {
		select {
		case <-done:
			return
		case event := <-changeCh:
			emit, scrub := shouldEmit(event)
			if !emit {
				continue
			}
			if scrub {
				event.Op = "touch"
				event.RowID = ""
			}
			payload, _ := json.Marshal(changePayload{
				Table: event.Table,
				Op:    event.Op,
				RowID: event.RowID,
			})
			fmt.Fprintf(w, "event: change\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (h *APIHandler) handleSchema(w http.ResponseWriter) {
	type endpoint struct {
		Name   string       `json:"name"`
		Method string       `json:"method"`
		Path   string       `json:"path"`
		Type   string       `json:"type"`
		Access AccessPolicy `json:"access"`
	}
	out := make([]endpoint, 0, len(h.app.views)+len(h.app.reducers))
	for _, v := range h.app.views {
		out = append(out, endpoint{
			Name: v.Name, Method: http.MethodGet, Path: "/api/view/" + v.Name, Type: "view", Access: v.Access,
		})
	}
	if h.hasBuiltinAuthMeView() {
		out = append(out, endpoint{
			Name: "auth_me", Method: http.MethodGet, Path: "/api/view/auth_me", Type: "view", Access: Authenticated(),
		})
	}
	for _, rd := range h.app.reducers {
		out = append(out, endpoint{
			Name: rd.Name, Method: http.MethodPost, Path: "/api/reduce/" + rd.Name, Type: "reducer", Access: rd.Access,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type == out[j].Type {
			return out[i].Name < out[j].Name
		}
		return out[i].Type < out[j].Type
	})
	jsonResponse(w, http.StatusOK, map[string]any{"endpoints": out})
}

func (h *APIHandler) hasBuiltinAuthMeView() bool {
	return h.db != nil && h.db.authService != nil
}

func (h *APIHandler) builtinView(name string) (builtinViewRuntime, bool) {
	if name != "auth_me" || !h.hasBuiltinAuthMeView() {
		return builtinViewRuntime{}, false
	}
	return builtinViewRuntime{
		name:   "auth_me",
		access: Authenticated(),
		decodeJSON: func(raw []byte) (any, error) {
			var in struct{}
			if err := decodeStrictJSON(raw, &in); err != nil {
				return nil, err
			}
			return in, nil
		},
		decodeQuery: func(values url.Values) (any, error) {
			var in struct{}
			if err := decodeQueryValues(values, &in); err != nil {
				return nil, err
			}
			return in, nil
		},
		run: func(auth *AuthContext) (any, []string, error) {
			payload, err := h.db.BuildAuthMePayload(auth)
			if err != nil {
				return nil, nil, err
			}
			return payload, h.db.BuildAuthMeReads(auth), nil
		},
	}, true
}

func (h *APIHandler) execBuiltinView(def builtinViewRuntime, auth *AuthContext) (any, []string, int, error) {
	out, reads, err := def.run(auth)
	if err != nil {
		if errors.Is(err, ErrAccessDenied) {
			return nil, nil, http.StatusForbidden, err
		}
		return nil, nil, http.StatusBadRequest, err
	}
	return out, reads, http.StatusOK, nil
}

func (h *APIHandler) handleAuth(w http.ResponseWriter, r *http.Request) {
	if h.db.authService == nil {
		jsonError(w, "auth not configured", http.StatusNotFound)
		return
	}
	switch r.URL.Path {
	case "/api/auth/register":
		if r.Method != http.MethodPost {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body map[string]any
		if err := decodeStrictJSONBody(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		email, _ := body["email"].(string)
		password, _ := body["password"].(string)
		name, _ := body["name"].(string)
		if strings.TrimSpace(email) == "" || password == "" {
			jsonError(w, "email and password required", http.StatusBadRequest)
			return
		}
		extraFields := map[string]any{}
		for key, value := range body {
			switch key {
			case "email", "password", "name":
				continue
			default:
				extraFields[key] = value
			}
		}
		token, refreshToken, auth, err := h.db.authService.Register(email, password, name, extraFields)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		appAuth := authContextFromSchema(auth)
		userPayload, err := h.db.BuildAuthUserPayload(appAuth)
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mePayload, err := h.db.BuildAuthMePayload(appAuth)
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]any{
			"token":        token,
			"refreshToken": refreshToken,
			"user":         userPayload,
			"me":           mePayload,
		})
	case "/api/auth/password":
		if r.Method != http.MethodPost {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var in struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := decodeStrictJSONBody(r, &in); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(in.Email) == "" || in.Password == "" {
			jsonError(w, "email and password required", http.StatusBadRequest)
			return
		}
		token, refresh, auth, err := h.db.authService.Login(in.Email, in.Password)
		if err != nil {
			jsonError(w, err.Error(), http.StatusUnauthorized)
			return
		}
		appAuth := authContextFromSchema(auth)
		userPayload, err := h.db.BuildAuthUserPayload(appAuth)
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mePayload, err := h.db.BuildAuthMePayload(appAuth)
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]any{
			"token":        token,
			"refreshToken": refresh,
			"user":         userPayload,
			"me":           mePayload,
		})
	case "/api/auth/refresh":
		if r.Method != http.MethodPost {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var in struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := decodeStrictJSONBody(r, &in); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(in.RefreshToken) == "" {
			jsonError(w, "refresh token required", http.StatusBadRequest)
			return
		}
		token, nextRefreshToken, err := h.db.authService.Refresh(in.RefreshToken)
		if err != nil {
			jsonError(w, err.Error(), http.StatusUnauthorized)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]any{"token": token, "refreshToken": nextRefreshToken})
	case "/api/auth/me":
		if r.Method != http.MethodGet {
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		auth := h.authFromRequest(r)
		if auth == nil {
			jsonError(w, "authentication required", http.StatusUnauthorized)
			return
		}
		mePayload, err := h.db.BuildAuthMePayload(auth)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, http.StatusOK, mePayload)
	default:
		jsonError(w, "unknown auth endpoint", http.StatusNotFound)
	}
}

func (h *APIHandler) handleView(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	traceCollector := reqtrace.Start()
	defer traceCollector.End()

	statusCode := http.StatusOK
	errMessage := ""
	paramBytes := 0
	hasAuth := false
	defer func() {
		h.recordAnalyticsEvent(analyticsEventInput{
			start:          start,
			traceCollector: traceCollector,
			routeType:      "view",
			routeName:      strings.TrimPrefix(r.URL.Path, "/api/view/"),
			method:         r.Method,
			path:           r.URL.Path,
			transport:      "http",
			statusCode:     statusCode,
			errMessage:     errMessage,
			userID:         ternaryUserID(h.authFromRequest(r)),
			details: map[string]any{
				"queryBytes": len(r.URL.RawQuery),
				"paramBytes": paramBytes,
				"hasAuth":    hasAuth,
				"source":     "api-handler",
			},
		})
	}()

	if r.Method != http.MethodGet {
		statusCode = http.StatusMethodNotAllowed
		errMessage = "method not allowed"
		jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/view/")
	if builtin, ok := h.builtinView(name); ok {
		auth, status, err := h.enforceAccess(r, builtin.access)
		hasAuth = auth != nil
		if err != nil {
			statusCode = status
			errMessage = err.Error()
			jsonError(w, err.Error(), status)
			return
		}
		paramBytes = len(r.URL.RawQuery)
		if _, err := builtin.decodeQuery(r.URL.Query()); err != nil {
			statusCode = http.StatusBadRequest
			errMessage = err.Error()
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, reads, status, err := h.execBuiltinView(builtin, auth)
		if err != nil {
			statusCode = status
			errMessage = err.Error()
			jsonError(w, err.Error(), status)
			return
		}
		if len(reads) > 0 {
			w.Header().Set("X-Flop-Reads", strings.Join(reads, ","))
		}
		jsonResponse(w, http.StatusOK, map[string]any{"ok": true, "data": result})
		return
	}
	def, ok := h.app.viewDefs[name]
	if !ok {
		statusCode = http.StatusNotFound
		errMessage = "view not found"
		jsonError(w, "view not found", http.StatusNotFound)
		return
	}
	auth, status, err := h.enforceAccess(r, def.access)
	hasAuth = auth != nil
	if err != nil {
		statusCode = status
		errMessage = err.Error()
		jsonError(w, err.Error(), status)
		return
	}
	paramBytes = len(r.URL.RawQuery)
	input, err := def.decodeQuery(r.URL.Query())
	if err != nil {
		statusCode = http.StatusBadRequest
		errMessage = err.Error()
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, reads, status, err := h.execView(def, input, auth)
	if err != nil {
		statusCode = status
		errMessage = err.Error()
		jsonError(w, err.Error(), status)
		return
	}
	if len(reads) > 0 {
		w.Header().Set("X-Flop-Reads", strings.Join(reads, ","))
	}
	jsonResponse(w, http.StatusOK, map[string]any{"ok": true, "data": result})
}

func (h *APIHandler) handleViewBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	type call struct {
		ID     string          `json:"id"`
		Name   string          `json:"name"`
		Params json.RawMessage `json:"params"`
	}
	type req struct {
		Calls []call `json:"calls"`
	}
	type result struct {
		ID    string   `json:"id"`
		Data  any      `json:"data,omitempty"`
		Reads []string `json:"reads,omitempty"`
		Error string   `json:"error,omitempty"`
	}

	var in req
	if err := decodeStrictJSONBody(r, &in); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(in.Calls) == 0 {
		jsonResponse(w, http.StatusOK, map[string]any{"ok": true, "results": []result{}})
		return
	}
	auth := h.authFromRequest(r)
	out := make([]result, 0, len(in.Calls))
	for _, c := range in.Calls {
		start := time.Now()
		traceCollector := reqtrace.Start()
		statusCode := http.StatusOK
		errMessage := ""
		details := map[string]any{
			"paramBytes": len(c.Params),
			"hasAuth":    auth != nil,
			"source":     "api-handler",
			"batch":      true,
			"callID":     c.ID,
			"batchSize":  len(in.Calls),
		}

		if builtin, ok := h.builtinView(c.Name); ok {
			if _, status, err := h.enforceAccessFromAuth(builtin.access, auth); err != nil {
				if status == http.StatusUnauthorized {
					statusCode = status
					errMessage = "authentication required"
					out = append(out, result{ID: c.ID, Error: "authentication required"})
				} else {
					statusCode = status
					errMessage = err.Error()
					out = append(out, result{ID: c.ID, Error: err.Error()})
				}
				traceCollector.End()
				h.recordAnalyticsEvent(analyticsEventInput{
					start:          start,
					traceCollector: traceCollector,
					routeType:      "view",
					routeName:      c.Name,
					method:         http.MethodPost,
					path:           "/api/view/" + c.Name,
					transport:      "http",
					statusCode:     statusCode,
					errMessage:     errMessage,
					userID:         ternaryUserID(auth),
					details:        details,
				})
				continue
			}
			if _, err := builtin.decodeJSON(c.Params); err != nil {
				statusCode = http.StatusBadRequest
				errMessage = err.Error()
				out = append(out, result{ID: c.ID, Error: err.Error()})
				traceCollector.End()
				h.recordAnalyticsEvent(analyticsEventInput{
					start:          start,
					traceCollector: traceCollector,
					routeType:      "view",
					routeName:      c.Name,
					method:         http.MethodPost,
					path:           "/api/view/" + c.Name,
					transport:      "http",
					statusCode:     statusCode,
					errMessage:     errMessage,
					userID:         ternaryUserID(auth),
					details:        details,
				})
				continue
			}
			data, reads, _, err := h.execBuiltinView(builtin, auth)
			if err != nil {
				statusCode = http.StatusBadRequest
				errMessage = err.Error()
				out = append(out, result{ID: c.ID, Error: err.Error()})
				traceCollector.End()
				h.recordAnalyticsEvent(analyticsEventInput{
					start:          start,
					traceCollector: traceCollector,
					routeType:      "view",
					routeName:      c.Name,
					method:         http.MethodPost,
					path:           "/api/view/" + c.Name,
					transport:      "http",
					statusCode:     statusCode,
					errMessage:     errMessage,
					userID:         ternaryUserID(auth),
					details:        details,
				})
				continue
			}
			out = append(out, result{ID: c.ID, Data: data, Reads: reads})
			traceCollector.End()
			h.recordAnalyticsEvent(analyticsEventInput{
				start:          start,
				traceCollector: traceCollector,
				routeType:      "view",
				routeName:      c.Name,
				method:         http.MethodPost,
				path:           "/api/view/" + c.Name,
				transport:      "http",
				statusCode:     statusCode,
				userID:         ternaryUserID(auth),
				details:        details,
			})
			continue
		}
		def, ok := h.app.viewDefs[c.Name]
		if !ok {
			statusCode = http.StatusNotFound
			errMessage = "view not found: " + c.Name
			out = append(out, result{ID: c.ID, Error: "view not found: " + c.Name})
			traceCollector.End()
			h.recordAnalyticsEvent(analyticsEventInput{
				start:          start,
				traceCollector: traceCollector,
				routeType:      "view",
				routeName:      c.Name,
				method:         http.MethodPost,
				path:           "/api/view/" + c.Name,
				transport:      "http",
				statusCode:     statusCode,
				errMessage:     errMessage,
				userID:         ternaryUserID(auth),
				details:        details,
			})
			continue
		}
		if _, status, err := h.enforceAccessFromAuth(def.access, auth); err != nil {
			if status == http.StatusUnauthorized {
				statusCode = status
				errMessage = "authentication required"
				out = append(out, result{ID: c.ID, Error: "authentication required"})
			} else {
				statusCode = status
				errMessage = err.Error()
				out = append(out, result{ID: c.ID, Error: err.Error()})
			}
			traceCollector.End()
			h.recordAnalyticsEvent(analyticsEventInput{
				start:          start,
				traceCollector: traceCollector,
				routeType:      "view",
				routeName:      c.Name,
				method:         http.MethodPost,
				path:           "/api/view/" + c.Name,
				transport:      "http",
				statusCode:     statusCode,
				errMessage:     errMessage,
				userID:         ternaryUserID(auth),
				details:        details,
			})
			continue
		}
		params, err := def.decodeJSON(c.Params)
		if err != nil {
			statusCode = http.StatusBadRequest
			errMessage = err.Error()
			out = append(out, result{ID: c.ID, Error: err.Error()})
			traceCollector.End()
			h.recordAnalyticsEvent(analyticsEventInput{
				start:          start,
				traceCollector: traceCollector,
				routeType:      "view",
				routeName:      c.Name,
				method:         http.MethodPost,
				path:           "/api/view/" + c.Name,
				transport:      "http",
				statusCode:     statusCode,
				errMessage:     errMessage,
				userID:         ternaryUserID(auth),
				details:        details,
			})
			continue
		}
		data, reads, _, err := h.execView(def, params, auth)
		if err != nil {
			statusCode = http.StatusBadRequest
			errMessage = err.Error()
			out = append(out, result{ID: c.ID, Error: err.Error()})
			traceCollector.End()
			h.recordAnalyticsEvent(analyticsEventInput{
				start:          start,
				traceCollector: traceCollector,
				routeType:      "view",
				routeName:      c.Name,
				method:         http.MethodPost,
				path:           "/api/view/" + c.Name,
				transport:      "http",
				statusCode:     statusCode,
				errMessage:     errMessage,
				userID:         ternaryUserID(auth),
				details:        details,
			})
			continue
		}
		out = append(out, result{ID: c.ID, Data: data, Reads: reads})
		traceCollector.End()
		h.recordAnalyticsEvent(analyticsEventInput{
			start:          start,
			traceCollector: traceCollector,
			routeType:      "view",
			routeName:      c.Name,
			method:         http.MethodPost,
			path:           "/api/view/" + c.Name,
			transport:      "http",
			statusCode:     statusCode,
			userID:         ternaryUserID(auth),
			details:        details,
		})
	}
	jsonResponse(w, http.StatusOK, map[string]any{"ok": true, "results": out})
}

func (h *APIHandler) handleReducer(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	traceCollector := reqtrace.Start()
	defer traceCollector.End()

	statusCode := http.StatusOK
	errMessage := ""
	paramBytes := 0
	hasAuth := false
	defer func() {
		h.recordAnalyticsEvent(analyticsEventInput{
			start:          start,
			traceCollector: traceCollector,
			routeType:      "reducer",
			routeName:      strings.TrimPrefix(r.URL.Path, "/api/reduce/"),
			method:         r.Method,
			path:           r.URL.Path,
			transport:      "http",
			statusCode:     statusCode,
			errMessage:     errMessage,
			userID:         ternaryUserID(h.authFromRequest(r)),
			details: map[string]any{
				"paramBytes": paramBytes,
				"hasAuth":    hasAuth,
				"source":     "api-handler",
			},
		})
	}()

	if r.Method != http.MethodPost {
		statusCode = http.StatusMethodNotAllowed
		errMessage = "method not allowed"
		jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/reduce/")
	def, ok := h.app.reduceDefs[name]
	if !ok {
		statusCode = http.StatusNotFound
		errMessage = "reducer not found"
		jsonError(w, "reducer not found", http.StatusNotFound)
		return
	}
	auth, status, err := h.enforceAccess(r, def.access)
	hasAuth = auth != nil
	if err != nil {
		statusCode = status
		errMessage = err.Error()
		jsonError(w, err.Error(), status)
		return
	}
	body, err := readAllAndClose(r)
	if err != nil {
		statusCode = http.StatusBadRequest
		errMessage = "failed to read body"
		jsonError(w, "failed to read body", http.StatusBadRequest)
		return
	}
	paramBytes = len(body)
	input, err := def.decodeJSON(body)
	if err != nil {
		statusCode = http.StatusBadRequest
		errMessage = err.Error()
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, writes, status, err := h.execReducer(def, input, auth)
	if err != nil {
		statusCode = status
		errMessage = err.Error()
		jsonError(w, err.Error(), status)
		return
	}
	if len(writes) > 0 {
		w.Header().Set("X-Flop-Writes", strings.Join(writes, ","))
	}
	jsonResponse(w, http.StatusOK, map[string]any{"ok": true, "data": result})
}

type analyticsEventInput struct {
	start          time.Time
	traceCollector *reqtrace.Collector
	routeType      string
	routeName      string
	method         string
	path           string
	transport      string
	statusCode     int
	errMessage     string
	userID         string
	details        map[string]any
}

func (h *APIHandler) recordAnalyticsEvent(in analyticsEventInput) {
	if h == nil || h.db == nil {
		return
	}
	analytics := h.db.RequestAnalytics()
	if analytics == nil {
		return
	}
	details := in.details
	if details == nil {
		details = map[string]any{}
	}
	if spans := in.traceCollector.Spans(); len(spans) > 0 {
		details["trace"] = spans
		details["traceSpans"] = len(spans)
	}
	analytics.Record(server.AnalyticsEvent{
		Timestamp:    time.Now(),
		RouteType:    in.routeType,
		RouteName:    in.routeName,
		Method:       in.method,
		Path:         in.path,
		Transport:    in.transport,
		Duration:     time.Since(in.start),
		OK:           in.statusCode < 400,
		StatusCode:   in.statusCode,
		ErrorMessage: in.errMessage,
		UserID:       in.userID,
		Details:      details,
	})
}

func ternaryUserID(auth *AuthContext) string {
	if auth == nil {
		return ""
	}
	return auth.ID
}

func (h *APIHandler) execView(def viewRuntime, input any, auth *AuthContext) (any, []string, int, error) {
	tracker := newTableAccessTracker(h.db.tableNames)
	ctx := &ViewCtx{
		Request: RequestContext{Auth: auth},
		DB:      h.db.trackedAccessor(tracker, auth),
	}
	out, err := def.run(ctx, input)
	if err != nil {
		if errors.Is(err, ErrAccessDenied) {
			return nil, nil, http.StatusForbidden, err
		}
		return nil, nil, http.StatusBadRequest, err
	}
	return out, tracker.reads(), http.StatusOK, nil
}

func (h *APIHandler) execReducer(def reducerRuntime, input any, auth *AuthContext) (any, []string, int, error) {
	tracker := newTableAccessTracker(h.db.tableNames)
	ctx := &ReducerCtx{
		Request: RequestContext{Auth: auth},
		DB:      h.db.trackedAccessor(tracker, auth),
	}
	out, err := def.run(ctx, input)
	if err != nil {
		if errors.Is(err, ErrAccessDenied) {
			return nil, nil, http.StatusForbidden, err
		}
		return nil, nil, http.StatusBadRequest, err
	}
	return out, tracker.writes(), http.StatusOK, nil
}

func (h *APIHandler) authFromRequest(r *http.Request) *AuthContext {
	token := server.ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		return nil
	}
	if h.db.authService != nil {
		if auth, err := h.db.authService.ValidateAccessToken(token); err == nil && auth != nil {
			return &AuthContext{
				ID:            auth.ID,
				Email:         auth.Email,
				Roles:         append([]string(nil), auth.Roles...),
				PrincipalType: auth.PrincipalType,
				SessionID:     auth.SessionID,
				InstanceID:    auth.InstanceID,
			}
		}
	}
	if h.db.superadminService != nil {
		if auth, err := h.db.superadminService.ValidateAccessToken(token); err == nil && auth != nil {
			return &AuthContext{
				ID:            auth.ID,
				Email:         auth.Email,
				Roles:         append([]string(nil), auth.Roles...),
				PrincipalType: auth.PrincipalType,
				SessionID:     auth.SessionID,
				InstanceID:    auth.InstanceID,
			}
		}
	}
	payload := server.VerifyJWT(token, h.db.jwtSecret)
	if payload == nil {
		return nil
	}
	return &AuthContext{
		ID:            payload.Sub,
		Email:         payload.Email,
		Roles:         append([]string(nil), payload.Roles...),
		PrincipalType: payload.PrincipalType,
		SessionID:     payload.SessionID,
		InstanceID:    payload.InstanceID,
	}
}

func (h *APIHandler) enforceAccess(r *http.Request, policy AccessPolicy) (*AuthContext, int, error) {
	return h.enforceAccessFromAuth(policy, h.authFromRequest(r))
}

func (h *APIHandler) enforceAccessFromAuth(policy AccessPolicy, auth *AuthContext) (*AuthContext, int, error) {
	switch policy.Type {
	case "", "authenticated":
		if auth == nil {
			return nil, http.StatusUnauthorized, fmt.Errorf("authentication required")
		}
		return auth, http.StatusOK, nil
	case "public":
		return auth, http.StatusOK, nil
	case "roles":
		if auth == nil {
			return nil, http.StatusUnauthorized, fmt.Errorf("authentication required")
		}
		for _, role := range auth.Roles {
			if role == "superadmin" {
				return auth, http.StatusOK, nil
			}
		}
		for _, need := range policy.Roles {
			for _, have := range auth.Roles {
				if strings.EqualFold(need, have) {
					return auth, http.StatusOK, nil
				}
			}
		}
		return nil, http.StatusForbidden, fmt.Errorf("insufficient role")
	default:
		return nil, http.StatusForbidden, fmt.Errorf("invalid access policy")
	}
}

func authContextFromSchema(auth *schema.AuthContext) *AuthContext {
	if auth == nil {
		return nil
	}
	return &AuthContext{
		ID:            auth.ID,
		Email:         auth.Email,
		Roles:         append([]string(nil), auth.Roles...),
		PrincipalType: auth.PrincipalType,
		SessionID:     auth.SessionID,
		InstanceID:    auth.InstanceID,
	}
}

func decodeStrictJSONBody(r *http.Request, out any) error {
	body, err := readAllAndClose(r)
	if err != nil {
		return err
	}
	return decodeStrictJSON(body, out)
}

func readAllAndClose(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func jsonResponse(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func jsonError(w http.ResponseWriter, message string, status int) {
	jsonResponse(w, status, map[string]any{"error": message})
}

func parseCSVQuery(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}
