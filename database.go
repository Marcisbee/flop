package flop

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/marcisbee/flop/internal/jsonx"
	"github.com/marcisbee/flop/internal/reqtrace"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/cron"
	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/images"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/server"
	"github.com/marcisbee/flop/internal/storage"
)

// Database wraps the internal engine and exposes table operations.
type Database struct {
	app                  *App
	db                   *engine.Database
	authService          *server.AuthService
	superadminService    *server.SuperadminService
	emailMu              sync.RWMutex
	emailSettings        EmailSettings
	mailer               *server.Mailer
	jwtSecret            string
	authInstanceID       string
	requestLogRetention  time.Duration
	authSessionRetention time.Duration
	authSessionCleanup   time.Duration
	enablePprof          bool
	analyticsMu          sync.Mutex
	analytics            *server.RequestAnalytics
	mediaIndexMu         sync.Mutex
	mediaIndexRebuild    bool
	cronRunner           *cron.Runner
	backgroundStop       chan struct{}
	backgroundWG         sync.WaitGroup
	backupManager        *backupManager
	buildAuthUser        func(*Database, *AuthContext) (map[string]any, error)
	buildAuthMe          func(*Database, *AuthContext) (map[string]any, error)
	readAuthMe           func(*Database, *AuthContext) []string
	tableNames           []string
	tableNameToID        map[string]int
	tableSpecs           map[string]*tableSpec
	tablePolicy          map[string]tablePolicyMeta
	materialized         map[string]*materializedRuntime
	cascadeRefs          map[string][]cascadeArchiveRef
}

// AnalyticsEvent records one retained observability entry in the admin analytics store.
// RouteType can be a custom category such as "igdb" in addition to built-in request/view/reducer.
type AnalyticsEvent struct {
	Timestamp    time.Time
	RouteType    string
	RouteName    string
	Method       string
	Path         string
	Transport    string
	Duration     time.Duration
	OK           bool
	StatusCode   int
	ErrorMessage string
	UserID       string
	Details      map[string]any
}

const systemSuperadminTableName = "_superadmin"
const systemAuthSessionTableName = "_auth_sessions"

const defaultAuthSessionRetention = 30 * 24 * time.Hour
const defaultAuthSessionCleanupInterval = time.Hour

type materializedRuntime struct {
	spec        *materializedSpec
	mu          sync.Mutex
	lastRefresh time.Time
	lastError   string
}

type cascadeArchiveRef struct {
	tableName string
	fieldName string
	multi     bool
}

type archiveTxn struct {
	db     *Database
	txBuf  map[string]*engine.WalBufEntry
	undo   []func()
	media  []mediaIndexOp
	after  []func() error
	closed bool
}

var testArchiveCommitHook func() error

// TableInstance wraps internal engine table and provides CRUD operations.
type TableInstance struct {
	ti            *engine.TableInstance
	db            *Database
	name          string
	tableID       int
	tracker       *tableAccessTracker
	auth          *AuthContext
	spec          *tableSpec
	policy        tablePolicyMeta
	enforcePolicy bool
}

type tablePolicyMeta struct {
	requiresReadFiltering bool
	requiresRowRead       bool
	hasFieldReadFiltering bool
	hasInsertPolicy       bool
	hasUpdatePolicy       bool
	hasDeletePolicy       bool
	writableFields        map[string]struct{}
}

func (ti *TableInstance) isNil() bool {
	return ti == nil || ti.ti == nil
}

func (ti *TableInstance) ensureMaterializedReadable() {
	if ti == nil || ti.ti == nil || ti.spec == nil || ti.spec.Materialized == nil {
		return
	}
	_ = ti.ti.RepairIndexesIfNeeded()
}

// AutocompleteEntry represents one row in an autocomplete index.
type AutocompleteEntry = engine.AutocompleteEntry

// AutocompleteIndex provides reusable in-memory autocomplete search.
type AutocompleteIndex struct {
	idx *engine.AutocompleteIndex
}

// Open initializes the database from the App schema definitions.
func (a *App) Open() (*Database, error) {
	if a == nil {
		return nil, fmt.Errorf("flop: app is nil")
	}

	tableDefs := a.buildTableDefs()

	db := engine.NewDatabase(engine.DatabaseConfig{
		DataDir:               a.config.DataDir,
		SyncMode:              a.config.SyncMode,
		AsyncSecondaryIndexes: a.config.AsyncSecondaryIndexes,
	})

	if err := db.Open(tableDefs); err != nil {
		return nil, err
	}

	retention := a.config.RequestLogRetention
	if retention <= 0 {
		retention = server.DefaultRequestLogRetention
	}
	d := &Database{
		app:                  a,
		db:                   db,
		requestLogRetention:  retention,
		authSessionRetention: a.config.AuthSessionRetention,
		authSessionCleanup:   a.config.AuthSessionCleanup,
		enablePprof:          a.config.EnablePprof,
		backgroundStop:       make(chan struct{}),
		tableNameToID:        make(map[string]int),
		tableSpecs:           make(map[string]*tableSpec),
		tablePolicy:          make(map[string]tablePolicyMeta),
		materialized:         make(map[string]*materializedRuntime),
		cascadeRefs:          make(map[string][]cascadeArchiveRef),
	}
	if d.authSessionRetention <= 0 {
		d.authSessionRetention = defaultAuthSessionRetention
	}
	if d.authSessionCleanup <= 0 {
		d.authSessionCleanup = defaultAuthSessionCleanupInterval
	}
	if a.config.AuthPayloads != nil {
		d.buildAuthUser = a.config.AuthPayloads.BuildUser
		d.buildAuthMe = a.config.AuthPayloads.BuildMe
		d.readAuthMe = a.config.AuthPayloads.ReadMe
	}
	d.authInstanceID = strings.TrimSpace(db.GetMeta().AuthInstanceID)
	for name, spec := range a.tables {
		d.tableSpecs[name] = spec
		d.tablePolicy[name] = buildTablePolicyMeta(spec)
		for _, fs := range spec.Fields {
			if fs.RefTable == "" || fs.CascadeDelete != "archive" {
				continue
			}
			d.cascadeRefs[fs.RefTable] = append(d.cascadeRefs[fs.RefTable], cascadeArchiveRef{
				tableName: name,
				fieldName: fs.JSONName,
				multi:     fs.Kind == "refMulti",
			})
		}
		if spec.Materialized != nil {
			d.materialized[name] = &materializedRuntime{spec: spec.Materialized}
		}
	}

	names := make([]string, 0, len(a.tables))
	for name := range a.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	d.tableNames = names
	for i, name := range names {
		d.tableNameToID[name] = i
	}

	secret := "flop-" + d.authInstanceID
	d.jwtSecret = secret
	sessionTable := db.GetTable(systemAuthSessionTableName)
	if authTable := db.GetAuthTable(); authTable != nil {
		d.authService = server.NewAuthService(authTable, sessionTable, secret, d.authInstanceID)
	}
	if superadminTable := db.GetTable(systemSuperadminTableName); superadminTable != nil {
		d.superadminService = server.NewSuperadminService(superadminTable, sessionTable, secret, d.authInstanceID)
	}

	if err := d.initEmailSettings(); err != nil {
		return nil, err
	}

	// Wire up cached field triggers
	a.wireCachedFields(d)

	for name, rt := range d.materialized {
		if rt == nil || rt.spec == nil || !rt.spec.RefreshOnStartup || rt.spec.Refresh == nil {
			continue
		}
		if err := d.RefreshMaterialized(name); err != nil {
			return nil, err
		}
	}

	jobs := make([]cron.Job, 0, len(a.crons)+len(d.materialized))
	for _, cs := range a.crons {
		sched, err := cron.Parse(cs.Expr)
		if err != nil {
			return nil, fmt.Errorf("flop: invalid cron expression %q: %w", cs.Expr, err)
		}
		fn := cs.Fn
		db := d
		jobs = append(jobs, cron.Job{
			Schedule: sched,
			Fn:       func() { fn(db) },
		})
	}
	for name, rt := range d.materialized {
		if rt == nil || rt.spec == nil || rt.spec.Refresh == nil || strings.TrimSpace(rt.spec.Cron) == "" {
			continue
		}
		sched, err := cron.Parse(rt.spec.Cron)
		if err != nil {
			return nil, fmt.Errorf("flop: invalid cron expression %q for materialized table %s: %w", rt.spec.Cron, name, err)
		}
		tableName := name
		jobs = append(jobs, cron.Job{
			Schedule: sched,
			Fn: func() {
				_ = d.RefreshMaterialized(tableName)
			},
		})
	}
	if len(jobs) > 0 {
		d.cronRunner = cron.Start(context.Background(), jobs)
	}
	backupMgr, err := newBackupManager(d)
	if err != nil {
		return nil, err
	}
	d.backupManager = backupMgr
	d.startBackgroundWorkers()

	return d, nil
}

// wireCachedFields registers PubSub subscribers for cached field triggers
// and hydrates initial cached values for all existing rows.
func (a *App) wireCachedFields(d *Database) {
	type trigger struct {
		targetTable string
		fieldName   string
		sourceTable string
		foreignKey  string
		compute     func(Row, *Database) any
	}

	var triggers []trigger
	for name, ts := range a.tables {
		for _, cd := range ts.CachedDefs {
			for _, t := range cd.Triggers {
				triggers = append(triggers, trigger{
					targetTable: name,
					fieldName:   cd.FieldName,
					sourceTable: t.SourceTable,
					foreignKey:  t.ForeignKey,
					compute:     cd.Compute,
				})
			}
		}
	}

	if len(triggers) == 0 {
		return
	}

	// Register PubSub subscribers for each trigger
	for _, trig := range triggers {
		trig := trig // capture for closure
		d.db.GetPubSub().Subscribe([]string{trig.sourceTable}, func(event engine.ChangeEvent) {
			if event.Data == nil {
				return
			}
			fkValue := toString(event.Data[trig.foreignKey])
			if fkValue == "" {
				return
			}
			targetTI := d.db.GetTable(trig.targetTable)
			if targetTI == nil {
				return
			}
			targetRow, err := targetTI.Get(fkValue)
			if err != nil || targetRow == nil {
				return
			}
			pkField := targetTI.GetDef().CompiledSchema.Fields[0].Name
			pk := toString(targetRow[pkField])
			if pk == "" {
				return
			}
			row := Row{data: targetRow, pk: pk}
			newVal := trig.compute(row, d)
			targetTI.UpdateSilent(pk, map[string]interface{}{trig.fieldName: newVal})
		})
	}

}

// SetJWTSecret sets the JWT secret used for auth tokens.
func (d *Database) SetJWTSecret(secret string) {
	d.jwtSecret = secret
	if d.authService != nil {
		d.authService = server.NewAuthService(d.db.GetAuthTable(), d.db.GetTable(systemAuthSessionTableName), secret, d.authInstanceID)
	}
	if d.superadminService != nil {
		d.superadminService = server.NewSuperadminService(d.db.GetTable(systemSuperadminTableName), d.db.GetTable(systemAuthSessionTableName), secret, d.authInstanceID)
	}
}

// ValidateAccessToken resolves the current authenticated principal for the given access token.
func (d *Database) ValidateAccessToken(token string) (*AuthContext, error) {
	if d == nil || token == "" {
		return nil, fmt.Errorf("authentication required")
	}
	if d.authService != nil {
		auth, err := d.authService.ValidateAccessToken(token)
		if err == nil && auth != nil {
			return &AuthContext{
				ID:            auth.ID,
				Email:         auth.Email,
				Roles:         append([]string(nil), auth.Roles...),
				PrincipalType: auth.PrincipalType,
				SessionID:     auth.SessionID,
				InstanceID:    auth.InstanceID,
			}, nil
		}
	}
	if d.superadminService != nil {
		auth, err := d.superadminService.ValidateAccessToken(token)
		if err == nil && auth != nil {
			return &AuthContext{
				ID:            auth.ID,
				Email:         auth.Email,
				Roles:         append([]string(nil), auth.Roles...),
				PrincipalType: auth.PrincipalType,
				SessionID:     auth.SessionID,
				InstanceID:    auth.InstanceID,
			}, nil
		}
	}
	return nil, fmt.Errorf("invalid or expired token")
}

// AuthenticateRequest resolves the authenticated principal from an HTTP request bearer token.
func (d *Database) AuthenticateRequest(r *http.Request) (*AuthContext, error) {
	if r == nil {
		return nil, fmt.Errorf("authentication required")
	}
	token := extractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		return nil, fmt.Errorf("authentication required")
	}
	return d.ValidateAccessToken(token)
}

func (d *Database) BuildAuthUserPayload(auth *AuthContext) (map[string]any, error) {
	if auth == nil {
		return nil, fmt.Errorf("authentication required")
	}
	if d != nil && d.buildAuthUser != nil {
		payload, err := d.buildAuthUser(d, auth)
		if err != nil {
			return nil, err
		}
		if payload != nil {
			return payload, nil
		}
	}
	return map[string]any{
		"id":    auth.ID,
		"email": auth.Email,
		"roles": append([]string(nil), auth.Roles...),
	}, nil
}

func (d *Database) BuildAuthMePayload(auth *AuthContext) (map[string]any, error) {
	if auth == nil {
		return nil, fmt.Errorf("authentication required")
	}
	if d != nil && d.buildAuthMe != nil {
		payload, err := d.buildAuthMe(d, auth)
		if err != nil {
			return nil, err
		}
		if payload != nil {
			if _, ok := payload["user"]; !ok {
				user, userErr := d.BuildAuthUserPayload(auth)
				if userErr == nil {
					payload["user"] = user
				}
			}
			if _, ok := payload["ok"]; !ok {
				payload["ok"] = true
			}
			return payload, nil
		}
	}
	user, err := d.BuildAuthUserPayload(auth)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":   true,
		"user": user,
	}, nil
}

func (d *Database) BuildAuthMeReads(auth *AuthContext) []string {
	if auth == nil {
		return nil
	}
	if d != nil && d.readAuthMe != nil {
		reads := d.readAuthMe(d, auth)
		if len(reads) == 0 {
			return nil
		}
		return append([]string(nil), reads...)
	}
	return nil
}

// Table returns a table instance by name.
func (d *Database) Table(name string) *TableInstance {
	ti := d.db.GetTable(name)
	if ti == nil {
		return nil
	}
	tableID := -1
	if id, ok := d.tableNameToID[name]; ok {
		tableID = id
	}
	return &TableInstance{
		ti:            ti,
		db:            d,
		name:          name,
		tableID:       tableID,
		spec:          d.tableSpecs[name],
		policy:        d.tablePolicy[name],
		enforcePolicy: false,
	}
}

func (d *Database) trackedAccessor(tracker *tableAccessTracker, auth *AuthContext) *DBAccessor {
	return &DBAccessor{db: d, tracker: tracker, auth: auth, enforcePolicy: true}
}

// Checkpoint flushes all pending writes to disk.
func (d *Database) Checkpoint() error {
	for _, t := range d.db.Tables {
		if err := t.Checkpoint(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the database and stops cron jobs.
func (d *Database) Close() error {
	if d.backupManager != nil {
		d.backupManager.stop()
	}
	if d.backgroundStop != nil {
		close(d.backgroundStop)
		d.backgroundStop = nil
	}
	d.backgroundWG.Wait()
	if d.cronRunner != nil {
		d.cronRunner.Stop()
	}
	return d.db.Close()
}

func (d *Database) reopen() (*Database, error) {
	if d == nil || d.app == nil {
		return nil, fmt.Errorf("database app context unavailable")
	}
	reopened, err := d.app.Open()
	if err != nil {
		return nil, err
	}
	if reopened.backgroundStop != nil {
		close(reopened.backgroundStop)
	}
	reopened.backgroundWG.Wait()
	reopened.backgroundStop = nil

	d.app = reopened.app
	d.db = reopened.db
	d.authService = reopened.authService
	d.superadminService = reopened.superadminService
	d.emailSettings = reopened.emailSettings
	d.mailer = reopened.mailer
	d.jwtSecret = reopened.jwtSecret
	d.authInstanceID = reopened.authInstanceID
	d.requestLogRetention = reopened.requestLogRetention
	d.authSessionRetention = reopened.authSessionRetention
	d.authSessionCleanup = reopened.authSessionCleanup
	d.enablePprof = reopened.enablePprof
	d.analytics = reopened.analytics
	d.mediaIndexRebuild = reopened.mediaIndexRebuild
	d.cronRunner = reopened.cronRunner
	d.backgroundStop = make(chan struct{})
	d.buildAuthUser = reopened.buildAuthUser
	d.buildAuthMe = reopened.buildAuthMe
	d.readAuthMe = reopened.readAuthMe
	d.tableNames = reopened.tableNames
	d.tableNameToID = reopened.tableNameToID
	d.tableSpecs = reopened.tableSpecs
	d.tablePolicy = reopened.tablePolicy
	d.materialized = reopened.materialized
	d.cascadeRefs = reopened.cascadeRefs
	d.backupManager = reopened.backupManager
	if d.backupManager != nil {
		d.backupManager.db = d
	}
	d.startBackgroundWorkers()
	return d, nil
}

func (d *Database) startBackgroundWorkers() {
	if d == nil || d.db == nil {
		return
	}
	if d.db.GetTable(systemAuthSessionTableName) != nil {
		d.backgroundWG.Add(1)
		go func() {
			defer d.backgroundWG.Done()
			d.runAuthSessionCleanupLoop()
		}()
	}
}

func (d *Database) runAuthSessionCleanupLoop() {
	if d == nil || d.authSessionCleanup <= 0 || d.authSessionRetention <= 0 {
		return
	}
	d.cleanupExpiredAuthSessions(time.Now())
	ticker := time.NewTicker(d.authSessionCleanup)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.cleanupExpiredAuthSessions(time.Now())
		case <-d.backgroundStop:
			return
		}
	}
}

func (d *Database) cleanupExpiredAuthSessions(now time.Time) (int, error) {
	if d == nil || d.authSessionRetention <= 0 {
		return 0, nil
	}
	sessionTable := d.Table(systemAuthSessionTableName)
	if sessionTable == nil {
		return 0, nil
	}
	cutoff := now.Add(-d.authSessionRetention).Unix()
	const chunkSize = 512
	total := sessionTable.Count()
	if total <= 0 {
		return 0, nil
	}
	staleIDs := make([]string, 0, 32)
	for offset := 0; offset < total; offset += chunkSize {
		rows, err := sessionTable.Scan(chunkSize, offset)
		if err != nil {
			return len(staleIDs), err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if !authSessionShouldCleanup(row, cutoff) {
				continue
			}
			id := strings.TrimSpace(toString(row["id"]))
			if id != "" {
				staleIDs = append(staleIDs, id)
			}
		}
	}
	deleted := 0
	for _, id := range staleIDs {
		ok, err := sessionTable.Delete(id)
		if err != nil {
			return deleted, err
		}
		if ok {
			deleted++
		}
	}
	return deleted, nil
}

func authSessionShouldCleanup(row map[string]any, cutoff int64) bool {
	if row == nil {
		return false
	}
	if revoked := authSessionTimestamp(row["revoked_at"]); revoked > 0 && revoked <= cutoff {
		return true
	}
	if expires := authSessionTimestamp(row["expires_at"]); expires > 0 && expires <= cutoff {
		return true
	}
	return false
}

func authSessionTimestamp(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return int64(f)
		}
	case string:
		n = strings.TrimSpace(n)
		if n == "" {
			return 0
		}
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int64(f)
		}
	}
	return 0
}

// GetDataDir returns the data directory path for this database.
func (d *Database) GetDataDir() string {
	return d.db.GetDataDir()
}

// FileHandler returns an http.Handler that serves files stored by flop.
// Mount it at "/api/files/" on your mux.
func (d *Database) FileHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/api/files/")
		if rel == "" || !d.filePathIsCurrentlyReferenced(rel) {
			http.NotFound(w, r)
			return
		}
		parts := strings.SplitN(rel, "/", 4)
		thumbParam := r.URL.Query().Get("thumb")
		if thumbParam == "" || len(parts) < 4 {
			filePath := filepath.Join(d.db.GetDataDir(), "_files", rel)
			http.ServeFile(w, r, filePath)
			return
		}

		tableName, rowID, fieldName, filename := parts[0], parts[1], parts[2], parts[3]
		ti := d.db.GetTable(tableName)
		if ti == nil {
			http.NotFound(w, r)
			return
		}
		field := ti.GetDef().CompiledSchema.FieldMap[fieldName]
		if field == nil || len(field.ThumbSizes) == 0 || !images.IsThumbAllowed(thumbParam, field.ThumbSizes) {
			http.Error(w, "thumb size not allowed", http.StatusBadRequest)
			return
		}
		size, err := images.ParseThumbSize(thumbParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		thumbPath := images.ThumbPath(d.db.GetDataDir(), tableName, rowID, fieldName, filename, size)
		if _, err := os.Stat(thumbPath); err == nil {
			http.ServeFile(w, r, thumbPath)
			return
		}

		srcPath := filepath.Join(d.db.GetDataDir(), "_files", tableName, rowID, fieldName, filename)
		if _, err := os.Stat(srcPath); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		if err := images.GenerateThumb(srcPath, thumbPath, size); err != nil {
			http.Error(w, "thumbnail generation failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		http.ServeFile(w, r, thumbPath)
	})
}

func (d *Database) filePathIsCurrentlyReferenced(rel string) bool {
	if d == nil || d.db == nil {
		return false
	}
	rel = strings.TrimSpace(rel)
	if rel == "" || strings.Contains(rel, "..") {
		return false
	}
	parts := strings.Split(rel, "/")
	if len(parts) < 4 {
		return false
	}
	tableName, rowID, fieldName := parts[0], parts[1], parts[2]
	ti := d.db.GetTable(tableName)
	if ti == nil {
		return false
	}
	field := ti.GetDef().CompiledSchema.FieldMap[fieldName]
	if field == nil {
		return false
	}
	row, err := ti.Get(rowID)
	if err != nil || row == nil {
		return false
	}
	targetPath := "_files/" + strings.Join(parts[:4], "/")
	for _, ref := range adminCollectMediaRefs(row[fieldName], field.Kind) {
		if strings.TrimSpace(ref.Path) == targetPath {
			return true
		}
	}
	return false
}

// RequestAnalytics returns the process-local analytics collector for this DB.
// Data is persisted under dataDir/_system/request_logs.ndjson.
func (d *Database) RequestAnalytics() *server.RequestAnalytics {
	if d == nil || d.db == nil {
		return nil
	}
	d.analyticsMu.Lock()
	defer d.analyticsMu.Unlock()
	if d.analytics == nil {
		path := filepath.Join(d.db.GetDataDir(), "_system", "request_logs.ndjson")
		d.analytics = server.NewRequestAnalyticsWithStorage(d.requestLogRetention, path)
	}
	return d.analytics
}

// RecordAnalyticsEvent appends a structured analytics event to the retained admin analytics log.
func (d *Database) RecordAnalyticsEvent(event AnalyticsEvent) {
	if d == nil {
		return
	}
	analytics := d.RequestAnalytics()
	if analytics == nil {
		return
	}
	analytics.Record(server.AnalyticsEvent{
		Timestamp:    event.Timestamp,
		RouteType:    event.RouteType,
		RouteName:    event.RouteName,
		Method:       event.Method,
		Path:         event.Path,
		Transport:    event.Transport,
		Duration:     event.Duration,
		OK:           event.OK,
		StatusCode:   event.StatusCode,
		ErrorMessage: event.ErrorMessage,
		UserID:       event.UserID,
		Details:      event.Details,
	})
}

func (d *Database) RefreshMaterialized(name string) error {
	if d == nil {
		return fmt.Errorf("flop: database is nil")
	}
	rt, ok := d.materialized[name]
	if !ok || rt == nil || rt.spec == nil || rt.spec.Refresh == nil {
		return fmt.Errorf("flop: materialized table not configured: %s", name)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err := d.repairTableIndexes(name); err != nil {
		rt.lastError = err.Error()
		return err
	}
	if err := rt.spec.Refresh(d); err != nil {
		rt.lastError = err.Error()
		return err
	}
	rt.lastRefresh = time.Now()
	rt.lastError = ""
	return nil
}

func (d *Database) repairTableIndexes(name string) error {
	if d == nil || d.db == nil {
		return fmt.Errorf("flop: database is nil")
	}
	ti, ok := d.db.Tables[name]
	if !ok || ti == nil {
		return fmt.Errorf("flop: unknown table: %s", name)
	}
	return ti.RepairIndexesIfNeeded()
}

// RepairTableIndexes rebuilds in-memory indexes when persisted index health
// diverges from the table file row state.
func (d *Database) RepairTableIndexes(name string) error {
	return d.repairTableIndexes(name)
}

// RebuildSecondaryIndexes forces a full secondary-index rebuild for one table.
func (d *Database) RebuildSecondaryIndexes(name string) error {
	if d == nil || d.db == nil {
		return fmt.Errorf("flop: database is nil")
	}
	ti, ok := d.db.Tables[name]
	if !ok || ti == nil {
		return fmt.Errorf("flop: unknown table: %s", name)
	}
	return ti.ForceRebuildSecondaryIndexes()
}

func (d *Database) materializedStatus(name string) (bool, time.Time, string) {
	if d == nil {
		return false, time.Time{}, ""
	}
	rt, ok := d.materialized[name]
	if !ok || rt == nil {
		return false, time.Time{}, ""
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return true, rt.lastRefresh, rt.lastError
}

// Insert inserts a row into the table. Returns the inserted row
// (with auto-generated fields filled).
func (ti *TableInstance) Insert(data map[string]any) (map[string]any, error) {
	return ti.insertWithTx(data, nil)
}

func (ti *TableInstance) insertWithTx(data map[string]any, tx *archiveTxn) (map[string]any, error) {
	ti.markWrite()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	if ti.enforcePolicy {
		if ti.requiresInsertPolicy() && !ti.allowInsert(data) {
			return nil, ErrAccessDenied
		}
		if ti.requiresFieldWritePolicyForIncoming(data) {
			if err := ti.checkWritableFields(nil, data, data); err != nil {
				return nil, err
			}
		}
	}
	var txBuf map[string]*engine.WalBufEntry
	if tx != nil {
		txBuf = tx.txBuf
	}
	row, err := ti.ti.Insert(data, txBuf)
	if err != nil {
		return nil, err
	}
	if txBuf == nil && tableDefCanContainMedia(ti.ti.GetDef()) {
		_ = ti.db.applyMediaIndexOps(mediaIndexSyncRowOp(ti.name, row))
	}
	if ti.enforcePolicy && ti.requiresReadFiltering() {
		row, ok := ti.filterReadableRow(row)
		if !ok {
			return nil, nil
		}
		return row, nil
	}
	return row, nil
}

// InsertMany inserts rows in buffered batches for higher import throughput.
// Returns the number of inserted rows.
func (ti *TableInstance) InsertMany(rows []map[string]any, flushEvery int) (int, error) {
	ti.markWrite()
	if ti.isNil() {
		return 0, fmt.Errorf("table is nil")
	}
	if ti.enforcePolicy {
		checkInsert := ti.requiresInsertPolicy()
		for _, row := range rows {
			if checkInsert && !ti.allowInsert(row) {
				return 0, ErrAccessDenied
			}
			if ti.requiresFieldWritePolicyForIncoming(row) {
				if err := ti.checkWritableFields(nil, row, row); err != nil {
					return 0, err
				}
			}
		}
	}
	return ti.ti.BulkInsert(rows, flushEvery)
}

// Get retrieves a row by primary key.
func (ti *TableInstance) Get(pk string) (map[string]any, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	ti.ensureMaterializedReadable()
	row, err := ti.ti.Get(pk)
	if err != nil || row == nil {
		return row, err
	}
	if !ti.enforcePolicy || !ti.requiresReadFiltering() {
		return row, nil
	}
	filtered, ok := ti.filterReadableRow(row)
	if !ok {
		return nil, nil
	}
	return filtered, nil
}

// Update updates a row by primary key. Returns the updated row.
func (ti *TableInstance) Update(pk string, fields map[string]any) (map[string]any, error) {
	return ti.updateWithTx(pk, fields, nil)
}

func (ti *TableInstance) updateWithTx(pk string, fields map[string]any, tx *archiveTxn) (map[string]any, error) {
	ti.markWrite()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	needsUpdatePolicy := ti.enforcePolicy && ti.requiresUpdatePolicy()
	needsFieldWrite := ti.enforcePolicy && ti.requiresFieldWritePolicyForIncoming(fields)
	if needsUpdatePolicy || needsFieldWrite {
		oldRow, err := ti.ti.Get(pk)
		if err != nil {
			return nil, err
		}
		if oldRow == nil {
			return nil, fmt.Errorf("row not found")
		}
		nextRow := cloneRow(oldRow)
		for k, v := range fields {
			nextRow[k] = v
		}
		if needsUpdatePolicy && !ti.allowUpdate(oldRow, nextRow) {
			return nil, ErrAccessDenied
		}
		if needsFieldWrite {
			if err := ti.checkWritableFields(oldRow, nextRow, fields); err != nil {
				return nil, err
			}
		}
	}
	var removedRefs []schema.FileRef
	oldRowForCleanup, err := ti.ti.Get(pk)
	if err != nil {
		return nil, err
	}
	if oldRowForCleanup == nil {
		return nil, fmt.Errorf("row not found")
	}
	if tableDefCanContainMedia(ti.ti.GetDef()) {
		nextRow := cloneRow(oldRowForCleanup)
		for k, v := range fields {
			nextRow[k] = v
		}
		removedRefs = removedMediaRefsForUpdate(ti.ti.GetDef(), oldRowForCleanup, nextRow, fields)
	}
	var txBuf map[string]*engine.WalBufEntry
	if tx != nil {
		txBuf = tx.txBuf
	}
	row, err := ti.ti.Update(pk, fields, txBuf)
	if err != nil {
		return nil, err
	}
	cleanupRemovedFiles := func() error {
		for _, ref := range removedRefs {
			refCopy := ref
			if err := storage.DeleteFileRef(ti.db.GetDataDir(), &refCopy); err != nil {
				return err
			}
		}
		return nil
	}
	if len(removedRefs) > 0 {
		if tx != nil {
			tx.addAfter(cleanupRemovedFiles)
		} else {
			if err := cleanupRemovedFiles(); err != nil {
				return nil, err
			}
		}
	}
	if txBuf == nil && tableDefCanContainMedia(ti.ti.GetDef()) {
		_ = ti.db.applyMediaIndexOps(mediaIndexSyncRowOp(ti.name, row))
	}
	if ti.enforcePolicy && ti.requiresReadFiltering() {
		row, ok := ti.filterReadableRow(row)
		if !ok {
			return nil, nil
		}
		return row, nil
	}
	return row, nil
}

func removedMediaRefsForUpdate(def *schema.TableDef, oldRow, newRow, updates map[string]any) []schema.FileRef {
	if def == nil || oldRow == nil || newRow == nil || len(updates) == 0 {
		return nil
	}
	var removed []schema.FileRef
	for _, field := range def.CompiledSchema.Fields {
		if field.Kind != schema.KindFileSingle && field.Kind != schema.KindFileMulti {
			continue
		}
		if _, touched := updates[field.Name]; !touched {
			continue
		}
		oldRefs := adminCollectMediaRefs(oldRow[field.Name], field.Kind)
		if len(oldRefs) == 0 {
			continue
		}
		newRefs := adminCollectMediaRefs(newRow[field.Name], field.Kind)
		newPaths := map[string]bool{}
		for _, ref := range newRefs {
			path := strings.TrimSpace(ref.Path)
			if path != "" {
				newPaths[path] = true
			}
		}
		for _, ref := range oldRefs {
			path := strings.TrimSpace(ref.Path)
			if path == "" || newPaths[path] {
				continue
			}
			removed = append(removed, ref)
		}
	}
	return removed
}

// Delete deletes a row by primary key. Returns true if the row existed.
func (ti *TableInstance) Delete(pk string) (bool, error) {
	return ti.deleteWithTx(pk, nil)
}

func (ti *TableInstance) deleteWithTx(pk string, tx *archiveTxn) (bool, error) {
	ti.markWrite()
	if ti.isNil() {
		return false, fmt.Errorf("table is nil")
	}
	if ti.enforcePolicy && ti.requiresDeletePolicy() {
		row, err := ti.ti.Get(pk)
		if err != nil {
			return false, err
		}
		if row == nil {
			return false, nil
		}
		if !ti.allowDelete(row) {
			return false, ErrAccessDenied
		}
	}
	var txBuf map[string]*engine.WalBufEntry
	if tx != nil {
		txBuf = tx.txBuf
	}
	ok, err := ti.ti.Delete(pk, txBuf)
	if err != nil {
		return false, err
	}
	if ok {
		fileCleanup := func() error {
			return storage.DeleteRowFiles(ti.db.GetDataDir(), ti.name, pk)
		}
		if tx != nil {
			tx.addAfter(fileCleanup)
		} else {
			if err := fileCleanup(); err != nil {
				return true, err
			}
		}
	}
	if ok && tx == nil && tableDefCanContainMedia(ti.ti.GetDef()) {
		_ = ti.db.applyMediaIndexOps(mediaIndexRemoveRowOp(ti.name, pk))
	}
	return ok, nil
}

type ArchiveOptions struct {
	CascadeGroupID   string
	CascadeRootTable string
	CascadeRootPK    string
	CascadeDepth     int
}

func (ti *TableInstance) Archive(pk string) (*storage.ArchivedRow, error) {
	return ti.ArchiveWithOptions(pk, ArchiveOptions{})
}

func (ti *TableInstance) ArchiveWithOptions(pk string, opts ArchiveOptions) (*storage.ArchivedRow, error) {
	ti.markWrite()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	if ti.enforcePolicy && ti.requiresDeletePolicy() {
		row, err := ti.ti.Get(pk)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if !ti.allowDelete(row) {
			return nil, ErrAccessDenied
		}
	}
	tx := newArchiveTxn(ti.db)
	rootTable := strings.TrimSpace(opts.CascadeRootTable)
	if rootTable == "" {
		rootTable = ti.name
	}
	rootPK := strings.TrimSpace(opts.CascadeRootPK)
	if rootPK == "" {
		rootPK = pk
	}
	record, err := ti.archiveCascade(tx, pk, strings.TrimSpace(opts.CascadeGroupID), rootTable, rootPK, opts.CascadeDepth)
	if err != nil {
		tx.rollback()
		return nil, err
	}
	if err := tx.commit(); err != nil {
		tx.rollback()
		return nil, err
	}
	return record, nil
}

func (ti *TableInstance) DeleteArchive(archiveID string) error {
	ti.markWrite()
	if ti.isNil() {
		return fmt.Errorf("table is nil")
	}
	record, err := ti.ti.DeleteArchived(archiveID, nil)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("archived row not found: %s", archiveID)
	}
	return storage.DeleteArchivedRowFiles(ti.db.GetDataDir(), ti.name, archiveID)
}

func (ti *TableInstance) RestoreArchive(archiveID string) error {
	ti.markWrite()
	if ti.isNil() {
		return fmt.Errorf("table is nil")
	}
	tx := newArchiveTxn(ti.db)
	if err := ti.restoreCascade(tx, archiveID, ""); err != nil {
		tx.rollback()
		return err
	}
	if err := tx.commit(); err != nil {
		tx.rollback()
		return err
	}
	return nil
}

// ReplaceAll replaces all rows in the table with the provided dataset.
func (ti *TableInstance) ReplaceAll(rows []map[string]any) error {
	ti.markWrite()
	if ti.isNil() {
		return fmt.Errorf("table is nil")
	}
	def := ti.ti.GetDef()
	if len(def.CompiledSchema.Fields) == 0 {
		return fmt.Errorf("table has no primary key")
	}
	pkField := def.CompiledSchema.Fields[0].Name
	desired := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		pk := toString(row[pkField])
		if pk == "" {
			return fmt.Errorf("row missing primary key field %s", pkField)
		}
		if _, exists := desired[pk]; exists {
			return fmt.Errorf("duplicate primary key in replace set: %s", pk)
		}
		desired[pk] = row
	}

	total := ti.ti.Count()
	if total > 0 {
		existing, err := ti.ti.Scan(total, 0)
		if err != nil {
			return err
		}
		for _, row := range existing {
			pk := toString(row[pkField])
			if pk == "" {
				continue
			}
			next, keep := desired[pk]
			if !keep {
				if _, err := ti.ti.Delete(pk, nil); err != nil {
					return err
				}
				continue
			}
			updates := cloneRow(next)
			delete(updates, pkField)
			if _, err := ti.ti.Update(pk, updates, nil); err != nil {
				return err
			}
			delete(desired, pk)
		}
	}
	if len(desired) == 0 {
		if !tableDefCanContainMedia(ti.ti.GetDef()) {
			return nil
		}
		return ti.db.applyMediaIndexOps(mediaIndexSyncTableOp(ti.name, rows))
	}
	toInsert := make([]map[string]any, 0, len(desired))
	for _, row := range desired {
		toInsert = append(toInsert, row)
	}
	_, err := ti.ti.BulkInsert(toInsert, 1000)
	if err != nil {
		return err
	}
	if !tableDefCanContainMedia(ti.ti.GetDef()) {
		return nil
	}
	return ti.db.applyMediaIndexOps(mediaIndexSyncTableOp(ti.name, rows))
}

// Scan returns rows with pagination.
func (ti *TableInstance) Scan(limit, offset int) ([]map[string]any, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	ti.ensureMaterializedReadable()
	if !ti.requiresReadFiltering() {
		return ti.ti.Scan(limit, offset)
	}
	if !ti.requiresRowReadFiltering() {
		rows, err := ti.ti.Scan(limit, offset)
		if err != nil {
			return nil, err
		}
		out := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			filtered, ok := ti.filterReadableRow(row)
			if !ok {
				continue
			}
			out = append(out, filtered)
		}
		return out, nil
	}
	if limit <= 0 {
		return []map[string]any{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	total := ti.ti.Count()
	if total <= 0 {
		return []map[string]any{}, nil
	}
	chunkSize := limit * 2
	if chunkSize < 256 {
		chunkSize = 256
	}
	if chunkSize > 4096 {
		chunkSize = 4096
	}
	physicalOffset := 0
	visibleSkipped := 0
	out := make([]map[string]any, 0, limit)
	for physicalOffset < total && len(out) < limit {
		rows, err := ti.ti.Scan(chunkSize, physicalOffset)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		physicalOffset += len(rows)
		for _, row := range rows {
			filtered, ok := ti.filterReadableRow(row)
			if !ok {
				continue
			}
			if visibleSkipped < offset {
				visibleSkipped++
				continue
			}
			out = append(out, filtered)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// Count returns the number of rows.
func (ti *TableInstance) Count() int {
	ti.markRead()
	if ti.isNil() {
		return 0
	}
	ti.ensureMaterializedReadable()
	if !ti.requiresRowReadFiltering() {
		return ti.ti.Count()
	}
	total := ti.ti.Count()
	if total <= 0 {
		return 0
	}
	const chunkSize = 512
	physicalOffset := 0
	visible := 0
	for physicalOffset < total {
		rows, err := ti.ti.Scan(chunkSize, physicalOffset)
		if err != nil || len(rows) == 0 {
			break
		}
		physicalOffset += len(rows)
		for _, row := range rows {
			if ti.allowRead(row) {
				visible++
			}
		}
	}
	return visible
}

func (ti *TableInstance) SecondaryIndexesReady() bool {
	ti.markRead()
	if ti.isNil() {
		return false
	}
	return ti.ti.SecondaryIndexesReady()
}

// RebuildSecondaryIndexes forces a full rebuild of this table's secondary indexes.
func (ti *TableInstance) RebuildSecondaryIndexes() error {
	ti.markRead()
	if ti.isNil() {
		return fmt.Errorf("table is nil")
	}
	return ti.ti.ForceRebuildSecondaryIndexes()
}

// FindByEmail finds a row by the "email" unique index.
func (ti *TableInstance) FindByEmail(email string) (map[string]any, bool) {
	ti.markRead()
	if ti.isNil() {
		return nil, false
	}
	ptr, ok := ti.ti.FindByIndex([]string{"email"}, email)
	if !ok {
		return nil, false
	}
	row, err := ti.ti.GetByPointer(ptr)
	if err != nil || row == nil {
		return nil, false
	}
	filtered, ok := ti.filterReadableRow(row)
	if !ok {
		return nil, false
	}
	return filtered, true
}

// FindByUniqueIndex finds a row by a unique index on the given field.
func (ti *TableInstance) FindByUniqueIndex(field string, value any) (map[string]any, bool) {
	ti.markRead()
	if ti.isNil() {
		return nil, false
	}
	ptr, ok := ti.ti.FindByIndex([]string{field}, value)
	if !ok {
		return nil, false
	}
	row, err := ti.ti.GetByPointer(ptr)
	if err != nil || row == nil {
		return nil, false
	}
	filtered, ok := ti.filterReadableRow(row)
	if !ok {
		return nil, false
	}
	return filtered, true
}

// FindByUniqueCompositeIndex finds a row by a unique composite index.
// Values are matched against fields in order.
func (ti *TableInstance) FindByUniqueCompositeIndex(fields []string, values ...any) (map[string]any, bool) {
	ti.markRead()
	if ti.isNil() {
		return nil, false
	}
	if len(fields) == 0 || len(fields) != len(values) {
		return nil, false
	}
	ptr, ok := ti.ti.FindByIndex(fields, values)
	if !ok {
		return nil, false
	}
	row, err := ti.ti.GetByPointer(ptr)
	if err != nil || row == nil {
		return nil, false
	}
	filtered, ok := ti.filterReadableRow(row)
	if !ok {
		return nil, false
	}
	return filtered, true
}

// CountByIndex returns the number of rows matching a non-unique index value.
func (ti *TableInstance) CountByIndex(field string, value any) int {
	ti.markRead()
	if ti.isNil() {
		return 0
	}
	ptrs := ti.ti.FindAllByIndex([]string{field}, value)
	if !ti.requiresRowReadFiltering() {
		return len(ptrs)
	}
	count := 0
	for _, ptr := range ptrs {
		row, err := ti.ti.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		if ti.allowRead(row) {
			count++
		}
	}
	return count
}

// FindByIndex returns all rows matching a non-unique index value.
func (ti *TableInstance) FindByIndex(field string, value any) ([]map[string]any, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	ptrs := ti.ti.FindAllByIndex([]string{field}, value)
	rows := make([]map[string]any, 0, len(ptrs))
	for _, ptr := range ptrs {
		row, err := ti.ti.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		filtered, ok := ti.filterReadableRow(row)
		if !ok {
			continue
		}
		rows = append(rows, filtered)
	}
	return rows, nil
}

// FindByCompositeIndex returns all rows matching a composite index value.
// Values are matched against fields in order.
func (ti *TableInstance) FindByCompositeIndex(fields []string, values ...any) ([]map[string]any, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	if len(fields) == 0 || len(fields) != len(values) {
		return []map[string]any{}, nil
	}
	ptrs := ti.ti.FindAllByIndex(fields, values)
	rows := make([]map[string]any, 0, len(ptrs))
	for _, ptr := range ptrs {
		row, err := ti.ti.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		filtered, ok := ti.filterReadableRow(row)
		if !ok {
			continue
		}
		rows = append(rows, filtered)
	}
	return rows, nil
}

// SearchFullText searches a configured full-text index on the selected fields.
func (ti *TableInstance) SearchFullText(fields []string, query string, limit int) ([]map[string]any, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	rows, err := ti.ti.SearchFullText(fields, query, limit)
	if err != nil {
		return nil, err
	}
	if !ti.requiresReadFiltering() {
		return rows, nil
	}
	if limit <= 0 {
		limit = len(rows)
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		filtered, ok := ti.filterReadableRow(row)
		if !ok {
			continue
		}
		out = append(out, filtered)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// NewAutocompleteIndex creates a reusable autocomplete index.
func NewAutocompleteIndex(entries []AutocompleteEntry) *AutocompleteIndex {
	return &AutocompleteIndex{idx: engine.NewAutocompleteIndex(entries)}
}

// Add inserts or updates autocomplete entries.
func (a *AutocompleteIndex) Add(entries []AutocompleteEntry) {
	if a == nil || a.idx == nil {
		return
	}
	a.idx.Add(entries)
}

// Query returns up to limit matching autocomplete entries.
func (a *AutocompleteIndex) Query(prefix string, limit int) []AutocompleteEntry {
	if a == nil || a.idx == nil {
		return []AutocompleteEntry{}
	}
	return a.idx.Query(prefix, limit)
}

// BuildAutocompleteEntries scans this table and builds entries for reuse
// in NewAutocompleteIndex.
func (ti *TableInstance) BuildAutocompleteEntries(keyField, textField string, payloadFields ...string) ([]AutocompleteEntry, error) {
	ti.markRead()
	if ti.isNil() {
		return nil, fmt.Errorf("table is nil")
	}
	if ti.requiresReadFiltering() {
		return nil, ErrAccessDenied
	}
	return ti.ti.BuildAutocompleteEntries(keyField, textField, payloadFields...)
}

func (ti *TableInstance) requiresReadFiltering() bool {
	return ti != nil && ti.enforcePolicy && ti.policy.requiresReadFiltering
}

func (ti *TableInstance) requiresRowReadFiltering() bool {
	return ti != nil && ti.enforcePolicy && ti.policy.requiresRowRead
}

func (ti *TableInstance) requiresInsertPolicy() bool {
	return ti != nil && ti.enforcePolicy && ti.policy.hasInsertPolicy
}

func (ti *TableInstance) requiresUpdatePolicy() bool {
	return ti != nil && ti.enforcePolicy && ti.policy.hasUpdatePolicy
}

func (ti *TableInstance) requiresDeletePolicy() bool {
	return ti != nil && ti.enforcePolicy && ti.policy.hasDeletePolicy
}

func (ti *TableInstance) requiresFieldWritePolicyForIncoming(incoming map[string]any) bool {
	if ti == nil || !ti.enforcePolicy || len(incoming) == 0 || len(ti.policy.writableFields) == 0 {
		return false
	}
	for key := range incoming {
		if _, ok := ti.policy.writableFields[key]; ok {
			return true
		}
	}
	return false
}

func (ti *TableInstance) allowRead(row map[string]any) bool {
	if ti == nil || !ti.enforcePolicy || ti.spec == nil || ti.spec.Access.Read == nil {
		return true
	}
	return ti.spec.Access.Read(&TableReadCtx{Auth: ti.auth, Row: row})
}

func (ti *TableInstance) allowInsert(next map[string]any) bool {
	if ti == nil || !ti.enforcePolicy || ti.spec == nil || ti.spec.Access.Insert == nil {
		return true
	}
	return ti.spec.Access.Insert(&TableInsertCtx{Auth: ti.auth, New: next})
}

func (ti *TableInstance) allowUpdate(oldRow, next map[string]any) bool {
	if ti == nil || !ti.enforcePolicy || ti.spec == nil || ti.spec.Access.Update == nil {
		return true
	}
	return ti.spec.Access.Update(&TableUpdateCtx{Auth: ti.auth, Old: oldRow, New: next})
}

func (ti *TableInstance) allowDelete(row map[string]any) bool {
	if ti == nil || !ti.enforcePolicy || ti.spec == nil || ti.spec.Access.Delete == nil {
		return true
	}
	return ti.spec.Access.Delete(&TableDeleteCtx{Auth: ti.auth, Row: row})
}

func (ti *TableInstance) filterReadableRow(row map[string]any) (map[string]any, bool) {
	if row == nil {
		return nil, false
	}
	if !ti.allowRead(row) {
		return nil, false
	}
	if ti == nil || !ti.enforcePolicy || ti.spec == nil {
		return row, true
	}
	if !ti.policy.hasFieldReadFiltering {
		return row, true
	}
	out := cloneRow(row)
	readCtx := &TableReadCtx{Auth: ti.auth, Row: row}
	for _, fs := range ti.spec.Fields {
		if fs.Access.Read == nil {
			continue
		}
		if !fs.Access.Read(readCtx) {
			delete(out, fs.JSONName)
		}
	}
	return out, true
}

func (ti *TableInstance) checkWritableFields(oldRow, nextRow, incoming map[string]any) error {
	if ti == nil || !ti.enforcePolicy || ti.spec == nil || len(incoming) == 0 {
		return nil
	}
	for key, value := range incoming {
		fs := ti.fieldByJSONName(key)
		if fs == nil || fs.Access.Write == nil {
			continue
		}
		if !fs.Access.Write(&FieldWriteCtx{
			Auth:  ti.auth,
			Field: key,
			Old:   oldRow,
			New:   nextRow,
			Value: value,
		}) {
			return ErrAccessDenied
		}
	}
	return nil
}

func (ti *TableInstance) archiveCascade(tx *archiveTxn, pk, cascadeGroupID, rootTable, rootPK string, depth int) (*storage.ArchivedRow, error) {
	if tx == nil || ti == nil || ti.ti == nil {
		return nil, fmt.Errorf("archive transaction unavailable")
	}
	if cascadeGroupID == "" {
		group, err := newArchiveGroupID()
		if err != nil {
			return nil, err
		}
		cascadeGroupID = group
	}
	record, err := ti.ti.Archive(pk, engine.ArchiveOptions{
		DeletedBy:        archiveDeletedBy(ti.auth),
		CascadeGroupID:   cascadeGroupID,
		CascadeRootTable: rootTable,
		CascadeRootPK:    rootPK,
		CascadeDepth:     depth,
	}, tx.txBuf)
	if err != nil || record == nil {
		return record, err
	}
	tx.addUndo(func() { _ = ti.ti.RollbackArchive(record) })
	tx.addAfter(func() error {
		return storage.ArchiveRowFiles(ti.db.GetDataDir(), ti.name, pk, record.ArchiveID)
	})
	if tableDefCanContainMedia(ti.ti.GetDef()) {
		tx.addMedia(mediaIndexRemoveRowOp(ti.name, pk))
	}

	for _, ref := range ti.db.cascadeRefs[ti.name] {
		child := ti.db.Table(ref.tableName)
		if child == nil {
			continue
		}
		child.auth = ti.auth
		child.enforcePolicy = ti.enforcePolicy
		rows, err := child.rowsReferencing(ref.fieldName, pk, ref.multi)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			childPK := toString(row[child.primaryKeyField()])
			if childPK == "" {
				continue
			}
			if _, err := child.archiveCascade(tx, childPK, cascadeGroupID, rootTable, rootPK, depth+1); err != nil {
				return nil, err
			}
		}
	}
	return record, nil
}

func (ti *TableInstance) restoreCascade(tx *archiveTxn, archiveID, groupID string) error {
	if tx == nil || ti == nil || ti.ti == nil {
		return fmt.Errorf("restore transaction unavailable")
	}
	record, _, err := ti.ti.GetArchived(archiveID)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("archived row not found: %s", archiveID)
	}
	if groupID == "" {
		groupID = record.CascadeGroupID
	}
	restoredRecord, restoredRow, err := ti.ti.RestoreArchived(archiveID, tx.txBuf)
	if err != nil {
		return err
	}
	tx.addUndo(func() { _ = ti.ti.RollbackRestore(restoredRecord) })
	tx.addAfter(func() error {
		pk := restoredRecord.OriginalPK
		if pk == "" && restoredRow != nil {
			pk = toString(restoredRow[ti.primaryKeyField()])
		}
		return storage.RestoreArchivedRowFiles(ti.db.GetDataDir(), ti.name, archiveID, pk)
	})
	if restoredRow != nil && tableDefCanContainMedia(ti.ti.GetDef()) {
		tx.addMedia(mediaIndexSyncRowOp(ti.name, restoredRow))
	}

	if groupID == "" {
		return nil
	}
	children, err := ti.db.findArchivedGroup(groupID)
	if err != nil {
		return err
	}
	for _, item := range children {
		if item == nil || item.record == nil || item.record.ArchiveID == archiveID {
			continue
		}
		if item.record.CascadeDepth <= record.CascadeDepth {
			continue
		}
		restoredChild, restoredChildRow, err := item.table.ti.RestoreArchived(item.record.ArchiveID, tx.txBuf)
		if err != nil {
			return err
		}
		table := item.table
		tx.addUndo(func() { _ = table.ti.RollbackRestore(restoredChild) })
		tx.addAfter(func() error {
			pk := restoredChild.OriginalPK
			if pk == "" && restoredChildRow != nil {
				pk = toString(restoredChildRow[table.primaryKeyField()])
			}
			return storage.RestoreArchivedRowFiles(table.db.GetDataDir(), table.name, restoredChild.ArchiveID, pk)
		})
		if restoredChildRow != nil && tableDefCanContainMedia(table.ti.GetDef()) {
			tx.addMedia(mediaIndexSyncRowOp(table.name, restoredChildRow))
		}
	}
	return nil
}

func (ti *TableInstance) rowsReferencing(fieldName, pk string, multi bool) ([]map[string]any, error) {
	total := ti.ti.Count()
	if total <= 0 {
		return []map[string]any{}, nil
	}
	rows, err := ti.ti.Scan(total, 0)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0)
	for _, row := range rows {
		val := row[fieldName]
		if !multi {
			if toString(val) == pk {
				out = append(out, row)
			}
			continue
		}
		switch items := val.(type) {
		case []interface{}:
			for _, item := range items {
				if toString(item) == pk {
					out = append(out, row)
					break
				}
			}
		case []string:
			for _, item := range items {
				if item == pk {
					out = append(out, row)
					break
				}
			}
		}
	}
	return out, nil
}

func (ti *TableInstance) primaryKeyField() string {
	if ti == nil || ti.ti == nil || ti.ti.GetDef() == nil || len(ti.ti.GetDef().CompiledSchema.Fields) == 0 {
		return "id"
	}
	return ti.ti.GetDef().CompiledSchema.Fields[0].Name
}

func (ti *TableInstance) rawRow(pk string) ([]byte, error) {
	if ti == nil || ti.ti == nil {
		return nil, fmt.Errorf("table is nil")
	}
	return ti.ti.GetRaw(pk)
}

func (ti *TableInstance) rollbackInserted(pk string) error {
	if ti == nil || ti.ti == nil {
		return fmt.Errorf("table is nil")
	}
	return ti.ti.RollbackInsert(pk)
}

func (ti *TableInstance) rollbackRawRow(raw []byte) error {
	if ti == nil || ti.ti == nil {
		return fmt.Errorf("table is nil")
	}
	return ti.ti.RollbackRawRow(raw)
}

type archivedGroupItem struct {
	table  *TableInstance
	record *storage.ArchivedRow
}

func (d *Database) findArchivedGroup(groupID string) ([]*archivedGroupItem, error) {
	if d == nil || d.db == nil {
		return nil, nil
	}
	items := make([]*archivedGroupItem, 0)
	for name := range d.db.Tables {
		table := d.Table(name)
		if table == nil {
			continue
		}
		records, _, err := table.ti.ScanArchived(1_000_000, 0)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if record != nil && record.CascadeGroupID == groupID {
				items = append(items, &archivedGroupItem{table: table, record: record})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].record.CascadeDepth == items[j].record.CascadeDepth {
			return items[i].record.DeletedAtUnixMs < items[j].record.DeletedAtUnixMs
		}
		return items[i].record.CascadeDepth < items[j].record.CascadeDepth
	})
	return items, nil
}

func newArchiveGroupID() (string, error) {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return "g_" + hex.EncodeToString(buf[:]), nil
}

func (ti *TableInstance) fieldByJSONName(name string) *fieldSpec {
	if ti == nil || ti.spec == nil {
		return nil
	}
	for _, fs := range ti.spec.Fields {
		if fs.JSONName == name {
			return fs
		}
	}
	return nil
}

func buildTablePolicyMeta(spec *tableSpec) tablePolicyMeta {
	if spec == nil {
		return tablePolicyMeta{}
	}
	meta := tablePolicyMeta{
		hasInsertPolicy: spec.Access.Insert != nil,
		hasUpdatePolicy: spec.Access.Update != nil,
		hasDeletePolicy: spec.Access.Delete != nil,
		requiresRowRead: spec.Access.Read != nil,
	}
	if meta.requiresRowRead {
		meta.requiresReadFiltering = true
	}
	for _, fs := range spec.Fields {
		if fs.Access.Read != nil {
			meta.hasFieldReadFiltering = true
			meta.requiresReadFiltering = true
		}
		if fs.Access.Write != nil {
			if meta.writableFields == nil {
				meta.writableFields = make(map[string]struct{})
			}
			meta.writableFields[fs.JSONName] = struct{}{}
		}
	}
	return meta
}

func cloneRow(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

func newArchiveTxn(db *Database) *archiveTxn {
	return &archiveTxn{
		db:    db,
		txBuf: make(map[string]*engine.WalBufEntry),
	}
}

func (tx *archiveTxn) addUndo(fn func()) {
	if tx == nil || fn == nil {
		return
	}
	tx.undo = append(tx.undo, fn)
}

func (tx *archiveTxn) addMedia(op mediaIndexOp) {
	if tx == nil || op == nil {
		return
	}
	tx.media = append(tx.media, op)
}

func (tx *archiveTxn) addAfter(fn func() error) {
	if tx == nil || fn == nil {
		return
	}
	tx.after = append(tx.after, fn)
}

func (tx *archiveTxn) rollback() {
	if tx == nil || tx.closed {
		return
	}
	for i := len(tx.undo) - 1; i >= 0; i-- {
		tx.undo[i]()
	}
	tx.closed = true
}

func (tx *archiveTxn) commit() error {
	if tx == nil || tx.closed {
		return nil
	}
	if testArchiveCommitHook != nil {
		if err := testArchiveCommitHook(); err != nil {
			return err
		}
	}
	if len(tx.txBuf) > 0 {
		if err := tx.db.db.EnqueueCommit(tx.txBuf); err != nil {
			return err
		}
	}
	tx.closed = true
	for _, fn := range tx.after {
		if err := fn(); err != nil {
			return err
		}
	}
	return tx.db.applyMediaIndexOps(tx.media...)
}

func archiveDeletedBy(auth *AuthContext) string {
	if auth == nil {
		return ""
	}
	if strings.TrimSpace(auth.ID) != "" {
		return auth.ID
	}
	return auth.Email
}

func (ti *TableInstance) markRead() {
	if ti == nil || ti.tracker == nil {
		return
	}
	ti.tracker.markRead(ti.tableID)
}

func (ti *TableInstance) markWrite() {
	if ti == nil || ti.tracker == nil {
		return
	}
	ti.tracker.markWrite(ti.tableID)
}

// BuildEngineTableDefs compiles this App schema to internal engine table defs.
func (a *App) BuildEngineTableDefs() map[string]*schema.TableDef {
	return a.buildTableDefs()
}

// buildTableDefs converts App table specs to engine TableDefs.
func (a *App) buildTableDefs() map[string]*schema.TableDef {
	if _, exists := a.tables[systemSuperadminTableName]; exists {
		panic("flop: table name reserved for system use: " + systemSuperadminTableName)
	}
	if _, exists := a.tables[systemAuthSessionTableName]; exists {
		panic("flop: table name reserved for system use: " + systemAuthSessionTableName)
	}
	defs := make(map[string]*schema.TableDef, len(a.tables)+2)

	for name, ts := range a.tables {
		fields := make([]schema.CompiledField, 0, len(ts.Fields))
		isAuth := false
		fieldByJSON := make(map[string]*fieldSpec, len(ts.Fields))

		// Sort field names for deterministic order
		fieldNames := make([]string, 0, len(ts.Fields))
		for fn := range ts.Fields {
			fieldNames = append(fieldNames, fn)
		}
		sort.Strings(fieldNames)

		var indexes []schema.IndexDef
		for _, fn := range fieldNames {
			fs := ts.Fields[fn]
			if fs.PrimaryStrategy == "autoincrement" && !isNumericKind(fs.Kind) {
				panic("flop: autoincrement primary strategy requires number/integer/timestamp field: " + fs.JSONName)
			}
			cf := schema.CompiledField{
				Name:             fs.JSONName,
				Kind:             mapKind(fs.Kind),
				Required:         fs.Required,
				Unique:           fs.Unique,
				DefaultValue:     fs.Default,
				AutoGenPattern:   fs.Autogen,
				AutoIDStrategy:   fs.PrimaryStrategy,
				BcryptRounds:     fs.BcryptRounds,
				EnumValues:       append([]string(nil), fs.EnumValues...),
				VectorDimensions: fs.VectorDimensions,
				RefTableName:     fs.RefTable,
				RefField:         fs.RefField,
				MimeTypes:        append([]string(nil), fs.MimeTypes...),
				ThumbSizes:       append([]string(nil), fs.ThumbSizes...),
				MaxUploadBytes:   fs.MaxUploadBytes,
				ImageMaxSize:     fs.ImageMaxSize,
				ImageFit:         fs.ImageFit,
				DiscardOriginal:  fs.DiscardOriginal,
				Cached:           fs.Cached,
			}
			fieldByJSON[fs.JSONName] = fs

			if fs.Kind == "roles" {
				isAuth = true
			}

			fields = append(fields, cf)

			// Build indexes
			if fs.Unique && !fs.Primary {
				indexes = append(indexes, schema.IndexDef{
					Fields: []string{fs.JSONName},
					Unique: true,
					Type:   schema.IndexTypeHash,
				})
			}
			if fs.Indexed && !fs.Unique {
				indexes = append(indexes, schema.IndexDef{
					Fields: []string{fs.JSONName},
					Unique: false,
					Type:   schema.IndexTypeHash,
				})
			}
			if fs.FullText {
				indexes = append(indexes, schema.IndexDef{
					Fields: []string{fs.JSONName},
					Unique: false,
					Type:   schema.IndexTypeFullText,
				})
			}
		}

		// Put primary key first
		sort.SliceStable(fields, func(i, j int) bool {
			fi := fieldByJSON[fields[i].Name]
			fj := fieldByJSON[fields[j].Name]
			if fi != nil && fj != nil {
				if fi.Primary && !fj.Primary {
					return true
				}
				if !fi.Primary && fj.Primary {
					return false
				}
			}
			return fields[i].Name < fields[j].Name
		})

		var migrations []schema.MigrationStep
		for _, m := range ts.Migrations {
			migrations = append(migrations, schema.MigrationStep{
				Version: m.Version,
				Rename:  m.Rename,
			})
		}

		defs[name] = &schema.TableDef{
			Name:           name,
			CompiledSchema: schema.NewCompiledSchema(fields),
			Indexes:        indexes,
			Auth:           isAuth,
			Migrations:     migrations,
		}
	}

	defs[systemSuperadminTableName] = systemSuperadminTableDef()
	defs[systemAuthSessionTableName] = systemAuthSessionTableDef()

	return defs
}

func systemSuperadminTableDef() *schema.TableDef {
	fields := []schema.CompiledField{
		{
			Name:           "id",
			Kind:           schema.KindString,
			Required:       true,
			Unique:         true,
			AutoGenPattern: "[a-z0-9]{12}",
			AutoIDStrategy: "random",
		},
		{
			Name:     "email",
			Kind:     schema.KindString,
			Required: true,
			Unique:   true,
		},
		{
			Name:         "password",
			Kind:         schema.KindBcrypt,
			Required:     true,
			BcryptRounds: 10,
		},
		{
			Name:     "name",
			Kind:     schema.KindString,
			Required: false,
		},
		{
			Name:         "createdAt",
			Kind:         schema.KindTimestamp,
			Required:     true,
			DefaultValue: "now",
		},
	}
	return &schema.TableDef{
		Name:           systemSuperadminTableName,
		CompiledSchema: schema.NewCompiledSchema(fields),
		Indexes: []schema.IndexDef{
			{
				Fields: []string{"email"},
				Unique: true,
				Type:   schema.IndexTypeHash,
			},
		},
	}
}

func systemAuthSessionTableDef() *schema.TableDef {
	fields := []schema.CompiledField{
		{
			Name:           "id",
			Kind:           schema.KindString,
			Required:       true,
			Unique:         true,
			AutoGenPattern: "[a-z0-9]{24}",
			AutoIDStrategy: "random",
		},
		{Name: "principal_type", Kind: schema.KindString, Required: true},
		{Name: "principal_id", Kind: schema.KindString, Required: true},
		{Name: "instance_id", Kind: schema.KindString, Required: true},
		{Name: "created_at", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "last_used_at", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "expires_at", Kind: schema.KindTimestamp, Required: true},
		{Name: "revoked_at", Kind: schema.KindTimestamp, Required: false},
		{Name: "replaced_by_session_id", Kind: schema.KindString, Required: false},
		{Name: "user_agent", Kind: schema.KindString, Required: false},
		{Name: "ip", Kind: schema.KindString, Required: false},
		{Name: "reason", Kind: schema.KindString, Required: false},
	}
	return &schema.TableDef{
		Name:           systemAuthSessionTableName,
		CompiledSchema: schema.NewCompiledSchema(fields),
		Indexes: []schema.IndexDef{
			{Fields: []string{"principal_id"}, Unique: false, Type: schema.IndexTypeHash},
			{Fields: []string{"instance_id"}, Unique: false, Type: schema.IndexTypeHash},
		},
	}
}

func mapKind(kind string) schema.FieldKind {
	switch kind {
	case "string":
		return schema.KindString
	case "number":
		return schema.KindNumber
	case "integer":
		return schema.KindInteger
	case "boolean":
		return schema.KindBoolean
	case "json":
		return schema.KindJson
	case "bcrypt":
		return schema.KindBcrypt
	case "refSingle", "ref":
		return schema.KindRef
	case "refMulti":
		return schema.KindRefMulti
	case "fileSingle":
		return schema.KindFileSingle
	case "fileMulti":
		return schema.KindFileMulti
	case "roles":
		return schema.KindRoles
	case "timestamp":
		return schema.KindTimestamp
	case "vector":
		return schema.KindVector
	case "set":
		return schema.KindSet
	case "enum":
		return schema.KindEnum
	default:
		return schema.KindString
	}
}

// EngineAdminProvider implements AdminProvider, AdminWriteProvider,
// AdminAuthProvider, and AdminSetupProvider using a real Database.
type EngineAdminProvider struct {
	DB        *Database
	JWTSecret string
}

func (p *EngineAdminProvider) AdminTables() ([]AdminTable, error) {
	tables := make([]AdminTable, 0, len(p.DB.db.Tables))
	for name, t := range p.DB.db.Tables {
		def := t.GetDef()
		s, _ := marshalOrderedSchema(def.CompiledSchema)
		materialized, lastRefresh, refreshError := p.DB.materializedStatus(name)
		if materialized {
			_ = p.DB.repairTableIndexes(name)
		}
		tables = append(tables, AdminTable{
			Name:                 name,
			Schema:               s,
			RowCount:             t.Count(),
			ReadOnly:             materialized,
			Materialized:         materialized,
			LastRefreshUnixMilli: lastRefresh.UnixMilli(),
			RefreshError:         refreshError,
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

func (p *EngineAdminProvider) AdminArchiveTables() ([]AdminTable, error) {
	tables := make([]AdminTable, 0, len(p.DB.db.Tables))
	for name, t := range p.DB.db.Tables {
		records, total, err := t.ScanArchived(1, 0)
		if err != nil {
			return nil, err
		}
		_ = records
		s, _ := marshalArchiveSchema(t.GetDef().CompiledSchema)
		tables = append(tables, AdminTable{
			Name:        name,
			SourceTable: name,
			Archive:     true,
			Schema:      s,
			RowCount:    total,
			ReadOnly:    true,
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

// marshalOrderedSchema produces an ordered JSON object of field definitions
// matching the format the admin SPA expects.
func marshalOrderedSchema(cs *schema.CompiledSchema) (jsonx.RawMessage, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range cs.Fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, _ := jsonx.Marshal(f.Name)
		buf.Write(key)
		buf.WriteByte(':')

		entry := map[string]any{
			"type":     string(f.Kind),
			"required": f.Required,
			"unique":   f.Unique,
		}
		if f.Cached {
			entry["cached"] = true
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
		val, _ := jsonx.Marshal(entry)
		buf.Write(val)
	}
	buf.WriteByte('}')
	return jsonx.RawMessage(buf.Bytes()), nil
}

func marshalArchiveSchema(cs *schema.CompiledSchema) (jsonx.RawMessage, error) {
	base, err := marshalOrderedSchema(cs)
	if err != nil {
		return nil, err
	}
	var raw map[string]map[string]any
	if err := jsonx.Unmarshal(base, &raw); err != nil {
		return nil, err
	}
	extra := map[string]map[string]any{
		"_archiveId":      {"type": "string", "required": true},
		"_originalPk":     {"type": "string", "required": true},
		"_deletedAt":      {"type": "timestamp", "required": true},
		"_deletedBy":      {"type": "string"},
		"_cascadeGroupId": {"type": "string"},
		"_cascadeDepth":   {"type": "integer"},
	}
	for k, v := range extra {
		raw[k] = v
	}
	return jsonx.Marshal(raw)
}

func (p *EngineAdminProvider) AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error) {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return AdminRowsPage{}, false, nil
	}
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		if err := p.DB.repairTableIndexes(table); err != nil {
			return AdminRowsPage{}, true, err
		}
	}
	rows, err := ti.Scan(limit, offset)
	if err != nil {
		return AdminRowsPage{}, false, err
	}

	def := ti.GetDef()

	// Sort by primary key for stable ordering
	if len(def.CompiledSchema.Fields) > 0 {
		pkDef := def.CompiledSchema.Fields[0]
		pkField := pkDef.Name
		sort.SliceStable(rows, func(i, j int) bool {
			return adminSortValueLess(rows[i][pkField], rows[j][pkField], pkDef.Kind)
		})
	}

	// Redact bcrypt fields
	for _, row := range rows {
		for _, f := range def.CompiledSchema.Fields {
			if f.Kind == schema.KindBcrypt && row[f.Name] != nil {
				row[f.Name] = "[REDACTED]"
			}
		}
		normalizeAdminFileFields(row, def.CompiledSchema.Fields)
	}

	return AdminRowsPage{
		Table:  table,
		Rows:   rows,
		Total:  ti.Count(),
		Offset: offset,
		Limit:  limit,
	}, true, nil
}

func (p *EngineAdminProvider) AdminArchiveRows(table string, limit, offset int) (AdminRowsPage, bool, error) {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return AdminRowsPage{}, false, nil
	}
	records, total, err := ti.ScanArchived(limit, offset)
	if err != nil {
		return AdminRowsPage{}, true, err
	}
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		row, err := ti.DeserializeArchivedRow(record)
		if err != nil {
			return AdminRowsPage{}, true, err
		}
		row["_archiveId"] = record.ArchiveID
		row["_originalPk"] = record.OriginalPK
		row["_deletedAt"] = record.DeletedAtUnixMs
		row["_deletedBy"] = record.DeletedBy
		row["_cascadeGroupId"] = record.CascadeGroupID
		row["_cascadeDepth"] = record.CascadeDepth
		normalizeAdminFileFields(row, ti.GetDef().CompiledSchema.Fields)
		rows = append(rows, row)
	}
	return AdminRowsPage{
		Table:   table,
		Archive: true,
		Rows:    rows,
		Total:   total,
		Offset:  offset,
		Limit:   limit,
	}, true, nil
}

func normalizeAdminFileFields(row map[string]any, fields []schema.CompiledField) {
	if row == nil {
		return
	}
	for _, field := range fields {
		switch field.Kind {
		case schema.KindFileSingle:
			row[field.Name] = normalizeAdminFileValue(row[field.Name])
		case schema.KindFileMulti:
			row[field.Name] = normalizeAdminFileList(row[field.Name])
		}
	}
}

func normalizeAdminFileValue(value any) any {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]any); ok {
		if _, hasURL := m["url"].(string); hasURL {
			if name, _ := m["name"].(string); name == "" {
				if derived := deriveAdminFileName(m); derived != "" {
					m["name"] = derived
				}
			}
			return m
		}
	}
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		return map[string]any{
			"url":  s,
			"path": s,
			"name": adminFileLabelFromString(s),
		}
	}
	return value
}

func normalizeAdminFileList(value any) any {
	if value == nil {
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return value
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		normalized := normalizeAdminFileValue(item)
		if normalized != nil {
			out = append(out, normalized)
		}
	}
	return out
}

func deriveAdminFileName(m map[string]any) string {
	if path, _ := m["path"].(string); path != "" {
		return adminFileLabelFromString(path)
	}
	if url, _ := m["url"].(string); url != "" {
		return adminFileLabelFromString(url)
	}
	return ""
}

func adminFileLabelFromString(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if idx := strings.IndexByte(s, '?'); idx >= 0 {
		s = s[:idx]
	}
	s = strings.TrimRight(s, "/")
	if s == "" {
		return ""
	}
	base := filepath.Base(s)
	if base == "." || base == "/" {
		return s
	}
	return base
}

func (p *EngineAdminProvider) AdminFilterRows(table string, match func(map[string]any) bool, limit, offset int, indexField, indexValue string) ([]map[string]any, int, bool, error) {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return nil, 0, false, nil
	}
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		if err := p.DB.repairTableIndexes(table); err != nil {
			return nil, 0, true, err
		}
	}

	def := ti.GetDef()

	// Try index-based lookup when a simple field="value" hint is provided.
	if indexField != "" {
		rows, total, used := ti.LookupByField(indexField, indexValue, limit, offset)
		if used {
			// Redact bcrypt fields on the page of results
			for _, row := range rows {
				for _, f := range def.CompiledSchema.Fields {
					if f.Kind == schema.KindBcrypt && row[f.Name] != nil {
						row[f.Name] = "[REDACTED]"
					}
				}
			}
			return rows, total, true, nil
		}
		// No index available — fall through to scan
	}

	// ScanFilter handles pagination internally — counts all matches but only
	// collects rows within the [offset, offset+limit) window.
	matched, total, err := ti.ScanFilter(func(row map[string]any) bool {
		for _, f := range def.CompiledSchema.Fields {
			if f.Kind == schema.KindBcrypt && row[f.Name] != nil {
				row[f.Name] = "[REDACTED]"
			}
		}
		return match(row)
	}, limit, offset)
	if err != nil {
		return nil, 0, false, err
	}

	// Sort only the page of results
	if len(matched) > 1 && len(def.CompiledSchema.Fields) > 0 {
		pkDef := def.CompiledSchema.Fields[0]
		pkField := pkDef.Name
		sort.SliceStable(matched, func(i, j int) bool {
			return adminSortValueLess(matched[i][pkField], matched[j][pkField], pkDef.Kind)
		})
	}

	return matched, total, true, nil
}

func (p *EngineAdminProvider) AdminCreateRow(table string, data map[string]any) (map[string]any, error) {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return nil, fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.Table(table)
	if ti == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}
	if table == systemSuperadminTableName {
		if err := prepareSuperadminWrite(data, true); err != nil {
			return nil, err
		}
	}
	return ti.Insert(data)
}

func (p *EngineAdminProvider) AdminUpdateRow(table, pk string, fields map[string]any) error {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.Table(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	if table == systemSuperadminTableName {
		if err := prepareSuperadminWrite(fields, false); err != nil {
			return err
		}
	}
	_, err := ti.Update(pk, fields)
	return err
}

func (p *EngineAdminProvider) AdminDeleteRow(table, pk string) error {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.Table(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	deleted, err := ti.Delete(pk)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("row not found: %s", pk)
	}
	return nil
}

func (p *EngineAdminProvider) AdminArchiveRow(table, pk string) error {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.Table(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	record, err := ti.Archive(pk)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("row not found: %s", pk)
	}
	return nil
}

func (p *EngineAdminProvider) AdminRestoreRow(table, archiveID string) error {
	ti := p.DB.Table(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return ti.RestoreArchive(archiveID)
}

func (p *EngineAdminProvider) AdminDeleteArchivedRow(table, archiveID string) error {
	ti := p.DB.Table(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return ti.DeleteArchive(archiveID)
}

func (p *EngineAdminProvider) secret() string {
	if p.JWTSecret != "" {
		return p.JWTSecret
	}
	return p.DB.jwtSecret
}

// AdminAnalytics returns the analytics collector used by admin analytics APIs.
func (p *EngineAdminProvider) AdminAnalytics() *server.RequestAnalytics {
	if p == nil || p.DB == nil {
		return nil
	}
	return p.DB.RequestAnalytics()
}

// AdminIndexStats returns per-table index diagnostics for observability pages.
func (p *EngineAdminProvider) AdminIndexStats() any {
	if p == nil || p.DB == nil || p.DB.db == nil {
		return map[string]any{
			"generatedAtUnixMilli":  time.Now().UnixMilli(),
			"tableCount":            0,
			"primaryIndexCount":     0,
			"secondaryIndexCount":   0,
			"estimatedPayloadBytes": uint64(0),
			"tables":                []any{},
		}
	}
	return p.DB.db.IndexStatsReport()
}

func (p *EngineAdminProvider) AdminMediaRows(limit, offset int, search string, orphansOnly bool) ([]AdminMediaRow, int, error) {
	if p == nil || p.DB == nil {
		return nil, 0, fmt.Errorf("database not available")
	}
	return p.DB.adminMediaRows(limit, offset, search, orphansOnly)
}

func (p *EngineAdminProvider) AdminBackupBusy() bool {
	return p != nil && p.DB != nil && p.DB.backupManager != nil && p.DB.backupManager.Busy()
}

func (p *EngineAdminProvider) AdminEmailSettings() (EmailSettings, error) {
	if p == nil || p.DB == nil {
		return EmailSettings{}, fmt.Errorf("database not available")
	}
	return p.DB.getEmailSettings(), nil
}

func (p *EngineAdminProvider) AdminUpdateEmailSettings(settings EmailSettings) (EmailSettings, error) {
	if p == nil || p.DB == nil {
		return EmailSettings{}, fmt.Errorf("database not available")
	}
	current := p.DB.rawEmailSettings()
	if settings.SMTP.Password == emailPasswordMask {
		settings.SMTP.Password = current.SMTP.Password
	}
	if settings.UseSMTP && settings.SMTP.Password == "" && current.SMTP.Password != "" &&
		settings.SMTP.Host == current.SMTP.Host &&
		settings.SMTP.Port == current.SMTP.Port &&
		settings.SMTP.Username == current.SMTP.Username &&
		settings.SMTP.TLS == current.SMTP.TLS &&
		settings.SMTP.AuthMethod == current.SMTP.AuthMethod &&
		settings.SMTP.LocalName == current.SMTP.LocalName {
		settings.SMTP.Password = current.SMTP.Password
	}
	return p.DB.updateEmailSettings(settings)
}

func (p *EngineAdminProvider) AdminTestEmail(settings EmailSettings, to string) error {
	if p == nil || p.DB == nil {
		return fmt.Errorf("database not available")
	}
	current := p.DB.rawEmailSettings()
	if settings.SMTP.Password == emailPasswordMask {
		settings.SMTP.Password = current.SMTP.Password
	}
	if settings.UseSMTP && settings.SMTP.Password == "" && current.SMTP.Password != "" &&
		settings.SMTP.Host == current.SMTP.Host &&
		settings.SMTP.Port == current.SMTP.Port &&
		settings.SMTP.Username == current.SMTP.Username &&
		settings.SMTP.TLS == current.SMTP.TLS &&
		settings.SMTP.AuthMethod == current.SMTP.AuthMethod &&
		settings.SMTP.LocalName == current.SMTP.LocalName &&
		settings.SenderName == current.SenderName &&
		settings.SenderAddress == current.SenderAddress {
		settings.SMTP.Password = current.SMTP.Password
	}
	return p.DB.testEmailSettings(settings, to)
}

func (p *EngineAdminProvider) AdminTestEmailTemplate(settings EmailSettings, to, templateName string) error {
	if p == nil || p.DB == nil {
		return fmt.Errorf("database not available")
	}
	current := p.DB.rawEmailSettings()
	if settings.SMTP.Password == emailPasswordMask {
		settings.SMTP.Password = current.SMTP.Password
	}
	if settings.UseSMTP && settings.SMTP.Password == "" && current.SMTP.Password != "" &&
		settings.SMTP.Host == current.SMTP.Host &&
		settings.SMTP.Port == current.SMTP.Port &&
		settings.SMTP.Username == current.SMTP.Username &&
		settings.SMTP.TLS == current.SMTP.TLS &&
		settings.SMTP.AuthMethod == current.SMTP.AuthMethod &&
		settings.SMTP.LocalName == current.SMTP.LocalName &&
		settings.SenderName == current.SenderName &&
		settings.SenderAddress == current.SenderAddress {
		settings.SMTP.Password = current.SMTP.Password
	}
	return p.DB.testEmailTemplate(settings, to, templateName)
}

func (p *EngineAdminProvider) AdminBackupSettings() (BackupSettings, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return BackupSettings{}, fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.getSettings(), nil
}

func (p *EngineAdminProvider) AdminUpdateBackupSettings(settings BackupSettings) (BackupSettings, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return BackupSettings{}, fmt.Errorf("backup manager unavailable")
	}
	current := p.DB.backupManager.rawSettings()
	if settings.S3.Enabled && settings.S3.Secret == backupSecretMask {
		settings.S3.Secret = current.S3.Secret
	}
	if settings.S3.Enabled && settings.S3.Secret == "" && current.S3.Secret != "" &&
		settings.S3.AccessKey == current.S3.AccessKey &&
		settings.S3.Endpoint == current.S3.Endpoint &&
		settings.S3.Region == current.S3.Region &&
		settings.S3.Bucket == current.S3.Bucket &&
		settings.S3.ForcePathStyle == current.S3.ForcePathStyle {
		settings.S3.Secret = current.S3.Secret
	}
	return p.DB.backupManager.updateSettings(settings)
}

func (p *EngineAdminProvider) AdminTestBackupS3(cfg BackupS3Config) error {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return fmt.Errorf("backup manager unavailable")
	}
	current := p.DB.backupManager.rawSettings()
	if cfg.Enabled && cfg.Secret == backupSecretMask {
		cfg.Secret = current.S3.Secret
	}
	return p.DB.backupManager.testS3(cfg)
}

func (p *EngineAdminProvider) AdminBackups() ([]AdminBackupFile, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return nil, fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.List(context.Background())
}

func (p *EngineAdminProvider) AdminCreateBackup() (string, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return "", fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.CreateManual(context.Background())
}

func (p *EngineAdminProvider) AdminUploadBackup(filename string, file io.Reader) (string, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return "", fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.Upload(context.Background(), filename, file)
}

func (p *EngineAdminProvider) AdminDeleteBackup(key string) error {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.Delete(context.Background(), key)
}

func (p *EngineAdminProvider) AdminRestoreBackup(key string) error {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.Restore(context.Background(), key)
}

func (p *EngineAdminProvider) AdminBackupStat(key string) (AdminBackupFile, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return AdminBackupFile{}, fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.Stat(context.Background(), key)
}

func (p *EngineAdminProvider) AdminBackupOpen(ctx context.Context, key string) (io.ReadCloser, error) {
	if p == nil || p.DB == nil || p.DB.backupManager == nil {
		return nil, fmt.Errorf("backup manager unavailable")
	}
	return p.DB.backupManager.Open(ctx, key)
}

// AdminEnablePprof reports whether profiling endpoints are enabled.
func (p *EngineAdminProvider) AdminRefreshMaterialized(table string) error {
	if p == nil || p.DB == nil {
		return fmt.Errorf("database not available")
	}
	return p.DB.RefreshMaterialized(table)
}

func (p *EngineAdminProvider) AdminEnablePprof() bool {
	return p != nil && p.DB != nil && p.DB.enablePprof
}

// WrapWithAnalytics records request timing/error telemetry while preserving the wrapped handler behavior.
func (p *EngineAdminProvider) WrapWithAnalytics(next http.Handler) http.Handler {
	if next == nil {
		next = http.NewServeMux()
	}
	analytics := p.AdminAnalytics()
	if analytics == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceCollector := reqtrace.Start()
		defer traceCollector.End()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		path := r.URL.Path
		// Skip analytics endpoints to avoid noisy self-observation loops.
		if strings.HasPrefix(path, "/_/api/analytics/") {
			return
		}
		// View/reducer routes self-report from APIHandler so batched calls can be recorded individually.
		if strings.HasPrefix(path, "/api/view/") || strings.HasPrefix(path, "/api/reduce/") {
			return
		}
		if !strings.HasPrefix(path, "/api/") {
			return
		}

		routeType, routeName := classifyAnalyticsRoute(path)
		transport := "http"
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			transport = "sse"
		}
		userID := ""
		token := extractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
		if token != "" {
			payload := server.VerifyJWT(token, p.secret())
			if payload != nil {
				userID = payload.Sub
			}
		}

		details := map[string]any{
			"queryBytes": len(r.URL.RawQuery),
			"hasAuth":    token != "",
			"source":     "go-middleware",
		}
		if spans := traceCollector.Spans(); len(spans) > 0 {
			details["trace"] = spans
			details["traceSpans"] = len(spans)
		}

		analytics.Record(server.AnalyticsEvent{
			Timestamp:    time.Now(),
			RouteType:    routeType,
			RouteName:    routeName,
			Method:       r.Method,
			Path:         path,
			Transport:    transport,
			Duration:     time.Since(start),
			OK:           rec.status < 400,
			StatusCode:   rec.status,
			ErrorMessage: rec.errorMessage,
			UserID:       userID,
			Details:      details,
		})
	})
}

var adminMediaURLPattern = regexp.MustCompile(`(?:https?://[^/\s"'<>]+)?(/api/files/[^\s"'<>]+)`)

func (d *Database) adminMediaRows(limit, offset int, search string, orphansOnly bool) ([]AdminMediaRow, int, error) {
	if d == nil || d.db == nil {
		return nil, 0, fmt.Errorf("database not available")
	}
	idx, err := loadMediaIndex(d.db.GetDataDir())
	if err != nil {
		d.ensureMediaIndexBackground()
		idx = newMediaIndex()
	} else if !idx.Complete {
		d.ensureMediaIndexBackground()
	}
	mediaRows := flattenMediaIndexRows(idx)

	if search = strings.ToLower(strings.TrimSpace(search)); search != "" || orphansOnly {
		filtered := mediaRows[:0]
		for _, row := range mediaRows {
			if orphansOnly && !row.Orphaned {
				continue
			}
			if search != "" {
				haystack := strings.ToLower(strings.Join([]string{
					row.Path,
					row.Name,
					row.Mime,
					row.TableName,
					row.RowID,
					row.FieldName,
					strings.Join(row.Thumbs, " "),
				}, "\n"))
				if !strings.Contains(haystack, search) {
					continue
				}
			}
			filtered = append(filtered, row)
		}
		mediaRows = filtered
	}

	sort.Slice(mediaRows, func(i, j int) bool {
		if mediaRows[i].Orphaned != mediaRows[j].Orphaned {
			return !mediaRows[i].Orphaned && mediaRows[j].Orphaned
		}
		if mediaRows[i].TableName != mediaRows[j].TableName {
			return mediaRows[i].TableName < mediaRows[j].TableName
		}
		if mediaRows[i].RowID != mediaRows[j].RowID {
			return mediaRows[i].RowID < mediaRows[j].RowID
		}
		if mediaRows[i].FieldName != mediaRows[j].FieldName {
			return mediaRows[i].FieldName < mediaRows[j].FieldName
		}
		return mediaRows[i].Path < mediaRows[j].Path
	})

	total := len(mediaRows)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	pageRows := mediaRows[offset:end]
	if pageRows == nil {
		pageRows = []AdminMediaRow{}
	}
	if len(pageRows) > 0 {
		if err := d.enrichAdminMediaRows(pageRows); err != nil {
			return nil, 0, err
		}
	}
	return pageRows, total, nil
}

func (d *Database) enrichAdminMediaRows(rows []AdminMediaRow) error {
	if d == nil || d.db == nil || len(rows) == 0 {
		return nil
	}
	d.mediaIndexMu.Lock()
	defer d.mediaIndexMu.Unlock()

	idx, err := loadMediaIndex(d.db.GetDataDir())
	if err != nil {
		return nil
	}

	changed := false
	seen := map[string]*mediaIndexRecord{}
	for i := range rows {
		path := strings.TrimSpace(rows[i].Path)
		if path == "" {
			continue
		}
		record := seen[path]
		if record == nil {
			record = idx.Files[path]
			if record == nil {
				continue
			}
			if _, err := ensureMediaIndexRecord(idx, d, schema.FileRef{
				Path: path,
				Name: record.Name,
				URL:  record.URL,
				Mime: record.Mime,
				Size: record.RefSize,
			}, true); err == nil {
				record = idx.Files[path]
				changed = true
			}
			seen[path] = record
		}
		if record == nil {
			continue
		}
		rows[i].ThumbCount = record.ThumbCount
		rows[i].ThumbBytes = record.ThumbBytes
		rows[i].Width = record.Width
		rows[i].Height = record.Height
	}
	if changed {
		return saveMediaIndex(d.db.GetDataDir(), idx)
	}
	return nil
}

func scanAdminMediaFilesOnDisk(dataDir string, itemsByPath map[string]*AdminMediaRow, existingPaths map[string]bool) {
	filesRoot := filepath.Join(dataDir, "_files")
	_ = filepath.Walk(filesRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, "_files/") {
			return nil
		}
		existingPaths[rel] = true
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
		item := ensureAdminMediaItem(itemsByPath, dataDir, ref)
		if item != nil {
			item.Orphaned = true
		}
		return nil
	})
}

func ensureAdminMediaItem(items map[string]*AdminMediaRow, dataDir string, ref schema.FileRef) *AdminMediaRow {
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

	item := &AdminMediaRow{
		Path:    ref.Path,
		Name:    ref.Name,
		URL:     ref.URL,
		Mime:    ref.Mime,
		RefSize: ref.Size,
	}
	fillAdminMediaItemDetails(item, dataDir)
	items[ref.Path] = item
	return item
}

func fillAdminMediaItemDetails(item *AdminMediaRow, dataDir string) {
	fullPath := filepath.Join(dataDir, filepath.FromSlash(item.Path))
	if stat, err := os.Stat(fullPath); err == nil {
		item.DiskSize = stat.Size()
	}
	if strings.HasPrefix(item.Mime, "image/") || adminLooksLikeImagePath(item.Path) {
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

func adminCollectMediaRefs(value interface{}, kind schema.FieldKind) []schema.FileRef {
	switch kind {
	case schema.KindFileSingle:
		if ref, ok := adminMediaRefFromAny(value); ok {
			return []schema.FileRef{ref}
		}
	case schema.KindFileMulti:
		rv := reflect.ValueOf(value)
		if !rv.IsValid() || rv.Kind() != reflect.Slice {
			return nil
		}
		refs := make([]schema.FileRef, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			if ref, ok := adminMediaRefFromAny(rv.Index(i).Interface()); ok {
				refs = append(refs, ref)
			}
		}
		return refs
	case schema.KindString:
		s, ok := value.(string)
		if !ok || !strings.Contains(s, "/api/files/") {
			return nil
		}
		return adminExtractMediaRefsFromText(s)
	case schema.KindJson:
		if value == nil {
			return nil
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		text := string(data)
		if !strings.Contains(text, "/api/files/") {
			return nil
		}
		return adminExtractMediaRefsFromText(text)
	}
	return nil
}

func adminExtractMediaRefsFromText(text string) []schema.FileRef {
	seen := map[string]bool{}
	var refs []schema.FileRef
	for _, match := range adminMediaURLPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 2 {
			continue
		}
		if ref, ok := adminMediaRefFromAPIURL(match[1]); ok && !seen[ref.Path] {
			seen[ref.Path] = true
			refs = append(refs, ref)
		}
	}
	return refs
}

func adminMediaRefFromAny(value interface{}) (schema.FileRef, bool) {
	switch v := value.(type) {
	case map[string]any:
		ref := schema.FileRef{
			Path: adminStringValue(v["path"]),
			Name: adminStringValue(v["name"]),
			URL:  adminStringValue(v["url"]),
			Mime: adminStringValue(v["mime"]),
			Size: adminInt64Value(v["size"]),
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
		if strings.TrimSpace(ref.Path) == "" {
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
	case FileRef:
		if strings.TrimSpace(v.Path) == "" {
			return schema.FileRef{}, false
		}
		return schema.FileRef{
			Path: v.Path,
			Name: v.Name,
			URL:  v.URL,
			Mime: v.Mime,
			Size: v.Size,
		}, true
	case *FileRef:
		if v == nil || strings.TrimSpace(v.Path) == "" {
			return schema.FileRef{}, false
		}
		return schema.FileRef{
			Path: v.Path,
			Name: v.Name,
			URL:  v.URL,
			Mime: v.Mime,
			Size: v.Size,
		}, true
	case string:
		path := strings.TrimSpace(v)
		if path == "" || !strings.HasPrefix(path, "_files/") {
			return adminMediaRefFromAPIURL(path)
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

func adminMediaRefFromAPIURL(raw string) (schema.FileRef, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return schema.FileRef{}, false
	}
	idx := strings.Index(raw, "/api/files/")
	if idx < 0 {
		return schema.FileRef{}, false
	}
	rel := raw[idx+len("/api/files/"):]
	if cut := strings.IndexAny(rel, "?#"); cut >= 0 {
		rel = rel[:cut]
	}
	rel = strings.TrimRight(rel, ").,;:]}")
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")
	if len(parts) < 4 {
		return schema.FileRef{}, false
	}
	path := "_files/" + strings.Join(parts[:4], "/")
	if strings.Contains(path, "..") {
		return schema.FileRef{}, false
	}
	return schema.FileRef{
		Path: path,
		Name: filepath.Base(path),
		URL:  "/api/files/" + strings.TrimPrefix(path, "_files/"),
		Mime: storage.MimeFromExtension(path),
	}, true
}

func adminFieldCanContainMedia(kind schema.FieldKind, value any) bool {
	switch kind {
	case schema.KindFileSingle, schema.KindFileMulti:
		return value != nil
	default:
		return false
	}
}

func adminStringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func adminInt64Value(v any) int64 {
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

func adminLooksLikeImagePath(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp":
		return true
	default:
		return false
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status       int
	errorMessage string
}

func (rw *statusRecorder) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *statusRecorder) Write(b []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	if rw.status >= 400 && rw.errorMessage == "" {
		var payload map[string]any
		if err := jsonx.Unmarshal(b, &payload); err == nil {
			if msg, ok := payload["error"].(string); ok {
				rw.errorMessage = msg
			}
		}
	}
	return rw.ResponseWriter.Write(b)
}

func (rw *statusRecorder) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rw *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijacker unsupported")
}

func (rw *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := rw.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (rw *statusRecorder) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := rw.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(rw.ResponseWriter, r)
}

func classifyAnalyticsRoute(path string) (routeType string, routeName string) {
	if strings.HasPrefix(path, "/api/view/") {
		return "view", strings.TrimPrefix(path, "/api/view/")
	}
	if strings.HasPrefix(path, "/api/reduce/") {
		return "reducer", strings.TrimPrefix(path, "/api/reduce/")
	}
	return "request", strings.TrimPrefix(path, "/api/")
}

func (p *EngineAdminProvider) AdminLogin(email, password string) (string, string, error) {
	if p.DB.superadminService == nil {
		return "", "", fmt.Errorf("superadmin auth not configured")
	}
	tok, refresh, auth, err := p.DB.superadminService.Login(email, password)
	if err != nil {
		return "", "", err
	}
	// Require superadmin role
	hasSuperadmin := false
	for _, r := range auth.Roles {
		if r == "superadmin" {
			hasSuperadmin = true
			break
		}
	}
	if !hasSuperadmin {
		return "", "", fmt.Errorf("insufficient privileges. Requires superadmin role")
	}
	return tok, refresh, nil
}

func (p *EngineAdminProvider) AdminRefresh(refreshToken string) (string, string, error) {
	if p.DB.superadminService == nil {
		return "", "", fmt.Errorf("superadmin auth not configured")
	}
	return p.DB.superadminService.Refresh(refreshToken)
}

func (p *EngineAdminProvider) AdminIsAuthorized(token string) bool {
	if p.DB.superadminService == nil {
		return false
	}
	auth, err := p.DB.superadminService.ValidateAccessToken(token)
	if err != nil || auth == nil {
		return false
	}
	for _, role := range auth.Roles {
		if role == "superadmin" {
			return true
		}
	}
	return false
}

func (p *EngineAdminProvider) AdminHasSuperadmin() bool {
	if p.DB.superadminService == nil {
		return false
	}
	return p.DB.superadminService.HasSuperadmin()
}

func (p *EngineAdminProvider) AdminRegisterSuperadmin(email, password string, extraFields map[string]any) error {
	if p.DB.superadminService == nil {
		return fmt.Errorf("superadmin auth not configured")
	}
	name := ""
	if extraFields != nil {
		if rawName, ok := extraFields["name"]; ok {
			name = strings.TrimSpace(fmt.Sprintf("%v", rawName))
		}
	}
	_, _, err := p.DB.superadminService.Register(email, password, name)
	return err
}

func (p *EngineAdminProvider) AdminSetupExtraFields() []SetupField {
	return nil
}

func prepareSuperadminWrite(data map[string]any, create bool) error {
	if data == nil {
		return nil
	}
	rawPassword, hasPassword := data["password"]
	if create && !hasPassword {
		return fmt.Errorf("password is required")
	}
	if hasPassword {
		password := strings.TrimSpace(fmt.Sprintf("%v", rawPassword))
		if password == "" {
			if create {
				return fmt.Errorf("password is required")
			}
			delete(data, "password")
		} else if password == "[REDACTED]" {
			delete(data, "password")
		} else {
			hashed, err := server.HashPassword(password)
			if err != nil {
				return err
			}
			data["password"] = hashed
		}
	}
	if rawName, ok := data["name"]; ok {
		name := strings.TrimSpace(fmt.Sprintf("%v", rawName))
		if name == "" {
			delete(data, "name")
		} else {
			data["name"] = name
		}
	}
	if rawEmail, ok := data["email"]; ok {
		email := strings.TrimSpace(strings.ToLower(fmt.Sprintf("%v", rawEmail)))
		if email == "" {
			return fmt.Errorf("email is required")
		}
		data["email"] = email
	}
	return nil
}

func (p *EngineAdminProvider) AdminSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = jsonx.NewEncoder(w).Encode(map[string]any{"error": "SSE not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Send initial table counts
	tableCounts := make(map[string]int)
	for name, table := range p.DB.db.Tables {
		tableCounts[name] = table.Count()
	}
	data, _ := jsonx.Marshal(map[string]any{"tableCounts": tableCounts})
	fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)
	flusher.Flush()

	done := r.Context().Done()
	changeCh := make(chan engine.ChangeEvent, 256)
	unsubscribe := p.DB.db.GetPubSub().SubscribeAll(func(event engine.ChangeEvent) {
		select {
		case changeCh <- event:
		default:
		}
	})
	defer unsubscribe()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-done:
			return
		case event := <-changeCh:
			data, _ := jsonx.Marshal(event)
			fmt.Fprintf(w, "event: change\ndata: %s\n\n", data)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func adminSortValueLess(a, b any, kind schema.FieldKind) bool {
	switch kind {
	case schema.KindNumber, schema.KindInteger, schema.KindTimestamp:
		an, aok := adminSortToFloat(a)
		bn, bok := adminSortToFloat(b)
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

func adminSortToFloat(v any) (float64, bool) {
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

// toString safely converts any value to string.
func toString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// toStringSlice converts any value to a string slice.
func toStringSlice(v any) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []any:
		result := make([]string, len(val))
		for i, item := range val {
			result[i] = toString(item)
		}
		return result
	default:
		return nil
	}
}

// contains checks if a string slice contains a value.
func contains(ss []string, s string) bool {
	for _, v := range ss {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
