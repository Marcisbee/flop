package flop

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/marcisbee/flop/internal/jsonx"
	"github.com/marcisbee/flop/internal/reqtrace"
	"html/template"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/cron"
	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/server"
	"github.com/marcisbee/flop/internal/storage"
)

// Database wraps the internal engine and exposes table operations.
type Database struct {
	db                  *engine.Database
	authService         *server.AuthService
	mailer              *server.Mailer
	jwtSecret           string
	requestLogRetention time.Duration
	enablePprof         bool
	analyticsMu         sync.Mutex
	analytics           *server.RequestAnalytics
	cronRunner          *cron.Runner
	tableNames          []string
	tableNameToID       map[string]int
	tableSpecs          map[string]*tableSpec
	tablePolicy         map[string]tablePolicyMeta
	materialized        map[string]*materializedRuntime
	cascadeRefs         map[string][]cascadeArchiveRef
}

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
	closed bool
}

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
		db:                  db,
		requestLogRetention: retention,
		enablePprof:         a.config.EnablePprof,
		tableNameToID:       make(map[string]int),
		tableSpecs:          make(map[string]*tableSpec),
		tablePolicy:         make(map[string]tablePolicyMeta),
		materialized:        make(map[string]*materializedRuntime),
		cascadeRefs:         make(map[string][]cascadeArchiveRef),
	}
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

	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	d.tableNames = names
	for i, name := range names {
		d.tableNameToID[name] = i
	}

	// Set up auth service if there's an auth table
	authTable := db.GetAuthTable()
	if authTable != nil {
		secret := "flop-dev-secret"
		d.jwtSecret = secret
		d.authService = server.NewAuthService(authTable, secret)
	}

	// Set up mailer if SMTP configured
	if a.config.SMTP != nil {
		d.mailer = server.NewMailer(server.SMTPConfig{
			Host:     a.config.SMTP.Host,
			Port:     a.config.SMTP.Port,
			Username: a.config.SMTP.Username,
			Password: a.config.SMTP.Password,
			From:     a.config.SMTP.From,
		})
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

	return d, nil
}

// SetEmailTemplate overrides a named email template used by auth flows.
// Supported names: "verification", "email-change", "password-reset".
func (d *Database) SetEmailTemplate(name string, tmpl *template.Template) {
	if d.mailer != nil {
		d.mailer.SetTemplate(name, tmpl)
	}
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
		d.authService = server.NewAuthService(d.db.GetAuthTable(), secret)
	}
}

// Table returns a table instance by name.
func (d *Database) Table(name string) *TableInstance {
	ti := d.db.GetTable(name)
	if ti == nil {
		return nil
	}
	return &TableInstance{
		ti:            ti,
		db:            d,
		name:          name,
		tableID:       d.tableNameToID[name],
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
	if d.cronRunner != nil {
		d.cronRunner.Stop()
	}
	for _, t := range d.db.Tables {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
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
		filePath := filepath.Join(d.db.GetDataDir(), "_files", rel)
		http.ServeFile(w, r, filePath)
	})
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

func (ti *TableInstance) insertWithTx(data map[string]any, txBuf map[string]*engine.WalBufEntry) (map[string]any, error) {
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
	row, err := ti.ti.Insert(data, txBuf)
	if err != nil {
		return nil, err
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

func (ti *TableInstance) updateWithTx(pk string, fields map[string]any, txBuf map[string]*engine.WalBufEntry) (map[string]any, error) {
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
	row, err := ti.ti.Update(pk, fields, txBuf)
	if err != nil {
		return nil, err
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

// Delete deletes a row by primary key. Returns true if the row existed.
func (ti *TableInstance) Delete(pk string) (bool, error) {
	return ti.deleteWithTx(pk, nil)
}

func (ti *TableInstance) deleteWithTx(pk string, txBuf map[string]*engine.WalBufEntry) (bool, error) {
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
	return ti.ti.Delete(pk, txBuf)
}

func (ti *TableInstance) Archive(pk string) (*storage.ArchivedRow, error) {
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
	record, err := ti.archiveCascade(tx, pk, "", ti.name, pk, 0)
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
		return nil
	}
	toInsert := make([]map[string]any, 0, len(desired))
	for _, row := range desired {
		toInsert = append(toInsert, row)
	}
	_, err := ti.ti.BulkInsert(toInsert, 1000)
	return err
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
	restoredRecord, _, err := ti.ti.RestoreArchived(archiveID, tx.txBuf)
	if err != nil {
		return err
	}
	tx.addUndo(func() { _ = ti.ti.RollbackRestore(restoredRecord) })

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
		restoredChild, _, err := item.table.ti.RestoreArchived(item.record.ArchiveID, tx.txBuf)
		if err != nil {
			return err
		}
		table := item.table
		tx.addUndo(func() { _ = table.ti.RollbackRestore(restoredChild) })
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
	tx.closed = true
	if len(tx.txBuf) == 0 {
		return nil
	}
	return tx.db.db.EnqueueCommit(tx.txBuf)
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
	defs := make(map[string]*schema.TableDef, len(a.tables))

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

	return defs
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
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}
	return ti.Insert(data, nil)
}

func (p *EngineAdminProvider) AdminUpdateRow(table, pk string, fields map[string]any) error {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	_, err := ti.Update(pk, fields, nil)
	return err
}

func (p *EngineAdminProvider) AdminDeleteRow(table, pk string) error {
	if ok, _, _ := p.DB.materializedStatus(table); ok {
		return fmt.Errorf("materialized table is read-only: %s", table)
	}
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	deleted, err := ti.Delete(pk, nil)
	if err != nil {
		return err
	}
	if !deleted {
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
	if p.DB.authService == nil {
		return "", "", fmt.Errorf("auth not configured")
	}
	tok, refresh, auth, err := p.DB.authService.Login(email, password)
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

func (p *EngineAdminProvider) AdminRefresh(refreshToken string) (string, error) {
	if p.DB.authService == nil {
		return "", fmt.Errorf("auth not configured")
	}
	return p.DB.authService.Refresh(refreshToken)
}

func (p *EngineAdminProvider) AdminIsAuthorized(token string) bool {
	payload := server.VerifyJWT(token, p.secret())
	if payload == nil {
		return false
	}
	for _, r := range payload.Roles {
		if r == "superadmin" {
			return true
		}
	}
	return false
}

func (p *EngineAdminProvider) AdminHasSuperadmin() bool {
	if p.DB.authService == nil {
		return false
	}
	return p.DB.authService.HasSuperadmin()
}

func (p *EngineAdminProvider) AdminRegisterSuperadmin(email, password string, extraFields map[string]any) error {
	if p.DB.authService == nil {
		return fmt.Errorf("auth not configured")
	}
	extra := make(map[string]interface{}, len(extraFields))
	for k, v := range extraFields {
		extra[k] = v
	}
	_, _, err := p.DB.authService.RegisterSuperadmin(email, password, extra)
	return err
}

func (p *EngineAdminProvider) AdminSetupExtraFields() []SetupField {
	authTable := p.DB.db.GetAuthTable()
	if authTable == nil {
		return nil
	}
	def := authTable.GetDef()
	// Fields already handled by the standard setup form
	skip := map[string]bool{
		"email": true, "password": true,
		"roles": true, "verified": true,
	}
	var fields []SetupField
	for _, f := range def.CompiledSchema.Fields {
		if skip[f.Name] {
			continue
		}
		// Skip auto-generated fields (primary key, timestamps with defaults)
		if f.AutoGenPattern != "" {
			continue
		}
		// Skip bcrypt fields (password is already handled)
		if f.Kind == schema.KindBcrypt {
			continue
		}
		// Skip non-required fields — the form only needs to show required ones
		if !f.Required {
			continue
		}
		fields = append(fields, SetupField{
			Name:       f.Name,
			Type:       string(f.Kind),
			Required:   f.Required,
			EnumValues: f.EnumValues,
		})
	}
	return fields
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
