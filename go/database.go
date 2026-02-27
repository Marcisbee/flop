package flop

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/server"
)

// Database wraps the internal engine and exposes table operations.
type Database struct {
	db          *engine.Database
	authService *server.AuthService
	jwtSecret   string
}

// TableInstance wraps internal engine table and provides CRUD operations.
type TableInstance struct {
	ti *engine.TableInstance
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

	d := &Database{db: db}

	// Set up auth service if there's an auth table
	authTable := db.GetAuthTable()
	if authTable != nil {
		secret := "flop-dev-secret"
		d.jwtSecret = secret
		d.authService = server.NewAuthService(authTable, secret)
	}

	return d, nil
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
	return &TableInstance{ti: ti}
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

// Close closes the database.
func (d *Database) Close() error {
	for _, t := range d.db.Tables {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Insert inserts a row into the table. Returns the inserted row
// (with auto-generated fields filled).
func (ti *TableInstance) Insert(data map[string]any) (map[string]any, error) {
	return ti.ti.Insert(data, nil)
}

// InsertMany inserts rows in buffered batches for higher import throughput.
// Returns the number of inserted rows.
func (ti *TableInstance) InsertMany(rows []map[string]any, flushEvery int) (int, error) {
	return ti.ti.BulkInsert(rows, flushEvery)
}

// Get retrieves a row by primary key.
func (ti *TableInstance) Get(pk string) (map[string]any, error) {
	return ti.ti.Get(pk)
}

// Update updates a row by primary key. Returns the updated row.
func (ti *TableInstance) Update(pk string, fields map[string]any) (map[string]any, error) {
	return ti.ti.Update(pk, fields, nil)
}

// Delete deletes a row by primary key. Returns true if the row existed.
func (ti *TableInstance) Delete(pk string) (bool, error) {
	return ti.ti.Delete(pk, nil)
}

// Scan returns rows with pagination.
func (ti *TableInstance) Scan(limit, offset int) ([]map[string]any, error) {
	return ti.ti.Scan(limit, offset)
}

// Count returns the number of rows.
func (ti *TableInstance) Count() int {
	return ti.ti.Count()
}

// SecondaryIndexesReady reports whether non-primary indexes are fully built.
func (ti *TableInstance) SecondaryIndexesReady() bool {
	return ti.ti.SecondaryIndexesReady()
}

// FindByEmail finds a row by the "email" unique index.
func (ti *TableInstance) FindByEmail(email string) (map[string]any, bool) {
	ptr, ok := ti.ti.FindByIndex([]string{"email"}, email)
	if !ok {
		return nil, false
	}
	row, err := ti.ti.GetByPointer(ptr)
	if err != nil || row == nil {
		return nil, false
	}
	return row, true
}

// FindByUniqueIndex finds a row by a unique index on the given field.
func (ti *TableInstance) FindByUniqueIndex(field string, value any) (map[string]any, bool) {
	ptr, ok := ti.ti.FindByIndex([]string{field}, value)
	if !ok {
		return nil, false
	}
	row, err := ti.ti.GetByPointer(ptr)
	if err != nil || row == nil {
		return nil, false
	}
	return row, true
}

// SearchFullText searches a configured full-text index on the selected fields.
func (ti *TableInstance) SearchFullText(fields []string, query string, limit int) ([]map[string]any, error) {
	return ti.ti.SearchFullText(fields, query, limit)
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
	if ti == nil || ti.ti == nil {
		return nil, fmt.Errorf("table is nil")
	}
	keyField = strings.TrimSpace(keyField)
	textField = strings.TrimSpace(textField)
	if keyField == "" || textField == "" {
		return nil, fmt.Errorf("keyField and textField are required")
	}

	count := ti.ti.Count()
	if count == 0 {
		return []AutocompleteEntry{}, nil
	}
	rows, err := ti.ti.Scan(count, 0)
	if err != nil {
		return nil, err
	}

	out := make([]AutocompleteEntry, 0, len(rows))
	for _, row := range rows {
		key := toStringAny(row[keyField])
		text := toStringAny(row[textField])
		if key == "" || text == "" {
			continue
		}
		var data map[string]interface{}
		if len(payloadFields) > 0 {
			data = make(map[string]interface{}, len(payloadFields))
			for _, field := range payloadFields {
				field = strings.TrimSpace(field)
				if field == "" {
					continue
				}
				data[field] = row[field]
			}
			if len(data) == 0 {
				data = nil
			}
		}
		out = append(out, AutocompleteEntry{
			Key:  key,
			Text: text,
			Data: data,
		})
	}
	return out, nil
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
			cf := schema.CompiledField{
				Name:             fs.JSONName,
				Kind:             mapKind(fs.Kind),
				Required:         fs.Required,
				Unique:           fs.Unique,
				DefaultValue:     fs.Default,
				AutoGenPattern:   fs.Autogen,
				BcryptRounds:     fs.BcryptRounds,
				EnumValues:       append([]string(nil), fs.EnumValues...),
				VectorDimensions: fs.VectorDimensions,
				RefTableName:     fs.RefTable,
				RefField:         fs.RefField,
				MimeTypes:        append([]string(nil), fs.MimeTypes...),
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

		defs[name] = &schema.TableDef{
			Name:           name,
			CompiledSchema: schema.NewCompiledSchema(fields),
			Indexes:        indexes,
			Auth:           isAuth,
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

func toStringAny(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
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
		tables = append(tables, AdminTable{
			Name:     name,
			Schema:   s,
			RowCount: t.Count(),
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

// marshalOrderedSchema produces an ordered JSON object of field definitions
// matching the format the admin SPA expects.
func marshalOrderedSchema(cs *schema.CompiledSchema) (json.RawMessage, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range cs.Fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, _ := json.Marshal(f.Name)
		buf.Write(key)
		buf.WriteByte(':')

		entry := map[string]any{
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
		val, _ := json.Marshal(entry)
		buf.Write(val)
	}
	buf.WriteByte('}')
	return json.RawMessage(buf.Bytes()), nil
}

func (p *EngineAdminProvider) AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error) {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return AdminRowsPage{}, false, nil
	}
	rows, err := ti.Scan(limit, offset)
	if err != nil {
		return AdminRowsPage{}, false, err
	}

	def := ti.GetDef()

	// Sort by primary key for stable ordering
	if len(def.CompiledSchema.Fields) > 0 {
		pkField := def.CompiledSchema.Fields[0].Name
		sort.SliceStable(rows, func(i, j int) bool {
			return fmt.Sprint(rows[i][pkField]) < fmt.Sprint(rows[j][pkField])
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

func (p *EngineAdminProvider) AdminCreateRow(table string, data map[string]any) (map[string]any, error) {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}
	return ti.Insert(data, nil)
}

func (p *EngineAdminProvider) AdminUpdateRow(table, pk string, fields map[string]any) error {
	ti := p.DB.db.GetTable(table)
	if ti == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	_, err := ti.Update(pk, fields, nil)
	return err
}

func (p *EngineAdminProvider) AdminDeleteRow(table, pk string) error {
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

func (p *EngineAdminProvider) secret() string {
	if p.JWTSecret != "" {
		return p.JWTSecret
	}
	return p.DB.jwtSecret
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

func (p *EngineAdminProvider) AdminRegisterSuperadmin(email, password, name string) error {
	if p.DB.authService == nil {
		return fmt.Errorf("auth not configured")
	}
	_, _, err := p.DB.authService.RegisterSuperadmin(email, password, name)
	return err
}

func (p *EngineAdminProvider) AdminSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "SSE not supported"})
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
	data, _ := json.Marshal(map[string]any{"tableCounts": tableCounts})
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
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "event: change\ndata: %s\n\n", data)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
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
