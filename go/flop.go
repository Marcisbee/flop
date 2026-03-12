package flop

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/marcisbee/flop/internal/storage"
)

// ErrNotImplemented marks API scaffolding that is defined but not wired yet.
var ErrNotImplemented = errors.New("flop: not implemented")

// ErrAccessDenied is returned when table/field access policy rejects an operation.
var ErrAccessDenied = errors.New("flop: access denied")

type Config struct {
	DataDir               string        `json:"dataDir,omitempty"`
	SyncMode              string        `json:"syncMode,omitempty"`
	AsyncSecondaryIndexes bool          `json:"asyncSecondaryIndexes,omitempty"`
	RequestLogRetention   time.Duration `json:"-"`
	EnablePprof           bool          `json:"-"`
	SMTP                  *SMTPConfig   `json:"-"`
}

// CachedTypeHint identifies the storage type for a cached field.
type CachedTypeHint int

const (
	Int    CachedTypeHint = iota // maps to KindInteger
	Number                       // maps to KindNumber
	Str                          // maps to KindString
)

// Row provides read-only access to a row during cached field computation.
type Row struct {
	data map[string]any
	pk   string
}

// ID returns the primary key of the row.
func (r Row) ID() string { return r.pk }

// Get returns the value of a field.
func (r Row) Get(field string) any { return r.data[field] }

// cronSpec holds a cron expression and handler registered before Open().
type cronSpec struct {
	Expr string
	Fn   func(*Database)
}

type materializedSpec struct {
	Refresh          func(*Database) error
	Cron             string
	RefreshOnStartup bool
}

// App is the top-level runtime registry.
// In this phase, it stores typed metadata used for generation and future runtime wiring.
type App struct {
	config     Config
	tables     map[string]*tableSpec
	views      []endpointSpec
	reducers   []endpointSpec
	viewDefs   map[string]viewRuntime
	reduceDefs map[string]reducerRuntime
	layouts    []layoutSpec
	pages      []pageSpec
	crons      []cronSpec
}

// Cron registers a cron job that runs on the given schedule after Open().
// The expression uses standard 5-field cron format: minute hour day-of-month month day-of-week.
// Examples: "* * * * *" (every minute), "*/5 * * * *" (every 5 min), "0 0 * * *" (daily at midnight).
func (a *App) Cron(expr string, fn func(*Database)) {
	if a == nil {
		panic("flop: app is nil")
	}
	if fn == nil {
		panic("flop: cron handler is nil")
	}
	a.crons = append(a.crons, cronSpec{Expr: expr, Fn: fn})
}

func New(config Config) *App {
	return &App{
		config:     config,
		tables:     make(map[string]*tableSpec),
		viewDefs:   make(map[string]viewRuntime),
		reduceDefs: make(map[string]reducerRuntime),
	}
}

// FileRef is the built-in file field value type exposed to apps and generators.
type FileRef struct {
	Path string `json:"path"`
	URL  string `json:"url"`
	Mime string `json:"mime"`
	Size int64  `json:"size"`
}

type Table[T any] struct {
	app  *App
	name string
	spec *tableSpec
}

type TableBuilder[T any] struct {
	table *Table[T]
}

type MaterializedBuilder struct {
	table *Table[map[string]any]
	spec  *materializedSpec
}

type FieldBuilder[T any] struct {
	table *Table[T]
	spec  *fieldSpec
}

type TableReadCtx struct {
	Auth *AuthContext
	Row  map[string]any
}

type TableInsertCtx struct {
	Auth *AuthContext
	New  map[string]any
}

type TableUpdateCtx struct {
	Auth *AuthContext
	Old  map[string]any
	New  map[string]any
}

type TableDeleteCtx struct {
	Auth *AuthContext
	Row  map[string]any
}

type FieldWriteCtx struct {
	Auth  *AuthContext
	Field string
	Old   map[string]any
	New   map[string]any
	Value any
}

type TableAccess struct {
	Read   func(*TableReadCtx) bool
	Insert func(*TableInsertCtx) bool
	Update func(*TableUpdateCtx) bool
	Delete func(*TableDeleteCtx) bool
}

type FieldAccess struct {
	Read  func(*TableReadCtx) bool
	Write func(*FieldWriteCtx) bool
}

func AutoTable[T any](app *App, name string, configure func(*TableBuilder[T])) *Table[T] {
	if app == nil {
		panic("flop: app is nil")
	}
	if name == "" {
		panic("flop: table name is empty")
	}
	if _, exists := app.tables[name]; exists {
		panic("flop: duplicate table name: " + name)
	}

	rowType := baseStructType(reflectTypeOf[T]())
	spec := &tableSpec{
		Name:    name,
		RowType: rowType.String(),
		RowTS:   tsTypeFromReflect(rowType),
		Fields:  inferFields(rowType),
	}
	app.tables[name] = spec

	t := &Table[T]{app: app, name: name, spec: spec}
	if configure != nil {
		configure(&TableBuilder[T]{table: t})
	}
	return t
}

func (t *Table[T]) tableName() string {
	if t == nil {
		return ""
	}
	return t.name
}

func (tb *TableBuilder[T]) Field(name string) *FieldBuilder[T] {
	if tb == nil || tb.table == nil || tb.table.spec == nil {
		panic("flop: invalid table builder")
	}
	fs := tb.table.spec.findOrCreateField(name)
	return &FieldBuilder[T]{table: tb.table, spec: fs}
}

func (tb *TableBuilder[T]) Access(access TableAccess) *TableBuilder[T] {
	if tb == nil || tb.table == nil || tb.table.spec == nil {
		panic("flop: invalid table builder")
	}
	tb.table.spec.Access = access
	return tb
}

func (fb *FieldBuilder[T]) Primary(strategy ...string) *FieldBuilder[T] {
	fb.spec.Primary = true
	if len(strategy) > 0 {
		fb.spec.PrimaryStrategy = normalizePrimaryStrategy(strategy[0])
	}
	return fb
}

func (fb *FieldBuilder[T]) Required() *FieldBuilder[T] {
	fb.spec.Required = true
	return fb
}

func (fb *FieldBuilder[T]) Unique() *FieldBuilder[T] {
	fb.spec.Unique = true
	return fb
}

func (fb *FieldBuilder[T]) Default(value any) *FieldBuilder[T] {
	fb.spec.Default = value
	return fb
}

func (fb *FieldBuilder[T]) DefaultNow() *FieldBuilder[T] {
	fb.spec.Default = "now"
	return fb
}

func (fb *FieldBuilder[T]) Autogen(pattern string) *FieldBuilder[T] {
	fb.spec.Autogen = pattern
	return fb
}

func (fb *FieldBuilder[T]) Bcrypt(rounds int) *FieldBuilder[T] {
	fb.spec.Kind = "bcrypt"
	fb.spec.BcryptRounds = rounds
	return fb
}

func (fb *FieldBuilder[T]) Roles() *FieldBuilder[T] {
	fb.spec.Kind = "roles"
	return fb
}

func (fb *FieldBuilder[T]) Timestamp() *FieldBuilder[T] {
	fb.spec.Kind = "timestamp"
	return fb
}

func (fb *FieldBuilder[T]) Ref(other any, field string) *FieldBuilder[T] {
	fb.spec.Kind = "refSingle"
	if nt, ok := other.(interface{ tableName() string }); ok {
		fb.spec.RefTable = nt.tableName()
	}
	fb.spec.RefField = field
	return fb
}

func (fb *FieldBuilder[T]) FileSingle(mime ...string) *FieldBuilder[T] {
	fb.spec.Kind = "fileSingle"
	fb.spec.MimeTypes = append([]string(nil), mime...)
	return fb
}

func (fb *FieldBuilder[T]) FileMulti(mime ...string) *FieldBuilder[T] {
	fb.spec.Kind = "fileMulti"
	fb.spec.MimeTypes = append([]string(nil), mime...)
	return fb
}

// Thumbs defines allowed thumbnail sizes for file fields (AutoTable API).
func (fb *FieldBuilder[T]) Thumbs(sizes ...string) *FieldBuilder[T] {
	fb.spec.ThumbSizes = append(fb.spec.ThumbSizes[:0:0], sizes...)
	return fb
}

func (fb *FieldBuilder[T]) HasMany(other any, foreignField string) *FieldBuilder[T] {
	fb.spec.Kind = "relation"
	fb.spec.Relation = "hasMany"
	if nt, ok := other.(interface{ tableName() string }); ok {
		fb.spec.RelationTable = nt.tableName()
	}
	fb.spec.RelationField = foreignField
	return fb
}

func (fb *FieldBuilder[T]) BelongsTo(other any, foreignField string) *FieldBuilder[T] {
	fb.spec.Kind = "relation"
	fb.spec.Relation = "belongsTo"
	if nt, ok := other.(interface{ tableName() string }); ok {
		fb.spec.RelationTable = nt.tableName()
	}
	fb.spec.RelationField = foreignField
	return fb
}

func (fb *FieldBuilder[T]) Virtual() *FieldBuilder[T] {
	fb.spec.Virtual = true
	return fb
}

func (fb *FieldBuilder[T]) Index() *FieldBuilder[T] {
	fb.spec.Indexed = true
	return fb
}

func (fb *FieldBuilder[T]) FullText() *FieldBuilder[T] {
	fb.spec.FullText = true
	return fb
}

func (fb *FieldBuilder[T]) Access(access FieldAccess) *FieldBuilder[T] {
	fb.spec.Access = access
	return fb
}

// SchemaBuilder provides a schema-first table definition API.
type SchemaBuilder struct {
	table *tableSpec
}

type StringFieldRules struct{ spec *fieldSpec }
type NumberFieldRules struct{ spec *fieldSpec }
type IntegerFieldRules struct{ spec *fieldSpec }
type BooleanFieldRules struct{ spec *fieldSpec }
type JSONFieldRules struct{ spec *fieldSpec }
type TimestampFieldRules struct{ spec *fieldSpec }
type BcryptFieldRules struct{ spec *fieldSpec }
type RolesFieldRules struct{ spec *fieldSpec }
type EnumFieldRules struct{ spec *fieldSpec }
type RefFieldRules struct{ spec *fieldSpec }
type RefMultiFieldRules struct{ spec *fieldSpec }
type FileSingleFieldRules struct{ spec *fieldSpec }
type FileMultiFieldRules struct{ spec *fieldSpec }
type SetFieldRules struct{ spec *fieldSpec }
type VectorFieldRules struct{ spec *fieldSpec }
type CachedFieldRules struct {
	spec     *fieldSpec
	ts       *tableSpec
	triggers []cachedTriggerDef
}

// Define creates a schema-first table with typed field builders.
func Define(app *App, name string, configure func(*SchemaBuilder)) *Table[map[string]any] {
	if app == nil {
		panic("flop: app is nil")
	}
	if name == "" {
		panic("flop: table name is empty")
	}
	if _, exists := app.tables[name]; exists {
		panic("flop: duplicate table name: " + name)
	}

	spec := &tableSpec{
		Name:    name,
		RowType: defaultRowTypeName(name),
		Fields:  make(map[string]*fieldSpec),
	}
	app.tables[name] = spec

	t := &Table[map[string]any]{app: app, name: name, spec: spec}
	if configure != nil {
		configure(&SchemaBuilder{table: spec})
	}
	spec.RowTS = rowTSTypeFromFields(spec.Fields)
	return t
}

// Materialized creates a normal table whose contents are engine-managed by a refresh callback.
func Materialized(app *App, name string, configure func(*MaterializedBuilder)) *Table[map[string]any] {
	t := Define(app, name, nil)
	if t == nil {
		return nil
	}
	ms := &materializedSpec{RefreshOnStartup: true}
	t.spec.Materialized = ms
	if configure != nil {
		configure(&MaterializedBuilder{table: t, spec: ms})
	}
	t.spec.RowTS = rowTSTypeFromFields(t.spec.Fields)
	return t
}

func (a *App) Materialized(name string) *MaterializedBuilder {
	if a == nil {
		panic("flop: app is nil")
	}
	ts, ok := a.tables[name]
	if !ok {
		panic("flop: unknown table: " + name)
	}
	if ts.Materialized == nil {
		ts.Materialized = &materializedSpec{RefreshOnStartup: true}
	}
	return &MaterializedBuilder{table: &Table[map[string]any]{app: a, name: name, spec: ts}, spec: ts.Materialized}
}

func (sb *SchemaBuilder) String(name string) *StringFieldRules {
	return &StringFieldRules{spec: sb.field(name, "string", "string")}
}

func (sb *SchemaBuilder) Number(name string) *NumberFieldRules {
	return &NumberFieldRules{spec: sb.field(name, "number", "number")}
}

func (sb *SchemaBuilder) Integer(name string) *IntegerFieldRules {
	return &IntegerFieldRules{spec: sb.field(name, "integer", "number")}
}

func (sb *SchemaBuilder) Boolean(name string) *BooleanFieldRules {
	return &BooleanFieldRules{spec: sb.field(name, "boolean", "boolean")}
}

func (sb *SchemaBuilder) JSON(name string) *JSONFieldRules {
	return &JSONFieldRules{spec: sb.field(name, "json", "any")}
}

func (sb *SchemaBuilder) Timestamp(name string) *TimestampFieldRules {
	return &TimestampFieldRules{spec: sb.field(name, "timestamp", "number")}
}

func (sb *SchemaBuilder) Bcrypt(name string, rounds int) *BcryptFieldRules {
	fs := sb.field(name, "bcrypt", "string")
	fs.BcryptRounds = rounds
	return &BcryptFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Roles(name string) *RolesFieldRules {
	fs := sb.field(name, "roles", "(string)[]")
	if fs.Default == nil {
		fs.Default = []string{}
	}
	return &RolesFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Enum(name string, values ...string) *EnumFieldRules {
	fs := sb.field(name, "enum", tsEnumType(values))
	fs.EnumValues = append([]string(nil), values...)
	return &EnumFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Ref(name string, other any, field string) *RefFieldRules {
	fs := sb.field(name, "refSingle", "string")
	if nt, ok := other.(interface{ tableName() string }); ok {
		fs.RefTable = nt.tableName()
	}
	fs.RefField = field
	return &RefFieldRules{spec: fs}
}

func (sb *SchemaBuilder) RefMulti(name string, other any, field string) *RefMultiFieldRules {
	fs := sb.field(name, "refMulti", "(string)[]")
	if nt, ok := other.(interface{ tableName() string }); ok {
		fs.RefTable = nt.tableName()
	}
	fs.RefField = field
	if fs.Default == nil {
		fs.Default = []string{}
	}
	return &RefMultiFieldRules{spec: fs}
}

func (sb *SchemaBuilder) FileSingle(name string, mime ...string) *FileSingleFieldRules {
	fs := sb.field(name, "fileSingle", "{ path: string; url: string; mime: string; size: number }")
	fs.MimeTypes = append([]string(nil), mime...)
	return &FileSingleFieldRules{spec: fs}
}

func (sb *SchemaBuilder) FileMulti(name string, mime ...string) *FileMultiFieldRules {
	fs := sb.field(name, "fileMulti", "({ path: string; url: string; mime: string; size: number })[]")
	fs.MimeTypes = append([]string(nil), mime...)
	return &FileMultiFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Set(name string) *SetFieldRules {
	fs := sb.field(name, "set", "(string)[]")
	if fs.Default == nil {
		fs.Default = []string{}
	}
	return &SetFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Vector(name string, dimensions int) *VectorFieldRules {
	fs := sb.field(name, "vector", "(number)[]")
	fs.VectorDimensions = dimensions
	return &VectorFieldRules{spec: fs}
}

func (sb *SchemaBuilder) Access(access TableAccess) {
	if sb == nil || sb.table == nil {
		panic("flop: invalid schema builder")
	}
	sb.table.Access = access
}

// Cached creates an engine-managed computed field.
// The value is automatically recomputed when a source table changes.
func (sb *SchemaBuilder) Cached(name string, hint CachedTypeHint) *CachedFieldRules {
	var kind, tsType string
	var def any
	switch hint {
	case Number:
		kind = "number"
		tsType = "number"
		def = float64(0)
	case Str:
		kind = "string"
		tsType = "string"
		def = ""
	default: // Int
		kind = "integer"
		tsType = "number"
		def = float64(0)
	}
	fs := sb.field(name, kind, tsType)
	fs.Cached = true
	fs.Required = false
	fs.Default = def
	return &CachedFieldRules{spec: fs, ts: sb.table}
}

func (sb *SchemaBuilder) field(name, kind, tsType string) *fieldSpec {
	if sb == nil || sb.table == nil {
		panic("flop: invalid schema builder")
	}
	if name == "" {
		panic("flop: field name is empty")
	}
	fs := sb.table.findOrCreateFieldByJSON(name)
	fs.Kind = kind
	fs.TSType = tsType
	return fs
}

// Migration declares a migration step for this table's schema version.
// This allows field type changes (e.g. string → fileSingle) to pass validation.
func (sb *SchemaBuilder) Migration(version int, rename ...map[string]string) {
	step := migrationStep{Version: version}
	if len(rename) > 0 {
		step.Rename = rename[0]
	}
	sb.table.Migrations = append(sb.table.Migrations, step)
}

func (mb *MaterializedBuilder) ensureSchema() *SchemaBuilder {
	if mb == nil || mb.table == nil || mb.table.spec == nil {
		panic("flop: invalid materialized builder")
	}
	return &SchemaBuilder{table: mb.table.spec}
}

func (mb *MaterializedBuilder) String(name string) *StringFieldRules {
	return mb.ensureSchema().String(name)
}

func (mb *MaterializedBuilder) Number(name string) *NumberFieldRules {
	return mb.ensureSchema().Number(name)
}

func (mb *MaterializedBuilder) Integer(name string) *IntegerFieldRules {
	return mb.ensureSchema().Integer(name)
}

func (mb *MaterializedBuilder) Boolean(name string) *BooleanFieldRules {
	return mb.ensureSchema().Boolean(name)
}

func (mb *MaterializedBuilder) JSON(name string) *JSONFieldRules {
	return mb.ensureSchema().JSON(name)
}

func (mb *MaterializedBuilder) Timestamp(name string) *TimestampFieldRules {
	return mb.ensureSchema().Timestamp(name)
}

func (mb *MaterializedBuilder) Bcrypt(name string, rounds int) *BcryptFieldRules {
	return mb.ensureSchema().Bcrypt(name, rounds)
}

func (mb *MaterializedBuilder) Roles(name string) *RolesFieldRules {
	return mb.ensureSchema().Roles(name)
}

func (mb *MaterializedBuilder) Enum(name string, values ...string) *EnumFieldRules {
	return mb.ensureSchema().Enum(name, values...)
}

func (mb *MaterializedBuilder) Ref(name string, other any, field string) *RefFieldRules {
	return mb.ensureSchema().Ref(name, other, field)
}

func (mb *MaterializedBuilder) RefMulti(name string, other any, field string) *RefMultiFieldRules {
	return mb.ensureSchema().RefMulti(name, other, field)
}

func (mb *MaterializedBuilder) FileSingle(name string, mime ...string) *FileSingleFieldRules {
	return mb.ensureSchema().FileSingle(name, mime...)
}

func (mb *MaterializedBuilder) FileMulti(name string, mime ...string) *FileMultiFieldRules {
	return mb.ensureSchema().FileMulti(name, mime...)
}

func (mb *MaterializedBuilder) Set(name string) *SetFieldRules {
	return mb.ensureSchema().Set(name)
}

func (mb *MaterializedBuilder) Vector(name string, dimensions int) *VectorFieldRules {
	return mb.ensureSchema().Vector(name, dimensions)
}

func (mb *MaterializedBuilder) Access(access TableAccess) {
	mb.ensureSchema().Access(access)
}

func (mb *MaterializedBuilder) Migration(version int, rename ...map[string]string) {
	mb.ensureSchema().Migration(version, rename...)
}

func (mb *MaterializedBuilder) Refresh(fn func(*Database) error) {
	if mb == nil || mb.spec == nil {
		panic("flop: invalid materialized builder")
	}
	if fn == nil {
		panic("flop: materialized refresh handler is nil")
	}
	mb.spec.Refresh = fn
}

func (mb *MaterializedBuilder) Cron(expr string) {
	if mb == nil || mb.spec == nil {
		panic("flop: invalid materialized builder")
	}
	mb.spec.Cron = strings.TrimSpace(expr)
}

func (mb *MaterializedBuilder) RefreshOnStartup(enabled bool) {
	if mb == nil || mb.spec == nil {
		panic("flop: invalid materialized builder")
	}
	mb.spec.RefreshOnStartup = enabled
}

func (b *StringFieldRules) Primary(strategy ...string) *StringFieldRules {
	b.spec.Primary = true
	if len(strategy) > 0 {
		b.spec.PrimaryStrategy = normalizePrimaryStrategy(strategy[0])
	}
	return b
}
func (b *StringFieldRules) Required() *StringFieldRules        { b.spec.Required = true; return b }
func (b *StringFieldRules) Unique() *StringFieldRules          { b.spec.Unique = true; return b }
func (b *StringFieldRules) Default(v any) *StringFieldRules    { b.spec.Default = v; return b }
func (b *StringFieldRules) Autogen(p string) *StringFieldRules { b.spec.Autogen = p; return b }
func (b *StringFieldRules) Index() *StringFieldRules           { b.spec.Indexed = true; return b }
func (b *StringFieldRules) FullText() *StringFieldRules        { b.spec.FullText = true; return b }
func (b *StringFieldRules) Virtual() *StringFieldRules         { b.spec.Virtual = true; return b }
func (b *StringFieldRules) MinLen(n int) *StringFieldRules     { b.spec.MinLen = &n; return b }
func (b *StringFieldRules) MaxLen(n int) *StringFieldRules     { b.spec.MaxLen = &n; return b }
func (b *StringFieldRules) Pattern(expr string) *StringFieldRules {
	b.spec.Pattern = expr
	return b
}
func (b *StringFieldRules) Email() *StringFieldRules { b.spec.Format = "email"; return b }
func (b *StringFieldRules) Access(access FieldAccess) *StringFieldRules {
	b.spec.Access = access
	return b
}

func (b *NumberFieldRules) Required() *NumberFieldRules { b.spec.Required = true; return b }
func (b *NumberFieldRules) Primary(strategy ...string) *NumberFieldRules {
	b.spec.Primary = true
	if len(strategy) > 0 {
		b.spec.PrimaryStrategy = normalizePrimaryStrategy(strategy[0])
	}
	return b
}
func (b *NumberFieldRules) Unique() *NumberFieldRules       { b.spec.Unique = true; return b }
func (b *NumberFieldRules) Default(v any) *NumberFieldRules { b.spec.Default = v; return b }
func (b *NumberFieldRules) Index() *NumberFieldRules        { b.spec.Indexed = true; return b }
func (b *NumberFieldRules) Virtual() *NumberFieldRules      { b.spec.Virtual = true; return b }
func (b *NumberFieldRules) Min(v float64) *NumberFieldRules { b.spec.Min = &v; return b }
func (b *NumberFieldRules) Max(v float64) *NumberFieldRules { b.spec.Max = &v; return b }
func (b *NumberFieldRules) Access(access FieldAccess) *NumberFieldRules {
	b.spec.Access = access
	return b
}

func (b *IntegerFieldRules) Required() *IntegerFieldRules { b.spec.Required = true; return b }
func (b *IntegerFieldRules) Primary(strategy ...string) *IntegerFieldRules {
	b.spec.Primary = true
	if len(strategy) > 0 {
		b.spec.PrimaryStrategy = normalizePrimaryStrategy(strategy[0])
	}
	return b
}
func (b *IntegerFieldRules) Unique() *IntegerFieldRules       { b.spec.Unique = true; return b }
func (b *IntegerFieldRules) Default(v any) *IntegerFieldRules { b.spec.Default = v; return b }
func (b *IntegerFieldRules) Index() *IntegerFieldRules        { b.spec.Indexed = true; return b }
func (b *IntegerFieldRules) Virtual() *IntegerFieldRules      { b.spec.Virtual = true; return b }
func (b *IntegerFieldRules) Min(v float64) *IntegerFieldRules { b.spec.Min = &v; return b }
func (b *IntegerFieldRules) Max(v float64) *IntegerFieldRules { b.spec.Max = &v; return b }
func (b *IntegerFieldRules) Access(access FieldAccess) *IntegerFieldRules {
	b.spec.Access = access
	return b
}

func (b *BooleanFieldRules) Required() *BooleanFieldRules     { b.spec.Required = true; return b }
func (b *BooleanFieldRules) Default(v any) *BooleanFieldRules { b.spec.Default = v; return b }
func (b *BooleanFieldRules) Index() *BooleanFieldRules        { b.spec.Indexed = true; return b }
func (b *BooleanFieldRules) Virtual() *BooleanFieldRules      { b.spec.Virtual = true; return b }
func (b *BooleanFieldRules) Access(access FieldAccess) *BooleanFieldRules {
	b.spec.Access = access
	return b
}

func (b *JSONFieldRules) Required() *JSONFieldRules     { b.spec.Required = true; return b }
func (b *JSONFieldRules) Default(v any) *JSONFieldRules { b.spec.Default = v; return b }
func (b *JSONFieldRules) Index() *JSONFieldRules        { b.spec.Indexed = true; return b }
func (b *JSONFieldRules) Virtual() *JSONFieldRules      { b.spec.Virtual = true; return b }
func (b *JSONFieldRules) Access(access FieldAccess) *JSONFieldRules {
	b.spec.Access = access
	return b
}

func (b *TimestampFieldRules) Required() *TimestampFieldRules     { b.spec.Required = true; return b }
func (b *TimestampFieldRules) Unique() *TimestampFieldRules       { b.spec.Unique = true; return b }
func (b *TimestampFieldRules) Default(v any) *TimestampFieldRules { b.spec.Default = v; return b }
func (b *TimestampFieldRules) DefaultNow() *TimestampFieldRules   { b.spec.Default = "now"; return b }
func (b *TimestampFieldRules) Index() *TimestampFieldRules        { b.spec.Indexed = true; return b }
func (b *TimestampFieldRules) Virtual() *TimestampFieldRules      { b.spec.Virtual = true; return b }
func (b *TimestampFieldRules) Min(v float64) *TimestampFieldRules { b.spec.Min = &v; return b }
func (b *TimestampFieldRules) Max(v float64) *TimestampFieldRules { b.spec.Max = &v; return b }
func (b *TimestampFieldRules) Access(access FieldAccess) *TimestampFieldRules {
	b.spec.Access = access
	return b
}

func (b *BcryptFieldRules) Required() *BcryptFieldRules { b.spec.Required = true; return b }
func (b *BcryptFieldRules) Index() *BcryptFieldRules    { b.spec.Indexed = true; return b }
func (b *BcryptFieldRules) Virtual() *BcryptFieldRules  { b.spec.Virtual = true; return b }
func (b *BcryptFieldRules) Access(access FieldAccess) *BcryptFieldRules {
	b.spec.Access = access
	return b
}

func (b *RolesFieldRules) Required() *RolesFieldRules { b.spec.Required = true; return b }
func (b *RolesFieldRules) Index() *RolesFieldRules    { b.spec.Indexed = true; return b }
func (b *RolesFieldRules) Virtual() *RolesFieldRules  { b.spec.Virtual = true; return b }
func (b *RolesFieldRules) Access(access FieldAccess) *RolesFieldRules {
	b.spec.Access = access
	return b
}

func (b *EnumFieldRules) Required() *EnumFieldRules     { b.spec.Required = true; return b }
func (b *EnumFieldRules) Unique() *EnumFieldRules       { b.spec.Unique = true; return b }
func (b *EnumFieldRules) Default(v any) *EnumFieldRules { b.spec.Default = v; return b }
func (b *EnumFieldRules) Index() *EnumFieldRules        { b.spec.Indexed = true; return b }
func (b *EnumFieldRules) Virtual() *EnumFieldRules      { b.spec.Virtual = true; return b }
func (b *EnumFieldRules) Access(access FieldAccess) *EnumFieldRules {
	b.spec.Access = access
	return b
}

func (b *RefFieldRules) Primary(strategy ...string) *RefFieldRules {
	b.spec.Primary = true
	if len(strategy) > 0 {
		b.spec.PrimaryStrategy = normalizePrimaryStrategy(strategy[0])
	}
	return b
}
func (b *RefFieldRules) Required() *RefFieldRules        { b.spec.Required = true; return b }
func (b *RefFieldRules) Unique() *RefFieldRules          { b.spec.Unique = true; return b }
func (b *RefFieldRules) Default(v any) *RefFieldRules    { b.spec.Default = v; return b }
func (b *RefFieldRules) Autogen(p string) *RefFieldRules { b.spec.Autogen = p; return b }
func (b *RefFieldRules) Index() *RefFieldRules           { b.spec.Indexed = true; return b }
func (b *RefFieldRules) CascadeArchive() *RefFieldRules {
	b.spec.CascadeDelete = "archive"
	b.spec.Indexed = true
	return b
}
func (b *RefFieldRules) Virtual() *RefFieldRules { b.spec.Virtual = true; return b }
func (b *RefFieldRules) Access(access FieldAccess) *RefFieldRules {
	b.spec.Access = access
	return b
}

func (b *RefMultiFieldRules) Required() *RefMultiFieldRules     { b.spec.Required = true; return b }
func (b *RefMultiFieldRules) Default(v any) *RefMultiFieldRules { b.spec.Default = v; return b }
func (b *RefMultiFieldRules) Index() *RefMultiFieldRules        { b.spec.Indexed = true; return b }
func (b *RefMultiFieldRules) CascadeArchive() *RefMultiFieldRules {
	b.spec.CascadeDelete = "archive"
	b.spec.Indexed = true
	return b
}
func (b *RefMultiFieldRules) Virtual() *RefMultiFieldRules { b.spec.Virtual = true; return b }
func (b *RefMultiFieldRules) Access(access FieldAccess) *RefMultiFieldRules {
	b.spec.Access = access
	return b
}

func (b *FileSingleFieldRules) Required() *FileSingleFieldRules { b.spec.Required = true; return b }
func (b *FileSingleFieldRules) Virtual() *FileSingleFieldRules  { b.spec.Virtual = true; return b }
func (b *FileSingleFieldRules) Access(access FieldAccess) *FileSingleFieldRules {
	b.spec.Access = access
	return b
}

// Thumbs defines allowed thumbnail sizes for image file fields.
// Format: "WxH" where W or H can be 0 for aspect-ratio preservation.
// Example: Thumbs("160x160", "80x80") or Thumbs("1200x0") for max-width.
func (b *FileSingleFieldRules) Thumbs(sizes ...string) *FileSingleFieldRules {
	b.spec.ThumbSizes = append(b.spec.ThumbSizes[:0:0], sizes...)
	return b
}

func (b *FileMultiFieldRules) Required() *FileMultiFieldRules { b.spec.Required = true; return b }
func (b *FileMultiFieldRules) Virtual() *FileMultiFieldRules  { b.spec.Virtual = true; return b }
func (b *FileMultiFieldRules) Access(access FieldAccess) *FileMultiFieldRules {
	b.spec.Access = access
	return b
}

// Thumbs defines allowed thumbnail sizes for image file fields.
func (b *FileMultiFieldRules) Thumbs(sizes ...string) *FileMultiFieldRules {
	b.spec.ThumbSizes = append(b.spec.ThumbSizes[:0:0], sizes...)
	return b
}

func (b *SetFieldRules) Required() *SetFieldRules     { b.spec.Required = true; return b }
func (b *SetFieldRules) Default(v any) *SetFieldRules { b.spec.Default = v; return b }
func (b *SetFieldRules) Index() *SetFieldRules        { b.spec.Indexed = true; return b }
func (b *SetFieldRules) Virtual() *SetFieldRules      { b.spec.Virtual = true; return b }
func (b *SetFieldRules) Access(access FieldAccess) *SetFieldRules {
	b.spec.Access = access
	return b
}

func (b *VectorFieldRules) Required() *VectorFieldRules     { b.spec.Required = true; return b }
func (b *VectorFieldRules) Default(v any) *VectorFieldRules { b.spec.Default = v; return b }
func (b *VectorFieldRules) Index() *VectorFieldRules        { b.spec.Indexed = true; return b }
func (b *VectorFieldRules) Virtual() *VectorFieldRules      { b.spec.Virtual = true; return b }
func (b *VectorFieldRules) Access(access FieldAccess) *VectorFieldRules {
	b.spec.Access = access
	return b
}

// OnChange registers a source table trigger for this cached field.
// When a row in sourceTable is inserted/updated/deleted, the foreignKey
// field on that row identifies the target row to recompute.
func (c *CachedFieldRules) OnChange(sourceTable, foreignKey string) *CachedFieldRules {
	c.triggers = append(c.triggers, cachedTriggerDef{
		SourceTable: sourceTable,
		ForeignKey:  foreignKey,
	})
	return c
}

// Compute sets the function that calculates the cached value.
func (c *CachedFieldRules) Compute(fn func(row Row, db *Database) any) *CachedFieldRules {
	c.ts.CachedDefs = append(c.ts.CachedDefs, cachedFieldRuntime{
		FieldName: c.spec.JSONName,
		Triggers:  append([]cachedTriggerDef(nil), c.triggers...),
		Compute:   fn,
	})
	return c
}

func tsEnumType(values []string) string {
	if len(values) == 0 {
		return "string"
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, strconv.Quote(v))
	}
	return strings.Join(out, " | ")
}

func defaultRowTypeName(tableName string) string {
	name := toExportedGoName(tableName)
	if name == "" {
		return "Row"
	}
	if strings.HasSuffix(name, "s") && len(name) > 1 {
		name = name[:len(name)-1]
	}
	return name
}

func rowTSTypeFromFields(fields map[string]*fieldSpec) string {
	if len(fields) == 0 {
		return "{ [key: string]: any }"
	}
	items := make([]string, 0, len(fields))
	for _, fs := range fields {
		key := fs.JSONName
		if !isTSIdentifier(key) {
			key = strconv.Quote(key)
		}
		opt := ""
		if !fs.Required {
			opt = "?"
		}
		tsType := fs.TSType
		if tsType == "" {
			tsType = "any"
		}
		items = append(items, key+opt+": "+tsType)
	}
	sort.Strings(items)
	return "{ " + strings.Join(items, "; ") + " }"
}

func isTSIdentifier(s string) bool {
	if s == "" {
		return false
	}
	r := []rune(s)
	if !(unicode.IsLetter(r[0]) || r[0] == '_' || r[0] == '$') {
		return false
	}
	for _, ch := range r[1:] {
		if !(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '$') {
			return false
		}
	}
	return true
}

type Update map[string]any

func Set(field string, value any) Update {
	return Update{field: value}
}

func (t *Table[T]) Insert(scope any, row T) (T, error) {
	if t == nil {
		var zero T
		return zero, ErrNotImplemented
	}
	ti, tx, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		var zero T
		return zero, err
	}
	data, err := valueToRowMap(row)
	if err != nil {
		var zero T
		return zero, err
	}
	var out map[string]any
	if tx != nil {
		out, err = ti.insertWithTx(data, tx.inner.txBuf)
		if err == nil && tx.inner != nil {
			pk := toString(out[ti.primaryKeyField()])
			if pk != "" {
				tx.inner.addUndo(func() { _ = ti.rollbackInserted(pk) })
			}
		}
	} else {
		out, err = ti.Insert(data)
	}
	if err != nil {
		var zero T
		return zero, err
	}
	var typed T
	if err := rowMapToValue(out, &typed); err != nil {
		var zero T
		return zero, err
	}
	return typed, nil
}

func (t *Table[T]) Get(scope any, id string) (*T, error) {
	ti, _, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		return nil, err
	}
	row, err := ti.Get(id)
	if err != nil || row == nil {
		return nil, err
	}
	var typed T
	if err := rowMapToValue(row, &typed); err != nil {
		return nil, err
	}
	return &typed, nil
}

func (t *Table[T]) Update(scope any, id string, updates ...Update) error {
	ti, tx, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		return err
	}
	merged := make(map[string]any)
	for _, update := range updates {
		for k, v := range update {
			merged[k] = v
		}
	}
	if tx != nil {
		before, err := ti.rawRow(id)
		if err != nil {
			return err
		}
		_, err = ti.updateWithTx(id, merged, tx.inner.txBuf)
		if err == nil && len(before) > 0 && tx.inner != nil {
			tx.inner.addUndo(func() { _ = ti.rollbackRawRow(before) })
		}
		return err
	}
	_, err = ti.Update(id, merged)
	return err
}

func (t *Table[T]) Delete(scope any, id string) (bool, error) {
	ti, tx, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		return false, err
	}
	if tx != nil {
		before, err := ti.rawRow(id)
		if err != nil {
			return false, err
		}
		ok, err := ti.deleteWithTx(id, tx.inner.txBuf)
		if err == nil && ok && len(before) > 0 && tx.inner != nil {
			tx.inner.addUndo(func() { _ = ti.rollbackRawRow(before) })
		}
		return ok, err
	}
	return ti.Delete(id)
}

func (t *Table[T]) Archive(scope any, id string) (*storage.ArchivedRow, error) {
	return t.ArchiveWithOptions(scope, id, ArchiveOptions{})
}

func (t *Table[T]) ArchiveWithOptions(scope any, id string, opts ArchiveOptions) (*storage.ArchivedRow, error) {
	ti, tx, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		rootTable := strings.TrimSpace(opts.CascadeRootTable)
		if rootTable == "" {
			rootTable = t.name
		}
		rootPK := strings.TrimSpace(opts.CascadeRootPK)
		if rootPK == "" {
			rootPK = id
		}
		return ti.archiveCascade(tx.inner, id, strings.TrimSpace(opts.CascadeGroupID), rootTable, rootPK, opts.CascadeDepth)
	}
	return ti.ArchiveWithOptions(id, opts)
}

func (t *Table[T]) Scan(scope any, limit, offset int) ([]T, error) {
	ti, _, err := resolveTypedTableScope(scope, t.name)
	if err != nil {
		return nil, err
	}
	rows, err := ti.Scan(limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		var typed T
		if err := rowMapToValue(row, &typed); err != nil {
			return nil, err
		}
		out = append(out, typed)
	}
	return out, nil
}

func (t *Table[T]) Count(scope any) int {
	ti, _, err := resolveTypedTableScope(scope, t.name)
	if err != nil || ti == nil {
		return 0
	}
	return ti.Count()
}

type AccessPolicy struct {
	Type  string   `json:"type"`
	Roles []string `json:"roles,omitempty"`
}

func Authenticated() AccessPolicy {
	return AccessPolicy{Type: "authenticated"}
}

func Public() AccessPolicy {
	return AccessPolicy{Type: "public"}
}

func Roles(roles ...string) AccessPolicy {
	return AccessPolicy{Type: "roles", Roles: append([]string(nil), roles...)}
}

type AuthContext struct {
	ID    string
	Email string
	Roles []string
}

func (a *AuthContext) HasRole(role string) bool {
	if a == nil {
		return false
	}
	need := strings.TrimSpace(role)
	if need == "" {
		return false
	}
	for _, have := range a.Roles {
		if strings.EqualFold(have, need) {
			return true
		}
	}
	return false
}

type RequestContext struct {
	Auth *AuthContext
}

type ViewCtx struct {
	Request RequestContext
	DB      *DBAccessor
}

type ReducerCtx struct {
	Request RequestContext
	DB      *DBAccessor
}

type Tx struct {
	DB    *DBAccessor
	inner *archiveTxn
}

func (ctx *ViewCtx) RequireAuth() (*AuthContext, error) {
	if ctx == nil || ctx.Request.Auth == nil {
		return nil, errors.New("authentication required")
	}
	return ctx.Request.Auth, nil
}

func (ctx *ReducerCtx) RequireAuth() (*AuthContext, error) {
	if ctx == nil || ctx.Request.Auth == nil {
		return nil, errors.New("authentication required")
	}
	return ctx.Request.Auth, nil
}

func Transaction[T any](ctx *ReducerCtx, fn func(*Tx) (T, error)) (T, error) {
	if fn == nil {
		var zero T
		return zero, errors.New("transaction function is nil")
	}
	if ctx == nil || ctx.DB == nil || ctx.DB.db == nil {
		var zero T
		return zero, errors.New("transaction requires reducer context with database")
	}
	inner := newArchiveTxn(ctx.DB.db)
	tx := &Tx{
		DB: &DBAccessor{
			db:            ctx.DB.db,
			tracker:       ctx.DB.tracker,
			auth:          ctx.DB.auth,
			enforcePolicy: ctx.DB.enforcePolicy,
		},
		inner: inner,
	}
	result, err := fn(tx)
	if err != nil {
		inner.rollback()
		var zero T
		return zero, err
	}
	if err := inner.commit(); err != nil {
		inner.rollback()
		var zero T
		return zero, err
	}
	return result, nil
}

func resolveTypedTableScope(scope any, tableName string) (*TableInstance, *Tx, error) {
	switch s := scope.(type) {
	case *Tx:
		if s == nil || s.DB == nil {
			return nil, nil, errors.New("transaction scope is nil")
		}
		ti := s.DB.Table(tableName)
		if ti == nil {
			return nil, nil, fmt.Errorf("table not found: %s", tableName)
		}
		return ti, s, nil
	case *ReducerCtx:
		if s == nil || s.DB == nil {
			return nil, nil, errors.New("reducer scope is nil")
		}
		ti := s.DB.Table(tableName)
		if ti == nil {
			return nil, nil, fmt.Errorf("table not found: %s", tableName)
		}
		return ti, nil, nil
	case *ViewCtx:
		if s == nil || s.DB == nil {
			return nil, nil, errors.New("view scope is nil")
		}
		ti := s.DB.Table(tableName)
		if ti == nil {
			return nil, nil, fmt.Errorf("table not found: %s", tableName)
		}
		return ti, nil, nil
	case *DBAccessor:
		if s == nil {
			return nil, nil, errors.New("db scope is nil")
		}
		ti := s.Table(tableName)
		if ti == nil {
			return nil, nil, fmt.Errorf("table not found: %s", tableName)
		}
		return ti, nil, nil
	case *Database:
		if s == nil {
			return nil, nil, errors.New("database scope is nil")
		}
		ti := s.Table(tableName)
		if ti == nil {
			return nil, nil, fmt.Errorf("table not found: %s", tableName)
		}
		return ti, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported scope type %T", scope)
	}
}

func valueToRowMap(v any) (map[string]any, error) {
	if v == nil {
		return map[string]any{}, nil
	}
	if row, ok := v.(map[string]any); ok {
		return row, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any)
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func rowMapToValue(row map[string]any, out any) error {
	raw, err := json.Marshal(row)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

type ViewDef[In, Out any] struct {
	Name    string
	Access  AccessPolicy
	Handler func(*ViewCtx, In) (Out, error)
}

type ReducerDef[In, Out any] struct {
	Name    string
	Access  AccessPolicy
	Handler func(*ReducerCtx, In) (Out, error)
}

func View[In, Out any](app *App, name string, access AccessPolicy, handler func(*ViewCtx, In) (Out, error)) *ViewDef[In, Out] {
	if app == nil {
		panic("flop: app is nil")
	}
	if strings.TrimSpace(name) == "" {
		panic("flop: view name is empty")
	}
	if _, exists := app.viewDefs[name]; exists {
		panic("flop: duplicate view name: " + name)
	}
	if access.Type == "" {
		access = Authenticated()
	}
	app.views = append(app.views, endpointSpec{
		Name:     name,
		Access:   access,
		InputTS:  tsTypeFromReflect(reflectTypeOf[In]()),
		OutputTS: tsTypeFromReflect(reflectTypeOf[Out]()),
	})
	app.viewDefs[name] = buildViewRuntime(name, access, handler)
	return &ViewDef[In, Out]{Name: name, Access: access, Handler: handler}
}

func Reducer[In, Out any](app *App, name string, access AccessPolicy, handler func(*ReducerCtx, In) (Out, error)) *ReducerDef[In, Out] {
	if app == nil {
		panic("flop: app is nil")
	}
	if strings.TrimSpace(name) == "" {
		panic("flop: reducer name is empty")
	}
	if _, exists := app.reduceDefs[name]; exists {
		panic("flop: duplicate reducer name: " + name)
	}
	if access.Type == "" {
		access = Authenticated()
	}
	app.reducers = append(app.reducers, endpointSpec{
		Name:     name,
		Access:   access,
		InputTS:  tsTypeFromReflect(reflectTypeOf[In]()),
		OutputTS: tsTypeFromReflect(reflectTypeOf[Out]()),
	})
	app.reduceDefs[name] = buildReducerRuntime(name, access, handler)
	return &ReducerDef[In, Out]{Name: name, Access: access, Handler: handler}
}

type MetaTag struct {
	Name    string
	Content string
}

type LinkTag struct {
	Rel  string
	Href string
}

type ScriptTag struct {
	Type    string
	Content string
	Src     string
}

type OpenGraph struct {
	Title       string
	Description string
	Type        string
	Image       string
}

type Head struct {
	Title    string
	Charset  string
	Viewport string
	Meta     []MetaTag
	Link     []LinkTag
	Script   []ScriptTag
	OG       *OpenGraph
	RawHTML  template.HTML
}

type LoaderCtx struct {
	Request RequestContext
}

type HeadCtx[P, D any] struct {
	Params P
	Data   D
}

type LayoutConfig struct {
	Entry       string
	Head        func(*HeadCtx[struct{}, struct{}]) (Head, error)
	RawHeadHTML func(*HeadCtx[struct{}, struct{}]) (template.HTML, error)
}

type PageConfig[P, D any] struct {
	Entry       string
	Loader      func(*LoaderCtx, P) (D, error)
	Head        func(*HeadCtx[P, D]) (Head, error)
	RawHeadHTML func(*HeadCtx[P, D]) (template.HTML, error)
}

type LayoutDef struct {
	Path string
	Cfg  LayoutConfig
}

type PageDef[P, D any] struct {
	Path string
	Cfg  PageConfig[P, D]
}

func Layout(app *App, path string, cfg LayoutConfig) *LayoutDef {
	if app == nil {
		panic("flop: app is nil")
	}
	app.layouts = append(app.layouts, layoutSpec{
		Path:  path,
		Entry: cfg.Entry,
	})
	return &LayoutDef{Path: path, Cfg: cfg}
}

func Page[P, D any](app *App, path string, cfg PageConfig[P, D]) *PageDef[P, D] {
	if app == nil {
		panic("flop: app is nil")
	}
	app.pages = append(app.pages, pageSpec{
		Path:     path,
		Entry:    cfg.Entry,
		ParamsTS: tsTypeFromReflect(reflectTypeOf[P]()),
		DataTS:   tsTypeFromReflect(reflectTypeOf[D]()),
	})
	return &PageDef[P, D]{Path: path, Cfg: cfg}
}

// AppSpec is a serializable summary of app metadata used by codegen.
type AppSpec struct {
	Config   Config          `json:"config"`
	Tables   []TableSpec     `json:"tables"`
	Views    []EndpointSpec  `json:"views"`
	Reducers []EndpointSpec  `json:"reducers"`
	Layouts  []LayoutSpec    `json:"layouts"`
	Pages    []PageSpec      `json:"pages"`
	Types    map[string]Type `json:"types,omitempty"`
}

type TableSpec struct {
	Name    string      `json:"name"`
	RowType string      `json:"rowType"`
	RowTS   string      `json:"rowTs"`
	Fields  []FieldSpec `json:"fields"`
}

type FieldSpec struct {
	GoName          string   `json:"goName"`
	JSONName        string   `json:"jsonName"`
	Kind            string   `json:"kind"`
	TSType          string   `json:"tsType"`
	Required        bool     `json:"required,omitempty"`
	Unique          bool     `json:"unique,omitempty"`
	Primary         bool     `json:"primary,omitempty"`
	Indexed         bool     `json:"indexed,omitempty"`
	FullText        bool     `json:"fullText,omitempty"`
	Virtual         bool     `json:"virtual,omitempty"`
	Cached          bool     `json:"cached,omitempty"`
	Default         any      `json:"default,omitempty"`
	Autogen         string   `json:"autogen,omitempty"`
	PrimaryStrategy string   `json:"primaryStrategy,omitempty"`
	BcryptRounds    int      `json:"bcryptRounds,omitempty"`
	EnumValues      []string `json:"enumValues,omitempty"`
	VectorDims      int      `json:"vectorDims,omitempty"`
	RefTable        string   `json:"refTable,omitempty"`
	RefField        string   `json:"refField,omitempty"`
	CascadeDelete   string   `json:"cascadeDelete,omitempty"`
	Relation        string   `json:"relation,omitempty"`
	RelationTable   string   `json:"relationTable,omitempty"`
	RelationField   string   `json:"relationField,omitempty"`
	MimeTypes       []string `json:"mimeTypes,omitempty"`
	ThumbSizes      []string `json:"thumbSizes,omitempty"`
	MinLen          *int     `json:"minLen,omitempty"`
	MaxLen          *int     `json:"maxLen,omitempty"`
	Min             *float64 `json:"min,omitempty"`
	Max             *float64 `json:"max,omitempty"`
	Pattern         string   `json:"pattern,omitempty"`
	Format          string   `json:"format,omitempty"`
}

type EndpointSpec struct {
	Name     string       `json:"name"`
	Access   AccessPolicy `json:"access"`
	InputTS  string       `json:"inputTs"`
	OutputTS string       `json:"outputTs"`
}

type LayoutSpec struct {
	Path  string `json:"path"`
	Entry string `json:"entry"`
}

type PageSpec struct {
	Path     string `json:"path"`
	Entry    string `json:"entry"`
	ParamsTS string `json:"paramsTs"`
	DataTS   string `json:"dataTs"`
}

// Type is reserved for future richer schema metadata in generation artifacts.
type Type struct {
	Name string `json:"name"`
	TS   string `json:"ts"`
}

func (a *App) Spec() AppSpec {
	if a == nil {
		return AppSpec{}
	}

	tableNames := make([]string, 0, len(a.tables))
	for name := range a.tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	tables := make([]TableSpec, 0, len(tableNames))
	for _, name := range tableNames {
		ts := a.tables[name]
		fields := make([]FieldSpec, 0, len(ts.Fields))
		for _, fs := range ts.Fields {
			fields = append(fields, fs.toPublic())
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i].JSONName < fields[j].JSONName })

		tables = append(tables, TableSpec{
			Name:    ts.Name,
			RowType: ts.RowType,
			RowTS:   ts.RowTS,
			Fields:  fields,
		})
	}

	views := make([]EndpointSpec, len(a.views))
	for i, v := range a.views {
		views[i] = EndpointSpec{Name: v.Name, Access: v.Access, InputTS: v.InputTS, OutputTS: v.OutputTS}
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	reducers := make([]EndpointSpec, len(a.reducers))
	for i, r := range a.reducers {
		reducers[i] = EndpointSpec{Name: r.Name, Access: r.Access, InputTS: r.InputTS, OutputTS: r.OutputTS}
	}
	sort.Slice(reducers, func(i, j int) bool { return reducers[i].Name < reducers[j].Name })

	layouts := make([]LayoutSpec, len(a.layouts))
	for i, l := range a.layouts {
		layouts[i] = LayoutSpec{Path: l.Path, Entry: l.Entry}
	}

	pages := make([]PageSpec, len(a.pages))
	for i, p := range a.pages {
		pages[i] = PageSpec{Path: p.Path, Entry: p.Entry, ParamsTS: p.ParamsTS, DataTS: p.DataTS}
	}

	return AppSpec{
		Config:   a.config,
		Tables:   tables,
		Views:    views,
		Reducers: reducers,
		Layouts:  layouts,
		Pages:    pages,
	}
}

func (a *App) WriteSpec(path string) error {
	spec := a.Spec()
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// migrationStep is the public-facing migration declaration (mirrors schema.MigrationStep).
type migrationStep struct {
	Version int
	Rename  map[string]string
}

type tableSpec struct {
	Name         string
	RowType      string
	RowTS        string
	Fields       map[string]*fieldSpec
	Access       TableAccess
	CachedDefs   []cachedFieldRuntime
	Migrations   []migrationStep
	Materialized *materializedSpec
}

// cachedFieldRuntime stores the compute function + triggers for a cached field.
type cachedFieldRuntime struct {
	FieldName string
	Triggers  []cachedTriggerDef
	Compute   func(row Row, db *Database) any
}

// cachedTriggerDef identifies a source table + foreign key that triggers recomputation.
type cachedTriggerDef struct {
	SourceTable string
	ForeignKey  string
}

func (ts *tableSpec) findOrCreateField(name string) *fieldSpec {
	if fs, ok := ts.Fields[name]; ok {
		return fs
	}
	for _, fs := range ts.Fields {
		if fs.JSONName == name {
			return fs
		}
	}
	fs := &fieldSpec{
		GoName:   name,
		JSONName: lowerCamel(name),
		Kind:     "string",
		TSType:   "string",
	}
	ts.Fields[name] = fs
	return fs
}

func (ts *tableSpec) findOrCreateFieldByJSON(name string) *fieldSpec {
	for _, fs := range ts.Fields {
		if fs.JSONName == name {
			if fs.GoName == "" {
				fs.GoName = toExportedGoName(name)
			}
			return fs
		}
	}
	goName := toExportedGoName(name)
	if fs, ok := ts.Fields[goName]; ok {
		fs.JSONName = name
		return fs
	}
	fs := &fieldSpec{
		GoName:   goName,
		JSONName: name,
		Kind:     "string",
		TSType:   "string",
	}
	ts.Fields[goName] = fs
	return fs
}

type fieldSpec struct {
	GoName           string
	JSONName         string
	Kind             string
	TSType           string
	Required         bool
	Unique           bool
	Primary          bool
	Indexed          bool
	FullText         bool
	Virtual          bool
	Cached           bool
	Default          any
	Autogen          string
	PrimaryStrategy  string
	BcryptRounds     int
	EnumValues       []string
	VectorDimensions int
	RefTable         string
	RefField         string
	CascadeDelete    string
	Relation         string
	RelationTable    string
	RelationField    string
	MimeTypes        []string
	ThumbSizes       []string
	MinLen           *int
	MaxLen           *int
	Min              *float64
	Max              *float64
	Pattern          string
	Format           string
	Access           FieldAccess
}

func (fs *fieldSpec) toPublic() FieldSpec {
	return FieldSpec{
		GoName:          fs.GoName,
		JSONName:        fs.JSONName,
		Kind:            fs.Kind,
		TSType:          fs.TSType,
		Required:        fs.Required,
		Unique:          fs.Unique,
		Primary:         fs.Primary,
		Indexed:         fs.Indexed,
		FullText:        fs.FullText,
		Virtual:         fs.Virtual,
		Cached:          fs.Cached,
		Default:         fs.Default,
		Autogen:         fs.Autogen,
		PrimaryStrategy: fs.PrimaryStrategy,
		BcryptRounds:    fs.BcryptRounds,
		EnumValues:      append([]string(nil), fs.EnumValues...),
		VectorDims:      fs.VectorDimensions,
		RefTable:        fs.RefTable,
		RefField:        fs.RefField,
		CascadeDelete:   fs.CascadeDelete,
		Relation:        fs.Relation,
		RelationTable:   fs.RelationTable,
		RelationField:   fs.RelationField,
		MimeTypes:       append([]string(nil), fs.MimeTypes...),
		ThumbSizes:      append([]string(nil), fs.ThumbSizes...),
		MinLen:          copyIntPtr(fs.MinLen),
		MaxLen:          copyIntPtr(fs.MaxLen),
		Min:             copyFloatPtr(fs.Min),
		Max:             copyFloatPtr(fs.Max),
		Pattern:         fs.Pattern,
		Format:          fs.Format,
	}
}

func normalizePrimaryStrategy(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "", "uuidv7", "ulid", "nanoid", "random":
		return s
	case "autoincrement", "auto_increment", "increment":
		return "autoincrement"
	case "auto":
		return "autoincrement"
	default:
		panic("flop: unsupported primary strategy: " + raw)
	}
}

func isNumericKind(kind string) bool {
	switch kind {
	case "number", "integer", "timestamp":
		return true
	default:
		return false
	}
}

type endpointSpec struct {
	Name     string
	Access   AccessPolicy
	InputTS  string
	OutputTS string
}

type layoutSpec struct {
	Path  string
	Entry string
}

type pageSpec struct {
	Path     string
	Entry    string
	ParamsTS string
	DataTS   string
}

var (
	timeType     = reflect.TypeOf(time.Time{})
	fileRefType  = reflect.TypeOf(FileRef{})
	fileRefPtr   = reflect.TypeOf(&FileRef{})
	stringSliceT = reflect.TypeOf([]string{})
)

func inferFields(rowType reflect.Type) map[string]*fieldSpec {
	fields := make(map[string]*fieldSpec)
	for i := 0; i < rowType.NumField(); i++ {
		sf := rowType.Field(i)
		if sf.PkgPath != "" && !sf.Anonymous {
			continue
		}

		jsonName, omitEmpty, skip := parseJSONTag(sf)
		if skip {
			continue
		}
		if jsonName == "" {
			jsonName = lowerCamel(sf.Name)
		}

		ft := sf.Type
		kind, ts := inferKindAndTS(ft)
		required := !isNullableType(ft) && !omitEmpty
		if kind == "roles" {
			required = false
		}

		fields[sf.Name] = &fieldSpec{
			GoName:   sf.Name,
			JSONName: jsonName,
			Kind:     kind,
			TSType:   ts,
			Required: required,
		}
	}
	return fields
}

func inferKindAndTS(t reflect.Type) (string, string) {
	ts := tsTypeFromReflect(t)
	bt := t
	for bt.Kind() == reflect.Pointer {
		bt = bt.Elem()
	}

	switch {
	case t == fileRefType || t == fileRefPtr:
		return "fileSingle", ts
	case bt.Kind() == reflect.Slice && (bt.Elem() == fileRefType || bt.Elem() == fileRefPtr):
		return "fileMulti", ts
	case bt.AssignableTo(timeType):
		return "timestamp", ts
	case bt == stringSliceT:
		return "roles", ts
	}

	switch bt.Kind() {
	case reflect.String:
		return "string", ts
	case reflect.Bool:
		return "boolean", ts
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer", ts
	case reflect.Float32, reflect.Float64:
		return "number", ts
	case reflect.Array, reflect.Slice:
		return "array", ts
	case reflect.Map, reflect.Struct:
		return "json", ts
	default:
		return "json", "any"
	}
}

func parseJSONTag(sf reflect.StructField) (name string, omitempty bool, skip bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	if tag == "" {
		return "", false, false
	}
	parts := strings.Split(tag, ",")
	if len(parts) > 0 {
		name = parts[0]
	}
	for _, p := range parts[1:] {
		if p == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty, false
}

func isNullableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map:
		return true
	default:
		return false
	}
}

func reflectTypeOf[T any]() reflect.Type {
	var p *T
	return reflect.TypeOf(p).Elem()
}

func baseStructType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("flop: AutoTable[T] requires struct type")
	}
	return t
}

func tsTypeFromReflect(t reflect.Type) string {
	seen := map[reflect.Type]bool{}
	return tsTypeFromReflectInner(t, seen)
}

func tsTypeFromReflectInner(t reflect.Type, seen map[reflect.Type]bool) string {
	if t == nil {
		return "any"
	}

	switch t.Kind() {
	case reflect.Pointer:
		return tsTypeFromReflectInner(t.Elem(), seen) + " | null"
	case reflect.Interface:
		return "any"
	}

	switch t {
	case timeType:
		return "string"
	case fileRefType:
		return "{ path: string; url: string; mime: string; size: number }"
	}

	if seen[t] {
		return "any"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Slice, reflect.Array:
		return "(" + tsTypeFromReflectInner(t.Elem(), seen) + ")[]"
	case reflect.Map:
		if t.Key().Kind() == reflect.String {
			return "Record<string, " + tsTypeFromReflectInner(t.Elem(), seen) + ">"
		}
		return "Record<string, any>"
	case reflect.Struct:
		seen[t] = true
		props := make([]string, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if sf.PkgPath != "" && !sf.Anonymous {
				continue
			}
			jsonName, omitempty, skip := parseJSONTag(sf)
			if skip {
				continue
			}
			if jsonName == "" {
				jsonName = lowerCamel(sf.Name)
			}
			opt := ""
			if omitempty || isNullableType(sf.Type) {
				opt = "?"
			}
			props = append(props, jsonName+opt+": "+tsTypeFromReflectInner(sf.Type, seen))
		}
		sort.Strings(props)
		return "{ " + strings.Join(props, "; ") + " }"
	default:
		return "any"
	}
}

func lowerCamel(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

func toExportedGoName(s string) string {
	parts := splitIdentifier(s)
	if len(parts) == 0 {
		return "Field"
	}
	for i, p := range parts {
		u := strings.ToUpper(p)
		switch u {
		case "ID", "URL", "URI", "UUID", "API", "HTTP", "HTTPS", "IP", "JSON", "JWT", "SQL", "TS":
			parts[i] = u
		default:
			parts[i] = strings.ToUpper(p[:1]) + strings.ToLower(p[1:])
		}
	}
	return strings.Join(parts, "")
}

func splitIdentifier(s string) []string {
	if s == "" {
		return nil
	}
	runes := []rune(s)
	out := make([]string, 0, 4)
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		out = append(out, string(current))
		current = current[:0]
	}
	for i, ch := range runes {
		if ch == '_' || ch == '-' || ch == ' ' || ch == '.' {
			flush()
			continue
		}
		if i > 0 && unicode.IsUpper(ch) && len(current) > 0 && unicode.IsLower(current[len(current)-1]) {
			flush()
		}
		current = append(current, ch)
	}
	flush()
	return out
}

func copyIntPtr(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}

func copyFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
