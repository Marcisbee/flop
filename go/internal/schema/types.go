package schema

// FieldKind identifies the logical type of a schema field.
type FieldKind string

const (
	KindString     FieldKind = "string"
	KindNumber     FieldKind = "number"
	KindBoolean    FieldKind = "boolean"
	KindJson       FieldKind = "json"
	KindBcrypt     FieldKind = "bcrypt"
	KindRef        FieldKind = "ref"
	KindRefMulti   FieldKind = "refMulti"
	KindFileSingle FieldKind = "fileSingle"
	KindFileMulti  FieldKind = "fileMulti"
	KindRoles      FieldKind = "roles"
	KindEnum       FieldKind = "enum"
	KindInteger    FieldKind = "integer"
	KindVector     FieldKind = "vector"
	KindSet        FieldKind = "set"
	KindTimestamp  FieldKind = "timestamp"
)

// TypeTag is the binary serialization tag for field values.
type TypeTag byte

const (
	TagNull       TypeTag = 0x00
	TagString     TypeTag = 0x01
	TagNumber     TypeTag = 0x02
	TagBoolean    TypeTag = 0x03
	TagArray      TypeTag = 0x04
	TagInteger    TypeTag = 0x05
	TagVector     TypeTag = 0x06
	TagJson       TypeTag = 0x0D
	TagFileSingle TypeTag = 0x0E
	TagFileMulti  TypeTag = 0x0F
)

// FieldTypeTag returns the binary type tag for a given field kind.
func FieldTypeTag(kind FieldKind) TypeTag {
	switch kind {
	case KindString, KindBcrypt, KindRef, KindEnum:
		return TagString
	case KindNumber, KindTimestamp:
		return TagNumber
	case KindInteger:
		return TagInteger
	case KindBoolean:
		return TagBoolean
	case KindJson:
		return TagJson
	case KindFileSingle:
		return TagFileSingle
	case KindFileMulti:
		return TagFileMulti
	case KindRoles, KindRefMulti, KindSet:
		return TagArray
	case KindVector:
		return TagVector
	default:
		return TagNull
	}
}

// CompiledField holds metadata for a single field in a compiled schema.
type CompiledField struct {
	Name             string
	Kind             FieldKind
	Required         bool
	Unique           bool
	DefaultValue     interface{}
	AutoGenPattern   string // regex source, e.g. "[a-z0-9]{12}"
	AutoIDStrategy   string // e.g. "uuidv7", "ulid", "nanoid", "random", "autoincrement"
	BcryptRounds     int
	RefTableName     string
	RefField         string
	MimeTypes        []string
	EnumValues       []string
	VectorDimensions int
	Cached           bool // engine-managed computed field
}

// CompiledSchema is the in-memory representation of a table schema.
type CompiledSchema struct {
	Fields   []CompiledField
	FieldMap map[string]*CompiledField
}

// NewCompiledSchema builds a CompiledSchema from a slice of fields.
func NewCompiledSchema(fields []CompiledField) *CompiledSchema {
	fm := make(map[string]*CompiledField, len(fields))
	cs := &CompiledSchema{Fields: fields, FieldMap: fm}
	for i := range cs.Fields {
		fm[cs.Fields[i].Name] = &cs.Fields[i]
	}
	return cs
}

// FieldNames returns field names in schema order.
func (cs *CompiledSchema) FieldNames() []string {
	names := make([]string, len(cs.Fields))
	for i, f := range cs.Fields {
		names[i] = f.Name
	}
	return names
}

// StoredColumnDef is the JSON-serialized column in _meta.flop.
type StoredColumnDef struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	Required *bool       `json:"required,omitempty"`
	Unique   *bool       `json:"unique,omitempty"`
	Default  interface{} `json:"default,omitempty"`
}

// StoredSchema is the JSON-serialized schema in _meta.flop.
type StoredSchema struct {
	Columns []StoredColumnDef `json:"columns"`
}

// StoredTableMeta tracks schema version history for a table.
type StoredTableMeta struct {
	CurrentSchemaVersion int                   `json:"currentSchemaVersion"`
	Schemas              map[int]*StoredSchema `json:"schemas"`
}

// StoredMeta is the top-level JSON payload of _meta.flop.
type StoredMeta struct {
	Version int                         `json:"version"`
	Created string                      `json:"created"`
	Tables  map[string]*StoredTableMeta `json:"tables"`
}

// RowPointer locates a row within a table file.
type RowPointer struct {
	PageNumber uint32
	SlotIndex  uint16
}

// IndexDef describes a secondary index.
type IndexType string

const (
	IndexTypeHash     IndexType = "hash"
	IndexTypeFullText IndexType = "fullText"
)

type IndexDef struct {
	Fields []string
	Unique bool
	Type   IndexType
}

// MigrationStep is user-defined migration info (extracted from JS).
type MigrationStep struct {
	Version int
	Rename  map[string]string
	// Transform is handled on the JS side
}

// TableDef holds the full definition for a table.
type TableDef struct {
	Name           string
	CompiledSchema *CompiledSchema
	Indexes        []IndexDef
	Auth           bool
	Migrations     []MigrationStep
}

// AccessPolicy controls endpoint access.
type AccessPolicy struct {
	Type  string   // "public", "authenticated", "roles"
	Roles []string // only used when Type == "roles"
}

// AuthContext holds authenticated user info extracted from JWT.
type AuthContext struct {
	ID    string   `json:"id"`
	Email string   `json:"email"`
	Roles []string `json:"roles"`
}

// FileRef references a stored file asset.
type FileRef struct {
	Path string `json:"path"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mime string `json:"mime"`
	URL  string `json:"url"`
}

// Page format constants.
const (
	PageSize       = 4096
	FileHeaderSize = 64
	PageHeaderSize = 12
	SlotSize       = 4 // offset(2) + length(2)
)

// Table file magic bytes "FLPT".
var TableFileMagic = [4]byte{0x46, 0x4C, 0x50, 0x54}

// Meta file magic bytes "FLOP".
var MetaFileMagic = [4]byte{0x46, 0x4C, 0x4F, 0x50}

// Index file magic bytes "FLPI".
var IndexFileMagic = [4]byte{0x46, 0x4C, 0x50, 0x49}

// WAL file magic bytes "FLPW".
var WALFileMagic = [4]byte{0x46, 0x4C, 0x50, 0x57}

// CompiledToStored converts a CompiledSchema to a StoredSchema for persistence.
func CompiledToStored(cs *CompiledSchema) *StoredSchema {
	cols := make([]StoredColumnDef, len(cs.Fields))
	for i, f := range cs.Fields {
		col := StoredColumnDef{
			Name: f.Name,
			Type: string(f.Kind),
		}
		if f.Required {
			b := true
			col.Required = &b
		}
		if f.Unique {
			b := true
			col.Unique = &b
		}
		cols[i] = col
	}
	return &StoredSchema{Columns: cols}
}
