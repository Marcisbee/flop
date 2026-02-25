package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/marcisbee/flop/internal/schema"
)

// AppMeta holds all the metadata extracted from the user's app.ts.
type AppMeta struct {
	Tables   map[string]TableMeta   `json:"tables"`
	Views    map[string]EndpointMeta `json:"views"`
	Reducers map[string]EndpointMeta `json:"reducers"`
	Routes   []FlatRoute             `json:"routeTree"`
	Config   AppConfig               `json:"config"`
}

// TableMeta describes a table extracted from the JS module.
type TableMeta struct {
	Name       string           `json:"name"`
	Fields     []FieldMeta      `json:"fields"`
	Auth       bool             `json:"auth"`
	Migrations []MigrationMeta  `json:"migrations"`
	Indexes    []IndexMeta      `json:"indexes"`
}

// FieldMeta describes a single field from the JS schema.
type FieldMeta struct {
	Name             string      `json:"name"`
	Kind             string      `json:"kind"`
	Required         bool        `json:"required"`
	Unique           bool        `json:"unique"`
	DefaultValue     interface{} `json:"defaultValue,omitempty"`
	AutoGenPattern   string      `json:"autoGenPattern,omitempty"`
	BcryptRounds     int         `json:"bcryptRounds,omitempty"`
	RefField         string      `json:"refField,omitempty"`
	MimeTypes        []string    `json:"mimeTypes,omitempty"`
	EnumValues       []string    `json:"enumValues,omitempty"`
	VectorDimensions int         `json:"vectorDimensions,omitempty"`
}

// MigrationMeta describes a migration step from JS.
type MigrationMeta struct {
	Version int               `json:"version"`
	Rename  map[string]string `json:"rename,omitempty"`
}

// IndexMeta describes a secondary index from JS.
type IndexMeta struct {
	Fields []string `json:"fields"`
	Unique bool     `json:"unique"`
}

// EndpointMeta describes a view or reducer.
type EndpointMeta struct {
	Name            string                `json:"name"`
	Params          map[string]FieldMeta  `json:"params"`
	Access          AccessMeta            `json:"access"`
	DependentTables []string              `json:"dependentTables,omitempty"`
	HandlerRef      string                `json:"handlerRef"`
}

// AccessMeta describes access control.
type AccessMeta struct {
	Type  string   `json:"type"`
	Roles []string `json:"roles,omitempty"`
}

// FlatRoute is a flattened page route.
type FlatRoute struct {
	Pattern         string   `json:"pattern"`
	ComponentPaths  []string `json:"componentPaths"`
	HasHead         bool     `json:"hasHead"`
	HeadChainLength int      `json:"headChainLength"`
}

// AppConfig holds database config from flop() call.
type AppConfig struct {
	DataDir  string `json:"dataDir,omitempty"`
	SyncMode string `json:"syncMode,omitempty"`
}

// DiscoverApp runs the bundled user code in QuickJS and extracts metadata.
func DiscoverApp(vm *VM, bundledCode string) (*AppMeta, error) {
	// The bundled code is IIFE format, exports are assigned to globalThis.__FLOP_EXPORTS__
	_, err := vm.Eval(bundledCode)
	if err != nil {
		return nil, fmt.Errorf("eval app bundle: %w", err)
	}

	// Collect metadata: exports are now on globalThis.__FLOP_EXPORTS__
	collectCode := `
	(function() {
		var exports = globalThis.__FLOP_EXPORTS__ || {};
		if (typeof globalThis.__FLOP_COLLECT__ === 'function') {
			return globalThis.__FLOP_COLLECT__(exports);
		}
		return JSON.stringify({ tables: {}, views: {}, reducers: {}, routeTree: [], config: {} });
	})()
	`

	resultJSON, err := vm.GetString(collectCode)
	if err != nil {
		return nil, fmt.Errorf("collect metadata: %w", err)
	}

	var meta AppMeta
	if err := json.Unmarshal([]byte(resultJSON), &meta); err != nil {
		return nil, fmt.Errorf("parse metadata JSON: %w\nJSON: %s", err, resultJSON)
	}

	return &meta, nil
}

// BuildTableDefs converts AppMeta into Go schema.TableDef objects.
func BuildTableDefs(meta *AppMeta) map[string]*schema.TableDef {
	defs := make(map[string]*schema.TableDef, len(meta.Tables))

	for name, tm := range meta.Tables {
		fields := make([]schema.CompiledField, len(tm.Fields))
		for i, fm := range tm.Fields {
			fields[i] = schema.CompiledField{
				Name:             fm.Name,
				Kind:             schema.FieldKind(fm.Kind),
				Required:         fm.Required,
				Unique:           fm.Unique,
				DefaultValue:     fm.DefaultValue,
				AutoGenPattern:   fm.AutoGenPattern,
				BcryptRounds:     fm.BcryptRounds,
				RefField:         fm.RefField,
				MimeTypes:        fm.MimeTypes,
				EnumValues:       fm.EnumValues,
				VectorDimensions: fm.VectorDimensions,
			}
		}

		cs := schema.NewCompiledSchema(fields)

		indexes := make([]schema.IndexDef, len(tm.Indexes))
		for i, idx := range tm.Indexes {
			indexes[i] = schema.IndexDef{
				Fields: idx.Fields,
				Unique: idx.Unique,
			}
		}

		migrations := make([]schema.MigrationStep, len(tm.Migrations))
		for i, m := range tm.Migrations {
			migrations[i] = schema.MigrationStep{
				Version: m.Version,
				Rename:  m.Rename,
			}
		}

		defs[name] = &schema.TableDef{
			Name:           name,
			CompiledSchema: cs,
			Indexes:        indexes,
			Auth:           tm.Auth,
			Migrations:     migrations,
		}
	}

	return defs
}

// BuildRouteInfo converts view/reducer metadata into route definitions for the HTTP server.
type RouteInfo struct {
	Name       string
	Type       string // "view" or "reducer"
	Method     string // "GET" or "POST"
	Path       string
	Access     schema.AccessPolicy
	ParamDefs  map[string]FieldMeta
}

func BuildRoutes(meta *AppMeta) []RouteInfo {
	var routes []RouteInfo

	for name, v := range meta.Views {
		routes = append(routes, RouteInfo{
			Name:   name,
			Type:   "view",
			Method: "GET",
			Path:   fmt.Sprintf("/api/view/%s", name),
			Access: schema.AccessPolicy{
				Type:  v.Access.Type,
				Roles: v.Access.Roles,
			},
			ParamDefs: v.Params,
		})
	}

	for name, r := range meta.Reducers {
		routes = append(routes, RouteInfo{
			Name:   name,
			Type:   "reducer",
			Method: "POST",
			Path:   fmt.Sprintf("/api/reduce/%s", name),
			Access: schema.AccessPolicy{
				Type:  r.Access.Type,
				Roles: r.Access.Roles,
			},
			ParamDefs: r.Params,
		})
	}

	return routes
}
