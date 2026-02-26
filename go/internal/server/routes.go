package server

import "github.com/marcisbee/flop/internal/schema"

// RouteInfo describes a view or reducer HTTP endpoint.
type RouteInfo struct {
	Name            string
	Type            string // "view" or "reducer"
	Method          string // "GET" or "POST"
	Path            string
	Access          schema.AccessPolicy
	ParamDefs       map[string]FieldMeta
	DependentTables []string
}

// FieldMeta describes a single field from a schema.
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

// FlatRoute is a flattened page route for client-side rendering.
type FlatRoute struct {
	Pattern         string   `json:"pattern"`
	ComponentPaths  []string `json:"componentPaths"`
	HasHead         bool     `json:"hasHead"`
	HeadChainLength int      `json:"headChainLength"`
}
