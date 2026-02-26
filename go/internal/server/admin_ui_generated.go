package server

import _ "embed"

// Admin UI HTML pages shared between TS and Go implementations.
// Source of truth: shared/admin/*.html (copied here by go generate)

//go:embed login.html
var adminLoginHTML string

//go:embed setup.html
var adminSetupHTML string

//go:embed admin.html
var adminPageHTML string
