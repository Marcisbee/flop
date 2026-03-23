package server

import _ "embed"

// Admin UI HTML pages shared between TS and Go implementations.
// Source of truth: shared/admin/*.html (copied here by go generate)

//go:embed login.html
var AdminLoginHTML string

//go:embed setup.html
var AdminSetupHTML string

//go:embed admin.html
var AdminPageHTML string
