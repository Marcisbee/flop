package server

import _ "embed"

// Admin UI HTML built from shared/admin/src with npm run build.
// Generated shared/admin/*.html is copied here by go generate.

//go:embed login.html
var AdminLoginHTML string

//go:embed setup.html
var AdminSetupHTML string

//go:embed admin.html
var AdminPageHTML string
