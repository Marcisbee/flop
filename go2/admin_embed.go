package flop

import _ "embed"

//go:embed admin.html
var adminPageHTML string

//go:embed login.html
var adminLoginHTML string

//go:embed setup.html
var adminSetupHTML string
