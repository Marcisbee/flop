package flop

import "net/http"

type DefaultMounts struct {
	API   http.Handler
	Admin *AdminConfig
}

// MountDefaultHandlers mounts both public API and admin handlers
// on their default paths.
func MountDefaultHandlers(mux *http.ServeMux, app *App, db *Database) *DefaultMounts {
	if mux == nil {
		panic("flop: mux is nil")
	}
	if db == nil {
		panic("flop: database is nil")
	}
	api := MountDefaultAPI(mux, app, db)
	admin := MountDefaultAdmin(mux, &EngineAdminProvider{DB: db})
	return &DefaultMounts{
		API:   api,
		Admin: admin,
	}
}
