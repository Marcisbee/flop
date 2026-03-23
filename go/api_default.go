package flop

import "net/http"

// MountDefaultAPI mounts the app API routes on the standard paths.
// It wires "/api/*" and "/_schema" to the same API handler.
func MountDefaultAPI(mux *http.ServeMux, app *App, db *Database) http.Handler {
	if mux == nil {
		panic("flop: mux is nil")
	}
	h := app.APIHandler(db)
	mux.Handle("/api/", h)
	mux.Handle("/_schema", h)
	return h
}
