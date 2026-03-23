package server

import (
	"net/http"
	"net/http/pprof"
	"strings"
)

// ServePrefixedPprof serves net/http/pprof endpoints under a custom prefix.
// Example prefix: "/_/debug/pprof".
func ServePrefixedPprof(prefix string, w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == prefix {
		http.Redirect(w, r, prefix+"/", http.StatusMovedPermanently)
		return
	}
	if path == prefix+"/" {
		pprof.Index(w, r)
		return
	}

	switch path {
	case prefix + "/cmdline":
		pprof.Cmdline(w, r)
		return
	case prefix + "/profile":
		pprof.Profile(w, r)
		return
	case prefix + "/symbol":
		pprof.Symbol(w, r)
		return
	case prefix + "/trace":
		pprof.Trace(w, r)
		return
	}

	name := strings.TrimPrefix(path, prefix+"/")
	if name == "" {
		pprof.Index(w, r)
		return
	}
	pprof.Handler(name).ServeHTTP(w, r)
}
