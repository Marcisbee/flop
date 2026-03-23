package main

import (
	"bytes"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	flop "github.com/marcisbee/flop"
	blog "github.com/marcisbee/flop/examples/blog-go-react/app"
)

func main() {
	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}

	webDir := filepath.Join(projectRoot, "web")
	application := blog.Build()

	db, err := application.Open()
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	blog.Seed(db)

	mux := http.NewServeMux()

	mux.HandleFunc("/api/spec", func(w http.ResponseWriter, r *http.Request) {
		spec := application.Spec()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(spec)
	})

	mux.HandleFunc("/api/head", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Query().Get("path"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":   true,
			"data": blog.ResolveHead(db, path),
		})
	})
	mounts := flop.MountDefaultHandlers(mux, application, db)

	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(filepath.Join(webDir, "assets")))))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Path)
		html, err := renderAppHTML(db, path)
		if err != nil {
			http.Error(w, "failed to render app shell", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(html)
	})

	addr := ":1985"
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}
	port := flop.PortFromAddr(addr, 1985)
	serverInfo := flop.DefaultServerInfo{
		AppName:    "blog-go-react",
		Port:       port,
		DataDir:    filepath.Join(projectRoot, "data"),
		Engine:     "flop go package",
		AdminPath:  "/_",
		SetupToken: mounts.Admin.SetupToken,
		Use: []string{
			"make dev",
		},
	}
	if err := flop.RunDefaultServer(serverInfo, flop.DefaultServeOptions{
		Server:     srv,
		Checkpoint: db.Checkpoint,
		Close:      db.Close,
	}); err != nil {
		log.Fatal(err)
	}
}

func renderAppHTML(db *flop.Database, path string) ([]byte, error) {
	head := blog.ResolveHead(db, path)
	headJSON, err := json.Marshal(head)
	if err != nil {
		return nil, err
	}
	pathJSON, err := json.Marshal(path)
	if err != nil {
		return nil, err
	}

	data := struct {
		Title    string
		Meta     []blog.HeadMeta
		HeadJSON template.JS
		PathJSON template.JS
	}{
		Title:    head.Title,
		Meta:     head.Meta,
		HeadJSON: template.JS(headJSON),
		PathJSON: template.JS(pathJSON),
	}

	const shell = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ .Title }}</title>
    {{- range .Meta }}
    <meta name="{{ .Name }}" content="{{ .Content }}" data-flop-managed="1" />
    {{- end }}
    <link rel="icon" href="/assets/favicon.svg" type="image/svg+xml" />
    <link rel="stylesheet" href="/assets/app.css" />
  </head>
  <body>
    <div id="app"></div>
    <script>
      window.__FLOP_INITIAL_PATH__ = {{ .PathJSON }};
      window.__FLOP_INITIAL_HEAD__ = {{ .HeadJSON }};
    </script>
    <script type="module" src="/assets/app.js"></script>
  </body>
</html>`

	tpl, err := template.New("app-shell").Parse(shell)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if filepath.Base(path) == "index.html" {
		return "/"
	}
	if path != "/" && strings.HasSuffix(path, "/") {
		return strings.TrimSuffix(path, "/")
	}
	return path
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", os.ErrNotExist
		}
		dir = next
	}
}
