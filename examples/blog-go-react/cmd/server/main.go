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

	mux.HandleFunc("/api/posts", func(w http.ResponseWriter, r *http.Request) {
		posts := db.Table("posts")
		if posts == nil {
			adminJSONError(w, "posts table not found", http.StatusInternalServerError)
			return
		}
		rows, err := posts.Scan(100, 0)
		if err != nil {
			adminJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":   true,
			"data": rows,
		})
	})

	mux.HandleFunc("/api/head", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Query().Get("path"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":   true,
			"data": blog.ResolveHead(db, path),
		})
	})

	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(filepath.Join(webDir, "assets")))))

	adminProvider := &flop.EngineAdminProvider{DB: db}
	adminCfg := flop.MountDefaultAdmin(mux, adminProvider)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Path)

		// Keep source and generated files private.
		if isPrivatePath(path) {
			http.NotFound(w, r)
			return
		}

		if isAppPath(path) {
			html, err := renderAppHTML(db, path)
			if err != nil {
				http.Error(w, "failed to render app shell", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if path == "/404" {
				w.WriteHeader(http.StatusNotFound)
			}
			_, _ = w.Write(html)
			return
		}

		notFoundPath := filepath.Join(webDir, "404.html")
		content, err := os.ReadFile(notFoundPath)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(content)
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
		SetupToken: adminCfg.SetupToken,
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

func isPrivatePath(path string) bool {
	if strings.HasSuffix(path, ".ts") || strings.HasSuffix(path, ".tsx") {
		return true
	}
	privatePrefixes := []string{
		"/src/",
		"/web/",
		"/.flop/",
		"/app/",
		"/cmd/",
	}
	for _, prefix := range privatePrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func isAppPath(path string) bool {
	if path == "/" || path == "/about" {
		return true
	}
	if path == "/404" {
		return true
	}
	if strings.HasPrefix(path, "/post/") {
		slug := strings.TrimPrefix(path, "/post/")
		return slug != "" && !strings.Contains(slug, "/")
	}
	return false
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

func adminJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": message})
}
