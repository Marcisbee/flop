package main

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	flop "github.com/marcisbee/flop"
	movies "github.com/marcisbee/flop/examples/movies-go-react/app"
)

func main() {
	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	webDir := filepath.Join(projectRoot, "web")
	assetVersion := computeAssetVersion(webDir)
	application := movies.BuildWithDataDir(dataDir)

	db, err := application.Open()
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	if table := db.Table("movies"); table != nil {
		log.Printf("movies-go-react: loaded %d movies from %s", table.Count(), dataDir)
	}

	// Full-text index on "title" is built automatically by the engine
	// (AsyncSecondaryIndexes: true) so no manual autocomplete index needed.

	mux := http.NewServeMux()

	mux.HandleFunc("/api/spec", func(w http.ResponseWriter, r *http.Request) {
		spec := application.Spec()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(spec)
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		moviesTable := db.Table("movies")
		if moviesTable == nil {
			adminJSONError(w, "movies table not found", http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{
			"ok": true,
			"data": map[string]any{
				"movies": moviesTable.Count(),
			},
		})
	})

	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		limit := clampInt(queryInt(r, "limit", 24), 1, 200)
		offset := max(queryInt(r, "offset", 0), 0)
		rows, err := movies.ListMovies(db, limit, offset)
		if err != nil {
			adminJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "data": rows})
	})

	mux.HandleFunc("/api/movies/autocomplete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		limit := clampInt(queryInt(r, "limit", 10), 1, 20)
		moviesTable := db.Table("movies")
		if moviesTable == nil || q == "" {
			writeJSON(w, map[string]any{"ok": true, "data": []any{}})
			return
		}
		rows, err := moviesTable.SearchFullText([]string{"title"}, q, limit)
		if err != nil {
			writeJSON(w, map[string]any{"ok": true, "data": []any{}})
			return
		}
		out := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			out = append(out, map[string]any{
				"slug":  row["slug"],
				"title": row["title"],
				"year":  row["year"],
			})
		}
		writeJSON(w, map[string]any{"ok": true, "data": out})
	})

	mux.HandleFunc("/api/movies/slug/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		slug := strings.TrimPrefix(r.URL.Path, "/api/movies/slug/")
		slug = strings.TrimSpace(slug)
		if slug == "" || strings.Contains(slug, "/") {
			adminJSONError(w, "invalid slug", http.StatusBadRequest)
			return
		}

		movie := movies.FindMovieBySlug(db, slug)
		if movie == nil {
			adminJSONError(w, "movie not found", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "data": movie})
	})

	mux.HandleFunc("/api/head", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Query().Get("path"))
		writeJSON(w, map[string]any{
			"ok":   true,
			"data": movies.ResolveHead(db, path),
		})
	})

	assetFS := http.FileServer(http.Dir(filepath.Join(webDir, "assets")))
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		assetFS.ServeHTTP(w, r)
	})))

	adminProvider := &flop.EngineAdminProvider{DB: db}
	adminCfg := flop.MountDefaultAdmin(mux, adminProvider)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Path)

		if isPrivatePath(path) {
			http.NotFound(w, r)
			return
		}

		if isAppPath(path) {
			html, err := renderAppHTML(db, path, assetVersion)
			if err != nil {
				http.Error(w, "failed to render app shell", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store, max-age=0")
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
		AppName:    "movies-go-react",
		Port:       port,
		DataDir:    dataDir,
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

func renderAppHTML(db *flop.Database, path string, assetVersion string) ([]byte, error) {
	head := movies.ResolveHead(db, path)
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
		Meta     []movies.HeadMeta
		HeadJSON template.JS
		PathJSON template.JS
		AssetV   string
	}{
		Title:    head.Title,
		Meta:     head.Meta,
		HeadJSON: template.JS(headJSON),
		PathJSON: template.JS(pathJSON),
		AssetV:   assetVersion,
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
    <link rel="icon" href="/assets/favicon.svg?v={{ .AssetV }}" type="image/svg+xml" />
    <link rel="stylesheet" href="/assets/app.css?v={{ .AssetV }}" />
  </head>
  <body>
    <div id="app"></div>
    <script>
      window.__FLOP_INITIAL_PATH__ = {{ .PathJSON }};
      window.__FLOP_INITIAL_HEAD__ = {{ .HeadJSON }};
    </script>
    <script type="module" src="/assets/app.js?v={{ .AssetV }}"></script>
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
	privatePrefixes := []string{"/src/", "/web/", "/.flop/", "/app/", "/cmd/"}
	for _, prefix := range privatePrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func isAppPath(path string) bool {
	if path == "/" || path == "/404" {
		return true
	}
	if strings.HasPrefix(path, "/movie/") {
		slug := strings.TrimPrefix(path, "/movie/")
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

func writeJSON(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func queryInt(r *http.Request, key string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func computeAssetVersion(webDir string) string {
	assets := []string{"app.js", "app.css", "favicon.svg"}
	var b strings.Builder
	for _, name := range assets {
		path := filepath.Join(webDir, "assets", name)
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(info.Size(), 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(info.ModTime().UnixNano(), 10))
		b.WriteByte('|')
	}
	if b.Len() == 0 {
		return strconv.FormatInt(time.Now().Unix(), 36)
	}
	return strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(b.String()))), 36)
}

func toInt(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case float32:
		return int(val)
	default:
		return 0
	}
}

