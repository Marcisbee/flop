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
	twitter "github.com/marcisbee/flop/examples/twitter-go-react/app"
)

func main() {
	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	webDir := filepath.Join(projectRoot, "web")
	assetVersion := computeAssetVersion(webDir)
	application := twitter.BuildWithDataDir(dataDir)

	db, err := application.Open()
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	db.SetJWTSecret(twitter.JWTSecret)

	for _, name := range []string{"users", "tweets", "likes", "retweets", "follows", "notifications"} {
		t := db.Table(name)
		if t != nil {
			log.Printf("twitter: %s = %d rows", name, t.Count())
		}
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/head", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Query().Get("path"))
		writeJSON(w, map[string]any{
			"ok":   true,
			"data": twitter.ResolveHead(db, path),
		})
	})

	mounts := flop.MountDefaultHandlers(mux, application, db)

	assetFS := http.FileServer(http.Dir(filepath.Join(webDir, "assets")))
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		assetFS.ServeHTTP(w, r)
	})))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Path)
		html, err := renderAppHTML(db, path, assetVersion)
		if err != nil {
			http.Error(w, "failed to render app shell", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		_, _ = w.Write(html)
	})

	analyticsProvider := &flop.EngineAdminProvider{DB: db}
	addr := ":1986"
	srv := &http.Server{
		Addr:         addr,
		Handler:      analyticsProvider.WrapWithAnalytics(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}
	port := flop.PortFromAddr(addr, 1986)
	serverInfo := flop.DefaultServerInfo{
		AppName:    "chirp (twitter clone)",
		Port:       port,
		DataDir:    dataDir,
		Engine:     "flop go package",
		AdminPath:  "/_",
		SetupToken: mounts.Admin.SetupToken,
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
	head := twitter.ResolveHead(db, path)
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
		Meta     []twitter.HeadMeta
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
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap" rel="stylesheet" />
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

func normalizePath(path string) string {
	if path == "" {
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

func writeJSON(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func computeAssetVersion(webDir string) string {
	assets := []string{"app.js", "app.css", "favicon.svg"}
	var b strings.Builder
	for _, name := range assets {
		p := filepath.Join(webDir, "assets", name)
		info, err := os.Stat(p)
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
