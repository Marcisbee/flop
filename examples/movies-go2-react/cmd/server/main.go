package main

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/crc32"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	flop "github.com/marcisbee/flop/go2"
	"github.com/marcisbee/flop/examples/movies-go2-react/app"
)

func main() {
	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	webDir := filepath.Join(projectRoot, "web")
	assetVersion := computeAssetVersion(webDir)

	db, err := flop.OpenDB(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.CreateTable(app.MoviesSchema); err != nil {
		log.Fatal(err)
	}

	srv := flop.NewServer(db)

	// CORS
	srv.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	app.Setup(db, srv)

	// SearchFullText endpoint (autocomplete)
	srv.HandleFunc("GET /api/search", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"data":[],"total":0}`))
			return
		}
		limit := 10
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 50 {
				limit = n
			}
		}

		results, err := db.SearchFullText("movies", []string{"title"}, q, limit)
		if err != nil {
			http.Error(w, `{"error":"search failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data":  results,
			"total": len(results),
		})
	})

	// Static files
	staticFS := http.FileServer(http.Dir(filepath.Join(webDir, "assets")))
	srv.Handle("/static/", http.StripPrefix("/static/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		staticFS.ServeHTTP(w, r)
	})))

	// SPA fallback
	srv.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/static/") {
			http.NotFound(w, r)
			return
		}
		html, err := renderAppHTML(assetVersion)
		if err != nil {
			http.Error(w, "failed to render app shell", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Write(html)
	})

	addr := ":3001"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		log.Printf("Movies app running on http://localhost%s", addr)
		if err := srv.ListenAndServe(addr); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")
	db.Flush()
	db.Close()
}

func renderAppHTML(assetVersion string) ([]byte, error) {
	pathJSON, _ := json.Marshal("/")

	data := struct {
		PathJSON template.JS
		AssetV   string
	}{
		PathJSON: template.JS(pathJSON),
		AssetV:   assetVersion,
	}

	const shell = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Movies - Flop DB Demo</title>
    <link rel="icon" href="/static/favicon.svg?v={{ .AssetV }}" type="image/svg+xml" />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
    <link rel="stylesheet" href="/static/app.css?v={{ .AssetV }}" />
  </head>
  <body>
    <div id="app"></div>
    <script>
      window.__FLOP_INITIAL_PATH__ = {{ .PathJSON }};
    </script>
    <script type="module" src="/static/app.js?v={{ .AssetV }}"></script>
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
