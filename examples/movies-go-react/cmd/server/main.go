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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	flop "github.com/marcisbee/flop"
	movies "github.com/marcisbee/flop/examples/movies-go-react/app"
)

type autocompleteItem struct {
	Norm          string
	NormNoArticle string
	Slug          string
	Title         string
	Year          int
}

type autocompleteIndex struct {
	mu     sync.RWMutex
	items  []autocompleteItem
	bySlug map[string]autocompleteItem
}

type indexBuildState struct {
	mu         sync.RWMutex
	building   bool
	ready      bool
	total      int
	lastError  string
	startedAt  time.Time
	finishedAt time.Time
}

func newAutocompleteIndex(entries []movies.MovieIndexEntry) *autocompleteIndex {
	idx := &autocompleteIndex{bySlug: make(map[string]autocompleteItem, len(entries))}
	idx.add(entries)
	return idx
}

func (a *autocompleteIndex) add(entries []movies.MovieIndexEntry) {
	if len(entries) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.bySlug == nil {
		a.bySlug = make(map[string]autocompleteItem, len(entries))
	}

	for _, entry := range entries {
		if entry.Slug == "" || entry.Title == "" {
			continue
		}
		item := autocompleteItem{
			Norm:          normalizeSearch(entry.Title),
			NormNoArticle: removeLeadingArticle(normalizeSearch(entry.Title)),
			Slug:          entry.Slug,
			Title:         entry.Title,
			Year:          entry.Year,
		}
		a.bySlug[item.Slug] = item
	}

	a.items = a.items[:0]
	a.items = make([]autocompleteItem, 0, len(a.bySlug))
	for _, item := range a.bySlug {
		a.items = append(a.items, item)
	}

	sort.Slice(a.items, func(i, j int) bool {
		if a.items[i].Norm == a.items[j].Norm {
			return a.items[i].Slug < a.items[j].Slug
		}
		return a.items[i].Norm < a.items[j].Norm
	})
}

func (a *autocompleteIndex) query(prefix string, limit int) []map[string]any {
	norm := normalizeSearch(prefix)
	if norm == "" {
		return []map[string]any{}
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.items) == 0 {
		return []map[string]any{}
	}

	if limit <= 0 {
		limit = 10
	}

	start := sort.Search(len(a.items), func(i int) bool {
		return a.items[i].Norm >= norm
	})

	seen := make(map[string]struct{}, limit*2)
	out := make([]map[string]any, 0, limit)
	appendMatch := func(item autocompleteItem) {
		if _, exists := seen[item.Slug]; exists {
			return
		}
		seen[item.Slug] = struct{}{}
		out = append(out, map[string]any{
			"slug":  item.Slug,
			"title": item.Title,
			"year":  item.Year,
		})
	}

	for i := start; i < len(a.items) && len(out) < limit; i++ {
		item := a.items[i]
		if !strings.HasPrefix(item.Norm, norm) {
			break
		}
		appendMatch(item)
	}

	// Fallback for intuitive search:
	// 1) ignore leading articles ("the", "a", "an")
	// 2) allow word-prefix match ("runner" => "Blade Runner 2049")
	if len(out) < limit {
		for _, item := range a.items {
			if strings.HasPrefix(item.NormNoArticle, norm) ||
				hasWordPrefix(item.Norm, norm) ||
				strings.Contains(item.Norm, norm) ||
				hasOrderedTokenPrefixMatch(item.Norm, norm) {
				appendMatch(item)
				if len(out) >= limit {
					break
				}
			}
		}
	}

	return out
}

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

	movies.Seed(db)
	if table := db.Table("movies"); table != nil {
		log.Printf("movies-go-react: loaded %d movies from %s", table.Count(), dataDir)
	}

	autocomplete := newAutocompleteIndex(nil)
	indexState := &indexBuildState{
		building:  true,
		startedAt: time.Now(),
	}
	go func() {
		entries, err := movies.AllMovieIndexEntries(db)
		indexState.mu.Lock()
		defer indexState.mu.Unlock()
		indexState.building = false
		indexState.finishedAt = time.Now()
		if err != nil {
			indexState.lastError = err.Error()
			log.Printf("movies-go-react: autocomplete index build failed: %v", err)
			return
		}
		autocomplete.add(entries)
		indexState.ready = true
		indexState.total = len(entries)
		log.Printf("movies-go-react: autocomplete index ready with %d entries in %s", len(entries), time.Since(indexState.startedAt).Round(time.Millisecond))
	}()

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
		indexState.mu.RLock()
		building := indexState.building
		ready := indexState.ready
		total := indexState.total
		lastErr := indexState.lastError
		var buildMs int64
		if !indexState.startedAt.IsZero() {
			end := time.Now()
			if !indexState.finishedAt.IsZero() {
				end = indexState.finishedAt
			}
			buildMs = end.Sub(indexState.startedAt).Milliseconds()
		}
		indexState.mu.RUnlock()

		writeJSON(w, map[string]any{
			"ok": true,
			"data": map[string]any{
				"movies":            moviesTable.Count(),
				"autocompleteReady": ready,
				"autocompleteBuild": building,
				"autocompleteItems": total,
				"autocompleteMs":    buildMs,
				"autocompleteError": lastErr,
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
		writeJSON(w, map[string]any{"ok": true, "data": autocomplete.query(q, limit)})
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

func normalizeSearch(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	lastSpace := true
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	norm := strings.TrimSpace(b.String())
	if norm == "" {
		return ""
	}

	// Canonicalize Roman sequel numerals to Arabic numbers so:
	// "Mortal Kombat II" and "Mortal Kombat 2" normalize identically.
	tokens := strings.Fields(norm)
	for i := range tokens {
		if arabic, ok := romanNumeralArabic(tokens[i]); ok {
			tokens[i] = arabic
		}
	}
	return strings.Join(tokens, " ")
}

func romanNumeralArabic(token string) (string, bool) {
	switch token {
	case "ii":
		return "2", true
	case "iii":
		return "3", true
	case "iv":
		return "4", true
	case "v":
		return "5", true
	case "vi":
		return "6", true
	case "vii":
		return "7", true
	case "viii":
		return "8", true
	case "ix":
		return "9", true
	case "x":
		return "10", true
	case "xi":
		return "11", true
	case "xii":
		return "12", true
	case "xiii":
		return "13", true
	case "xiv":
		return "14", true
	case "xv":
		return "15", true
	case "xvi":
		return "16", true
	case "xvii":
		return "17", true
	case "xviii":
		return "18", true
	case "xix":
		return "19", true
	case "xx":
		return "20", true
	default:
		return "", false
	}
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

func removeLeadingArticle(s string) string {
	s = strings.TrimSpace(s)
	for _, prefix := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(s, prefix))
		}
	}
	return s
}

func hasWordPrefix(text, query string) bool {
	if text == "" || query == "" {
		return false
	}
	for _, word := range strings.Fields(text) {
		if strings.HasPrefix(word, query) {
			return true
		}
	}
	return false
}

func hasOrderedTokenPrefixMatch(text, query string) bool {
	if text == "" || query == "" {
		return false
	}

	titleTokens := strings.Fields(text)
	queryTokens := strings.Fields(query)
	if len(titleTokens) == 0 || len(queryTokens) == 0 {
		return false
	}

	ti := 0
	for _, q := range queryTokens {
		found := false
		for ti < len(titleTokens) {
			if strings.HasPrefix(titleTokens[ti], q) {
				found = true
				ti++
				break
			}
			ti++
		}
		if !found {
			return false
		}
	}
	return true
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
