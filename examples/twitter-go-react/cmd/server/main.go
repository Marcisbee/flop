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

	// Log table counts
	for _, name := range []string{"users", "tweets", "likes", "retweets", "follows", "notifications"} {
		t := db.Table(name)
		if t != nil {
			log.Printf("twitter: %s = %d rows", name, t.Count())
		}
	}

	// Build user autocomplete index in background
	userAutocomplete := flop.NewAutocompleteIndex(nil)
	go func() {
		table := db.Table("users")
		if table == nil {
			return
		}
		entries, err := table.BuildAutocompleteEntries("id", "handle", "displayName")
		if err != nil {
			log.Printf("twitter: user autocomplete build failed: %v", err)
			return
		}
		userAutocomplete.Add(entries)
		log.Printf("twitter: user autocomplete ready with %d entries", len(entries))
	}()

	mux := http.NewServeMux()

	// ---- Auth endpoints ----

	// POST /api/auth/register
	mux.HandleFunc("/api/auth/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Email       string `json:"email"`
			Password    string `json:"password"`
			Handle      string `json:"handle"`
			DisplayName string `json:"displayName"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.Email == "" || body.Password == "" || body.Handle == "" || body.DisplayName == "" {
			writeError(w, "all fields required", http.StatusBadRequest)
			return
		}

		users := db.Table("users")
		hashedPw, err := flop.HashPassword(body.Password)
		if err != nil {
			writeError(w, "failed to hash password", http.StatusInternalServerError)
			return
		}

		row, err := users.Insert(map[string]any{
			"email":       body.Email,
			"password":    hashedPw,
			"handle":      strings.ToLower(body.Handle),
			"displayName": body.DisplayName,
			"roles":       []any{"user"},
		})
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		userID := twitter.Str(row["id"])
		token := flop.CreateJWT(&flop.JWTPayload{
			Sub:   userID,
			Email: body.Email,
			Roles: []string{"user"},
			Exp:   time.Now().Add(24 * time.Hour).Unix(),
		}, "chirp-dev-secret")

		// Add to autocomplete index so new user is immediately searchable
		userAutocomplete.Add([]flop.AutocompleteEntry{{
			Key:  userID,
			Text: strings.ToLower(body.Handle),
			Data: body.DisplayName,
		}})

		writeJSON(w, map[string]any{
			"ok":    true,
			"token": token,
			"user":  twitter.ToUserPublic(row),
		})
	})

	// POST /api/auth/login
	mux.HandleFunc("/api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, "invalid json", http.StatusBadRequest)
			return
		}

		users := db.Table("users")
		row, ok := users.FindByEmail(body.Email)
		if !ok {
			writeError(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		pwHash := twitter.Str(row["password"])
		if !flop.VerifyPassword(body.Password, pwHash) {
			writeError(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		userID := twitter.Str(row["id"])
		token := flop.CreateJWT(&flop.JWTPayload{
			Sub:   userID,
			Email: body.Email,
			Roles: []string{"user"},
			Exp:   time.Now().Add(24 * time.Hour).Unix(),
		}, "chirp-dev-secret")

		writeJSON(w, map[string]any{
			"ok":    true,
			"token": token,
			"user":  twitter.ToUserPublic(row),
		})
	})

	// GET /api/auth/me
	mux.HandleFunc("/api/auth/me", func(w http.ResponseWriter, r *http.Request) {
		userID := getAuthUserID(r)
		if userID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		users := db.Table("users")
		row, err := users.Get(userID)
		if err != nil || row == nil {
			writeError(w, "user not found", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "user": twitter.ToUserPublic(row)})
	})

	// ---- Timeline ----

	// GET /api/timeline?limit=N&offset=N
	mux.HandleFunc("/api/timeline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		limit := clampInt(queryInt(r, "limit", 50), 1, 200)
		offset := maxInt(queryInt(r, "offset", 0), 0)
		viewerID := getAuthUserID(r)
		tweets := twitter.ListTimeline(db, limit, offset, viewerID)
		writeJSON(w, map[string]any{"ok": true, "data": tweets})
	})

	// ---- Tweets CRUD ----

	// POST /api/tweets
	mux.HandleFunc("/api/tweets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// redirect to timeline
			limit := clampInt(queryInt(r, "limit", 50), 1, 200)
			offset := maxInt(queryInt(r, "offset", 0), 0)
			viewerID := getAuthUserID(r)
			tweets := twitter.ListTimeline(db, limit, offset, viewerID)
			writeJSON(w, map[string]any{"ok": true, "data": tweets})
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		userID := getAuthUserID(r)
		if userID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var body struct {
			Content   string `json:"content"`
			ReplyToID string `json:"replyToId,omitempty"`
			QuoteOfID string `json:"quoteOfId,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.Content == "" || len(body.Content) > 280 {
			writeError(w, "content must be 1-280 characters", http.StatusBadRequest)
			return
		}

		tweets := db.Table("tweets")
		data := map[string]any{
			"authorId": userID,
			"content":  body.Content,
		}
		if body.ReplyToID != "" {
			data["replyToId"] = body.ReplyToID
		}
		if body.QuoteOfID != "" {
			data["quoteOfId"] = body.QuoteOfID
		}

		row, err := tweets.Insert(data)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		tweetID := twitter.Str(row["id"])

		// Send reply notification
		if body.ReplyToID != "" {
			parent, err := tweets.Get(body.ReplyToID)
			if err == nil && parent != nil {
				parentAuthor := twitter.Str(parent["authorId"])
				if parentAuthor != userID {
					notifs := db.Table("notifications")
					if notifs != nil {
						notifs.Insert(map[string]any{
							"userId":  parentAuthor,
							"actorId": userID,
							"type":    "reply",
							"tweetId": tweetID,
						})
					}
				}
			}
		}

		// Send quote notification
		if body.QuoteOfID != "" {
			quoted, err := tweets.Get(body.QuoteOfID)
			if err == nil && quoted != nil {
				quotedAuthor := twitter.Str(quoted["authorId"])
				if quotedAuthor != userID {
					notifs := db.Table("notifications")
					if notifs != nil {
						notifs.Insert(map[string]any{
							"userId":  quotedAuthor,
							"actorId": userID,
							"type":    "quote",
							"tweetId": tweetID,
						})
					}
				}
			}
		}

		writeJSON(w, map[string]any{"ok": true, "data": twitter.ToTweetWithAuthor(row, db, userID)})
	})

	// GET /api/tweets/{id}
	mux.HandleFunc("/api/tweets/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/tweets/")
		parts := strings.SplitN(path, "/", 2)
		tweetID := parts[0]

		if tweetID == "" {
			writeError(w, "tweet id required", http.StatusBadRequest)
			return
		}

		// /api/tweets/{id}/replies
		if len(parts) > 1 && parts[1] == "replies" {
			limit := clampInt(queryInt(r, "limit", 50), 1, 200)
			offset := maxInt(queryInt(r, "offset", 0), 0)
			viewerID := getAuthUserID(r)
			replies := twitter.ListReplies(db, tweetID, limit, offset, viewerID)
			writeJSON(w, map[string]any{"ok": true, "data": replies})
			return
		}

		// GET /api/tweets/{id}
		tweets := db.Table("tweets")
		row, err := tweets.Get(tweetID)
		if err != nil || row == nil {
			writeError(w, "tweet not found", http.StatusNotFound)
			return
		}
		viewerID := getAuthUserID(r)
		writeJSON(w, map[string]any{"ok": true, "data": twitter.ToTweetWithAuthor(row, db, viewerID)})
	})

	// ---- Likes ----

	// POST /api/tweets/{id}/like
	mux.HandleFunc("/api/like/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		userID := getAuthUserID(r)
		if userID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		tweetID := strings.TrimPrefix(r.URL.Path, "/api/like/")
		if tweetID == "" {
			writeError(w, "tweet id required", http.StatusBadRequest)
			return
		}

		likes := db.Table("likes")
		edgeKey := userID + ":" + tweetID
		existing, ok := likes.FindByUniqueIndex("edgeKey", edgeKey)
		if ok && existing != nil {
			// Unlike
			likes.Delete(twitter.Str(existing["id"]))
			writeJSON(w, map[string]any{"ok": true, "liked": false})
			return
		}

		// Like
		likes.Insert(map[string]any{
			"edgeKey": edgeKey,
			"userId":  userID,
			"tweetId": tweetID,
		})
		// Send like notification
		tweets := db.Table("tweets")
		tweet, err := tweets.Get(tweetID)
		if err == nil && tweet != nil {
			tweetAuthor := twitter.Str(tweet["authorId"])
			if tweetAuthor != userID {
				notifs := db.Table("notifications")
				if notifs != nil {
					notifs.Insert(map[string]any{
						"userId":  tweetAuthor,
						"actorId": userID,
						"type":    "like",
						"tweetId": tweetID,
					})
				}
			}
		}
		writeJSON(w, map[string]any{"ok": true, "liked": true})
	})

	// ---- Retweets ----

	// POST /api/retweet/{id}
	mux.HandleFunc("/api/retweet/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		userID := getAuthUserID(r)
		if userID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		tweetID := strings.TrimPrefix(r.URL.Path, "/api/retweet/")
		if tweetID == "" {
			writeError(w, "tweet id required", http.StatusBadRequest)
			return
		}

		retweets := db.Table("retweets")
		tweets := db.Table("tweets")
		edgeKey := userID + ":" + tweetID
		existing, ok := retweets.FindByUniqueIndex("edgeKey", edgeKey)
		if ok && existing != nil {
			// Undo retweet
			retweets.Delete(twitter.Str(existing["id"]))
			writeJSON(w, map[string]any{"ok": true, "retweeted": false})
			return
		}

		retweets.Insert(map[string]any{
			"edgeKey": edgeKey,
			"userId":  userID,
			"tweetId": tweetID,
		})
		// Send retweet notification
		tweet, err := tweets.Get(tweetID)
		if err == nil && tweet != nil {
			tweetAuthor := twitter.Str(tweet["authorId"])
			if tweetAuthor != userID {
				notifs := db.Table("notifications")
				if notifs != nil {
					notifs.Insert(map[string]any{
						"userId":  tweetAuthor,
						"actorId": userID,
						"type":    "retweet",
						"tweetId": tweetID,
					})
				}
			}
		}
		writeJSON(w, map[string]any{"ok": true, "retweeted": true})
	})

	// ---- Follows ----

	// POST /api/follow/{userId}
	mux.HandleFunc("/api/follow/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		followerID := getAuthUserID(r)
		if followerID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		followingID := strings.TrimPrefix(r.URL.Path, "/api/follow/")
		if followingID == "" || followingID == followerID {
			writeError(w, "invalid follow target", http.StatusBadRequest)
			return
		}

		follows := db.Table("follows")
		edgeKey := followerID + ":" + followingID
		existing, ok := follows.FindByUniqueIndex("edgeKey", edgeKey)
		if ok && existing != nil {
			// Unfollow
			follows.Delete(twitter.Str(existing["id"]))
			writeJSON(w, map[string]any{"ok": true, "following": false})
			return
		}

		follows.Insert(map[string]any{
			"edgeKey":     edgeKey,
			"followerId":  followerID,
			"followingId": followingID,
		})
		notifs := db.Table("notifications")
		if notifs != nil {
			notifs.Insert(map[string]any{
				"userId":  followingID,
				"actorId": followerID,
				"type":    "follow",
			})
		}
		writeJSON(w, map[string]any{"ok": true, "following": true})
	})

	// ---- User profiles ----

	// GET /api/users/{handle}
	mux.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/users/")
		parts := strings.SplitN(path, "/", 2)
		handle := parts[0]

		if handle == "" {
			writeError(w, "handle required", http.StatusBadRequest)
			return
		}

		userRow := twitter.FindUserByHandle(db, handle)
		if userRow == nil {
			writeError(w, "user not found", http.StatusNotFound)
			return
		}

		userID := twitter.Str(userRow["id"])
		viewerID := getAuthUserID(r)

		// /api/users/{handle}/tweets
		if len(parts) > 1 && parts[1] == "tweets" {
			limit := clampInt(queryInt(r, "limit", 50), 1, 200)
			offset := maxInt(queryInt(r, "offset", 0), 0)
			tweets := twitter.ListUserTweets(db, userID, limit, offset, viewerID)
			writeJSON(w, map[string]any{"ok": true, "data": tweets})
			return
		}

		profile := twitter.GetUserProfile(db, userID, viewerID)
		writeJSON(w, map[string]any{"ok": true, "data": profile})
	})

	// ---- Search ----

	// GET /api/search?q=...&type=tweets|users
	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		if q == "" {
			writeJSON(w, map[string]any{"ok": true, "tweets": []any{}, "users": []any{}})
			return
		}
		searchType := r.URL.Query().Get("type")
		limit := clampInt(queryInt(r, "limit", 20), 1, 50)
		viewerID := getAuthUserID(r)

		result := map[string]any{"ok": true}

		if searchType == "" || searchType == "tweets" {
			result["tweets"] = twitter.SearchTweets(db, q, limit, viewerID)
		}
		if searchType == "" || searchType == "users" {
			result["users"] = twitter.SearchUsers(db, userAutocomplete, q, limit)
		}

		writeJSON(w, result)
	})

	// ---- Notifications ----

	// GET /api/notifications
	mux.HandleFunc("/api/notifications", func(w http.ResponseWriter, r *http.Request) {
		userID := getAuthUserID(r)
		if userID == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		limit := clampInt(queryInt(r, "limit", 50), 1, 200)
		offset := maxInt(queryInt(r, "offset", 0), 0)
		notifs := twitter.GetNotifications(db, userID, limit, offset)
		writeJSON(w, map[string]any{"ok": true, "data": notifs})
	})

	// ---- Stats ----

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]any{}
		for _, name := range []string{"users", "tweets", "likes", "retweets", "follows", "notifications"} {
			t := db.Table(name)
			if t != nil {
				stats[name] = t.Count()
			}
		}
		writeJSON(w, map[string]any{"ok": true, "data": stats})
	})

	// GET /api/head
	mux.HandleFunc("/api/head", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Query().Get("path"))
		writeJSON(w, map[string]any{
			"ok":   true,
			"data": twitter.ResolveHead(db, path),
		})
	})

	// ---- Static assets ----
	assetFS := http.FileServer(http.Dir(filepath.Join(webDir, "assets")))
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		assetFS.ServeHTTP(w, r)
	})))

	// ---- Admin panel ----
	adminProvider := &flop.EngineAdminProvider{DB: db}
	adminCfg := flop.MountDefaultAdmin(mux, adminProvider)

	// ---- Catch-all: SPA shell ----
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := normalizePath(r.URL.Path)
		if isPrivatePath(path) {
			http.NotFound(w, r)
			return
		}

		html, err := renderAppHTML(db, path, assetVersion)
		if err != nil {
			http.Error(w, "failed to render app shell", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		_, _ = w.Write(html)
	})

	addr := ":1986"
	srv := &http.Server{
		Addr:         addr,
		Handler:      adminProvider.WrapWithAnalytics(mux),
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
		SetupToken: adminCfg.SetupToken,
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

func getAuthUserID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	payload := flop.VerifyJWT(token, "chirp-dev-secret")
	if payload == nil {
		return ""
	}
	return payload.Sub
}

func isPrivatePath(path string) bool {
	if strings.HasSuffix(path, ".ts") || strings.HasSuffix(path, ".tsx") {
		return true
	}
	for _, prefix := range []string{"/src/", "/web/", "/.flop/", "/app/", "/cmd/"} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
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

func writeError(w http.ResponseWriter, message string, status int) {
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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
