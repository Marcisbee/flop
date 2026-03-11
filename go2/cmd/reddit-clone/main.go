package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

func main() {
	dataDir := filepath.Join(".", "reddit-data")
	assetsDir := filepath.Join(dataDir, "assets")

	db, err := flop.OpenDB(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	// ===== SCHEMAS =====

	userSchema := &flop.Schema{
		Name:   "users",
		IsAuth: true,
		Fields: []flop.Field{
			{Name: "email", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 255},
			{Name: "password", Type: flop.FieldString, Required: true},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 30},
			{Name: "display_name", Type: flop.FieldString, MaxLen: 50},
			{Name: "bio", Type: flop.FieldString, MaxLen: 500},
			{Name: "avatar", Type: flop.FieldString},
			{Name: "karma", Type: flop.FieldInt},
		},
	}

	communitySchema := &flop.Schema{
		Name: "communities",
		Fields: []flop.Field{
			{Name: "name", Type: flop.FieldString, Required: true, MaxLen: 100},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 30},
			{Name: "description", Type: flop.FieldString, Searchable: true, MaxLen: 1000},
			{Name: "rules", Type: flop.FieldString, MaxLen: 5000},
			{Name: "creator_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "member_count", Type: flop.FieldInt},
			{Name: "visibility", Type: flop.FieldString, EnumValues: []string{"public", "private", "restricted"}},
		},
	}

	membershipSchema := &flop.Schema{
		Name: "memberships",
		Fields: []flop.Field{
			{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
			{Name: "role", Type: flop.FieldString, EnumValues: []string{"member", "moderator", "admin"}},
		},
		UniqueConstraints: [][]string{{"user_id", "community_id"}},
	}

	postSchema := &flop.Schema{
		Name: "posts",
		Fields: []flop.Field{
			{Name: "title", Type: flop.FieldString, Required: true, Searchable: true, MaxLen: 300},
			{Name: "body", Type: flop.FieldString, Searchable: true, MaxLen: 40000},
			{Name: "link", Type: flop.FieldString, MaxLen: 2048},
			{Name: "image", Type: flop.FieldString},
			{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
			{Name: "score", Type: flop.FieldInt},
			{Name: "hot_rank", Type: flop.FieldFloat},
			{Name: "comment_count", Type: flop.FieldInt},
			{Name: "repost_of", Type: flop.FieldRef, RefTable: "posts"},
		},
		CascadeDeletes: []string{"comments", "votes"},
	}

	commentSchema := &flop.Schema{
		Name: "comments",
		Fields: []flop.Field{
			{Name: "body", Type: flop.FieldString, Required: true, Searchable: true, MaxLen: 10000},
			{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
			{Name: "parent_id", Type: flop.FieldRef, RefTable: "comments", SelfRef: true},
			{Name: "depth", Type: flop.FieldInt},
			{Name: "path", Type: flop.FieldString}, // materialized path for nesting
			{Name: "score", Type: flop.FieldInt},
		},
		CascadeDeletes: []string{"comment_votes"},
	}

	voteSchema := &flop.Schema{
		Name: "votes",
		Fields: []flop.Field{
			{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
			{Name: "value", Type: flop.FieldInt},
		},
		UniqueConstraints: [][]string{{"user_id", "post_id"}},
	}

	commentVoteSchema := &flop.Schema{
		Name: "comment_votes",
		Fields: []flop.Field{
			{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "comment_id", Type: flop.FieldRef, RefTable: "comments", Required: true},
			{Name: "value", Type: flop.FieldInt},
		},
		UniqueConstraints: [][]string{{"user_id", "comment_id"}},
	}

	for _, s := range []*flop.Schema{userSchema, communitySchema, membershipSchema, postSchema, commentSchema, voteSchema, commentVoteSchema} {
		if _, err := db.CreateTable(s); err != nil {
			log.Fatal(err)
		}
	}

	// ===== AUTH =====

	authConfig := flop.DefaultAuthConfig()
	authConfig.TableName = "users"
	authMgr := flop.NewAuthManager(db, authConfig)

	// ===== ASSETS =====

	assetMgr := flop.NewAssetManager(assetsDir)

	// ===== SERVER =====

	srv := flop.NewServer(db)
	srv.SetAuth(authMgr.Authenticate)

	// CORS middleware
	srv.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	// ===== AUTH ENDPOINTS =====

	srv.RegisterReducer("POST /api/auth/register", &flop.ReducerDef{
		Name:   "register",
		Table:  "users",
		Action: flop.ActionCustom,
		Validate: func(data map[string]any) error {
			if data["email"] == nil || data["password"] == nil || data["handle"] == nil {
				return fmt.Errorf("email, password, and handle are required")
			}
			return nil
		},
	})

	// Custom auth routes (bypass reducer for auth-specific logic)
	http.HandleFunc("POST /api/auth/register", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Email       string `json:"email"`
			Password    string `json:"password"`
			Handle      string `json:"handle"`
			DisplayName string `json:"display_name"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}

		user, err := authMgr.Register(input.Email, input.Password, map[string]any{
			"handle":       input.Handle,
			"display_name": input.DisplayName,
			"karma":        0,
		})
		if err != nil {
			writeError(w, err.Error(), 409)
			return
		}

		session, err := authMgr.Login(input.Email, input.Password)
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}

		writeJSON(w, map[string]any{
			"user":         sanitizeUser(user),
			"token":        session.Token,
			"refreshToken": session.RefreshToken,
		})
	})

	http.HandleFunc("POST /api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}

		session, err := authMgr.Login(input.Email, input.Password)
		if err != nil {
			writeError(w, "invalid credentials", 401)
			return
		}

		user, _ := db.Table("users").Get(session.UserID)
		writeJSON(w, map[string]any{
			"user":         sanitizeUser(user),
			"token":        session.Token,
			"refreshToken": session.RefreshToken,
		})
	})

	// ===== COMMUNITY VIEWS =====

	srv.RegisterView("/api/communities", &flop.ViewDef{
		Name:    "list_communities",
		Table:   "communities",
		OrderBy: "member_count",
		Order:   flop.Desc,
		Limit:   50,
	})

	srv.RegisterView("/api/communities/{handle}", &flop.ViewDef{
		Name:  "get_community",
		Table: "communities",
		Filters: []flop.Filter{
			{Field: "handle", Op: flop.OpEq},
		},
		Limit:    1,
		Includes: []string{"creator_id"},
	})

	// ===== COMMUNITY REDUCERS =====

	srv.RegisterReducer("/api/communities", &flop.ReducerDef{
		Name:   "create_community",
		Table:  "communities",
		Action: flop.ActionInsert,
		Validate: func(data map[string]any) error {
			if data["name"] == nil || data["handle"] == nil {
				return fmt.Errorf("name and handle are required")
			}
			return nil
		},
		PermCheck: func(auth any, data map[string]any) bool {
			return auth != nil
		},
		Transform: func(data map[string]any) map[string]any {
			data["member_count"] = 1
			if data["visibility"] == nil {
				data["visibility"] = "public"
			}
			return data
		},
	})

	srv.RegisterReducer("/api/communities/{community_id}/join", &flop.ReducerDef{
		Name:   "join_community",
		Table:  "memberships",
		Action: flop.ActionInsert,
		PermCheck: func(auth any, data map[string]any) bool {
			return auth != nil
		},
		Transform: func(data map[string]any) map[string]any {
			data["role"] = "member"
			return data
		},
	})

	// ===== POST VIEWS =====

	// Hot feed (all communities)
	srv.RegisterView("/api/feed/hot", &flop.ViewDef{
		Name:     "hot_feed",
		Table:    "posts",
		OrderBy:  "hot_rank",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	// New feed
	srv.RegisterView("/api/feed/new", &flop.ViewDef{
		Name:     "new_feed",
		Table:    "posts",
		OrderBy:  "id", // newest first by ID
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	// Best feed (by score)
	srv.RegisterView("/api/feed/best", &flop.ViewDef{
		Name:     "best_feed",
		Table:    "posts",
		OrderBy:  "score",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	// Community posts
	srv.RegisterView("/api/c/{community_id}/posts", &flop.ViewDef{
		Name:  "community_posts",
		Table: "posts",
		Filters: []flop.Filter{
			{Field: "community_id", Op: flop.OpEq},
		},
		OrderBy:  "hot_rank",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id"},
	})

	// Single post
	srv.RegisterView("/api/posts/{id}", &flop.ViewDef{
		Name:  "get_post",
		Table: "posts",
		Filters: []flop.Filter{
			{Field: "id", Op: flop.OpEq},
		},
		Limit:    1,
		Includes: []string{"author_id", "community_id"},
	})

	// Search posts
	srv.RegisterView("/api/search/posts", &flop.ViewDef{
		Name:     "search_posts",
		Table:    "posts",
		OrderBy:  "score",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	// User posts
	srv.RegisterView("/api/users/{author_id}/posts", &flop.ViewDef{
		Name:  "user_posts",
		Table: "posts",
		Filters: []flop.Filter{
			{Field: "author_id", Op: flop.OpEq},
		},
		OrderBy: "id",
		Order:   flop.Desc,
		Limit:   25,
	})

	// ===== POST REDUCERS =====

	srv.RegisterReducer("/api/posts", &flop.ReducerDef{
		Name:   "create_post",
		Table:  "posts",
		Action: flop.ActionInsert,
		Validate: func(data map[string]any) error {
			if data["title"] == nil {
				return fmt.Errorf("title is required")
			}
			if data["community_id"] == nil {
				return fmt.Errorf("community_id is required")
			}
			return nil
		},
		PermCheck: func(auth any, data map[string]any) bool {
			return auth != nil
		},
		Transform: func(data map[string]any) map[string]any {
			data["score"] = 1
			data["hot_rank"] = hotRank(1, time.Now())
			data["comment_count"] = 0
			return data
		},
	})

	// Image post upload
	http.HandleFunc("POST /api/posts/image", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}

		asset, err := assetMgr.StoreFromRequest(r, "image")
		if err != nil {
			writeError(w, err.Error(), 400)
			return
		}

		user := auth.(*flop.Row)

		title := r.FormValue("title")
		communityID := r.FormValue("community_id")

		row, err := db.Insert("posts", map[string]any{
			"title":         title,
			"body":          r.FormValue("body"),
			"image":         asset.Path,
			"author_id":     user.ID,
			"community_id":  communityID,
			"score":         1,
			"hot_rank":      hotRank(1, time.Now()),
			"comment_count": 0,
		})
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}

		writeJSON(w, map[string]any{"post": row})
	})

	// ===== VOTE REDUCERS =====

	http.HandleFunc("POST /api/posts/{post_id}/vote", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}

		user := auth.(*flop.Row)
		var input struct {
			Value int `json:"value"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}
		if input.Value != 1 && input.Value != -1 && input.Value != 0 {
			writeError(w, "value must be -1, 0, or 1", 400)
			return
		}

		postIDStr := r.PathValue("post_id")
		postID := parseUint64(postIDStr)

		// Find existing vote
		var existingVote *flop.Row
		db.Table("votes").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["user_id"]) == user.ID &&
				flop.ToUint64(row.Data["post_id"]) == postID {
				existingVote = row
				return false
			}
			return true
		})

		if existingVote != nil {
			if input.Value == 0 {
				db.Delete("votes", existingVote.ID)
			} else {
				db.Update("votes", existingVote.ID, map[string]any{"value": input.Value})
			}
		} else if input.Value != 0 {
			db.Insert("votes", map[string]any{
				"user_id": user.ID,
				"post_id": postID,
				"value":   input.Value,
			})
		}

		// Recalculate score
		score := 0
		db.Table("votes").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["post_id"]) == postID {
				score += int(flop.ToUint64(row.Data["value"]))
			}
			return true
		})

		post, _ := db.Table("posts").Get(postID)
		if post != nil {
			db.Update("posts", postID, map[string]any{
				"score":    score,
				"hot_rank": hotRank(score, post.CreatedAt),
			})
		}

		writeJSON(w, map[string]any{"score": score})
	})

	// ===== COMMENT VIEWS =====

	srv.RegisterView("/api/posts/{post_id}/comments", &flop.ViewDef{
		Name:  "post_comments",
		Table: "comments",
		Filters: []flop.Filter{
			{Field: "post_id", Op: flop.OpEq},
		},
		OrderBy:  "path",
		Order:    flop.Asc,
		Limit:    200,
		Includes: []string{"author_id"},
	})

	// ===== COMMENT REDUCERS =====

	http.HandleFunc("POST /api/posts/{post_id}/comments", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}

		user := auth.(*flop.Row)
		postID := parseUint64(r.PathValue("post_id"))

		var input struct {
			Body     string `json:"body"`
			ParentID uint64 `json:"parent_id"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}

		depth := 0
		path := ""

		if input.ParentID > 0 {
			parent, _ := db.Table("comments").Get(input.ParentID)
			if parent != nil {
				depth = int(flop.ToUint64(parent.Data["depth"])) + 1
				if p, ok := parent.Data["path"].(string); ok {
					path = p
				}
			}
		}

		comment, err := db.Insert("comments", map[string]any{
			"body":      input.Body,
			"author_id": user.ID,
			"post_id":   postID,
			"parent_id": input.ParentID,
			"depth":     depth,
			"path":      "", // will be set after we have the ID
			"score":     0,
		})
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}

		// Set path with comment ID
		commentPath := fmt.Sprintf("%s/%08d", path, comment.ID)
		db.Update("comments", comment.ID, map[string]any{
			"path": commentPath,
		})

		// Update post comment count
		count := 0
		db.Table("comments").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["post_id"]) == postID {
				count++
			}
			return true
		})
		db.Update("posts", postID, map[string]any{"comment_count": count})

		writeJSON(w, map[string]any{"comment": comment})
	})

	// ===== REPOST =====

	http.HandleFunc("POST /api/posts/{post_id}/repost", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}

		user := auth.(*flop.Row)
		originalPostID := parseUint64(r.PathValue("post_id"))

		var input struct {
			CommunityID uint64 `json:"community_id"`
			Title       string `json:"title"`
		}
		readJSON(r, &input)

		original, _ := db.Table("posts").Get(originalPostID)
		if original == nil {
			writeError(w, "post not found", 404)
			return
		}

		title := input.Title
		if title == "" {
			title = fmt.Sprintf("Repost: %s", original.Data["title"])
		}

		communityID := input.CommunityID
		if communityID == 0 {
			communityID = flop.ToUint64(original.Data["community_id"])
		}

		repost, err := db.Insert("posts", map[string]any{
			"title":         title,
			"body":          original.Data["body"],
			"author_id":     user.ID,
			"community_id":  communityID,
			"score":         1,
			"hot_rank":      hotRank(1, time.Now()),
			"comment_count": 0,
			"repost_of":     originalPostID,
		})
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}

		writeJSON(w, map[string]any{"post": repost})
	})

	// ===== ASSETS =====

	http.Handle("/assets/", assetMgr.ServeHandler())

	// ===== START SERVER =====

	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		log.Printf("Reddit clone running on http://localhost%s", addr)
		log.Printf("  POST /api/auth/register   - Register")
		log.Printf("  POST /api/auth/login      - Login")
		log.Printf("  GET  /api/feed/hot        - Hot feed")
		log.Printf("  GET  /api/feed/new        - New feed")
		log.Printf("  GET  /api/feed/best       - Best feed")
		log.Printf("  GET  /api/communities     - List communities")
		log.Printf("  POST /api/communities     - Create community")
		log.Printf("  POST /api/posts           - Create post")
		log.Printf("  POST /api/posts/{id}/vote - Vote on post")
		log.Printf("  GET  /api/posts/{id}/comments      - Get comments")
		log.Printf("  POST /api/posts/{id}/comments      - Add comment")
		log.Printf("  GET  /api/feed/hot/live   - SSE hot feed")
		log.Printf("  GET  /api/search/posts?q= - Search posts")
		if err := srv.ListenAndServe(addr); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")
	srv.Shutdown(context.Background())
}

// hotRank implements Reddit's hot ranking algorithm.
func hotRank(score int, created time.Time) float64 {
	order := math.Log10(math.Max(math.Abs(float64(score)), 1))
	sign := 0.0
	if score > 0 {
		sign = 1
	} else if score < 0 {
		sign = -1
	}
	seconds := float64(created.Unix()-1134028003) / 45000.0
	return sign*order + seconds
}

func parseUint64(s string) uint64 {
	var n uint64
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + uint64(c-'0')
		}
	}
	return n
}

func sanitizeUser(row *flop.Row) map[string]any {
	if row == nil {
		return nil
	}
	data := make(map[string]any)
	for k, v := range row.Data {
		if k != "password" {
			data[k] = v
		}
	}
	data["id"] = row.ID
	return data
}
