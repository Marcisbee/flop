package app

import (
	"fmt"
	"math"
	"net/http"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

func Setup(db *flop.DB, srv *flop.Server, authMgr *flop.AuthManager, assetMgr *flop.AssetManager) {
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
			{Name: "path", Type: flop.FieldString},
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
			panic(fmt.Sprintf("create table %s: %v", s.Name, err))
		}
	}

	// ===== AUTH ENDPOINTS =====

	srv.HandleFunc("POST /api/auth/register", func(w http.ResponseWriter, r *http.Request) {
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
		if input.Email == "" || input.Password == "" || input.Handle == "" {
			writeError(w, "email, password, and handle are required", 400)
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

	srv.HandleFunc("POST /api/auth/login", func(w http.ResponseWriter, r *http.Request) {
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

	srv.HandleFunc("POST /api/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}
		session, err := authMgr.Refresh(input.RefreshToken)
		if err != nil {
			writeError(w, "invalid refresh token", 401)
			return
		}
		writeJSON(w, map[string]any{"token": session.Token})
	})

	srv.HandleFunc("GET /api/auth/me", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}
		user := auth.(*flop.Row)
		writeJSON(w, map[string]any{"user": sanitizeUser(user)})
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

	srv.HandleFunc("POST /api/communities", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}
		user := auth.(*flop.Row)

		var input struct {
			Name        string `json:"name"`
			Handle      string `json:"handle"`
			Description string `json:"description"`
		}
		if err := readJSON(r, &input); err != nil {
			writeError(w, err.Error(), 400)
			return
		}
		if input.Name == "" || input.Handle == "" {
			writeError(w, "name and handle are required", 400)
			return
		}

		community, err := db.Insert("communities", map[string]any{
			"name":         input.Name,
			"handle":       input.Handle,
			"description":  input.Description,
			"creator_id":   user.ID,
			"member_count": 1,
			"visibility":   "public",
		})
		if err != nil {
			writeError(w, err.Error(), 409)
			return
		}

		// Auto-join creator as admin
		db.Insert("memberships", map[string]any{
			"user_id":      user.ID,
			"community_id": community.ID,
			"role":         "admin",
		})

		writeJSON(w, map[string]any{"community": community})
	})

	// Toggle join/leave community
	srv.HandleFunc("POST /api/communities/{community_id}/toggle_join", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}
		user := auth.(*flop.Row)
		communityID := parseUint64(r.PathValue("community_id"))

		// Find existing membership
		var existing *flop.Row
		db.Table("memberships").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["user_id"]) == user.ID &&
				flop.ToUint64(row.Data["community_id"]) == communityID {
				existing = row
				return false
			}
			return true
		})

		if existing != nil {
			// Leave: delete membership
			db.Delete("memberships", existing.ID)
			community, _ := db.Table("communities").Get(communityID)
			if community != nil {
				count := int(flop.ToUint64(community.Data["member_count"])) - 1
				if count < 0 {
					count = 0
				}
				db.Update("communities", communityID, map[string]any{"member_count": count})
			}
			writeJSON(w, map[string]any{"joined": false})
		} else {
			// Join: create membership
			_, err = db.Insert("memberships", map[string]any{
				"user_id":      user.ID,
				"community_id": communityID,
				"role":         "member",
			})
			if err != nil {
				writeError(w, err.Error(), 500)
				return
			}
			community, _ := db.Table("communities").Get(communityID)
			if community != nil {
				count := int(flop.ToUint64(community.Data["member_count"])) + 1
				db.Update("communities", communityID, map[string]any{"member_count": count})
			}
			writeJSON(w, map[string]any{"joined": true})
		}
	})

	// Check membership status
	srv.HandleFunc("GET /api/communities/{community_id}/membership", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeJSON(w, map[string]any{"joined": false})
			return
		}
		user := auth.(*flop.Row)
		communityID := parseUint64(r.PathValue("community_id"))

		joined := false
		db.Table("memberships").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["user_id"]) == user.ID &&
				flop.ToUint64(row.Data["community_id"]) == communityID {
				joined = true
				return false
			}
			return true
		})
		writeJSON(w, map[string]any{"joined": joined})
	})

	// ===== POST VIEWS =====

	srv.RegisterView("/api/feed/hot", &flop.ViewDef{
		Name:     "hot_feed",
		Table:    "posts",
		OrderBy:  "hot_rank",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	srv.RegisterView("/api/feed/new", &flop.ViewDef{
		Name:     "new_feed",
		Table:    "posts",
		OrderBy:  "id",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

	srv.RegisterView("/api/feed/best", &flop.ViewDef{
		Name:     "best_feed",
		Table:    "posts",
		OrderBy:  "score",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

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

	srv.RegisterView("/api/posts/{id}", &flop.ViewDef{
		Name:  "get_post",
		Table: "posts",
		Filters: []flop.Filter{
			{Field: "id", Op: flop.OpEq},
		},
		Limit:    1,
		Includes: []string{"author_id", "community_id"},
	})

	srv.RegisterView("/api/search/posts", &flop.ViewDef{
		Name:     "search_posts",
		Table:    "posts",
		OrderBy:  "score",
		Order:    flop.Desc,
		Limit:    25,
		Includes: []string{"author_id", "community_id"},
	})

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
		AuthTransform: func(auth any, data map[string]any) map[string]any {
			data["score"] = 1
			data["hot_rank"] = hotRank(1, time.Now())
			data["comment_count"] = 0
			if user, ok := auth.(*flop.Row); ok {
				data["author_id"] = user.ID
			}
			return data
		},
	})

	// Image post upload
	srv.HandleFunc("POST /api/posts/image", func(w http.ResponseWriter, r *http.Request) {
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

	// ===== VOTE ENDPOINT =====

	srv.HandleFunc("POST /api/posts/{post_id}/vote", func(w http.ResponseWriter, r *http.Request) {
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

		score := 0
		db.Table("votes").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["post_id"]) == postID {
				v := row.Data["value"]
				switch n := v.(type) {
				case float64:
					score += int(n)
				case int:
					score += n
				}
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

		// Return the user's current vote value
		userVote := input.Value
		writeJSON(w, map[string]any{"score": score, "user_vote": userVote})
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

	// ===== COMMENT ENDPOINTS =====

	srv.HandleFunc("POST /api/posts/{post_id}/comments", func(w http.ResponseWriter, r *http.Request) {
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
			"path":      "",
			"score":     0,
		})
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}

		commentPath := fmt.Sprintf("%s/%08d", path, comment.ID)
		db.Update("comments", comment.ID, map[string]any{"path": commentPath})

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

	// ===== COMMENT VOTE =====

	srv.HandleFunc("POST /api/comments/{comment_id}/vote", func(w http.ResponseWriter, r *http.Request) {
		auth, err := authMgr.Authenticate(r)
		if err != nil || auth == nil {
			writeError(w, "unauthorized", 401)
			return
		}

		user := auth.(*flop.Row)
		commentID := parseUint64(r.PathValue("comment_id"))

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

		var existingVote *flop.Row
		db.Table("comment_votes").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["user_id"]) == user.ID &&
				flop.ToUint64(row.Data["comment_id"]) == commentID {
				existingVote = row
				return false
			}
			return true
		})

		if existingVote != nil {
			if input.Value == 0 {
				db.Delete("comment_votes", existingVote.ID)
			} else {
				db.Update("comment_votes", existingVote.ID, map[string]any{"value": input.Value})
			}
		} else if input.Value != 0 {
			db.Insert("comment_votes", map[string]any{
				"user_id":    user.ID,
				"comment_id": commentID,
				"value":      input.Value,
			})
		}

		score := 0
		db.Table("comment_votes").Scan(func(row *flop.Row) bool {
			if flop.ToUint64(row.Data["comment_id"]) == commentID {
				v := row.Data["value"]
				switch n := v.(type) {
				case float64:
					score += int(n)
				case int:
					score += n
				}
			}
			return true
		})

		db.Update("comments", commentID, map[string]any{"score": score})
		writeJSON(w, map[string]any{"score": score, "user_vote": input.Value})
	})

	// ===== REPOST =====

	srv.HandleFunc("POST /api/posts/{post_id}/repost", func(w http.ResponseWriter, r *http.Request) {
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

	srv.Handle("/assets/", assetMgr.ServeHandler())
}

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
