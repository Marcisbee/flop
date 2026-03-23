package app

import (
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/marcisbee/flop"
)

type HeadMeta struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type HeadPayload struct {
	Title string     `json:"title"`
	Meta  []HeadMeta `json:"meta,omitempty"`
}

type ListPostsIn struct{}

type GetPostIn struct {
	Slug string `json:"slug"`
}

type GetCommentsIn struct {
	PostID string `json:"postId"`
}

type AddCommentIn struct {
	PostID string `json:"postId"`
	Body   string `json:"body"`
}

type CreatePostIn struct {
	Slug       string `json:"slug"`
	Title      string `json:"title"`
	Excerpt    string `json:"excerpt"`
	Body       string `json:"body"`
	CoverImage string `json:"coverImage"`
}

type UpdatePostIn struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Excerpt string `json:"excerpt"`
	Body    string `json:"body"`
}

type DeletePostIn struct {
	ID string `json:"id"`
}

// Build creates the flop App with table definitions.
func Build() *flop.App {
	application := flop.New(flop.Config{
		DataDir:  "./data",
		SyncMode: "normal",
	})

	users := flop.Define(application, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email().MaxLen(255)
		s.Bcrypt("password", 10).Required()
		s.String("name").Required().MinLen(2).MaxLen(80)
		s.Roles("roles")
	})

	posts := flop.Define(application, "posts", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("slug").Required().Unique()
		s.String("title").Required()
		s.String("excerpt")
		s.String("body").Required()
		s.String("internalNotes").Access(flop.FieldAccess{
			Read: func(c *flop.TableReadCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("superadmin")
			},
			Write: func(c *flop.FieldWriteCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("superadmin")
			},
		})
		s.FileSingle("coverImage", "image/*")
		s.Ref("authorId", users, "id").Required().Index()
		s.Boolean("published").Default(false)
		s.Timestamp("publishedAt")
		s.Timestamp("createdAt").DefaultNow()
		s.Access(flop.TableAccess{
			Read: func(c *flop.TableReadCtx) bool { return true },
			Insert: func(c *flop.TableInsertCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.New["authorId"]) == c.Auth.ID
			},
			Update: func(c *flop.TableUpdateCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.Old["authorId"]) == c.Auth.ID
			},
			Delete: func(c *flop.TableDeleteCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.Row["authorId"]) == c.Auth.ID
			},
		})
	})

	flop.Define(application, "comments", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.Ref("postId", posts, "id").Required().Index()
		s.Ref("authorId", users, "id").Required().Index()
		s.String("body").Required()
		s.Timestamp("createdAt").DefaultNow()
		s.Access(flop.TableAccess{
			Read: func(c *flop.TableReadCtx) bool { return true },
			Insert: func(c *flop.TableInsertCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.New["authorId"]) == c.Auth.ID
			},
			Update: func(c *flop.TableUpdateCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.Old["authorId"]) == c.Auth.ID
			},
			Delete: func(c *flop.TableDeleteCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("superadmin") {
					return true
				}
				return toString(c.Row["authorId"]) == c.Auth.ID
			},
		})
	})

	flop.Define(application, "secrets", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("key").Required().Unique()
		s.String("value").Required()
		s.Access(flop.TableAccess{
			Read: func(c *flop.TableReadCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
			Insert: func(c *flop.TableInsertCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
			Update: func(c *flop.TableUpdateCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
			Delete: func(c *flop.TableDeleteCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
		})
	})

	flop.View(application, "list_posts", flop.Public(), ListPostsView)
	flop.View(application, "get_post", flop.Public(), GetPostView)
	flop.View(application, "get_comments", flop.Public(), GetCommentsView)
	flop.Reducer(application, "add_comment", flop.Authenticated(), AddCommentReducer)
	flop.Reducer(application, "create_post", flop.Authenticated(), CreatePostReducer)
	flop.Reducer(application, "update_post", flop.Authenticated(), UpdatePostReducer)
	flop.Reducer(application, "delete_post", flop.Authenticated(), DeletePostReducer)

	return application
}

func ListPostsView(ctx *flop.ViewCtx, _ ListPostsIn) ([]map[string]any, error) {
	posts := ctx.DB.Table("posts")
	if posts == nil {
		return nil, fmt.Errorf("posts table not found")
	}
	users := ctx.DB.Table("users")
	rows, err := posts.Scan(200, 0)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return toInt64(rows[i]["publishedAt"]) > toInt64(rows[j]["publishedAt"])
	})

	out := make([]map[string]any, 0, len(rows))
	for _, post := range rows {
		if !toBool(post["published"]) {
			continue
		}
		authorName := "Unknown"
		if users != nil {
			if u, err := users.Get(toString(post["authorId"])); err == nil && u != nil {
				authorName = toString(u["name"])
			}
		}
		out = append(out, map[string]any{
			"id":         post["id"],
			"slug":       post["slug"],
			"title":      post["title"],
			"excerpt":    post["excerpt"],
			"body":       post["body"],
			"authorName": authorName,
			"createdAt":  post["createdAt"],
		})
	}
	return out, nil
}

func GetPostView(ctx *flop.ViewCtx, in GetPostIn) (map[string]any, error) {
	slug := strings.TrimSpace(in.Slug)
	if slug == "" {
		return nil, fmt.Errorf("slug is required")
	}
	posts := ctx.DB.Table("posts")
	if posts == nil {
		return nil, fmt.Errorf("posts table not found")
	}
	users := ctx.DB.Table("users")
	post, ok := posts.FindByUniqueIndex("slug", slug)
	if !ok || post == nil {
		return nil, nil
	}
	authorName := "Unknown"
	if users != nil {
		if u, err := users.Get(toString(post["authorId"])); err == nil && u != nil {
			authorName = toString(u["name"])
		}
	}
	return map[string]any{
		"post":       post,
		"authorName": authorName,
	}, nil
}

func GetCommentsView(ctx *flop.ViewCtx, in GetCommentsIn) ([]map[string]any, error) {
	postID := strings.TrimSpace(in.PostID)
	if postID == "" {
		return nil, fmt.Errorf("postId is required")
	}
	comments := ctx.DB.Table("comments")
	if comments == nil {
		return nil, fmt.Errorf("comments table not found")
	}
	users := ctx.DB.Table("users")
	rows, err := comments.FindByIndex("postId", postID)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return toInt64(rows[i]["createdAt"]) < toInt64(rows[j]["createdAt"])
	})

	out := make([]map[string]any, 0, len(rows))
	for _, c := range rows {
		authorName := "Unknown"
		if users != nil {
			if u, err := users.Get(toString(c["authorId"])); err == nil && u != nil {
				authorName = toString(u["name"])
			}
		}
		out = append(out, map[string]any{
			"comment":    c,
			"authorName": authorName,
		})
	}
	return out, nil
}

func AddCommentReducer(ctx *flop.ReducerCtx, in AddCommentIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	postID := strings.TrimSpace(in.PostID)
	body := strings.TrimSpace(in.Body)
	if postID == "" || body == "" {
		return nil, fmt.Errorf("postId and body are required")
	}
	posts := ctx.DB.Table("posts")
	comments := ctx.DB.Table("comments")
	if posts == nil || comments == nil {
		return nil, fmt.Errorf("required tables not found")
	}
	post, err := posts.Get(postID)
	if err != nil || post == nil {
		return nil, fmt.Errorf("post not found")
	}
	row, err := comments.Insert(map[string]any{
		"postId":   postID,
		"authorId": auth.ID,
		"body":     body,
	})
	if err != nil {
		return nil, err
	}
	return row, nil
}

func CreatePostReducer(ctx *flop.ReducerCtx, in CreatePostIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Slug) == "" || strings.TrimSpace(in.Title) == "" || strings.TrimSpace(in.Body) == "" {
		return nil, fmt.Errorf("slug, title and body are required")
	}
	posts := ctx.DB.Table("posts")
	if posts == nil {
		return nil, fmt.Errorf("posts table not found")
	}
	insert := map[string]any{
		"slug":        strings.TrimSpace(in.Slug),
		"title":       strings.TrimSpace(in.Title),
		"excerpt":     strings.TrimSpace(in.Excerpt),
		"body":        strings.TrimSpace(in.Body),
		"authorId":    auth.ID,
		"published":   true,
		"publishedAt": float64(time.Now().UnixMilli()),
	}
	row, err := posts.Insert(insert)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func UpdatePostReducer(ctx *flop.ReducerCtx, in UpdatePostIn) (map[string]any, error) {
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}
	posts := ctx.DB.Table("posts")
	if posts == nil {
		return nil, fmt.Errorf("posts table not found")
	}
	fields := map[string]any{}
	if title := strings.TrimSpace(in.Title); title != "" {
		fields["title"] = title
	}
	fields["excerpt"] = strings.TrimSpace(in.Excerpt)
	if body := strings.TrimSpace(in.Body); body != "" {
		fields["body"] = body
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("no fields to update")
	}
	return posts.Update(id, fields)
}

func DeletePostReducer(ctx *flop.ReducerCtx, in DeletePostIn) (map[string]any, error) {
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}
	posts := ctx.DB.Table("posts")
	if posts == nil {
		return nil, fmt.Errorf("posts table not found")
	}
	ok, err := posts.Delete(id)
	if err != nil {
		return nil, err
	}
	return map[string]any{"deleted": ok}, nil
}

// Seed inserts initial data if the database is empty.
func Seed(db *flop.Database) {
	users := db.Table("users")
	posts := db.Table("posts")
	comments := db.Table("comments")

	// Only seed if users table is empty (first run)
	if users.Count() > 0 {
		return
	}

	log.Println("Seeding database...")
	now := time.Now().UnixMilli()

	// Hash passwords before inserting (engine does not auto-hash)
	hashedPw, err := flop.HashPassword("password")
	if err != nil {
		log.Fatalf("seed hash password: %v", err)
	}

	marc, err := users.Insert(map[string]any{
		"email":    "marc@example.com",
		"password": hashedPw,
		"name":     "Marc",
		"roles":    []any{"superadmin"},
	})
	if err != nil {
		log.Fatalf("seed user marc: %v", err)
	}

	reader, err := users.Insert(map[string]any{
		"email":    "reader@example.com",
		"password": hashedPw,
		"name":     "Reader",
		"roles":    []any{"user"},
	})
	if err != nil {
		log.Fatalf("seed user reader: %v", err)
	}

	dev, err := users.Insert(map[string]any{
		"email":    "dev@example.com",
		"password": hashedPw,
		"name":     "Dev",
		"roles":    []any{"user"},
	})
	if err != nil {
		log.Fatalf("seed user dev: %v", err)
	}

	marcID := marc["id"].(string)
	readerID := reader["id"].(string)
	devID := dev["id"].(string)

	// Posts
	post1, err := posts.Insert(map[string]any{
		"slug":        "go-first-flop",
		"title":       "Flop Goes Go-First",
		"excerpt":     "Why the runtime is moving from QuickJS to native Go handlers.",
		"body":        "This is a sample post body for the Go+React scaffold.",
		"authorId":    marcID,
		"published":   true,
		"publishedAt": float64(now - 3600_000),
	})
	if err != nil {
		log.Fatalf("seed post 1: %v", err)
	}

	post2, err := posts.Insert(map[string]any{
		"slug":        "typed-head-and-loader",
		"title":       "Typed Head + Loader Design",
		"excerpt":     "How route head and route data stay type-safe from Go to React.",
		"body":        "This is another sample post body for the scaffold.",
		"authorId":    marcID,
		"published":   true,
		"publishedAt": float64(now - 1800_000),
	})
	if err != nil {
		log.Fatalf("seed post 2: %v", err)
	}

	post1ID := post1["id"].(string)
	post2ID := post2["id"].(string)

	// Comments
	if _, err := comments.Insert(map[string]any{
		"postId":   post1ID,
		"authorId": readerID,
		"body":     "Nice writeup.",
	}); err != nil {
		log.Fatalf("seed comment 1: %v", err)
	}

	if _, err := comments.Insert(map[string]any{
		"postId":   post2ID,
		"authorId": devID,
		"body":     "Looking forward to the next milestone.",
	}); err != nil {
		log.Fatalf("seed comment 2: %v", err)
	}

	log.Println("Seed complete.")
}

// ResolveHead returns head metadata for SSR.
func ResolveHead(db *flop.Database, path string) HeadPayload {
	switch {
	case path == "/":
		posts := db.Table("posts")
		count := posts.Count()
		return HeadPayload{
			Title: "My Blog",
			Meta: []HeadMeta{
				{Name: "description", Content: fmt.Sprintf("A blog with %d articles about software engineering", count)},
			},
		}
	case path == "/about":
		return HeadPayload{
			Title: "About - My Blog",
			Meta: []HeadMeta{
				{Name: "description", Content: "About this blog and its author"},
			},
		}
	case strings.HasPrefix(path, "/post/"):
		raw := strings.TrimPrefix(path, "/post/")
		slug, _ := url.PathUnescape(raw)
		post := findPostBySlug(db, slug)
		if post == nil {
			return HeadPayload{
				Title: "Not Found - My Blog",
				Meta: []HeadMeta{
					{Name: "description", Content: "Requested post was not found"},
				},
			}
		}
		excerpt, _ := post["excerpt"].(string)
		title, _ := post["title"].(string)
		desc := excerpt
		if desc == "" {
			desc = title
		}
		return HeadPayload{
			Title: title + " - My Blog",
			Meta: []HeadMeta{
				{Name: "description", Content: desc},
			},
		}
	default:
		return HeadPayload{
			Title: "Not Found - My Blog",
			Meta: []HeadMeta{
				{Name: "description", Content: "Page not found"},
			},
		}
	}
}

func findPostBySlug(db *flop.Database, slug string) map[string]any {
	posts := db.Table("posts")
	if posts == nil {
		return nil
	}
	row, ok := posts.FindByUniqueIndex("slug", slug)
	if !ok {
		return nil
	}
	return row
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	default:
		return 0
	}
}

func toBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case string:
		return strings.EqualFold(b, "true")
	case int:
		return b != 0
	case int64:
		return b != 0
	case float64:
		return b != 0
	default:
		return false
	}
}
