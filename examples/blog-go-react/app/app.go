package app

import (
	"fmt"
	"log"
	"net/url"
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

// Build creates the flop App with table definitions.
func Build() *flop.App {
	application := flop.New(flop.Config{
		DataDir:  "./data",
		SyncMode: "normal",
	})

	users := flop.Define(application, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{12}`)
		s.String("email").Required().Unique().Email().MaxLen(255)
		s.Bcrypt("password", 10).Required()
		s.String("name").Required().MinLen(2).MaxLen(80)
		s.Roles("roles")
	})

	posts := flop.Define(application, "posts", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{8}`)
		s.String("slug").Required().Unique()
		s.String("title").Required()
		s.String("excerpt")
		s.String("body").Required()
		s.FileSingle("coverImage", "image/*")
		s.Ref("authorId", users, "id").Required().Index()
		s.Boolean("published").Default(false)
		s.Timestamp("publishedAt")
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(application, "comments", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{12}`)
		s.Ref("postId", posts, "id").Required().Index()
		s.Ref("authorId", users, "id").Required().Index()
		s.String("body").Required()
		s.Timestamp("createdAt").DefaultNow()
	})

	return application
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
