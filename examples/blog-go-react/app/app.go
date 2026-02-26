package app

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/marcisbee/flop"
)

type User struct {
	ID       string   `json:"id"`
	Email    string   `json:"email"`
	Password string   `json:"password"`
	Name     string   `json:"name"`
	Roles    []string `json:"roles"`
}

type Post struct {
	ID          string        `json:"id"`
	Slug        string        `json:"slug"`
	Title       string        `json:"title"`
	Excerpt     string        `json:"excerpt,omitempty"`
	Body        string        `json:"body"`
	CoverImage  *flop.FileRef `json:"coverImage,omitempty"`
	AuthorID    string        `json:"authorId"`
	Published   bool          `json:"published"`
	PublishedAt int64         `json:"publishedAt"`
	CreatedAt   int64         `json:"createdAt"`
}

type Comment struct {
	ID        string `json:"id"`
	PostID    string `json:"postId"`
	AuthorID  string `json:"authorId"`
	Body      string `json:"body"`
	CreatedAt int64  `json:"createdAt"`
}

type PostWithAuthor struct {
	Post
	AuthorName string `json:"authorName"`
}

type CommentWithAuthor struct {
	Comment
	AuthorName string `json:"authorName"`
}

type GetPostIn struct {
	Slug string `json:"slug"`
}

type GetCommentsIn struct {
	PostID string `json:"postId"`
}

type CreatePostIn struct {
	Title      string `json:"title"`
	Slug       string `json:"slug"`
	Excerpt    string `json:"excerpt"`
	Body       string `json:"body"`
	CoverImage string `json:"coverImage"`
}

type AddCommentIn struct {
	PostID string `json:"postId"`
	Body   string `json:"body"`
}

func Build() *flop.App {
	application := flop.New(flop.Config{
		DataDir:  "./data",
		SyncMode: "normal",
	})

	users := flop.AutoTable[User](application, "users", func(t *flop.TableBuilder[User]) {
		t.Field("ID").Primary().Autogen(`[a-z0-9]{12}`)
		t.Field("Email").Required().Unique()
		t.Field("Password").Required().Bcrypt(10)
		t.Field("Name").Required()
		t.Field("Roles").Roles()
	})

	posts := flop.AutoTable[Post](application, "posts", func(t *flop.TableBuilder[Post]) {
		t.Field("ID").Primary().Autogen(`[a-z0-9]{8}`)
		t.Field("Slug").Required().Unique()
		t.Field("Title").Required()
		t.Field("Body").Required()
		t.Field("CoverImage").FileSingle("image/*")
		t.Field("AuthorID").Required().Ref(users, "ID").Index()
		t.Field("Published").Default(false)
		t.Field("PublishedAt").Timestamp()
		t.Field("CreatedAt").Timestamp().DefaultNow()
	})

	comments := flop.AutoTable[Comment](application, "comments", func(t *flop.TableBuilder[Comment]) {
		t.Field("ID").Primary().Autogen(`[a-z0-9]{12}`)
		t.Field("PostID").Required().Ref(posts, "ID").Index()
		t.Field("AuthorID").Required().Ref(users, "ID").Index()
		t.Field("Body").Required()
		t.Field("CreatedAt").Timestamp().DefaultNow()
	})
	_ = comments

	flop.View[struct{}, []PostWithAuthor](application, "list_posts", flop.Public(), func(ctx *flop.ViewCtx, _ struct{}) ([]PostWithAuthor, error) {
		_ = ctx
		return MockPosts(), nil
	})

	flop.View[GetPostIn, *PostWithAuthor](application, "get_post", flop.Public(), func(ctx *flop.ViewCtx, in GetPostIn) (*PostWithAuthor, error) {
		_ = ctx
		for _, p := range MockPosts() {
			if p.Slug == in.Slug {
				cp := p
				return &cp, nil
			}
		}
		return nil, nil
	})

	flop.View[GetCommentsIn, []CommentWithAuthor](application, "get_comments", flop.Public(), func(ctx *flop.ViewCtx, in GetCommentsIn) ([]CommentWithAuthor, error) {
		_ = ctx
		return MockComments(in.PostID), nil
	})

	flop.Reducer[CreatePostIn, Post](application, "create_post", flop.Roles("admin"), func(ctx *flop.ReducerCtx, in CreatePostIn) (Post, error) {
		if _, err := ctx.RequireAuth(); err != nil {
			return Post{}, err
		}
		_ = in
		return Post{}, flop.ErrNotImplemented
	})

	flop.Reducer[AddCommentIn, Comment](application, "add_comment", flop.Authenticated(), func(ctx *flop.ReducerCtx, in AddCommentIn) (Comment, error) {
		auth, err := ctx.RequireAuth()
		if err != nil {
			return Comment{}, err
		}
		if in.PostID == "" || in.Body == "" {
			return Comment{}, errors.New("postId and body are required")
		}
		return Comment{
			ID:        "preview-only",
			PostID:    in.PostID,
			AuthorID:  auth.ID,
			Body:      in.Body,
			CreatedAt: time.Now().UnixMilli(),
		}, nil
	})

	flop.Layout(application, "/", flop.LayoutConfig{
		Entry: "./pages/layout.tsx",
	})

	flop.Page[struct{}, []PostWithAuthor](application, "/", flop.PageConfig[struct{}, []PostWithAuthor]{
		Entry: "./pages/home.tsx",
		Loader: func(ctx *flop.LoaderCtx, _ struct{}) ([]PostWithAuthor, error) {
			_ = ctx
			return MockPosts(), nil
		},
	})

	flop.Page[GetPostIn, *PostWithAuthor](application, "/post/:slug", flop.PageConfig[GetPostIn, *PostWithAuthor]{
		Entry: "./pages/post.tsx",
		Loader: func(ctx *flop.LoaderCtx, in GetPostIn) (*PostWithAuthor, error) {
			_ = ctx
			for _, p := range MockPosts() {
				if p.Slug == in.Slug {
					cp := p
					return &cp, nil
				}
			}
			return nil, nil
		},
	})

	flop.Page[struct{}, struct{}](application, "/about", flop.PageConfig[struct{}, struct{}]{
		Entry: "./pages/about.tsx",
	})

	return application
}

func MockPosts() []PostWithAuthor {
	now := time.Now().UnixMilli()
	return []PostWithAuthor{
		{
			Post: Post{
				ID:          "post-1",
				Slug:        "go-first-flop",
				Title:       "Flop Goes Go-First",
				Excerpt:     "Why the runtime is moving from QuickJS to native Go handlers.",
				Body:        "This is a sample post body for the Go+React scaffold.",
				AuthorID:    "user-1",
				Published:   true,
				PublishedAt: now - 3600_000,
				CreatedAt:   now - 7200_000,
			},
			AuthorName: "Marc",
		},
		{
			Post: Post{
				ID:          "post-2",
				Slug:        "typed-head-and-loader",
				Title:       "Typed Head + Loader Design",
				Excerpt:     "How route head and route data stay type-safe from Go to React.",
				Body:        "This is another sample post body for the scaffold.",
				AuthorID:    "user-1",
				Published:   true,
				PublishedAt: now - 1800_000,
				CreatedAt:   now - 5400_000,
			},
			AuthorName: "Marc",
		},
	}
}

func MockComments(postID string) []CommentWithAuthor {
	now := time.Now().UnixMilli()
	all := []CommentWithAuthor{
		{
			Comment: Comment{
				ID:        "comment-1",
				PostID:    "post-1",
				AuthorID:  "user-2",
				Body:      "Nice writeup.",
				CreatedAt: now - 3000_000,
			},
			AuthorName: "Reader",
		},
		{
			Comment: Comment{
				ID:        "comment-2",
				PostID:    "post-2",
				AuthorID:  "user-3",
				Body:      "Looking forward to the next milestone.",
				CreatedAt: now - 2000_000,
			},
			AuthorName: "Dev",
		},
	}

	filtered := make([]CommentWithAuthor, 0, len(all))
	for _, c := range all {
		if c.PostID == postID {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

type HeadMeta struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type HeadPayload struct {
	Title string     `json:"title"`
	Meta  []HeadMeta `json:"meta,omitempty"`
}

func ResolveHead(path string) HeadPayload {
	switch {
	case path == "/":
		count := len(MockPosts())
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
		post := findPostBySlug(slug)
		if post == nil {
			return HeadPayload{
				Title: "Not Found - My Blog",
				Meta: []HeadMeta{
					{Name: "description", Content: "Requested post was not found"},
				},
			}
		}
		desc := post.Excerpt
		if desc == "" {
			desc = post.Title
		}
		return HeadPayload{
			Title: post.Title + " - My Blog",
			Meta: []HeadMeta{
				{Name: "description", Content: desc},
				{Name: "author", Content: post.AuthorName},
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

func findPostBySlug(slug string) *PostWithAuthor {
	for _, p := range MockPosts() {
		if p.Slug == slug {
			cp := p
			return &cp
		}
	}
	return nil
}

type AdminTableSummary struct {
	Name     string `json:"name"`
	RowCount int    `json:"rowCount"`
}

type AdminRowsPage struct {
	Table  string           `json:"table"`
	Rows   []map[string]any `json:"rows"`
	Total  int              `json:"total"`
	Offset int              `json:"offset"`
	Limit  int              `json:"limit"`
}

func AdminTables() []AdminTableSummary {
	users := mockUsers()
	posts := MockPosts()
	comments := append(MockComments("post-1"), MockComments("post-2")...)

	tables := []AdminTableSummary{
		{Name: "users", RowCount: len(users)},
		{Name: "posts", RowCount: len(posts)},
		{Name: "comments", RowCount: len(comments)},
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables
}

func AdminRows(table string, limit, offset int) (AdminRowsPage, bool) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	rows := make([]map[string]any, 0)
	switch table {
	case "users":
		for _, u := range mockUsers() {
			rows = append(rows, map[string]any{
				"id":    u.ID,
				"email": u.Email,
				"name":  u.Name,
				"roles": append([]string(nil), u.Roles...),
			})
		}
	case "posts":
		for _, p := range MockPosts() {
			rows = append(rows, map[string]any{
				"id":          p.ID,
				"slug":        p.Slug,
				"title":       p.Title,
				"excerpt":     p.Excerpt,
				"authorId":    p.AuthorID,
				"authorName":  p.AuthorName,
				"published":   p.Published,
				"publishedAt": p.PublishedAt,
				"createdAt":   p.CreatedAt,
			})
		}
	case "comments":
		for _, c := range append(MockComments("post-1"), MockComments("post-2")...) {
			rows = append(rows, map[string]any{
				"id":         c.ID,
				"postId":     c.PostID,
				"authorId":   c.AuthorID,
				"authorName": c.AuthorName,
				"body":       c.Body,
				"createdAt":  c.CreatedAt,
			})
		}
	default:
		return AdminRowsPage{}, false
	}

	total := len(rows)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	return AdminRowsPage{
		Table:  table,
		Rows:   rows[offset:end],
		Total:  total,
		Offset: offset,
		Limit:  limit,
	}, true
}

func mockUsers() []User {
	return []User{
		{
			ID:    "user-1",
			Email: "marc@example.com",
			Name:  "Marc",
			Roles: []string{"admin"},
		},
		{
			ID:    "user-2",
			Email: "reader@example.com",
			Name:  "Reader",
			Roles: []string{"user"},
		},
		{
			ID:    "user-3",
			Email: "dev@example.com",
			Name:  "Dev",
			Roles: []string{"user"},
		},
	}
}
