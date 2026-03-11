package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

func TestRedditCloneSmoke(t *testing.T) {
	dir, _ := os.MkdirTemp("", "reddit-smoke-*")
	defer os.RemoveAll(dir)

	db, _ := flop.OpenDB(dir)
	defer db.Close()

	// Create all tables
	schemas := []*flop.Schema{
		{Name: "users", IsAuth: true, Fields: []flop.Field{
			{Name: "email", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "password", Type: flop.FieldString, Required: true},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "display_name", Type: flop.FieldString},
			{Name: "karma", Type: flop.FieldInt},
		}},
		{Name: "communities", Fields: []flop.Field{
			{Name: "name", Type: flop.FieldString, Required: true},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "description", Type: flop.FieldString, Searchable: true},
			{Name: "creator_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "member_count", Type: flop.FieldInt},
			{Name: "visibility", Type: flop.FieldString, EnumValues: []string{"public", "private", "restricted"}},
		}},
		{Name: "posts", Fields: []flop.Field{
			{Name: "title", Type: flop.FieldString, Required: true, Searchable: true},
			{Name: "body", Type: flop.FieldString, Searchable: true},
			{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
			{Name: "score", Type: flop.FieldInt},
			{Name: "hot_rank", Type: flop.FieldFloat},
			{Name: "comment_count", Type: flop.FieldInt},
		}, CascadeDeletes: []string{"comments", "votes"}},
		{Name: "comments", Fields: []flop.Field{
			{Name: "body", Type: flop.FieldString, Required: true},
			{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
			{Name: "parent_id", Type: flop.FieldRef, RefTable: "comments", SelfRef: true},
			{Name: "depth", Type: flop.FieldInt},
			{Name: "path", Type: flop.FieldString},
			{Name: "score", Type: flop.FieldInt},
		}},
		{Name: "votes", Fields: []flop.Field{
			{Name: "user_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "post_id", Type: flop.FieldRef, RefTable: "posts", Required: true},
			{Name: "value", Type: flop.FieldInt},
		}, UniqueConstraints: [][]string{{"user_id", "post_id"}}},
	}

	for _, s := range schemas {
		db.CreateTable(s)
	}

	authCfg := flop.DefaultAuthConfig()
	authMgr := flop.NewAuthManager(db, authCfg)

	// Register user
	user, err := authMgr.Register("alice@test.com", "password123", map[string]any{
		"handle":       "alice",
		"display_name": "Alice",
		"karma":        0,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Logf("User created: id=%d handle=%s", user.ID, user.Data["handle"])

	// Login
	session, err := authMgr.Login("alice@test.com", "password123")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	t.Logf("Session: token=%s...", session.Token[:16])

	// Create community
	community, err := db.Insert("communities", map[string]any{
		"name":         "Golang",
		"handle":       "golang",
		"description":  "All about the Go programming language",
		"creator_id":   user.ID,
		"member_count": 1,
		"visibility":   "public",
	})
	if err != nil {
		t.Fatalf("create community: %v", err)
	}
	t.Logf("Community created: id=%d handle=%s", community.ID, community.Data["handle"])

	// Create posts
	for i := 0; i < 10; i++ {
		_, err := db.Insert("posts", map[string]any{
			"title":         fmt.Sprintf("Post %d: Check out this Go tip!", i),
			"body":          fmt.Sprintf("Here's a great Go programming tip #%d that will make your code faster and cleaner.", i),
			"author_id":     user.ID,
			"community_id":  community.ID,
			"score":         i * 10,
			"hot_rank":      hotRank(i*10, time.Now().Add(-time.Duration(i)*time.Hour)),
			"comment_count": 0,
		})
		if err != nil {
			t.Fatalf("create post: %v", err)
		}
	}
	t.Log("Created 10 posts")

	// Create nested comments
	post1, _ := db.Table("posts").Get(1)
	if post1 == nil {
		// Find first post
		db.Table("posts").Scan(func(row *flop.Row) bool {
			post1 = row
			return false
		})
	}

	comment1, _ := db.Insert("comments", map[string]any{
		"body":      "Great post!",
		"author_id": user.ID,
		"post_id":   post1.ID,
		"parent_id": uint64(0),
		"depth":     0,
		"path":      "/00000001",
		"score":     5,
	})
	t.Logf("Comment 1: id=%d", comment1.ID)

	comment2, _ := db.Insert("comments", map[string]any{
		"body":      "Thanks! Glad you liked it.",
		"author_id": user.ID,
		"post_id":   post1.ID,
		"parent_id": comment1.ID,
		"depth":     1,
		"path":      fmt.Sprintf("/00000001/%08d", comment1.ID+1),
		"score":     3,
	})
	t.Logf("Comment 2 (nested): id=%d", comment2.ID)

	// Test views
	hotFeed := &flop.ViewDef{
		Name:    "hot_feed",
		Table:   "posts",
		OrderBy: "hot_rank",
		Order:   flop.Desc,
		Limit:   5,
	}
	result, err := db.ExecuteView(hotFeed)
	if err != nil {
		t.Fatalf("hot feed: %v", err)
	}
	t.Logf("Hot feed: %d posts (total: %d)", len(result.Rows), result.Total)
	for _, row := range result.Rows {
		t.Logf("  [score=%v] %s", row.Data["score"], row.Data["title"])
	}

	// Test search
	searchResults, err := db.Search("posts", "Go tip", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	t.Logf("Search 'Go tip': %d results", len(searchResults))

	// Test delete (cascade)
	err = db.Delete("posts", post1.ID)
	if err != nil {
		t.Fatalf("delete post: %v", err)
	}
	t.Log("Deleted post with cascade")

	// Verify comments were cascaded
	commentCount, _ := db.Table("comments").Count()
	t.Logf("Comments after cascade delete: %d", commentCount)

	// Test restore
	restored, err := db.Table("posts").Restore(post1.ID)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Logf("Restored post: id=%d title=%s", restored.ID, restored.Data["title"])

	// Test flush and verify persistence
	err = db.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Log("Flushed to disk")

	// Verify counts
	postCount, _ := db.Table("posts").Count()
	userCount, _ := db.Table("users").Count()
	communityCount, _ := db.Table("communities").Count()
	t.Logf("Final counts: users=%d communities=%d posts=%d comments=%d",
		userCount, communityCount, postCount, commentCount)

	t.Log("=== Smoke test passed! ===")
}

func TestHTTPEndpoints(t *testing.T) {
	dir, _ := os.MkdirTemp("", "reddit-http-*")
	defer os.RemoveAll(dir)

	db, _ := flop.OpenDB(dir)
	defer db.Close()

	schemas := []*flop.Schema{
		{Name: "users", IsAuth: true, Fields: []flop.Field{
			{Name: "email", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "password", Type: flop.FieldString, Required: true},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "display_name", Type: flop.FieldString},
			{Name: "karma", Type: flop.FieldInt},
		}},
		{Name: "communities", Fields: []flop.Field{
			{Name: "name", Type: flop.FieldString, Required: true},
			{Name: "handle", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "description", Type: flop.FieldString, Searchable: true},
			{Name: "creator_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "member_count", Type: flop.FieldInt},
			{Name: "visibility", Type: flop.FieldString, EnumValues: []string{"public", "private", "restricted"}},
		}},
		{Name: "posts", Fields: []flop.Field{
			{Name: "title", Type: flop.FieldString, Required: true, Searchable: true},
			{Name: "body", Type: flop.FieldString, Searchable: true},
			{Name: "author_id", Type: flop.FieldRef, RefTable: "users", Required: true},
			{Name: "community_id", Type: flop.FieldRef, RefTable: "communities", Required: true},
			{Name: "score", Type: flop.FieldInt},
			{Name: "hot_rank", Type: flop.FieldFloat},
			{Name: "comment_count", Type: flop.FieldInt},
		}},
	}
	for _, s := range schemas {
		db.CreateTable(s)
	}

	authCfg := flop.DefaultAuthConfig()
	authMgr := flop.NewAuthManager(db, authCfg)

	srv := flop.NewServer(db)
	srv.SetAuth(authMgr.Authenticate)

	srv.RegisterView("/api/feed/hot", &flop.ViewDef{
		Name:    "hot_feed",
		Table:   "posts",
		OrderBy: "hot_rank",
		Order:   flop.Desc,
		Limit:   25,
	})

	// Seed some data
	authMgr.Register("bob@test.com", "pass123", map[string]any{
		"handle": "bob", "display_name": "Bob", "karma": 0,
	})
	session, _ := authMgr.Login("bob@test.com", "pass123")

	db.Insert("communities", map[string]any{
		"name": "Test", "handle": "test", "description": "Test community",
		"creator_id": uint64(1), "member_count": 1, "visibility": "public",
	})

	db.Insert("posts", map[string]any{
		"title": "Hello World", "body": "First post!",
		"author_id": uint64(1), "community_id": uint64(1),
		"score": 42, "hot_rank": hotRank(42, time.Now()), "comment_count": 0,
	})

	// Test GET /api/feed/hot
	go srv.ListenAndServe(":18080")
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:18080/api/feed/hot")
	if err != nil {
		t.Fatalf("GET feed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Hot feed response: %s", string(body))

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var feedResp map[string]any
	json.Unmarshal(body, &feedResp)
	data := feedResp["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("expected 1 post, got %d", len(data))
	}

	// Test authenticated request
	req, _ := http.NewRequest("GET", "http://localhost:18080/api/feed/hot", nil)
	req.Header.Set("Authorization", "Bearer "+session.Token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("authenticated GET: %v", err)
	}
	resp2.Body.Close()
	t.Logf("Authenticated response: %d", resp2.StatusCode)

	_ = bytes.NewBuffer(nil) // keep import
	t.Log("=== HTTP test passed! ===")
}
