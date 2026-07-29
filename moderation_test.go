package flop

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func moderationTestApp(t *testing.T, endpoint string) (*App, *Table[map[string]any], *Table[map[string]any]) {
	t.Helper()
	app := New(Config{
		DataDir:  t.TempDir(),
		SyncMode: "normal",
		Moderation: &ModerationConfig{
			OpenRouterAPIKey: "test-key",
			OpenRouterURL:    endpoint,
			Workers:          1,
		},
	})
	users := Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Boolean("blocked").Required().Default(false)
	})
	posts := Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("userId").Required().Index()
		s.String("body").Required()
	})
	return app, users, posts
}

func waitForModerationRun(t *testing.T, db *Database, rowID, status string) ModerationRun {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, _, err := db.ModerationRuns("", 100, 0)
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		for _, run := range runs {
			if run.RowID == rowID && run.Status == status {
				return run
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for row %s moderation status %s", rowID, status)
	return ModerationRun{}
}

func TestModerationHidesUntilClearedAndStopsAfterThreshold(t *testing.T) {
	var requests atomic.Int64
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization header = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		provider, _ := body["provider"].(map[string]any)
		if provider == nil {
			t.Errorf("provider preferences missing")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"verdict\":\"clear\",\"categories\":[],\"reasoning\":\"ordinary discussion\",\"recommended_action\":\"allow\"}"}}]}`))
	}))
	defer router.Close()

	app, users, posts := moderationTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := users.Insert(db, map[string]any{"id": "u1", "blocked": false}); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	moderator, err := db.SaveModerator(Moderator{
		Name:                "New user posts",
		Enabled:             true,
		Table:               "posts",
		Events:              []string{"insert"},
		ContentFields:       []string{"body"},
		UserField:           "userId",
		UserTable:           "users",
		UserBlockedField:    "blocked",
		NewUserClearedLimit: 1,
		Model:               "test/model",
		AllowedActions:      []string{"review", "delete", "block_user"},
		PublishBeforeReview: false,
	})
	if err != nil {
		t.Fatalf("save moderator: %v", err)
	}
	if moderator.ID == "" {
		t.Fatal("moderator ID was not generated")
	}

	if _, err := posts.Insert(db, map[string]any{"id": "p1", "userId": "u1", "body": "hello"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	publicPosts := db.trackedAccessor(nil, nil).Table("posts")
	if row, err := publicPosts.Get("p1"); err != nil || row != nil {
		t.Fatalf("pending row should be hidden, row=%v err=%v", row, err)
	}
	waitForModerationRun(t, db, "p1", "cleared")
	if row, err := publicPosts.Get("p1"); err != nil || row == nil {
		t.Fatalf("cleared row should be visible, row=%v err=%v", row, err)
	}

	if _, err := posts.Insert(db, map[string]any{"id": "p2", "userId": "u1", "body": "second"}); err != nil {
		t.Fatalf("insert second post: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	runs, total, err := db.ModerationRuns("", 100, 0)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("expected threshold to stop review after one cleared item, total=%d runs=%v", total, runs)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("OpenRouter requests = %d, want 1", got)
	}
}

func TestModerationDeletesViolation(t *testing.T) {
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"verdict\":\"violation\",\"categories\":[\"scam\"],\"reasoning\":\"credential theft\",\"recommended_action\":\"delete\"}"}}]}`))
	}))
	defer router.Close()

	app, _, posts := moderationTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.SaveModerator(Moderator{
		Name:                "Scam removal",
		Enabled:             true,
		Table:               "posts",
		Events:              []string{"insert"},
		ContentFields:       []string{"body"},
		Model:               "test/model",
		Provider:            "test-provider",
		AllowedActions:      []string{"review", "delete"},
		PublishBeforeReview: true,
	}); err != nil {
		t.Fatalf("save moderator: %v", err)
	}
	if _, err := posts.Insert(db, map[string]any{"id": "scam", "userId": "u1", "body": "send password"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	run := waitForModerationRun(t, db, "scam", "actioned")
	if run.Action != "delete" || !strings.Contains(run.Reasoning, "credential") {
		t.Fatalf("unexpected actioned run: %+v", run)
	}
	if row, err := db.Table("posts").Get("scam"); err != nil || row != nil {
		t.Fatalf("violating row should be deleted, row=%v err=%v", row, err)
	}
}

func TestModerationReportDeletesTarget(t *testing.T) {
	var sawTarget atomic.Bool
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(request.Messages) > 1 && strings.Contains(request.Messages[1].Content, "reported post") {
			sawTarget.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"verdict\":\"violation\",\"categories\":[\"illegal\"],\"reasoning\":\"valid report\",\"recommended_action\":\"delete\"}"}}]}`))
	}))
	defer router.Close()

	app, _, posts := moderationTestApp(t, router.URL)
	reports := Define(app, "reports", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("postId").Required()
		s.String("reason").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := posts.Insert(db, map[string]any{"id": "target", "userId": "u1", "body": "reported post"}); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if _, err := db.SaveModerator(Moderator{
		Name:                "Post reports",
		Enabled:             true,
		Table:               "reports",
		Events:              []string{"insert"},
		ContentFields:       []string{"reason"},
		TargetTable:         "posts",
		TargetIDField:       "postId",
		TargetContentFields: []string{"body"},
		Model:               "test/model",
		AllowedActions:      []string{"review", "delete"},
		PublishBeforeReview: true,
	}); err != nil {
		t.Fatalf("save moderator: %v", err)
	}
	if _, err := reports.Insert(db, map[string]any{"id": "r1", "postId": "target", "reason": "illegal content"}); err != nil {
		t.Fatalf("insert report: %v", err)
	}
	waitForModerationRun(t, db, "r1", "actioned")
	if !sawTarget.Load() {
		t.Fatal("OpenRouter payload did not include target content")
	}
	if row, err := db.Table("posts").Get("target"); err != nil || row != nil {
		t.Fatalf("reported target should be deleted, row=%v err=%v", row, err)
	}
}

func TestModerationBlocksUser(t *testing.T) {
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"verdict\":\"violation\",\"categories\":[\"spam\"],\"reasoning\":\"automated spam account\",\"recommended_action\":\"block_user\"}"}}]}`))
	}))
	defer router.Close()

	app, users, posts := moderationTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := users.Insert(db, map[string]any{"id": "spammer", "blocked": false}); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	if _, err := db.SaveModerator(Moderator{
		Name:             "Spam accounts",
		Enabled:          true,
		Table:            "posts",
		Events:           []string{"insert"},
		ContentFields:    []string{"body"},
		UserField:        "userId",
		UserTable:        "users",
		UserBlockedField: "blocked",
		Model:            "test/model",
		AllowedActions:   []string{"review", "block_user"},
	}); err != nil {
		t.Fatalf("save moderator: %v", err)
	}
	if _, err := posts.Insert(db, map[string]any{"id": "spam", "userId": "spammer", "body": "buy now"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	waitForModerationRun(t, db, "spam", "actioned")
	user, err := db.Table("users").Get("spammer")
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if user == nil || !truthy(user["blocked"]) {
		t.Fatalf("user was not blocked: %v", user)
	}
}
