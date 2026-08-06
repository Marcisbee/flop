package flop

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func workflowTestApp(t *testing.T, endpoint string) (*App, *Table[map[string]any], *Table[map[string]any], *Table[map[string]any]) {
	t.Helper()
	app := New(Config{
		DataDir:  t.TempDir(),
		SyncMode: "normal",
		Workflow: &WorkflowConfig{
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
		s.String("body").Required().FullText()
	})
	reports := Define(app, "reports", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("postId").Required()
		s.String("reason").Required()
	})
	return app, users, posts, reports
}

func workflowResponse(result string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": result}}},
		})
	})
}

func TestOpenRouterDataCollectionPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy string
		want   string
	}{
		{name: "defaults to deny", want: "deny"},
		{name: "allows provider collection when selected", policy: "allow", want: "allow"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Provider map[string]any `json:"provider"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode OpenRouter request: %v", err)
				}
				got, _ = body.Provider["data_collection"].(string)
				workflowResponse(`{"action":"approve","reasoning":"ok"}`).ServeHTTP(w, r)
			}))
			defer server.Close()

			service := &workflowService{
				config: WorkflowConfig{OpenRouterAPIKey: "test-key", OpenRouterURL: server.URL},
				client: server.Client(),
				ctx:    context.Background(),
			}
			_, err := service.askOpenRouter(Workflow{
				AI: WorkflowAIStep{
					Model:          "test/model",
					DataCollection: test.policy,
					Prompt:         "review",
				},
				Actions: []WorkflowAction{{Type: "approve"}},
			}, map[string]any{"body": "hello"})
			if err != nil {
				t.Fatalf("ask OpenRouter: %v", err)
			}
			if got != test.want {
				t.Fatalf("data_collection = %q, want %q", got, test.want)
			}
		})
	}
}

func waitForWorkflowRun(t *testing.T, db *Database, status string) WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, _, err := db.WorkflowRuns(status, 100, 0)
		if err != nil {
			t.Fatalf("list workflow runs: %v", err)
		}
		if len(runs) > 0 {
			return runs[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for workflow status %s", status)
	return WorkflowRun{}
}

type noAuthWorkflowAdmin struct {
	provider *EngineAdminProvider
}

func (p noAuthWorkflowAdmin) AdminTables() ([]AdminTable, error) {
	return p.provider.AdminTables()
}

func (p noAuthWorkflowAdmin) AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error) {
	return p.provider.AdminRows(table, limit, offset)
}

func (p noAuthWorkflowAdmin) AdminWorkflows() ([]Workflow, error) {
	return p.provider.AdminWorkflows()
}

func (p noAuthWorkflowAdmin) AdminWorkflowTemplates() []WorkflowTemplate {
	return p.provider.AdminWorkflowTemplates()
}

func (p noAuthWorkflowAdmin) AdminSaveWorkflow(workflow Workflow) (Workflow, error) {
	return p.provider.AdminSaveWorkflow(workflow)
}

func (p noAuthWorkflowAdmin) AdminDeleteWorkflow(id string) error {
	return p.provider.AdminDeleteWorkflow(id)
}

func (p noAuthWorkflowAdmin) AdminWorkflowRuns(status string, limit, offset int) ([]WorkflowRun, int, error) {
	return p.provider.AdminWorkflowRuns(status, limit, offset)
}

func (p noAuthWorkflowAdmin) AdminResolveWorkflowRun(id, action string) (WorkflowRun, error) {
	return p.provider.AdminResolveWorkflowRun(id, action)
}

func (p noAuthWorkflowAdmin) AdminRunWorkflow(id string, input map[string]any) (WorkflowRun, error) {
	return p.provider.AdminRunWorkflow(id, input)
}

func (p noAuthWorkflowAdmin) AdminWorkflowAPIKeyConfigured() bool {
	return p.provider.AdminWorkflowAPIKeyConfigured()
}

func moderationActions() []WorkflowAction {
	return []WorkflowAction{
		{Type: "approve"},
		{Type: "queue_review", RequireApproval: true},
		{Type: "delete", Table: "posts", IDPath: "input.row.postId", RequireApproval: true},
		{Type: "block", Table: "users", IDPath: "input.row.userId", Field: "blocked", RequireApproval: true},
	}
}

func TestWorkflowRowTriggerHoldsUntilApprovedAndStopsAfterThreshold(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"approve","reasoning":"ordinary discussion"}`))
	defer router.Close()
	app, users, posts, _ := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := users.Insert(db, map[string]any{"id": "u1", "blocked": false}); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	workflow, err := db.SaveWorkflow(Workflow{
		Name: "New user posts", Enabled: true, Category: "moderation",
		Trigger: WorkflowTrigger{Type: "row_insert", Table: "posts"},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "review", ResultSchema: defaultWorkflowResultSchema([]string{"approve"})},
		Actions: moderationActions(), HoldUntilComplete: true,
		SubjectPath: "input.row.userId", NewUserClearedLimit: 1,
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if workflow.ID == "" {
		t.Fatal("workflow ID was not generated")
	}

	if _, err := posts.Insert(db, map[string]any{"id": "p1", "userId": "u1", "body": "hello"}); err != nil {
		t.Fatalf("insert post: %v", err)
	}
	publicPosts := db.trackedAccessor(nil, nil).Table("posts")
	if row, err := publicPosts.Get("p1"); err != nil || row != nil {
		t.Fatalf("pending row should be hidden, row=%v err=%v", row, err)
	}
	run := waitForWorkflowRun(t, db, "completed")
	if run.Action != "approve" || run.Reasoning == "" {
		t.Fatalf("unexpected run: %+v", run)
	}
	if run.Table != "posts" || run.RowID != "p1" {
		t.Fatalf("workflow source = %s/%s, want posts/p1", run.Table, run.RowID)
	}
	inputRow, ok := run.Input["row"].(map[string]any)
	if !ok || inputRow["id"] != "p1" || inputRow["body"] != "hello" {
		t.Fatalf("workflow source snapshot = %#v", run.Input["row"])
	}
	if row, err := publicPosts.Get("p1"); err != nil || row == nil {
		t.Fatalf("completed row should be visible, row=%v err=%v", row, err)
	}

	if _, err := posts.Insert(db, map[string]any{"id": "p2", "userId": "u1", "body": "second"}); err != nil {
		t.Fatalf("insert second post: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	_, total, err := db.WorkflowRuns("", 100, 0)
	if err != nil || total != 1 {
		t.Fatalf("new-user threshold should stop after one completed run, total=%d err=%v", total, err)
	}
}

func TestReportedContentWorkflowRequiresApprovalBeforeDelete(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"delete","reasoning":"valid report"}`))
	defer router.Close()
	app, _, posts, reports := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := posts.Insert(db, map[string]any{"id": "target", "userId": "u1", "body": "reported post"}); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	workflow, err := db.SaveWorkflow(Workflow{
		Name: "Reports", Enabled: true, Category: "moderation",
		Trigger: WorkflowTrigger{Type: "report", Table: "reports", Events: []string{"insert"}},
		Lookups: []WorkflowLookup{{Name: "target", Type: "get", Table: "posts", InputPath: "input.row.postId"}},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "review report", ResultSchema: defaultWorkflowResultSchema([]string{"delete"})},
		Actions: []WorkflowAction{{Type: "delete", Table: "posts", IDPath: "input.row.postId", RequireApproval: true}},
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := reports.Insert(db, map[string]any{"id": "r1", "postId": "target", "reason": "illegal"}); err != nil {
		t.Fatalf("insert report: %v", err)
	}
	run := waitForWorkflowRun(t, db, "awaiting_approval")
	if run.LookupResults["target"] == nil || !run.ApprovalRequired {
		t.Fatalf("run lacks lookup or approval state: %+v", run)
	}
	if run.ActionEffect == nil || run.ActionEffect.Type != "delete" || run.ActionEffect.Table != "posts" || run.ActionEffect.ID != "target" {
		t.Fatalf("run action effect = %+v, want delete posts/target", run.ActionEffect)
	}
	if row, _ := db.Table("posts").Get("target"); row == nil {
		t.Fatal("sensitive action ran before approval")
	}
	workflow.Actions[0].IDPath = "input.row.id"
	if _, err := db.SaveWorkflow(workflow); err != nil {
		t.Fatalf("change workflow after decision: %v", err)
	}
	resolved, err := db.ResolveWorkflowRun(run.ID, "approve")
	if err != nil {
		t.Fatalf("approve run: %v", err)
	}
	if resolved.Status != "completed" {
		t.Fatalf("resolved status = %q", resolved.Status)
	}
	if row, _ := db.Table("posts").Get("target"); row != nil {
		t.Fatal("approved delete did not remove target")
	}
}

func TestWorkflowRunKeepsDecisionWhenActionFails(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"delete","reasoning":"remove the matching post"}`))
	defer router.Close()
	app, _, _, _ := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	workflow, err := db.SaveWorkflow(Workflow{
		Name:    "Manual delete",
		Enabled: true,
		Trigger: WorkflowTrigger{Type: "manual"},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "review", ResultSchema: defaultWorkflowResultSchema([]string{"delete"})},
		Actions: []WorkflowAction{{Type: "delete", Table: "posts", IDPath: "input.postId"}},
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := db.RunWorkflow(workflow.ID, map[string]any{}); err != nil {
		t.Fatalf("run workflow: %v", err)
	}
	run := waitForWorkflowRun(t, db, "error")
	if run.Reasoning != "remove the matching post" || run.RecommendedAction != "delete" {
		t.Fatalf("run lost model decision: %+v", run)
	}
	if run.ActionEffect == nil || run.ActionEffect.Type != "delete" || run.ActionEffect.Table != "posts" {
		t.Fatalf("run action effect = %+v, want delete posts", run.ActionEffect)
	}
}

func TestReportedContentWorkflowArchivesTargetAndDependents(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"archive","reasoning":"valid report"}`))
	defer router.Close()
	app, _, posts, reports := workflowTestApp(t, router.URL)
	Define(app, "comments", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Ref("postId", posts, "id").Required().CascadeArchive()
		s.String("body").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := posts.Insert(db, map[string]any{"id": "target", "userId": "u1", "body": "reported post"}); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if _, err := db.Table("comments").Insert(map[string]any{"id": "c1", "postId": "target", "body": "dependent"}); err != nil {
		t.Fatalf("insert dependent: %v", err)
	}
	_, err = db.SaveWorkflow(Workflow{
		Name: "Archive reports", Enabled: true, Category: "moderation",
		Trigger: WorkflowTrigger{Type: "report", Table: "reports", Events: []string{"insert"}},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "review report", ResultSchema: defaultWorkflowResultSchema([]string{"archive"})},
		Actions: []WorkflowAction{{Type: "archive", Table: "posts", IDPath: "input.row.postId", RequireApproval: true}},
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := reports.Insert(db, map[string]any{"id": "r1", "postId": "target", "reason": "illegal"}); err != nil {
		t.Fatalf("insert report: %v", err)
	}
	run := waitForWorkflowRun(t, db, "awaiting_approval")
	if _, err := db.ResolveWorkflowRun(run.ID, "approve"); err != nil {
		t.Fatalf("approve run: %v", err)
	}
	if row, _ := db.Table("posts").Get("target"); row != nil {
		t.Fatal("approved archive left target live")
	}
	if row, _ := db.Table("comments").Get("c1"); row != nil {
		t.Fatal("approved archive left dependent live")
	}
	for _, table := range []string{"posts", "comments"} {
		records, _, err := db.db.GetTable(table).ScanArchived(10, 0)
		if err != nil || len(records) != 1 {
			t.Fatalf("%s archived records=%d err=%v", table, len(records), err)
		}
	}
}

func TestDiscordReconciliationSearchesGamesAndCreatesApprovedAlias(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"propose_alias","game_id":"g1","alias":"Portal 2 Discord","reasoning":"strong candidate"}`))
	defer router.Close()
	app, _, _, _ := workflowTestApp(t, router.URL)
	games := Define(app, "games", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("name").Required().FullText()
	})
	Define(app, "game_aliases", func(s *SchemaBuilder) {
		s.String("id").Primary().Autogen("[a-z0-9]{12}")
		s.String("gameId").Required()
		s.String("alias").Required()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := games.Insert(db, map[string]any{"id": "g1", "name": "Portal 2"}); err != nil {
		t.Fatalf("insert game: %v", err)
	}
	workflow, err := db.SaveWorkflow(Workflow{
		Name: "Discord reconciliation", Enabled: true,
		Trigger: WorkflowTrigger{Type: "discord", Event: "activity_mismatch"},
		Lookups: []WorkflowLookup{{Name: "candidates", Type: "search", Table: "games", InputPath: "input.activity.name", SearchFields: []string{"name"}}},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "reconcile", ResultSchema: map[string]any{"type": "object"}},
		Actions: []WorkflowAction{{
			Type: "propose_alias", Table: "game_aliases", RequireApproval: true,
			Data: map[string]string{"gameId": "result.game_id", "alias": "result.alias"},
		}},
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if err := db.DispatchWorkflowEvent("discord", "activity_mismatch", map[string]any{
		"activity": map[string]any{"name": "Portal 2 Discord"},
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	run := waitForWorkflowRun(t, db, "awaiting_approval")
	if run.WorkflowID != workflow.ID || run.LookupResults["candidates"] == nil {
		t.Fatalf("unexpected reconciliation run: %+v", run)
	}
	if run.ActionEffect == nil || run.ActionEffect.Type != "propose_alias" || run.ActionEffect.Table != "game_aliases" || run.ActionEffect.Data["gameId"] != "g1" || run.ActionEffect.Data["alias"] != "Portal 2 Discord" {
		t.Fatalf("run action effect = %+v, want resolved alias data", run.ActionEffect)
	}
	if _, err := db.ResolveWorkflowRun(run.ID, "approve"); err != nil {
		t.Fatalf("approve alias: %v", err)
	}
	rawRows, scanErr := db.Table("game_aliases").Scan(10, 0)
	if scanErr != nil || len(rawRows) != 1 || rawRows[0]["alias"] != "Portal 2 Discord" {
		t.Fatalf("aliases=%v err=%v", rawRows, scanErr)
	}
}

func TestWorkflowRetriesAndKeepsErrors(t *testing.T) {
	var attempts atomic.Int32
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, `{"error":{"message":"temporary"}}`, http.StatusBadGateway)
			return
		}
		workflowResponse(`{"action":"approve","reasoning":"recovered"}`).ServeHTTP(w, r)
	}))
	defer router.Close()
	app, _, posts, _ := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.SaveWorkflow(Workflow{
		Name: "Retry", Enabled: true, Trigger: WorkflowTrigger{Type: "row_insert", Table: "posts"},
		AI:      WorkflowAIStep{Model: "test/model", Prompt: "review"},
		Actions: []WorkflowAction{{Type: "approve"}}, MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := posts.Insert(db, map[string]any{"id": "retry", "userId": "u1", "body": "hello"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	run := waitForWorkflowRun(t, db, "completed")
	if run.Attempt != 2 || len(run.Errors) != 1 {
		t.Fatalf("retry history missing: %+v", run)
	}
}

func TestManualWorkflowConditions(t *testing.T) {
	var requests atomic.Int32
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		workflowResponse(`{"action":"approve","reasoning":"matched"}`).ServeHTTP(w, r)
	}))
	defer router.Close()
	app, _, _, _ := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	workflow, err := db.SaveWorkflow(Workflow{
		Name: "Manual condition", Enabled: true, Trigger: WorkflowTrigger{Type: "manual"},
		Conditions: []WorkflowCondition{{Field: "input.kind", Operator: "equals", Value: "safe"}},
		AI:         WorkflowAIStep{Model: "test/model", Prompt: "classify"},
		Actions:    []WorkflowAction{{Type: "approve"}},
	})
	if err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := db.RunWorkflow(workflow.ID, map[string]any{"kind": "skip"}); err == nil {
		t.Fatal("non-matching manual run should not be queued")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("non-matching condition made %d AI requests", got)
	}
	if _, err := db.RunWorkflow(workflow.ID, map[string]any{"kind": "safe"}); err != nil {
		t.Fatalf("run matching workflow: %v", err)
	}
	waitForWorkflowRun(t, db, "completed")
	if got := requests.Load(); got != 1 {
		t.Fatalf("matching condition made %d AI requests", got)
	}
}

func TestWorkflowAdminConfigAndRunsRoutes(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"approve","reasoning":"ok"}`))
	defer router.Close()
	app, _, _, _ := workflowTestApp(t, router.URL)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	MountDefaultAdmin(mux, noAuthWorkflowAdmin{provider: &EngineAdminProvider{DB: db}})
	for _, path := range []string{"/_/api/workflows/config", "/_/api/workflows/runs"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, resp.Code, resp.Body.String())
		}
		if path == "/_/api/workflows/config" {
			var body struct {
				Templates []WorkflowTemplate `json:"templates"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode config: %v", err)
			}
			if len(body.Templates) != 3 {
				t.Fatalf("workflow templates=%d, want 3", len(body.Templates))
			}
		}
	}
}

func TestWorkflowAdminUsesApplicationTemplates(t *testing.T) {
	router := httptest.NewServer(workflowResponse(`{"action":"approve","reasoning":"ok"}`))
	defer router.Close()
	app, _, _, _ := workflowTestApp(t, router.URL)
	app.config.Workflow.Templates = []WorkflowTemplate{{
		ID:          "strike-post-moderator",
		Name:        "Strike post moderator",
		Description: "Review Strike posts.",
		Workflow: Workflow{
			Name:    "Strike post moderator",
			Enabled: true,
			Trigger: WorkflowTrigger{Type: "row_insert", Table: "posts"},
			AI:      WorkflowAIStep{Model: "test/model", Prompt: "review"},
			Actions: []WorkflowAction{{Type: "archive", Table: "posts", IDPath: "input.row.id"}},
		},
	}}
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	MountDefaultAdmin(mux, noAuthWorkflowAdmin{provider: &EngineAdminProvider{DB: db}})
	req := httptest.NewRequest(http.MethodGet, "/_/api/workflows/config", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("config status=%d body=%s", resp.Code, resp.Body.String())
	}
	var body struct {
		Templates []WorkflowTemplate `json:"templates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if len(body.Templates) != 1 || body.Templates[0].ID != "strike-post-moderator" {
		t.Fatalf("application templates not exposed: %+v", body.Templates)
	}
}
