package flop

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/schema"
)

const (
	systemWorkflowTableName    = "_workflows"
	systemWorkflowRunTableName = "_workflow_runs"
	defaultOpenRouterURL       = "https://openrouter.ai/api/v1/chat/completions"
)

// WorkflowConfig configures the OpenRouter connection and workflow workers.
// The API key defaults to OPENROUTER_API_KEY when it is not set.
type WorkflowConfig struct {
	OpenRouterAPIKey string
	OpenRouterURL    string
	HTTPClient       *http.Client
	Workers          int
	// Templates replaces the built-in admin workflow templates when non-nil.
	// Use an empty slice to expose no templates.
	Templates []WorkflowTemplate
}

// WorkflowTrigger selects events that start a workflow. Row triggers use Table.
// External triggers such as report and discord may additionally use Event.
type WorkflowTrigger struct {
	Type   string   `json:"type"`
	Table  string   `json:"table,omitempty"`
	Events []string `json:"events,omitempty"`
	Event  string   `json:"event,omitempty"`
}

// WorkflowCondition filters events before a run is created.
type WorkflowCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    any    `json:"value,omitempty"`
}

// WorkflowLookup enriches a run with indexed application data.
type WorkflowLookup struct {
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Table        string   `json:"table"`
	InputPath    string   `json:"inputPath"`
	Field        string   `json:"field,omitempty"`
	SearchFields []string `json:"searchFields,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

// WorkflowAIStep describes a structured OpenRouter call.
type WorkflowAIStep struct {
	Model          string         `json:"model"`
	Provider       string         `json:"provider,omitempty"`
	DataCollection string         `json:"dataCollection,omitempty"`
	Prompt         string         `json:"prompt"`
	ResultSchema   map[string]any `json:"resultSchema,omitempty"`
}

// WorkflowAction maps an AI action to a database operation.
type WorkflowAction struct {
	Type            string            `json:"type"`
	RequireApproval bool              `json:"requireApproval,omitempty"`
	Table           string            `json:"table,omitempty"`
	IDPath          string            `json:"idPath,omitempty"`
	Field           string            `json:"field,omitempty"`
	Value           any               `json:"value,omitempty"`
	Data            map[string]string `json:"data,omitempty"`
}

// Workflow is a durable, configurable automation definition.
type Workflow struct {
	ID                  string              `json:"id,omitempty"`
	Name                string              `json:"name"`
	Category            string              `json:"category,omitempty"`
	Template            string              `json:"template,omitempty"`
	Enabled             bool                `json:"enabled"`
	Trigger             WorkflowTrigger     `json:"trigger"`
	Conditions          []WorkflowCondition `json:"conditions,omitempty"`
	Lookups             []WorkflowLookup    `json:"lookups,omitempty"`
	AI                  WorkflowAIStep      `json:"ai"`
	Actions             []WorkflowAction    `json:"actions"`
	HoldUntilComplete   bool                `json:"holdUntilComplete,omitempty"`
	SubjectPath         string              `json:"subjectPath,omitempty"`
	NewUserClearedLimit int                 `json:"newUserClearedLimit,omitempty"`
	MaxRetries          int                 `json:"maxRetries,omitempty"`
	CreatedAtUnixMilli  int64               `json:"createdAtUnixMilli,omitempty"`
	UpdatedAtUnixMilli  int64               `json:"updatedAtUnixMilli,omitempty"`
}

// WorkflowRun is the durable execution and approval history for a workflow.
type WorkflowRun struct {
	ID                  string                `json:"id"`
	WorkflowID          string                `json:"workflowId"`
	WorkflowName        string                `json:"workflowName"`
	Trigger             string                `json:"trigger"`
	Event               string                `json:"event"`
	Table               string                `json:"table,omitempty"`
	RowID               string                `json:"rowId,omitempty"`
	SubjectID           string                `json:"subjectId,omitempty"`
	Status              string                `json:"status"`
	Input               map[string]any        `json:"input,omitempty"`
	LookupResults       map[string]any        `json:"lookupResults,omitempty"`
	Result              map[string]any        `json:"result,omitempty"`
	Reasoning           string                `json:"reasoning,omitempty"`
	RecommendedAction   string                `json:"recommendedAction,omitempty"`
	Action              string                `json:"action,omitempty"`
	ActionEffect        *WorkflowActionEffect `json:"actionEffect,omitempty"`
	ApprovalRequired    bool                  `json:"approvalRequired,omitempty"`
	Attempt             int                   `json:"attempt"`
	MaxRetries          int                   `json:"maxRetries"`
	Errors              []string              `json:"errors,omitempty"`
	Error               string                `json:"error,omitempty"`
	Model               string                `json:"model,omitempty"`
	Provider            string                `json:"provider,omitempty"`
	HoldUntilComplete   bool                  `json:"holdUntilComplete,omitempty"`
	CreatedAtUnixMilli  int64                 `json:"createdAtUnixMilli"`
	StartedAtUnixMilli  int64                 `json:"startedAtUnixMilli,omitempty"`
	FinishedAtUnixMilli int64                 `json:"finishedAtUnixMilli,omitempty"`
}

// WorkflowActionEffect is the resolved, durable effect of a workflow action.
// It records concrete targets and values so approval history remains accurate
// even when the workflow definition changes after a run.
type WorkflowActionEffect struct {
	Type  string         `json:"type"`
	Table string         `json:"table,omitempty"`
	ID    string         `json:"id,omitempty"`
	Field string         `json:"field,omitempty"`
	Value any            `json:"value,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

type workflowService struct {
	db      *Database
	config  WorkflowConfig
	client  *http.Client
	wake    chan struct{}
	start   sync.Once
	claimMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
}

func newWorkflowService(db *Database, config *WorkflowConfig) *workflowService {
	cfg := WorkflowConfig{}
	if config != nil {
		cfg = *config
	}
	if strings.TrimSpace(cfg.OpenRouterAPIKey) == "" {
		cfg.OpenRouterAPIKey = strings.TrimSpace(os.Getenv("OPENROUTER_API_KEY"))
	}
	if strings.TrimSpace(cfg.OpenRouterURL) == "" {
		cfg.OpenRouterURL = defaultOpenRouterURL
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.Workers > 16 {
		cfg.Workers = 16
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 90 * time.Second}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &workflowService{
		db:     db,
		config: cfg,
		client: client,
		wake:   make(chan struct{}, 1),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *workflowService) startWorkers() {
	if s == nil || s.db == nil || s.db.backgroundStop == nil {
		return
	}
	s.start.Do(func() {
		s.recoverInterruptedRuns()
		stop := s.db.backgroundStop
		for i := 0; i < s.config.Workers; i++ {
			s.db.backgroundWG.Add(1)
			go func(stop <-chan struct{}) {
				defer s.db.backgroundWG.Done()
				s.worker(stop)
			}(stop)
		}
		s.signal()
	})
}

func (s *workflowService) stopWorkers() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func (s *workflowService) signal() {
	if s == nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *workflowService) worker(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-s.wake:
		case <-ticker.C:
		}
		for {
			select {
			case <-stop:
				return
			default:
			}
			run, ok := s.claimPending()
			if !ok {
				break
			}
			s.execute(run)
		}
	}
}

func (s *workflowService) claimPending() (WorkflowRun, bool) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	table := s.db.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return WorkflowRun{}, false
	}
	ptrs := table.FindAllByIndex([]string{"status"}, "pending")
	for _, ptr := range ptrs {
		row, err := table.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		run := workflowRunFromRow(row)
		now := time.Now().UnixMilli()
		updated, err := table.Update(run.ID, map[string]any{
			"status":    "running",
			"startedAt": now,
			"attempt":   run.Attempt + 1,
			"error":     "",
		}, nil)
		if err != nil {
			continue
		}
		return workflowRunFromRow(updated), true
	}
	return WorkflowRun{}, false
}

func (s *workflowService) recoverInterruptedRuns() {
	table := s.db.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return
	}
	for _, ptr := range table.FindAllByIndex([]string{"status"}, "running") {
		row, err := table.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		_, _ = table.Update(toString(row["id"]), map[string]any{
			"status":    "pending",
			"startedAt": nil,
			"error":     "Previous workflow attempt was interrupted and queued again.",
		}, nil)
	}
}

func (s *workflowService) execute(run WorkflowRun) {
	workflow, err := s.getWorkflow(run.WorkflowID)
	if err != nil {
		s.failRun(run.ID, err)
		return
	}
	if run.Table != "" && run.RowID != "" {
		source := s.db.db.GetTable(run.Table)
		if source == nil {
			s.failRun(run.ID, fmt.Errorf("source table %q no longer exists", run.Table))
			return
		}
		row, getErr := source.Get(run.RowID)
		if getErr != nil {
			s.failRun(run.ID, getErr)
			return
		}
		if row == nil {
			s.finishRun(run.ID, map[string]any{
				"status":    "cancelled",
				"reasoning": "The source row was deleted before workflow completed.",
			})
			return
		}
		run.Input["row"] = row
	}
	lookups, err := s.runLookups(workflow, run.Input)
	if err != nil {
		s.failRun(run.ID, err)
		return
	}
	payload := map[string]any{"input": run.Input, "lookups": lookups}
	result, err := s.askOpenRouter(workflow, payload)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.requeueInterruptedRun(run.ID)
			return
		}
		s.failRun(run.ID, err)
		return
	}
	actionName := resultAction(result)
	reasoning := toString(result["reasoning"])
	action, ok := workflowAction(workflow, actionName)
	if !ok {
		s.failRun(run.ID, fmt.Errorf("AI returned unconfigured workflow action %q", actionName))
		return
	}
	run.LookupResults = lookups
	run.Result = result
	run.RecommendedAction = actionName
	actionEffect := resolveWorkflowActionEffect(run, action)
	decisionFields := map[string]any{
		"lookupResults":     lookups,
		"result":            result,
		"reasoning":         reasoning,
		"recommendedAction": actionName,
		"action":            action.Type,
		"actionEffect":      jsonValue(actionEffect),
	}
	if action.RequireApproval || action.Type == "queue_review" || action.Type == "propose_alias" {
		decisionFields["status"] = "awaiting_approval"
		decisionFields["approvalRequired"] = true
		s.finishRun(run.ID, decisionFields)
		return
	}
	s.updateRun(run.ID, decisionFields)
	if err := s.applyAction(run, action); err != nil {
		s.failRun(run.ID, err)
		return
	}
	s.finishRun(run.ID, map[string]any{
		"status": "completed",
	})
}

func (s *workflowService) requeueInterruptedRun(id string) {
	table := s.db.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return
	}
	_, _ = table.Update(id, map[string]any{
		"status":    "pending",
		"startedAt": nil,
		"error":     "Workflow was interrupted and will retry after restart.",
	}, nil)
}

func (s *workflowService) askOpenRouter(workflow Workflow, content map[string]any) (map[string]any, error) {
	if strings.TrimSpace(s.config.OpenRouterAPIKey) == "" {
		return nil, errors.New("OpenRouter API key is not configured")
	}
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	schema := workflow.AI.ResultSchema
	if len(schema) == 0 {
		actions := make([]string, 0, len(workflow.Actions))
		for _, action := range workflow.Actions {
			actions = append(actions, action.Type)
		}
		schema = defaultWorkflowResultSchema(actions)
	}
	dataCollection := "deny"
	if workflow.AI.DataCollection == "allow" {
		dataCollection = "allow"
	}
	userContent := any(string(contentJSON))
	imageRefs := workflowImageRefs(content)
	if len(imageRefs) > 0 && s.openRouterModelSupportsImages(workflow.AI.Model) {
		parts := []any{map[string]any{"type": "text", "text": string(contentJSON)}}
		for _, ref := range imageRefs {
			dataURL, ok := s.workflowImageDataURL(ref)
			if !ok {
				continue
			}
			parts = append(parts, map[string]any{
				"type":      "image_url",
				"image_url": map[string]any{"url": dataURL},
			})
		}
		if len(parts) > 1 {
			userContent = parts
		}
	}
	requestBody := map[string]any{
		"model": workflow.AI.Model,
		"messages": []map[string]any{
			{"role": "system", "content": workflow.AI.Prompt},
			{"role": "user", "content": userContent},
		},
		"temperature": 0,
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "workflow_decision",
				"strict": true,
				"schema": schema,
			},
		},
		"provider": map[string]any{
			"require_parameters": true,
			"data_collection":    dataCollection,
		},
	}
	if strings.TrimSpace(workflow.AI.Provider) != "" {
		requestBody["provider"] = map[string]any{
			"order":              []string{workflow.AI.Provider},
			"allow_fallbacks":    false,
			"require_parameters": true,
			"data_collection":    dataCollection,
		}
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, s.config.OpenRouterURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.config.OpenRouterAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Title", "Flop Workflow")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OpenRouter request: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error,omitempty"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("OpenRouter returned invalid JSON: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(resp.Status)
		if response.Error != nil && strings.TrimSpace(response.Error.Message) != "" {
			message = response.Error.Message
		}
		return nil, fmt.Errorf("OpenRouter: %s", message)
	}
	if response.Error != nil {
		return nil, fmt.Errorf("OpenRouter: %s", response.Error.Message)
	}
	if len(response.Choices) == 0 {
		return nil, errors.New("OpenRouter returned no workflow result")
	}
	if response.Choices[0].Error != nil {
		return nil, fmt.Errorf("OpenRouter: %s", response.Choices[0].Error.Message)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(response.Choices[0].Message.Content), &result); err != nil {
		return nil, fmt.Errorf("OpenRouter returned an invalid structured result: %w", err)
	}
	return result, nil
}

func workflowImageRefs(content any) []schema.FileRef {
	seen := map[string]bool{}
	refs := make([]schema.FileRef, 0)
	var collect func(any)
	collect = func(value any) {
		if ref, ok := workflowImageRefFromAny(value); ok {
			if !seen[ref.Path] {
				seen[ref.Path] = true
				refs = append(refs, ref)
			}
			return
		}
		switch typed := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				collect(typed[key])
			}
		case []any:
			for _, item := range typed {
				collect(item)
			}
		case []map[string]any:
			for _, item := range typed {
				collect(item)
			}
		}
	}
	collect(content)
	return refs
}

func workflowImageRefFromAny(value any) (schema.FileRef, bool) {
	var ref schema.FileRef
	switch typed := value.(type) {
	case map[string]any:
		path, _ := typed["path"].(string)
		mime, _ := typed["mime"].(string)
		name, _ := typed["name"].(string)
		ref = schema.FileRef{Path: path, Mime: mime, Name: name}
	case schema.FileRef:
		ref = typed
	case *schema.FileRef:
		if typed == nil {
			return schema.FileRef{}, false
		}
		ref = *typed
	case FileRef:
		ref = schema.FileRef{Path: typed.Path, Mime: typed.Mime, Name: typed.Name}
	case *FileRef:
		if typed == nil {
			return schema.FileRef{}, false
		}
		ref = schema.FileRef{Path: typed.Path, Mime: typed.Mime, Name: typed.Name}
	default:
		return schema.FileRef{}, false
	}
	ref.Path = filepath.ToSlash(strings.TrimSpace(ref.Path))
	if !strings.HasPrefix(ref.Path, "_files/") || filepath.ToSlash(filepath.Clean(ref.Path)) != ref.Path {
		return schema.FileRef{}, false
	}
	ref.Mime = strings.ToLower(strings.TrimSpace(ref.Mime))
	if !strings.HasPrefix(ref.Mime, "image/") {
		return schema.FileRef{}, false
	}
	return ref, true
}

func (s *workflowService) openRouterModelSupportsImages(model string) bool {
	endpoint, ok := openRouterModelEndpoint(s.config.OpenRouterURL, model)
	if !ok {
		return false
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+s.config.OpenRouterAPIKey)
	req.Header.Set("X-Title", "Flop Workflow")
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	var response struct {
		Data struct {
			Architecture struct {
				InputModalities []string `json:"input_modalities"`
			} `json:"architecture"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&response); err != nil {
		return false
	}
	for _, modality := range response.Data.Architecture.InputModalities {
		if modality == "image" {
			return true
		}
	}
	return false
}

func openRouterModelEndpoint(chatEndpoint, model string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(chatEndpoint))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	segments := strings.Split(strings.Trim(strings.TrimSpace(model), "/"), "/")
	if len(segments) < 2 {
		return "", false
	}
	for _, segment := range segments {
		if segment == "" {
			return "", false
		}
	}
	basePath := strings.TrimSuffix(parsed.Path, "/")
	basePath = strings.TrimSuffix(basePath, "/chat/completions")
	parsed.Path = strings.TrimSuffix(basePath, "/") + "/model/" + strings.Join(segments, "/")
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), true
}

func (s *workflowService) workflowImageDataURL(ref schema.FileRef) (string, bool) {
	if s == nil || s.db == nil || s.db.db == nil {
		return "", false
	}
	dataDir := s.db.db.GetDataDir()
	fullPath := filepath.Join(dataDir, filepath.FromSlash(ref.Path))
	relative, err := filepath.Rel(dataDir, fullPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", false
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", false
	}
	return "data:" + ref.Mime + ";base64," + base64.StdEncoding.EncodeToString(data), true
}

func (s *workflowService) enqueueMutation(tableName, event string, row map[string]any) error {
	if s == nil || row == nil || strings.HasPrefix(tableName, "_") {
		return nil
	}
	source := s.db.db.GetTable(tableName)
	if source == nil || len(source.GetDef().CompiledSchema.Fields) == 0 {
		return nil
	}
	rowID := toString(row[source.GetDef().CompiledSchema.Fields[0].Name])
	if rowID == "" {
		return fmt.Errorf("workflow: row in %s has no primary key", tableName)
	}
	trigger := "row_" + event
	return s.dispatch(trigger, event, tableName, rowID, map[string]any{
		"event": event, "table": tableName, "rowId": rowID, "row": row,
	}, "")
}

func (s *workflowService) dispatch(trigger, event, tableName, rowID string, input map[string]any, onlyWorkflow string) error {
	workflows, err := s.listWorkflows()
	if err != nil {
		return err
	}
	runTable := s.db.db.GetTable(systemWorkflowRunTableName)
	if runTable == nil {
		return errors.New("workflow run table unavailable")
	}
	created := false
	for _, workflow := range workflows {
		if !workflow.Enabled || (onlyWorkflow != "" && workflow.ID != onlyWorkflow) {
			continue
		}
		if !workflowMatches(workflow, trigger, event, tableName) || !conditionsMatch(workflow.Conditions, input) {
			continue
		}
		subjectID := toString(valueAtPath(map[string]any{"input": input}, workflow.SubjectPath))
		if workflow.NewUserClearedLimit > 0 && subjectID != "" && !s.shouldRunForSubject(workflow, subjectID) {
			continue
		}
		_, err := runTable.Insert(map[string]any{
			"workflowId":        workflow.ID,
			"workflowName":      workflow.Name,
			"trigger":           trigger,
			"event":             event,
			"table":             tableName,
			"rowId":             rowID,
			"rowKey":            workflowRowKey(tableName, rowID),
			"activeRowKey":      workflowActiveRowKey(workflow, tableName, rowID),
			"subjectId":         subjectID,
			"status":            "pending",
			"input":             input,
			"approvalRequired":  false,
			"attempt":           0,
			"maxRetries":        workflow.MaxRetries,
			"errors":            []any{},
			"visibilityTable":   workflowVisibilityTable(workflow),
			"model":             workflow.AI.Model,
			"provider":          workflow.AI.Provider,
			"holdUntilComplete": workflow.HoldUntilComplete,
		}, nil)
		if err != nil {
			return err
		}
		created = true
	}
	if created {
		s.signal()
	}
	return nil
}

func (s *workflowService) shouldRunForSubject(workflow Workflow, subjectID string) bool {
	runs := s.db.db.GetTable(systemWorkflowRunTableName)
	if runs == nil {
		return true
	}
	cleared := 0
	for _, ptr := range runs.FindAllByIndex([]string{"subjectId"}, subjectID) {
		row, err := runs.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		if toString(row["workflowId"]) == workflow.ID && toString(row["status"]) == "completed" {
			cleared++
			if cleared >= workflow.NewUserClearedLimit {
				return false
			}
		}
	}
	return true
}

func (s *workflowService) visible(tableName, rowID string) bool {
	if s == nil || tableName == "" || rowID == "" {
		return true
	}
	runs := s.db.db.GetTable(systemWorkflowRunTableName)
	if runs == nil {
		return true
	}
	for _, ptr := range runs.FindAllByIndex([]string{"activeRowKey"}, workflowRowKey(tableName, rowID)) {
		row, err := runs.GetByPointer(ptr)
		if err != nil || row == nil || !truthy(row["holdUntilComplete"]) {
			continue
		}
		switch toString(row["status"]) {
		case "pending", "running", "awaiting_approval", "error":
			return false
		}
	}
	return true
}

func (s *workflowService) runLookups(workflow Workflow, input map[string]any) (map[string]any, error) {
	results := make(map[string]any, len(workflow.Lookups))
	scope := map[string]any{"input": input, "lookups": results}
	for _, lookup := range workflow.Lookups {
		table := s.db.Table(lookup.Table)
		if table == nil {
			return nil, fmt.Errorf("workflow lookup table %q not found", lookup.Table)
		}
		value := valueAtPath(scope, lookup.InputPath)
		switch lookup.Type {
		case "get":
			row, err := table.Get(toString(value))
			if err != nil {
				return nil, err
			}
			results[lookup.Name] = row
		case "index":
			rows, err := table.FindByIndex(lookup.Field, value)
			if err != nil {
				return nil, err
			}
			results[lookup.Name] = rows
		case "search":
			limit := lookup.Limit
			if limit <= 0 || limit > 50 {
				limit = 10
			}
			rows, err := table.SearchFullText(lookup.SearchFields, toString(value), limit)
			if err != nil {
				return nil, err
			}
			results[lookup.Name] = rows
		default:
			return nil, fmt.Errorf("unsupported workflow lookup %q", lookup.Type)
		}
	}
	return results, nil
}

func (s *workflowService) applyAction(run WorkflowRun, action WorkflowAction) error {
	return s.applyActionEffect(resolveWorkflowActionEffect(run, action))
}

func resolveWorkflowActionEffect(run WorkflowRun, action WorkflowAction) WorkflowActionEffect {
	scope := map[string]any{"input": run.Input, "lookups": run.LookupResults, "result": run.Result}
	effect := WorkflowActionEffect{
		Type:  action.Type,
		Table: action.Table,
		Field: action.Field,
	}
	switch effect.Type {
	case "delete", "archive", "block":
		effect.ID = strings.TrimSpace(toString(valueAtPath(scope, action.IDPath)))
	}
	if effect.Type == "block" {
		effect.Value = action.Value
		if effect.Value == nil {
			effect.Value = true
		}
	}
	if effect.Type == "create_alias" || effect.Type == "propose_alias" {
		effect.Data = make(map[string]any, len(action.Data))
		for field, path := range action.Data {
			effect.Data[field] = valueAtPath(scope, path)
		}
	}
	return effect
}

func (s *workflowService) applyActionEffect(effect WorkflowActionEffect) error {
	switch effect.Type {
	case "approve", "queue_review":
		return nil
	case "delete":
		table := s.db.Table(effect.Table)
		if table == nil {
			return fmt.Errorf("workflow delete table %q not found", effect.Table)
		}
		if effect.ID == "" {
			return errors.New("workflow delete action resolved an empty ID")
		}
		_, err := table.Delete(effect.ID)
		return err
	case "archive":
		table := s.db.Table(effect.Table)
		if table == nil {
			return fmt.Errorf("workflow archive table %q not found", effect.Table)
		}
		if effect.ID == "" {
			return errors.New("workflow archive action resolved an empty ID")
		}
		_, err := table.Archive(effect.ID)
		return err
	case "block":
		table := s.db.db.GetTable(effect.Table)
		if table == nil {
			return fmt.Errorf("workflow block table %q not found", effect.Table)
		}
		if effect.ID == "" {
			return errors.New("workflow block action resolved an empty ID")
		}
		_, err := table.Update(effect.ID, map[string]any{effect.Field: effect.Value}, nil)
		return err
	case "create_alias", "propose_alias":
		table := s.db.db.GetTable(effect.Table)
		if table == nil {
			return fmt.Errorf("workflow alias table %q not found", effect.Table)
		}
		_, err := table.Insert(effect.Data, nil)
		return err
	default:
		return fmt.Errorf("unsupported workflow action %q", effect.Type)
	}
}

func (s *workflowService) failRun(id string, err error) {
	message := "workflow failed"
	if err != nil {
		message = err.Error()
	}
	table := s.db.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return
	}
	row, getErr := table.Get(id)
	if getErr != nil || row == nil {
		return
	}
	run := workflowRunFromRow(row)
	run.Errors = append(run.Errors, message)
	fields := map[string]any{"error": message, "errors": stringSliceAny(run.Errors)}
	if run.Attempt <= run.MaxRetries {
		fields["status"] = "pending"
		fields["startedAt"] = nil
		_, _ = table.Update(id, fields, nil)
		s.signal()
		return
	}
	fields["status"] = "error"
	s.finishRun(id, fields)
}

func (s *workflowService) finishRun(id string, fields map[string]any) {
	switch toString(fields["status"]) {
	case "completed", "cancelled":
		fields["finishedAt"] = time.Now().UnixMilli()
		fields["visibilityTable"] = ""
		fields["activeRowKey"] = ""
	case "error":
		fields["finishedAt"] = time.Now().UnixMilli()
	}
	s.updateRun(id, fields)
}

func (s *workflowService) updateRun(id string, fields map[string]any) {
	table := s.db.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return
	}
	_, _ = table.Update(id, fields, nil)
}

func (s *workflowService) listWorkflows() ([]Workflow, error) {
	table := s.db.db.GetTable(systemWorkflowTableName)
	if table == nil {
		return nil, errors.New("workflow table unavailable")
	}
	rows, err := table.Scan(table.Count(), 0)
	if err != nil {
		return nil, err
	}
	out := make([]Workflow, 0, len(rows))
	for _, row := range rows {
		out = append(out, workflowFromRow(row))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *workflowService) getWorkflow(id string) (Workflow, error) {
	table := s.db.db.GetTable(systemWorkflowTableName)
	if table == nil {
		return Workflow{}, errors.New("workflow table unavailable")
	}
	row, err := table.Get(id)
	if err != nil {
		return Workflow{}, err
	}
	if row == nil {
		return Workflow{}, fmt.Errorf("workflow %q not found", id)
	}
	return workflowFromRow(row), nil
}

func (s *workflowService) saveWorkflow(m Workflow) (Workflow, error) {
	if err := s.validateWorkflow(m); err != nil {
		return Workflow{}, err
	}
	table := s.db.db.GetTable(systemWorkflowTableName)
	now := time.Now().UnixMilli()
	row := workflowToRow(m)
	row["updatedAt"] = now
	var saved map[string]any
	var err error
	if strings.TrimSpace(m.ID) == "" {
		delete(row, "id")
		row["createdAt"] = now
		saved, err = table.Insert(row, nil)
	} else {
		delete(row, "id")
		saved, err = table.Update(m.ID, row, nil)
	}
	if err != nil {
		return Workflow{}, err
	}
	return workflowFromRow(saved), nil
}

func (s *workflowService) validateWorkflow(workflow Workflow) error {
	workflow.Name = strings.TrimSpace(workflow.Name)
	workflow.Trigger.Type = strings.TrimSpace(workflow.Trigger.Type)
	workflow.AI.Model = strings.TrimSpace(workflow.AI.Model)
	workflow.AI.Prompt = strings.TrimSpace(workflow.AI.Prompt)
	if workflow.Name == "" || workflow.Trigger.Type == "" || workflow.AI.Model == "" || workflow.AI.Prompt == "" {
		return errors.New("workflow name, trigger, AI model, and prompt are required")
	}
	if !containsString([]string{"", "allow", "deny"}, workflow.AI.DataCollection) {
		return errors.New("workflow AI data collection must be allow or deny")
	}
	if !containsString([]string{"row_insert", "row_update", "report", "discord", "manual"}, workflow.Trigger.Type) {
		return fmt.Errorf("unsupported workflow trigger %q", workflow.Trigger.Type)
	}
	if workflow.Trigger.Type == "row_insert" || workflow.Trigger.Type == "row_update" || workflow.Trigger.Type == "report" {
		if workflow.Trigger.Table == "" || strings.HasPrefix(workflow.Trigger.Table, "_") || s.db.db.GetTable(workflow.Trigger.Table) == nil {
			return fmt.Errorf("workflow trigger table %q not found", workflow.Trigger.Table)
		}
	}
	if workflow.MaxRetries < 0 || workflow.MaxRetries > 10 {
		return errors.New("workflow max retries must be between 0 and 10")
	}
	if len(workflow.Actions) == 0 {
		return errors.New("at least one workflow action is required")
	}
	seenLookups := make(map[string]struct{}, len(workflow.Lookups))
	for _, lookup := range workflow.Lookups {
		if lookup.Name == "" || lookup.Table == "" || lookup.InputPath == "" {
			return errors.New("workflow lookup name, table, and input path are required")
		}
		if _, exists := seenLookups[lookup.Name]; exists {
			return fmt.Errorf("duplicate workflow lookup %q", lookup.Name)
		}
		seenLookups[lookup.Name] = struct{}{}
		table := s.db.db.GetTable(lookup.Table)
		if table == nil || strings.HasPrefix(lookup.Table, "_") {
			return fmt.Errorf("workflow lookup table %q not found", lookup.Table)
		}
		switch lookup.Type {
		case "get":
		case "index":
			if lookup.Field == "" || table.GetDef().CompiledSchema.FieldMap[lookup.Field] == nil {
				return fmt.Errorf("workflow lookup field %q not found in table %q", lookup.Field, lookup.Table)
			}
		case "search":
			if len(lookup.SearchFields) == 0 {
				return fmt.Errorf("workflow search lookup %q needs search fields", lookup.Name)
			}
			for _, field := range lookup.SearchFields {
				if table.GetDef().CompiledSchema.FieldMap[field] == nil {
					return fmt.Errorf("workflow search field %q not found in table %q", field, lookup.Table)
				}
			}
		default:
			return fmt.Errorf("unsupported workflow lookup %q", lookup.Type)
		}
	}
	for _, action := range workflow.Actions {
		if !containsString([]string{"approve", "queue_review", "delete", "archive", "block", "create_alias", "propose_alias"}, action.Type) {
			return fmt.Errorf("unsupported workflow action %q", action.Type)
		}
		if containsString([]string{"delete", "archive", "block", "create_alias", "propose_alias"}, action.Type) {
			table := s.db.db.GetTable(action.Table)
			if table == nil || strings.HasPrefix(action.Table, "_") {
				return fmt.Errorf("workflow action table %q not found", action.Table)
			}
		}
		if (action.Type == "delete" || action.Type == "archive" || action.Type == "block") && action.IDPath == "" {
			return fmt.Errorf("workflow action %q needs an ID path", action.Type)
		}
		if action.Type == "block" && action.Field == "" {
			return errors.New("workflow block action needs a field")
		}
		if action.Type == "block" && s.db.db.GetTable(action.Table).GetDef().CompiledSchema.FieldMap[action.Field] == nil {
			return fmt.Errorf("workflow block field %q not found in table %q", action.Field, action.Table)
		}
		if (action.Type == "create_alias" || action.Type == "propose_alias") && len(action.Data) == 0 {
			return fmt.Errorf("workflow action %q needs data mappings", action.Type)
		}
		if action.Type == "create_alias" || action.Type == "propose_alias" {
			table := s.db.db.GetTable(action.Table)
			for field := range action.Data {
				if table.GetDef().CompiledSchema.FieldMap[field] == nil {
					return fmt.Errorf("workflow alias field %q not found in table %q", field, action.Table)
				}
			}
		}
	}
	return nil
}

// Workflows returns all configured workflows.
func (d *Database) Workflows() ([]Workflow, error) {
	if d == nil || d.workflow == nil {
		return nil, errors.New("workflow unavailable")
	}
	return d.workflow.listWorkflows()
}

// SaveWorkflow creates or updates a workflow.
func (d *Database) SaveWorkflow(m Workflow) (Workflow, error) {
	if d == nil || d.workflow == nil {
		return Workflow{}, errors.New("workflow unavailable")
	}
	return d.workflow.saveWorkflow(m)
}

// DeleteWorkflow deletes a workflow configuration. Existing audit records
// remain available.
func (d *Database) DeleteWorkflow(id string) error {
	if d == nil || d.db == nil {
		return errors.New("workflow unavailable")
	}
	table := d.db.GetTable(systemWorkflowTableName)
	if table == nil {
		return errors.New("workflow table unavailable")
	}
	ok, err := table.Delete(strings.TrimSpace(id), nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("workflow %q not found", id)
	}
	return nil
}

// DispatchWorkflowEvent sends a report, Discord, or other named external event
// through all matching workflows.
func (d *Database) DispatchWorkflowEvent(trigger, event string, input map[string]any) error {
	if d == nil || d.workflow == nil {
		return errors.New("workflow unavailable")
	}
	trigger = strings.TrimSpace(trigger)
	if trigger != "report" && trigger != "discord" {
		return fmt.Errorf("unsupported external workflow trigger %q", trigger)
	}
	if input == nil {
		input = map[string]any{}
	}
	return d.workflow.dispatch(trigger, strings.TrimSpace(event), "", "", input, "")
}

// RunWorkflow manually queues one workflow with the supplied input.
func (d *Database) RunWorkflow(id string, input map[string]any) (WorkflowRun, error) {
	if d == nil || d.workflow == nil {
		return WorkflowRun{}, errors.New("workflow unavailable")
	}
	workflow, err := d.workflow.getWorkflow(strings.TrimSpace(id))
	if err != nil {
		return WorkflowRun{}, err
	}
	if workflow.Trigger.Type != "manual" {
		return WorkflowRun{}, fmt.Errorf("workflow %q does not use a manual trigger", id)
	}
	if input == nil {
		input = map[string]any{}
	}
	runs := d.db.GetTable(systemWorkflowRunTableName)
	before := len(runs.FindAllByIndex([]string{"workflowId"}, workflow.ID))
	if err := d.workflow.dispatch("manual", "manual", "", "", input, workflow.ID); err != nil {
		return WorkflowRun{}, err
	}
	ptrs := runs.FindAllByIndex([]string{"workflowId"}, workflow.ID)
	if len(ptrs) <= before {
		return WorkflowRun{}, errors.New("manual workflow conditions did not match")
	}
	row, err := runs.GetByPointer(ptrs[len(ptrs)-1])
	if err != nil {
		return WorkflowRun{}, err
	}
	return workflowRunFromRow(row), nil
}

// WorkflowRuns returns recent audit records, newest first. Status may be
// empty to include every status.
func (d *Database) WorkflowRuns(status string, limit, offset int) ([]WorkflowRun, int, error) {
	if d == nil || d.db == nil {
		return nil, 0, errors.New("workflow unavailable")
	}
	table := d.db.GetTable(systemWorkflowRunTableName)
	if table == nil {
		return nil, 0, errors.New("workflow run table unavailable")
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if status != "" {
		ptrs := table.FindAllByIndex([]string{"status"}, status)
		total := len(ptrs)
		if offset >= total {
			return []WorkflowRun{}, total, nil
		}
		end := offset + limit
		if end > total {
			end = total
		}
		out := make([]WorkflowRun, 0, end-offset)
		for i := total - 1 - offset; i >= total-end; i-- {
			row, err := table.GetByPointer(ptrs[i])
			if err != nil || row == nil {
				continue
			}
			out = append(out, workflowRunFromRow(row))
		}
		return out, total, nil
	}
	total := table.Count()
	if offset >= total {
		return []WorkflowRun{}, total, nil
	}
	pageSize := limit
	if remaining := total - offset; pageSize > remaining {
		pageSize = remaining
	}
	physicalOffset := total - offset - pageSize
	rows, err := table.Scan(pageSize, physicalOffset)
	if err != nil {
		return nil, 0, err
	}
	out := make([]WorkflowRun, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		out = append(out, workflowRunFromRow(rows[i]))
	}
	return out, total, nil
}

// ResolveWorkflowRun approves, rejects, or retries a run.
func (d *Database) ResolveWorkflowRun(id, action string) (WorkflowRun, error) {
	if d == nil || d.workflow == nil {
		return WorkflowRun{}, errors.New("workflow unavailable")
	}
	table := d.db.GetTable(systemWorkflowRunTableName)
	row, err := table.Get(strings.TrimSpace(id))
	if err != nil {
		return WorkflowRun{}, err
	}
	if row == nil {
		return WorkflowRun{}, fmt.Errorf("workflow run %q not found", id)
	}
	run := workflowRunFromRow(row)
	switch action {
	case "retry":
		updated, err := table.Update(run.ID, map[string]any{
			"status":           "pending",
			"error":            "",
			"startedAt":        nil,
			"finishedAt":       nil,
			"action":           "",
			"actionEffect":     nil,
			"approvalRequired": false,
			"visibilityTable":  workflowRunVisibilityTable(run),
			"activeRowKey":     workflowRunActiveRowKey(run),
		}, nil)
		if err != nil {
			return WorkflowRun{}, err
		}
		d.workflow.signal()
		return workflowRunFromRow(updated), nil
	case "reject":
		updated, err := table.Update(run.ID, map[string]any{
			"status":           "completed",
			"action":           "rejected",
			"error":            "",
			"approvalRequired": false,
			"finishedAt":       time.Now().UnixMilli(),
			"visibilityTable":  "",
			"activeRowKey":     "",
		}, nil)
		if err != nil {
			return WorkflowRun{}, err
		}
		return workflowRunFromRow(updated), nil
	case "approve":
		var effect WorkflowActionEffect
		if run.ActionEffect != nil {
			effect = *run.ActionEffect
		} else {
			workflow, err := d.workflow.getWorkflow(run.WorkflowID)
			if err != nil {
				return WorkflowRun{}, err
			}
			configured, ok := workflowAction(workflow, run.RecommendedAction)
			if !ok {
				configured, ok = workflowAction(workflow, run.Action)
			}
			if !ok {
				return WorkflowRun{}, fmt.Errorf("workflow action %q is no longer configured", run.Action)
			}
			effect = resolveWorkflowActionEffect(run, configured)
		}
		if effect.Type == "propose_alias" {
			effect.Type = "create_alias"
		}
		if effect.Type != "queue_review" {
			if err := d.workflow.applyActionEffect(effect); err != nil {
				return WorkflowRun{}, err
			}
		}
		updated, err := table.Update(run.ID, map[string]any{
			"status":           "completed",
			"action":           effect.Type,
			"approvalRequired": false,
			"error":            "",
			"finishedAt":       time.Now().UnixMilli(),
			"visibilityTable":  "",
			"activeRowKey":     "",
		}, nil)
		if err != nil {
			return WorkflowRun{}, err
		}
		return workflowRunFromRow(updated), nil
	default:
		return WorkflowRun{}, fmt.Errorf("unsupported workflow action %q", action)
	}
}

func workflowSystemTableDefs() map[string]*schema.TableDef {
	workflows := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{16}", AutoIDStrategy: "random"},
		{Name: "name", Kind: schema.KindString, Required: true},
		{Name: "category", Kind: schema.KindString},
		{Name: "template", Kind: schema.KindString},
		{Name: "enabled", Kind: schema.KindBoolean, Required: true, DefaultValue: true},
		{Name: "triggerType", Kind: schema.KindString, Required: true},
		{Name: "triggerTable", Kind: schema.KindString},
		{Name: "trigger", Kind: schema.KindJson, Required: true},
		{Name: "conditions", Kind: schema.KindJson},
		{Name: "lookups", Kind: schema.KindJson},
		{Name: "ai", Kind: schema.KindJson, Required: true},
		{Name: "actions", Kind: schema.KindJson, Required: true},
		{Name: "holdUntilComplete", Kind: schema.KindBoolean, Required: true},
		{Name: "subjectPath", Kind: schema.KindString},
		{Name: "newUserClearedLimit", Kind: schema.KindInteger},
		{Name: "maxRetries", Kind: schema.KindInteger},
		{Name: "createdAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "updatedAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
	}
	runs := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{20}", AutoIDStrategy: "random"},
		{Name: "workflowId", Kind: schema.KindString, Required: true},
		{Name: "workflowName", Kind: schema.KindString, Required: true},
		{Name: "trigger", Kind: schema.KindString, Required: true},
		{Name: "event", Kind: schema.KindString, Required: true},
		{Name: "table", Kind: schema.KindString},
		{Name: "rowId", Kind: schema.KindString},
		{Name: "rowKey", Kind: schema.KindString},
		{Name: "activeRowKey", Kind: schema.KindString},
		{Name: "subjectId", Kind: schema.KindString},
		{Name: "status", Kind: schema.KindString, Required: true},
		{Name: "visibilityTable", Kind: schema.KindString},
		{Name: "input", Kind: schema.KindJson},
		{Name: "lookupResults", Kind: schema.KindJson},
		{Name: "result", Kind: schema.KindJson},
		{Name: "reasoning", Kind: schema.KindString},
		{Name: "recommendedAction", Kind: schema.KindString},
		{Name: "action", Kind: schema.KindString},
		{Name: "actionEffect", Kind: schema.KindJson},
		{Name: "approvalRequired", Kind: schema.KindBoolean, Required: true},
		{Name: "attempt", Kind: schema.KindInteger, Required: true},
		{Name: "maxRetries", Kind: schema.KindInteger, Required: true},
		{Name: "errors", Kind: schema.KindJson},
		{Name: "error", Kind: schema.KindString},
		{Name: "model", Kind: schema.KindString},
		{Name: "provider", Kind: schema.KindString},
		{Name: "holdUntilComplete", Kind: schema.KindBoolean, Required: true},
		{Name: "createdAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "startedAt", Kind: schema.KindTimestamp},
		{Name: "finishedAt", Kind: schema.KindTimestamp},
	}
	return map[string]*schema.TableDef{
		systemWorkflowTableName: {
			Name:           systemWorkflowTableName,
			CompiledSchema: schema.NewCompiledSchema(workflows),
			Indexes: []schema.IndexDef{
				{Fields: []string{"triggerType"}, Type: schema.IndexTypeHash},
				{Fields: []string{"triggerTable"}, Type: schema.IndexTypeHash},
				{Fields: []string{"enabled"}, Type: schema.IndexTypeHash},
			},
		},
		systemWorkflowRunTableName: {
			Name:           systemWorkflowRunTableName,
			CompiledSchema: schema.NewCompiledSchema(runs),
			Indexes: []schema.IndexDef{
				{Fields: []string{"status"}, Type: schema.IndexTypeHash},
				{Fields: []string{"visibilityTable"}, Type: schema.IndexTypeHash},
				{Fields: []string{"rowKey"}, Type: schema.IndexTypeHash},
				{Fields: []string{"activeRowKey"}, Type: schema.IndexTypeHash},
				{Fields: []string{"subjectId"}, Type: schema.IndexTypeHash},
				{Fields: []string{"workflowId"}, Type: schema.IndexTypeHash},
			},
		},
	}
}

func workflowToRow(m Workflow) map[string]any {
	return map[string]any{
		"id":                  m.ID,
		"name":                strings.TrimSpace(m.Name),
		"category":            strings.TrimSpace(m.Category),
		"template":            strings.TrimSpace(m.Template),
		"enabled":             m.Enabled,
		"triggerType":         strings.TrimSpace(m.Trigger.Type),
		"triggerTable":        strings.TrimSpace(m.Trigger.Table),
		"trigger":             jsonValue(m.Trigger),
		"conditions":          jsonValue(m.Conditions),
		"lookups":             jsonValue(m.Lookups),
		"ai":                  jsonValue(m.AI),
		"actions":             jsonValue(m.Actions),
		"holdUntilComplete":   m.HoldUntilComplete,
		"subjectPath":         strings.TrimSpace(m.SubjectPath),
		"newUserClearedLimit": m.NewUserClearedLimit,
		"maxRetries":          m.MaxRetries,
	}
}

func workflowFromRow(row map[string]any) Workflow {
	var trigger WorkflowTrigger
	var conditions []WorkflowCondition
	var lookups []WorkflowLookup
	var ai WorkflowAIStep
	var actions []WorkflowAction
	decodeJSONValue(row["trigger"], &trigger)
	decodeJSONValue(row["conditions"], &conditions)
	decodeJSONValue(row["lookups"], &lookups)
	decodeJSONValue(row["ai"], &ai)
	decodeJSONValue(row["actions"], &actions)
	return Workflow{
		ID:                  toString(row["id"]),
		Name:                toString(row["name"]),
		Category:            toString(row["category"]),
		Template:            toString(row["template"]),
		Enabled:             truthy(row["enabled"]),
		Trigger:             trigger,
		Conditions:          conditions,
		Lookups:             lookups,
		AI:                  ai,
		Actions:             actions,
		HoldUntilComplete:   truthy(row["holdUntilComplete"]),
		SubjectPath:         toString(row["subjectPath"]),
		NewUserClearedLimit: int(anyInt64(row["newUserClearedLimit"])),
		MaxRetries:          int(anyInt64(row["maxRetries"])),
		CreatedAtUnixMilli:  anyInt64(row["createdAt"]),
		UpdatedAtUnixMilli:  anyInt64(row["updatedAt"]),
	}
}

func workflowRunFromRow(row map[string]any) WorkflowRun {
	input := map[string]any{}
	lookups := map[string]any{}
	result := map[string]any{}
	var actionEffect *WorkflowActionEffect
	decodeJSONValue(row["input"], &input)
	decodeJSONValue(row["lookupResults"], &lookups)
	decodeJSONValue(row["result"], &result)
	if row["actionEffect"] != nil {
		var effect WorkflowActionEffect
		decodeJSONValue(row["actionEffect"], &effect)
		actionEffect = &effect
	}
	return WorkflowRun{
		ID:                  toString(row["id"]),
		WorkflowID:          toString(row["workflowId"]),
		WorkflowName:        toString(row["workflowName"]),
		Trigger:             toString(row["trigger"]),
		Table:               toString(row["table"]),
		RowID:               toString(row["rowId"]),
		Event:               toString(row["event"]),
		SubjectID:           toString(row["subjectId"]),
		Status:              toString(row["status"]),
		Input:               input,
		LookupResults:       lookups,
		Result:              result,
		Reasoning:           toString(row["reasoning"]),
		RecommendedAction:   toString(row["recommendedAction"]),
		Action:              toString(row["action"]),
		ActionEffect:        actionEffect,
		ApprovalRequired:    truthy(row["approvalRequired"]),
		Attempt:             int(anyInt64(row["attempt"])),
		MaxRetries:          int(anyInt64(row["maxRetries"])),
		Errors:              anyStringSlice(row["errors"]),
		Error:               toString(row["error"]),
		Model:               toString(row["model"]),
		Provider:            toString(row["provider"]),
		HoldUntilComplete:   truthy(row["holdUntilComplete"]),
		CreatedAtUnixMilli:  anyInt64(row["createdAt"]),
		StartedAtUnixMilli:  anyInt64(row["startedAt"]),
		FinishedAtUnixMilli: anyInt64(row["finishedAt"]),
	}
}

func workflowRowKey(table, rowID string) string { return table + "\x1f" + rowID }

func workflowVisibilityTable(m Workflow) string {
	if !m.HoldUntilComplete {
		return ""
	}
	return m.Trigger.Table
}

func workflowActiveRowKey(m Workflow, table, rowID string) string {
	if !m.HoldUntilComplete || table == "" || rowID == "" {
		return ""
	}
	return workflowRowKey(table, rowID)
}

func workflowRunVisibilityTable(run WorkflowRun) string {
	if !run.HoldUntilComplete {
		return ""
	}
	return run.Table
}

func workflowRunActiveRowKey(run WorkflowRun) string {
	if !run.HoldUntilComplete || run.Table == "" || run.RowID == "" {
		return ""
	}
	return workflowRowKey(run.Table, run.RowID)
}

func isWorkflowSystemTable(table string) bool {
	return table == systemWorkflowTableName || table == systemWorkflowRunTableName
}

// WorkflowTemplate is an editable starter definition exposed by the admin UI.
type WorkflowTemplate struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Workflow    Workflow `json:"workflow"`
}

// WorkflowTemplates returns built-in moderation and Discord reconciliation
// starters. Table and field names are conventional defaults and remain editable.
func WorkflowTemplates() []WorkflowTemplate {
	moderationSchema := defaultWorkflowResultSchema([]string{"approve", "queue_review", "delete", "block"})
	return []WorkflowTemplate{
		{
			ID:          "new-user-moderator",
			Name:        "New-user moderator",
			Description: "Review a new user's first items and hold them until the configured cleared-item limit is reached.",
			Workflow: Workflow{
				Name: "New-user moderator", Category: "moderation", Template: "new-user-moderator", Enabled: true,
				Trigger: WorkflowTrigger{Type: "row_insert", Table: "posts"},
				AI: WorkflowAIStep{
					Model:        "openai/gpt-4.1-mini",
					Prompt:       "Review the new user's content for spam, scams, malware, illegal content, and policy violations. Return an action and concise reasoning.",
					ResultSchema: moderationSchema,
				},
				Actions: []WorkflowAction{
					{Type: "approve"},
					{Type: "queue_review", RequireApproval: true},
					{Type: "delete", Table: "posts", IDPath: "input.rowId", RequireApproval: true},
					{Type: "block", Table: "users", IDPath: "input.row.userId", Field: "blocked", Value: true, RequireApproval: true},
				},
				HoldUntilComplete: true, SubjectPath: "input.row.userId", NewUserClearedLimit: 3, MaxRetries: 2,
			},
		},
		{
			ID:          "reported-content-moderator",
			Name:        "Reported-content moderator",
			Description: "Review reports together with their target content and queue sensitive actions for approval.",
			Workflow: Workflow{
				Name: "Reported-content moderator", Category: "moderation", Template: "reported-content-moderator", Enabled: true,
				Trigger: WorkflowTrigger{Type: "report", Table: "reports", Events: []string{"insert"}},
				Lookups: []WorkflowLookup{{Name: "reported_content", Type: "get", Table: "posts", InputPath: "input.row.postId"}},
				AI: WorkflowAIStep{
					Model:        "openai/gpt-4.1-mini",
					Prompt:       "Evaluate whether the report is valid by reviewing both the report and reported content. Return an action and concise reasoning.",
					ResultSchema: moderationSchema,
				},
				Actions: []WorkflowAction{
					{Type: "approve"},
					{Type: "queue_review", RequireApproval: true},
					{Type: "delete", Table: "posts", IDPath: "input.row.postId", RequireApproval: true},
					{Type: "block", Table: "users", IDPath: "lookups.reported_content.userId", Field: "blocked", Value: true, RequireApproval: true},
				},
				MaxRetries: 2,
			},
		},
		{
			ID:          "discord-game-reconciliation",
			Name:        "Discord game reconciliation",
			Description: "Search game candidates for mismatched Discord activity and create or propose an alias.",
			Workflow: Workflow{
				Name: "Discord game reconciliation", Category: "reconciliation", Template: "discord-game-reconciliation", Enabled: true,
				Trigger: WorkflowTrigger{Type: "discord", Event: "activity_mismatch"},
				Lookups: []WorkflowLookup{{
					Name: "game_candidates", Type: "search", Table: "games",
					InputPath: "input.activity.name", SearchFields: []string{"name", "aliases"}, Limit: 10,
				}},
				AI: WorkflowAIStep{
					Model:  "openai/gpt-4.1-mini",
					Prompt: "Reconcile the Discord activity with the supplied game candidates. Choose a game only when the match is strong. Return action, game_id, alias, and reasoning.",
					ResultSchema: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"action":    map[string]any{"type": "string", "enum": []string{"create_alias", "propose_alias", "queue_review"}},
							"game_id":   map[string]any{"type": "string"},
							"alias":     map[string]any{"type": "string"},
							"reasoning": map[string]any{"type": "string"},
						},
						"required": []string{"action", "game_id", "alias", "reasoning"}, "additionalProperties": false,
					},
				},
				Actions: []WorkflowAction{
					{Type: "create_alias", Table: "game_aliases", Data: map[string]string{"gameId": "result.game_id", "alias": "result.alias"}},
					{Type: "propose_alias", Table: "game_aliases", Data: map[string]string{"gameId": "result.game_id", "alias": "result.alias"}, RequireApproval: true},
					{Type: "queue_review", RequireApproval: true},
				},
				MaxRetries: 2,
			},
		},
	}
}

// WorkflowTemplates returns the application-configured workflow templates.
// Built-in templates are returned when WorkflowConfig.Templates is nil.
func (d *Database) WorkflowTemplates() []WorkflowTemplate {
	if d == nil || d.workflow == nil || d.workflow.config.Templates == nil {
		return WorkflowTemplates()
	}
	return append([]WorkflowTemplate(nil), d.workflow.config.Templates...)
}

func workflowMatches(workflow Workflow, trigger, event, tableName string) bool {
	t := workflow.Trigger
	switch t.Type {
	case "row_insert", "row_update":
		return t.Type == trigger && t.Table == tableName
	case "report":
		if t.Table != "" && trigger == "row_insert" {
			return t.Table == tableName && (len(t.Events) == 0 || containsString(t.Events, event))
		}
		return trigger == "report" && (t.Event == "" || t.Event == event)
	case "discord":
		return trigger == "discord" && (t.Event == "" || t.Event == event)
	case "manual":
		return trigger == "manual"
	default:
		return false
	}
}

func conditionsMatch(conditions []WorkflowCondition, input map[string]any) bool {
	scope := map[string]any{"input": input}
	for _, condition := range conditions {
		actual := valueAtPath(scope, condition.Field)
		switch condition.Operator {
		case "equals", "eq", "":
			if fmt.Sprint(actual) != fmt.Sprint(condition.Value) {
				return false
			}
		case "not_equals", "neq":
			if fmt.Sprint(actual) == fmt.Sprint(condition.Value) {
				return false
			}
		case "exists":
			if (actual != nil) != truthy(condition.Value) {
				return false
			}
		case "contains":
			if !strings.Contains(strings.ToLower(toString(actual)), strings.ToLower(toString(condition.Value))) {
				return false
			}
		case "in":
			found := false
			for _, value := range anySlice(condition.Value) {
				if fmt.Sprint(actual) == fmt.Sprint(value) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func workflowAction(workflow Workflow, actionType string) (WorkflowAction, bool) {
	for _, action := range workflow.Actions {
		if action.Type == actionType {
			return action, true
		}
	}
	return WorkflowAction{}, false
}

func resultAction(result map[string]any) string {
	for _, key := range []string{"action", "recommended_action", "recommendedAction"} {
		if value := strings.TrimSpace(toString(result[key])); value != "" {
			return value
		}
	}
	return "queue_review"
}

func defaultWorkflowResultSchema(actions []string) map[string]any {
	if len(actions) == 0 {
		actions = []string{"queue_review"}
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action":    map[string]any{"type": "string", "enum": actions},
			"reasoning": map[string]any{"type": "string"},
		},
		"required": []string{"action", "reasoning"}, "additionalProperties": false,
	}
}

func valueAtPath(root map[string]any, path string) any {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	var current any = root
	for _, part := range strings.Split(path, ".") {
		switch value := current.(type) {
		case map[string]any:
			current = value[part]
		default:
			return nil
		}
	}
	return current
}

func jsonValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out any
	if json.Unmarshal(data, &out) != nil {
		return nil
	}
	return out
}

func decodeJSONValue(value any, out any) {
	data, err := json.Marshal(value)
	if err == nil {
		_ = json.Unmarshal(data, out)
	}
}

func anySlice(value any) []any {
	switch values := value.(type) {
	case []any:
		return values
	case []string:
		out := make([]any, len(values))
		for i := range values {
			out[i] = values[i]
		}
		return out
	default:
		return nil
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func stringSliceAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func anyStringSlice(value any) []string {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text := toString(value); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return []string{}
	}
}

func anyInt64(value any) int64 {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case uint:
		return int64(value)
	case uint32:
		return int64(value)
	case uint64:
		return int64(value)
	case float64:
		return int64(value)
	case json.Number:
		n, _ := value.Int64()
		return n
	default:
		return 0
	}
}

func truthy(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		return value == "true" || value == "1"
	case int:
		return value != 0
	case int64:
		return value != 0
	case float64:
		return value != 0
	default:
		return false
	}
}
