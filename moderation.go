package flop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcisbee/flop/internal/schema"
)

const (
	systemModeratorTableName     = "_moderators"
	systemModerationRunTableName = "_moderation_runs"
	defaultOpenRouterURL         = "https://openrouter.ai/api/v1/chat/completions"
)

// ModerationConfig configures the OpenRouter connection and worker pool.
// The API key defaults to OPENROUTER_API_KEY when it is not set.
type ModerationConfig struct {
	OpenRouterAPIKey string
	OpenRouterURL    string
	HTTPClient       *http.Client
	Workers          int
}

// Moderator configures one AI moderator for one application table.
// ContentFields select the fields sent to the model. TargetTable and
// TargetIDField optionally make the source table a report table whose target
// should be deleted when the model recommends delete.
type Moderator struct {
	ID                  string   `json:"id,omitempty"`
	Name                string   `json:"name"`
	Enabled             bool     `json:"enabled"`
	Table               string   `json:"table"`
	Events              []string `json:"events"`
	ContentFields       []string `json:"contentFields"`
	UserField           string   `json:"userField,omitempty"`
	UserTable           string   `json:"userTable,omitempty"`
	UserBlockedField    string   `json:"userBlockedField,omitempty"`
	NewUserClearedLimit int      `json:"newUserClearedLimit,omitempty"`
	TargetTable         string   `json:"targetTable,omitempty"`
	TargetIDField       string   `json:"targetIdField,omitempty"`
	TargetContentFields []string `json:"targetContentFields,omitempty"`
	Provider            string   `json:"provider,omitempty"`
	Model               string   `json:"model"`
	Instructions        string   `json:"instructions,omitempty"`
	AllowedActions      []string `json:"allowedActions"`
	PublishBeforeReview bool     `json:"publishBeforeReview"`
	CreatedAtUnixMilli  int64    `json:"createdAtUnixMilli,omitempty"`
	UpdatedAtUnixMilli  int64    `json:"updatedAtUnixMilli,omitempty"`
}

// ModerationRun is the durable audit record for one moderator decision.
type ModerationRun struct {
	ID                  string   `json:"id"`
	ModeratorID         string   `json:"moderatorId"`
	ModeratorName       string   `json:"moderatorName"`
	Table               string   `json:"table"`
	RowID               string   `json:"rowId"`
	Event               string   `json:"event"`
	SubjectID           string   `json:"subjectId,omitempty"`
	TargetTable         string   `json:"targetTable,omitempty"`
	TargetRowID         string   `json:"targetRowId,omitempty"`
	Status              string   `json:"status"`
	Verdict             string   `json:"verdict,omitempty"`
	Categories          []string `json:"categories,omitempty"`
	Reasoning           string   `json:"reasoning,omitempty"`
	RecommendedAction   string   `json:"recommendedAction,omitempty"`
	Action              string   `json:"action,omitempty"`
	Error               string   `json:"error,omitempty"`
	Model               string   `json:"model,omitempty"`
	Provider            string   `json:"provider,omitempty"`
	PublishBeforeReview bool     `json:"publishBeforeReview"`
	CreatedAtUnixMilli  int64    `json:"createdAtUnixMilli"`
	StartedAtUnixMilli  int64    `json:"startedAtUnixMilli,omitempty"`
	FinishedAtUnixMilli int64    `json:"finishedAtUnixMilli,omitempty"`
}

type moderationService struct {
	db      *Database
	config  ModerationConfig
	client  *http.Client
	wake    chan struct{}
	start   sync.Once
	claimMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
}

type moderationDecision struct {
	Verdict           string   `json:"verdict"`
	Categories        []string `json:"categories"`
	Reasoning         string   `json:"reasoning"`
	RecommendedAction string   `json:"recommended_action"`
}

func newModerationService(db *Database, config *ModerationConfig) *moderationService {
	cfg := ModerationConfig{}
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
	return &moderationService{
		db:     db,
		config: cfg,
		client: client,
		wake:   make(chan struct{}, 1),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *moderationService) startWorkers() {
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

func (s *moderationService) stopWorkers() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func (s *moderationService) signal() {
	if s == nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *moderationService) worker(stop <-chan struct{}) {
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

func (s *moderationService) claimPending() (ModerationRun, bool) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	table := s.db.db.GetTable(systemModerationRunTableName)
	if table == nil {
		return ModerationRun{}, false
	}
	ptrs := table.FindAllByIndex([]string{"status"}, "pending")
	for _, ptr := range ptrs {
		row, err := table.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		run := moderationRunFromRow(row)
		now := time.Now().UnixMilli()
		updated, err := table.Update(run.ID, map[string]any{
			"status":    "running",
			"startedAt": now,
			"error":     "",
		}, nil)
		if err != nil {
			continue
		}
		return moderationRunFromRow(updated), true
	}
	return ModerationRun{}, false
}

func (s *moderationService) recoverInterruptedRuns() {
	table := s.db.db.GetTable(systemModerationRunTableName)
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
			"error":     "Previous moderation attempt was interrupted and queued again.",
		}, nil)
	}
}

func (s *moderationService) execute(run ModerationRun) {
	moderator, err := s.getModerator(run.ModeratorID)
	if err != nil {
		s.failRun(run.ID, err)
		return
	}
	source := s.db.db.GetTable(run.Table)
	if source == nil {
		s.failRun(run.ID, fmt.Errorf("source table %q no longer exists", run.Table))
		return
	}
	row, err := source.Get(run.RowID)
	if err != nil {
		s.failRun(run.ID, err)
		return
	}
	if row == nil {
		s.finishRun(run.ID, map[string]any{
			"status":    "cancelled",
			"reasoning": "The source row was deleted before moderation completed.",
		})
		return
	}

	payload := map[string]any{
		"event":   run.Event,
		"table":   run.Table,
		"rowId":   run.RowID,
		"content": selectModerationFields(row, moderator.ContentFields),
	}
	if run.TargetTable != "" && run.TargetRowID != "" {
		if targetTable := s.db.db.GetTable(run.TargetTable); targetTable != nil {
			target, getErr := targetTable.Get(run.TargetRowID)
			if getErr != nil {
				s.failRun(run.ID, getErr)
				return
			}
			if target != nil {
				payload["target"] = map[string]any{
					"table":   run.TargetTable,
					"rowId":   run.TargetRowID,
					"content": selectModerationFields(target, moderator.TargetContentFields),
				}
			}
		}
	}

	decision, err := s.askOpenRouter(moderator, payload)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.requeueInterruptedRun(run.ID)
			return
		}
		s.failRun(run.ID, err)
		return
	}
	action := "review"
	status := "review"
	if decision.Verdict == "clear" {
		action = "allow"
		status = "cleared"
	} else if containsString(moderator.AllowedActions, decision.RecommendedAction) {
		action = decision.RecommendedAction
		switch action {
		case "allow":
			status = "cleared"
		case "delete", "block_user":
			if err := s.applyAction(run, action); err != nil {
				s.failRun(run.ID, err)
				return
			}
			status = "actioned"
		default:
			action = "review"
		}
	}
	s.finishRun(run.ID, map[string]any{
		"status":            status,
		"verdict":           decision.Verdict,
		"categories":        decision.Categories,
		"reasoning":         decision.Reasoning,
		"recommendedAction": decision.RecommendedAction,
		"action":            action,
	})
}

func (s *moderationService) requeueInterruptedRun(id string) {
	table := s.db.db.GetTable(systemModerationRunTableName)
	if table == nil {
		return
	}
	_, _ = table.Update(id, map[string]any{
		"status":    "pending",
		"startedAt": nil,
		"error":     "Moderation was interrupted and will retry after restart.",
	}, nil)
}

func (s *moderationService) askOpenRouter(m Moderator, content map[string]any) (moderationDecision, error) {
	if strings.TrimSpace(s.config.OpenRouterAPIKey) == "" {
		return moderationDecision{}, errors.New("OpenRouter API key is not configured")
	}
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return moderationDecision{}, err
	}
	instructions := `You are a content moderator. Detect spam, scams, malware or viruses, and illegal content. Return a concise decision. A "clear" verdict means none of those categories apply. Use "violation" otherwise. Recommended action must be allow, review, delete, or block_user.`
	if m.TargetTable != "" {
		instructions += ` The source item is a report and the target is the reported content. Treat the report as valid only when the target violates the policy. Use "clear" for an invalid report and "violation" for a valid report.`
	}
	if strings.TrimSpace(m.Instructions) != "" {
		instructions += "\n\nAdditional policy:\n" + m.Instructions
	}
	requestBody := map[string]any{
		"model": m.Model,
		"messages": []map[string]string{
			{"role": "system", "content": instructions},
			{"role": "user", "content": string(contentJSON)},
		},
		"temperature": 0,
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "moderation_decision",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"verdict": map[string]any{"type": "string", "enum": []string{"clear", "violation"}},
						"categories": map[string]any{
							"type":  "array",
							"items": map[string]any{"type": "string", "enum": []string{"spam", "scam", "virus", "illegal", "other"}},
						},
						"reasoning":          map[string]any{"type": "string"},
						"recommended_action": map[string]any{"type": "string", "enum": []string{"allow", "review", "delete", "block_user"}},
					},
					"required":             []string{"verdict", "categories", "reasoning", "recommended_action"},
					"additionalProperties": false,
				},
			},
		},
		"provider": map[string]any{
			"require_parameters": true,
			"data_collection":    "deny",
		},
	}
	if strings.TrimSpace(m.Provider) != "" {
		requestBody["provider"] = map[string]any{
			"order":              []string{m.Provider},
			"allow_fallbacks":    false,
			"require_parameters": true,
			"data_collection":    "deny",
		}
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return moderationDecision{}, err
	}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, s.config.OpenRouterURL, bytes.NewReader(body))
	if err != nil {
		return moderationDecision{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.config.OpenRouterAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Title", "Flop Moderation")
	resp, err := s.client.Do(req)
	if err != nil {
		return moderationDecision{}, fmt.Errorf("OpenRouter request: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return moderationDecision{}, err
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
		return moderationDecision{}, fmt.Errorf("OpenRouter returned invalid JSON: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(resp.Status)
		if response.Error != nil && strings.TrimSpace(response.Error.Message) != "" {
			message = response.Error.Message
		}
		return moderationDecision{}, fmt.Errorf("OpenRouter: %s", message)
	}
	if response.Error != nil {
		return moderationDecision{}, fmt.Errorf("OpenRouter: %s", response.Error.Message)
	}
	if len(response.Choices) == 0 {
		return moderationDecision{}, errors.New("OpenRouter returned no moderation decision")
	}
	if response.Choices[0].Error != nil {
		return moderationDecision{}, fmt.Errorf("OpenRouter: %s", response.Choices[0].Error.Message)
	}
	var decision moderationDecision
	if err := json.Unmarshal([]byte(response.Choices[0].Message.Content), &decision); err != nil {
		return moderationDecision{}, fmt.Errorf("OpenRouter returned an invalid moderation decision: %w", err)
	}
	if decision.Verdict != "clear" && decision.Verdict != "violation" {
		return moderationDecision{}, fmt.Errorf("OpenRouter returned unsupported verdict %q", decision.Verdict)
	}
	if !containsString([]string{"allow", "review", "delete", "block_user"}, decision.RecommendedAction) {
		return moderationDecision{}, fmt.Errorf("OpenRouter returned unsupported action %q", decision.RecommendedAction)
	}
	return decision, nil
}

func (s *moderationService) enqueueMutation(tableName, event string, row map[string]any) error {
	if s == nil || row == nil || strings.HasPrefix(tableName, "_") {
		return nil
	}
	moderators, err := s.moderatorsForTable(tableName)
	if err != nil {
		return err
	}
	source := s.db.db.GetTable(tableName)
	if source == nil || len(source.GetDef().CompiledSchema.Fields) == 0 {
		return nil
	}
	rowID := toString(row[source.GetDef().CompiledSchema.Fields[0].Name])
	if rowID == "" {
		return fmt.Errorf("moderation: row in %s has no primary key", tableName)
	}
	runTable := s.db.db.GetTable(systemModerationRunTableName)
	if runTable == nil {
		return errors.New("moderation run table unavailable")
	}
	created := false
	for _, moderator := range moderators {
		if !moderator.Enabled || moderator.Table != tableName || !containsString(moderator.Events, event) {
			continue
		}
		subjectID := toString(row[moderator.UserField])
		if moderator.NewUserClearedLimit > 0 && subjectID != "" && !s.shouldReviewNewUser(moderator, subjectID) {
			continue
		}
		targetRowID := ""
		if moderator.TargetTable != "" && moderator.TargetIDField != "" {
			targetRowID = toString(row[moderator.TargetIDField])
		}
		_, err := runTable.Insert(map[string]any{
			"moderatorId":         moderator.ID,
			"moderatorName":       moderator.Name,
			"table":               tableName,
			"rowId":               rowID,
			"rowKey":              moderationRowKey(tableName, rowID),
			"activeRowKey":        moderationActiveRowKey(moderator, tableName, rowID),
			"event":               event,
			"subjectId":           subjectID,
			"targetTable":         moderator.TargetTable,
			"targetRowId":         targetRowID,
			"status":              "pending",
			"visibilityTable":     moderationVisibilityTable(moderator),
			"model":               moderator.Model,
			"provider":            moderator.Provider,
			"publishBeforeReview": moderator.PublishBeforeReview,
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

func (s *moderationService) moderatorsForTable(tableName string) ([]Moderator, error) {
	table := s.db.db.GetTable(systemModeratorTableName)
	if table == nil {
		return nil, errors.New("moderator table unavailable")
	}
	ptrs := table.FindAllByIndex([]string{"table"}, tableName)
	out := make([]Moderator, 0, len(ptrs))
	for _, ptr := range ptrs {
		row, err := table.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		out = append(out, moderatorFromRow(row))
	}
	return out, nil
}

func (s *moderationService) shouldReviewNewUser(m Moderator, subjectID string) bool {
	if m.UserTable != "" && m.UserBlockedField != "" {
		if users := s.db.db.GetTable(m.UserTable); users != nil {
			if user, _ := users.Get(subjectID); user != nil && truthy(user[m.UserBlockedField]) {
				return false
			}
		}
	}
	runs := s.db.db.GetTable(systemModerationRunTableName)
	if runs == nil {
		return true
	}
	cleared := 0
	for _, ptr := range runs.FindAllByIndex([]string{"subjectId"}, subjectID) {
		row, err := runs.GetByPointer(ptr)
		if err != nil || row == nil {
			continue
		}
		if toString(row["moderatorId"]) == m.ID && toString(row["status"]) == "cleared" {
			cleared++
			if cleared >= m.NewUserClearedLimit {
				return false
			}
		}
	}
	return true
}

func (s *moderationService) visible(tableName, rowID string) bool {
	if s == nil || tableName == "" || rowID == "" {
		return true
	}
	runs := s.db.db.GetTable(systemModerationRunTableName)
	if runs == nil {
		return true
	}
	for _, ptr := range runs.FindAllByIndex([]string{"activeRowKey"}, moderationRowKey(tableName, rowID)) {
		row, err := runs.GetByPointer(ptr)
		if err != nil || row == nil || truthy(row["publishBeforeReview"]) {
			continue
		}
		switch toString(row["status"]) {
		case "pending", "running", "review", "error":
			return false
		}
	}
	return true
}

func (s *moderationService) applyAction(run ModerationRun, action string) error {
	switch action {
	case "delete":
		tableName := run.TargetTable
		rowID := run.TargetRowID
		if tableName == "" {
			tableName = run.Table
			rowID = run.RowID
		}
		table := s.db.Table(tableName)
		if table == nil {
			return fmt.Errorf("moderation delete target table %q not found", tableName)
		}
		_, err := table.Delete(rowID)
		return err
	case "block_user":
		moderator, err := s.getModerator(run.ModeratorID)
		if err != nil {
			return err
		}
		if moderator.UserTable == "" || moderator.UserBlockedField == "" || run.SubjectID == "" {
			return errors.New("moderator does not configure a blockable user target")
		}
		users := s.db.db.GetTable(moderator.UserTable)
		if users == nil {
			return fmt.Errorf("moderator user table %q not found", moderator.UserTable)
		}
		_, err = users.Update(run.SubjectID, map[string]any{moderator.UserBlockedField: true}, nil)
		return err
	default:
		return nil
	}
}

func (s *moderationService) failRun(id string, err error) {
	message := "moderation failed"
	if err != nil {
		message = err.Error()
	}
	s.finishRun(id, map[string]any{"status": "error", "error": message})
}

func (s *moderationService) finishRun(id string, fields map[string]any) {
	table := s.db.db.GetTable(systemModerationRunTableName)
	if table == nil {
		return
	}
	fields["finishedAt"] = time.Now().UnixMilli()
	switch toString(fields["status"]) {
	case "cleared", "actioned", "cancelled":
		fields["visibilityTable"] = ""
		fields["activeRowKey"] = ""
	}
	_, _ = table.Update(id, fields, nil)
}

func (s *moderationService) listModerators() ([]Moderator, error) {
	table := s.db.db.GetTable(systemModeratorTableName)
	if table == nil {
		return nil, errors.New("moderator table unavailable")
	}
	rows, err := table.Scan(table.Count(), 0)
	if err != nil {
		return nil, err
	}
	out := make([]Moderator, 0, len(rows))
	for _, row := range rows {
		out = append(out, moderatorFromRow(row))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *moderationService) getModerator(id string) (Moderator, error) {
	table := s.db.db.GetTable(systemModeratorTableName)
	if table == nil {
		return Moderator{}, errors.New("moderator table unavailable")
	}
	row, err := table.Get(id)
	if err != nil {
		return Moderator{}, err
	}
	if row == nil {
		return Moderator{}, fmt.Errorf("moderator %q not found", id)
	}
	return moderatorFromRow(row), nil
}

func (s *moderationService) saveModerator(m Moderator) (Moderator, error) {
	if err := s.validateModerator(m); err != nil {
		return Moderator{}, err
	}
	table := s.db.db.GetTable(systemModeratorTableName)
	now := time.Now().UnixMilli()
	row := moderatorToRow(m)
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
		return Moderator{}, err
	}
	return moderatorFromRow(saved), nil
}

func (s *moderationService) validateModerator(m Moderator) error {
	m.Name = strings.TrimSpace(m.Name)
	m.Table = strings.TrimSpace(m.Table)
	m.Model = strings.TrimSpace(m.Model)
	if m.Name == "" || m.Table == "" || m.Model == "" {
		return errors.New("moderator name, table, and model are required")
	}
	table := s.db.db.GetTable(m.Table)
	if table == nil || strings.HasPrefix(m.Table, "_") {
		return fmt.Errorf("application table %q not found", m.Table)
	}
	if len(m.ContentFields) == 0 {
		return errors.New("at least one content field is required")
	}
	for _, field := range m.ContentFields {
		if table.GetDef().CompiledSchema.FieldMap[field] == nil {
			return fmt.Errorf("content field %q not found in table %q", field, m.Table)
		}
	}
	if len(m.Events) == 0 {
		return errors.New("at least one event is required")
	}
	for _, event := range m.Events {
		if event != "insert" && event != "update" {
			return fmt.Errorf("unsupported moderation event %q", event)
		}
	}
	for _, action := range m.AllowedActions {
		if !containsString([]string{"allow", "review", "delete", "block_user"}, action) {
			return fmt.Errorf("unsupported moderation action %q", action)
		}
	}
	if m.TargetTable != "" {
		target := s.db.db.GetTable(m.TargetTable)
		if target == nil || strings.HasPrefix(m.TargetTable, "_") {
			return fmt.Errorf("target table %q not found", m.TargetTable)
		}
		if m.TargetIDField == "" || table.GetDef().CompiledSchema.FieldMap[m.TargetIDField] == nil {
			return fmt.Errorf("target ID field %q not found in table %q", m.TargetIDField, m.Table)
		}
		for _, field := range m.TargetContentFields {
			if target.GetDef().CompiledSchema.FieldMap[field] == nil {
				return fmt.Errorf("target content field %q not found in table %q", field, m.TargetTable)
			}
		}
	}
	if containsString(m.AllowedActions, "block_user") || m.NewUserClearedLimit > 0 {
		if m.UserField == "" || table.GetDef().CompiledSchema.FieldMap[m.UserField] == nil {
			return fmt.Errorf("user field %q not found in table %q", m.UserField, m.Table)
		}
		users := s.db.db.GetTable(m.UserTable)
		if users == nil || strings.HasPrefix(m.UserTable, "_") {
			return fmt.Errorf("user table %q not found", m.UserTable)
		}
		if m.UserBlockedField == "" || users.GetDef().CompiledSchema.FieldMap[m.UserBlockedField] == nil {
			return fmt.Errorf("user blocked field %q not found in table %q", m.UserBlockedField, m.UserTable)
		}
	}
	return nil
}

// Moderators returns all configured moderators.
func (d *Database) Moderators() ([]Moderator, error) {
	if d == nil || d.moderation == nil {
		return nil, errors.New("moderation unavailable")
	}
	return d.moderation.listModerators()
}

// SaveModerator creates or updates a moderator.
func (d *Database) SaveModerator(m Moderator) (Moderator, error) {
	if d == nil || d.moderation == nil {
		return Moderator{}, errors.New("moderation unavailable")
	}
	return d.moderation.saveModerator(m)
}

// DeleteModerator deletes a moderator configuration. Existing audit records
// remain available.
func (d *Database) DeleteModerator(id string) error {
	if d == nil || d.db == nil {
		return errors.New("moderation unavailable")
	}
	table := d.db.GetTable(systemModeratorTableName)
	if table == nil {
		return errors.New("moderator table unavailable")
	}
	ok, err := table.Delete(strings.TrimSpace(id), nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("moderator %q not found", id)
	}
	return nil
}

// ModerationRuns returns recent audit records, newest first. Status may be
// empty to include every status.
func (d *Database) ModerationRuns(status string, limit, offset int) ([]ModerationRun, int, error) {
	if d == nil || d.db == nil {
		return nil, 0, errors.New("moderation unavailable")
	}
	table := d.db.GetTable(systemModerationRunTableName)
	if table == nil {
		return nil, 0, errors.New("moderation run table unavailable")
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
			return []ModerationRun{}, total, nil
		}
		end := offset + limit
		if end > total {
			end = total
		}
		out := make([]ModerationRun, 0, end-offset)
		for i := total - 1 - offset; i >= total-end; i-- {
			row, err := table.GetByPointer(ptrs[i])
			if err != nil || row == nil {
				continue
			}
			out = append(out, moderationRunFromRow(row))
		}
		return out, total, nil
	}
	total := table.Count()
	if offset >= total {
		return []ModerationRun{}, total, nil
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
	out := make([]ModerationRun, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		out = append(out, moderationRunFromRow(rows[i]))
	}
	return out, total, nil
}

// ResolveModerationRun applies an administrator decision. Supported actions
// are allow, delete, block_user, and retry.
func (d *Database) ResolveModerationRun(id, action string) (ModerationRun, error) {
	if d == nil || d.moderation == nil {
		return ModerationRun{}, errors.New("moderation unavailable")
	}
	table := d.db.GetTable(systemModerationRunTableName)
	row, err := table.Get(strings.TrimSpace(id))
	if err != nil {
		return ModerationRun{}, err
	}
	if row == nil {
		return ModerationRun{}, fmt.Errorf("moderation run %q not found", id)
	}
	run := moderationRunFromRow(row)
	switch action {
	case "retry":
		updated, err := table.Update(run.ID, map[string]any{
			"status":          "pending",
			"error":           "",
			"startedAt":       nil,
			"finishedAt":      nil,
			"action":          "",
			"visibilityTable": moderationRunVisibilityTable(run),
			"activeRowKey":    moderationRunActiveRowKey(run),
		}, nil)
		if err != nil {
			return ModerationRun{}, err
		}
		d.moderation.signal()
		return moderationRunFromRow(updated), nil
	case "allow":
		updated, err := table.Update(run.ID, map[string]any{
			"status":          "cleared",
			"action":          "allow",
			"error":           "",
			"finishedAt":      time.Now().UnixMilli(),
			"visibilityTable": "",
			"activeRowKey":    "",
		}, nil)
		if err != nil {
			return ModerationRun{}, err
		}
		return moderationRunFromRow(updated), nil
	case "delete", "block_user":
		if err := d.moderation.applyAction(run, action); err != nil {
			return ModerationRun{}, err
		}
		updated, err := table.Update(run.ID, map[string]any{
			"status":          "actioned",
			"action":          action,
			"error":           "",
			"finishedAt":      time.Now().UnixMilli(),
			"visibilityTable": "",
			"activeRowKey":    "",
		}, nil)
		if err != nil {
			return ModerationRun{}, err
		}
		return moderationRunFromRow(updated), nil
	default:
		return ModerationRun{}, fmt.Errorf("unsupported moderation action %q", action)
	}
}

func moderationSystemTableDefs() map[string]*schema.TableDef {
	moderators := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{16}", AutoIDStrategy: "random"},
		{Name: "name", Kind: schema.KindString, Required: true},
		{Name: "enabled", Kind: schema.KindBoolean, Required: true, DefaultValue: true},
		{Name: "table", Kind: schema.KindString, Required: true},
		{Name: "events", Kind: schema.KindJson, Required: true},
		{Name: "contentFields", Kind: schema.KindJson, Required: true},
		{Name: "userField", Kind: schema.KindString},
		{Name: "userTable", Kind: schema.KindString},
		{Name: "userBlockedField", Kind: schema.KindString},
		{Name: "newUserClearedLimit", Kind: schema.KindInteger},
		{Name: "targetTable", Kind: schema.KindString},
		{Name: "targetIdField", Kind: schema.KindString},
		{Name: "targetContentFields", Kind: schema.KindJson},
		{Name: "provider", Kind: schema.KindString},
		{Name: "model", Kind: schema.KindString, Required: true},
		{Name: "instructions", Kind: schema.KindString},
		{Name: "allowedActions", Kind: schema.KindJson, Required: true},
		{Name: "publishBeforeReview", Kind: schema.KindBoolean, Required: true},
		{Name: "createdAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "updatedAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
	}
	runs := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{20}", AutoIDStrategy: "random"},
		{Name: "moderatorId", Kind: schema.KindString, Required: true},
		{Name: "moderatorName", Kind: schema.KindString, Required: true},
		{Name: "table", Kind: schema.KindString, Required: true},
		{Name: "rowId", Kind: schema.KindString, Required: true},
		{Name: "rowKey", Kind: schema.KindString, Required: true},
		{Name: "activeRowKey", Kind: schema.KindString},
		{Name: "event", Kind: schema.KindString, Required: true},
		{Name: "subjectId", Kind: schema.KindString},
		{Name: "targetTable", Kind: schema.KindString},
		{Name: "targetRowId", Kind: schema.KindString},
		{Name: "status", Kind: schema.KindString, Required: true},
		{Name: "visibilityTable", Kind: schema.KindString},
		{Name: "verdict", Kind: schema.KindString},
		{Name: "categories", Kind: schema.KindJson},
		{Name: "reasoning", Kind: schema.KindString},
		{Name: "recommendedAction", Kind: schema.KindString},
		{Name: "action", Kind: schema.KindString},
		{Name: "error", Kind: schema.KindString},
		{Name: "model", Kind: schema.KindString},
		{Name: "provider", Kind: schema.KindString},
		{Name: "publishBeforeReview", Kind: schema.KindBoolean, Required: true},
		{Name: "createdAt", Kind: schema.KindTimestamp, Required: true, DefaultValue: "now"},
		{Name: "startedAt", Kind: schema.KindTimestamp},
		{Name: "finishedAt", Kind: schema.KindTimestamp},
	}
	return map[string]*schema.TableDef{
		systemModeratorTableName: {
			Name:           systemModeratorTableName,
			CompiledSchema: schema.NewCompiledSchema(moderators),
			Indexes: []schema.IndexDef{
				{Fields: []string{"table"}, Type: schema.IndexTypeHash},
				{Fields: []string{"enabled"}, Type: schema.IndexTypeHash},
			},
		},
		systemModerationRunTableName: {
			Name:           systemModerationRunTableName,
			CompiledSchema: schema.NewCompiledSchema(runs),
			Indexes: []schema.IndexDef{
				{Fields: []string{"status"}, Type: schema.IndexTypeHash},
				{Fields: []string{"visibilityTable"}, Type: schema.IndexTypeHash},
				{Fields: []string{"rowKey"}, Type: schema.IndexTypeHash},
				{Fields: []string{"activeRowKey"}, Type: schema.IndexTypeHash},
				{Fields: []string{"subjectId"}, Type: schema.IndexTypeHash},
				{Fields: []string{"moderatorId"}, Type: schema.IndexTypeHash},
			},
		},
	}
}

func moderatorToRow(m Moderator) map[string]any {
	return map[string]any{
		"id":                  m.ID,
		"name":                strings.TrimSpace(m.Name),
		"enabled":             m.Enabled,
		"table":               strings.TrimSpace(m.Table),
		"events":              stringSliceAny(m.Events),
		"contentFields":       stringSliceAny(m.ContentFields),
		"userField":           strings.TrimSpace(m.UserField),
		"userTable":           strings.TrimSpace(m.UserTable),
		"userBlockedField":    strings.TrimSpace(m.UserBlockedField),
		"newUserClearedLimit": m.NewUserClearedLimit,
		"targetTable":         strings.TrimSpace(m.TargetTable),
		"targetIdField":       strings.TrimSpace(m.TargetIDField),
		"targetContentFields": stringSliceAny(m.TargetContentFields),
		"provider":            strings.TrimSpace(m.Provider),
		"model":               strings.TrimSpace(m.Model),
		"instructions":        strings.TrimSpace(m.Instructions),
		"allowedActions":      stringSliceAny(m.AllowedActions),
		"publishBeforeReview": m.PublishBeforeReview,
	}
}

func moderatorFromRow(row map[string]any) Moderator {
	return Moderator{
		ID:                  toString(row["id"]),
		Name:                toString(row["name"]),
		Enabled:             truthy(row["enabled"]),
		Table:               toString(row["table"]),
		Events:              anyStringSlice(row["events"]),
		ContentFields:       anyStringSlice(row["contentFields"]),
		UserField:           toString(row["userField"]),
		UserTable:           toString(row["userTable"]),
		UserBlockedField:    toString(row["userBlockedField"]),
		NewUserClearedLimit: int(anyInt64(row["newUserClearedLimit"])),
		TargetTable:         toString(row["targetTable"]),
		TargetIDField:       toString(row["targetIdField"]),
		TargetContentFields: anyStringSlice(row["targetContentFields"]),
		Provider:            toString(row["provider"]),
		Model:               toString(row["model"]),
		Instructions:        toString(row["instructions"]),
		AllowedActions:      anyStringSlice(row["allowedActions"]),
		PublishBeforeReview: truthy(row["publishBeforeReview"]),
		CreatedAtUnixMilli:  anyInt64(row["createdAt"]),
		UpdatedAtUnixMilli:  anyInt64(row["updatedAt"]),
	}
}

func moderationRunFromRow(row map[string]any) ModerationRun {
	return ModerationRun{
		ID:                  toString(row["id"]),
		ModeratorID:         toString(row["moderatorId"]),
		ModeratorName:       toString(row["moderatorName"]),
		Table:               toString(row["table"]),
		RowID:               toString(row["rowId"]),
		Event:               toString(row["event"]),
		SubjectID:           toString(row["subjectId"]),
		TargetTable:         toString(row["targetTable"]),
		TargetRowID:         toString(row["targetRowId"]),
		Status:              toString(row["status"]),
		Verdict:             toString(row["verdict"]),
		Categories:          anyStringSlice(row["categories"]),
		Reasoning:           toString(row["reasoning"]),
		RecommendedAction:   toString(row["recommendedAction"]),
		Action:              toString(row["action"]),
		Error:               toString(row["error"]),
		Model:               toString(row["model"]),
		Provider:            toString(row["provider"]),
		PublishBeforeReview: truthy(row["publishBeforeReview"]),
		CreatedAtUnixMilli:  anyInt64(row["createdAt"]),
		StartedAtUnixMilli:  anyInt64(row["startedAt"]),
		FinishedAtUnixMilli: anyInt64(row["finishedAt"]),
	}
}

func selectModerationFields(row map[string]any, fields []string) map[string]any {
	out := make(map[string]any, len(fields))
	for _, field := range fields {
		out[field] = row[field]
	}
	return out
}

func moderationRowKey(table, rowID string) string { return table + "\x1f" + rowID }

func moderationVisibilityTable(m Moderator) string {
	if m.PublishBeforeReview {
		return ""
	}
	return m.Table
}

func moderationActiveRowKey(m Moderator, table, rowID string) string {
	if m.PublishBeforeReview {
		return ""
	}
	return moderationRowKey(table, rowID)
}

func moderationRunVisibilityTable(run ModerationRun) string {
	if run.PublishBeforeReview {
		return ""
	}
	return run.Table
}

func moderationRunActiveRowKey(run ModerationRun) string {
	if run.PublishBeforeReview {
		return ""
	}
	return moderationRowKey(run.Table, run.RowID)
}

func isModerationSystemTable(table string) bool {
	return table == systemModeratorTableName || table == systemModerationRunTableName
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
