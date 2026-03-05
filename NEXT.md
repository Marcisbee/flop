# Flop NEXT API: App Development Guide

This document describes the proposed **NEXT** app API:

- reducers/views are registered as **separate typed functions**
- request validation is strict (invalid input is rejected)
- table/row/cell access is enforced in the engine (optional policies)
- table dependencies are tracked at **runtime** (reads/writes)
- JS SDK uses `client.view(name, params)` and `client.reducer(name, params)`
- multiple `view()` calls in one frame are auto-batched (single call stays direct)
- reducer writes auto-trigger refetch of affected watched views

## Goals

1. Keep handlers native-fast (plain Go functions).
2. Keep developer experience simple and type-safe.
3. Make UI refresh behavior automatic and correct.

## 1. Define your app schema

```go
package app

import flop "github.com/marcisbee/flop"

func Build() *flop.App {
	app := flop.New(flop.Config{
		DataDir:               "./data",
		SyncMode:              "normal",
		AsyncSecondaryIndexes: true,
	})

	users := flop.Define(app, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 10).Required()
		s.String("name").Required()
		s.Roles("roles")
	})

	projects := flop.Define(app, "projects", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("name").Required()
		s.Ref("ownerId", users, "id").Required().Index()
		s.Timestamp("createdAt").DefaultNow()
	})

	tasks := flop.Define(app, "tasks", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.Ref("projectId", projects, "id").Required().Index()
		s.String("title").Required()
		s.String("description")
		s.String("internalNotes").Access(flop.FieldAccess{
			Read: func(c *flop.TableReadCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
			Write: func(c *flop.FieldWriteCtx) bool {
				return c.Auth != nil && c.Auth.HasRole("admin")
			},
		})
		s.Enum("status", "todo", "in_progress", "done").Required().Default("todo").Index()
		s.Integer("position").Required().Default(0).Index()
		s.Ref("createdBy", users, "id").Required().Index()
		s.Timestamp("updatedAt").DefaultNow()
		s.Access(flop.TableAccess{
			// same view code can run for all users; engine filters rows automatically.
			Read: func(c *flop.TableReadCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return c.Auth.ID == fmt.Sprintf("%v", c.Row["createdBy"])
			},
			Insert: func(c *flop.TableInsertCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return c.Auth.ID == fmt.Sprintf("%v", c.New["createdBy"])
			},
			Update: func(c *flop.TableUpdateCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return c.Auth.ID == fmt.Sprintf("%v", c.Old["createdBy"])
			},
			Delete: func(c *flop.TableDeleteCtx) bool {
				if c.Auth == nil {
					return false
				}
				if c.Auth.HasRole("admin") {
					return true
				}
				return c.Auth.ID == fmt.Sprintf("%v", c.Row["createdBy"])
			},
		})
	})

	flop.Define(app, "task_assignments", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.Ref("taskId", tasks, "id").Required().Index()
		s.Ref("userId", users, "id").Required().Index()
		s.Timestamp("assignedAt").DefaultNow()
	})

	flop.Define(app, "task_comments", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.Ref("taskId", tasks, "id").Required().Index()
		s.Ref("authorId", users, "id").Required().Index()
		s.String("body").Required()
		s.Timestamp("createdAt").DefaultNow()
	})

	return app
}
```

## 2. Register views/reducers as typed functions

No switch-based mega-handler. Each endpoint is a dedicated function with typed input/output.

```go
type GetProjectBoardIn struct {
	ProjectID string `json:"projectId"`
}

type MoveTaskIn struct {
	TaskID    string `json:"taskId"`
	Status    string `json:"status"` // todo | in_progress | done
	Position  int64  `json:"position"`
}

type AddTaskCommentIn struct {
	TaskID string `json:"taskId"`
	Body   string `json:"body"`
}

func RegisterEndpoints(app *flop.App) {
	flop.View(app, "get_project_board", flop.Authenticated(), GetProjectBoard)
	flop.Reducer(app, "move_task", flop.Authenticated(), MoveTask)
	flop.Reducer(app, "add_task_comment", flop.Authenticated(), AddTaskComment)
}
```

Handler examples:

```go
func GetProjectBoard(ctx *flop.ViewCtx, in GetProjectBoardIn) (any, error) {
	if in.ProjectID == "" {
		return nil, fmt.Errorf("projectId is required")
	}
	// read tasks + assignments + comments
	// runtime tracker automatically marks read tables
	return map[string]any{}, nil
}

func MoveTask(ctx *flop.ReducerCtx, in MoveTaskIn) (any, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("taskId is required")
	}
	switch in.Status {
	case "todo", "in_progress", "done":
	default:
		return nil, fmt.Errorf("invalid status")
	}
	// update tasks row
	// runtime tracker automatically marks write tables
	return map[string]any{"ok": true}, nil
}

func AddTaskComment(ctx *flop.ReducerCtx, in AddTaskCommentIn) (any, error) {
	if in.TaskID == "" || strings.TrimSpace(in.Body) == "" {
		return nil, fmt.Errorf("taskId and body are required")
	}
	// insert into task_comments
	// runtime tracker automatically marks write tables
	return map[string]any{"ok": true}, nil
}
```

## 3. Validation model

Incoming data is rejected if invalid:

1. Unknown fields are rejected.
2. Type mismatch is rejected.
3. Missing required fields are rejected.
4. Handler-level business validation still applies.

Result: invalid requests fail fast with `400`.

## 3.5 Engine-level table access (automatic filtering)

Table access is optional. If no access functions are attached, performance stays on fast-path with no policy checks.

When access functions are attached:

1. `Get`, `Scan`, `Find...`, `SearchFullText` automatically return only readable rows.
2. `limit/offset` are applied to the **visible** rows.
3. Field `.Access(...)` hides protected cells from returned rows.
4. `Insert/Update/Delete` are rejected with `403` when policy returns false.

This means reducers/views stay focused on business logic. You do not repeat owner/admin checks in each handler.

## 4. Runtime dependency tracking

Tracking is runtime-only (no compile-time dependency declarations required).

- View execution collects read tables.
- Reducer execution collects write tables.

Response metadata:

- views: `X-Flop-Reads: tasks,task_assignments,task_comments,users`
- reducers: `X-Flop-Writes: task_comments`

This is used by the SDK for automatic invalidation/refetch.

## 5. JS SDK usage (no proxy)

Use explicit endpoint names:

```ts
const client = new Flop<AppSchema>({
  host: "http://localhost:1985",
  batchViews: "frame",
  autoRefetch: true,
});

const board = await client.view("get_project_board", { projectId: "p1" });
await client.reducer("add_task_comment", { taskId: "t1", body: "Looks good" });
```

### Auto-batched `view()`

`client.view(...)` calls are frame-coalesced:

1. if a flush has exactly 1 view call, SDK sends direct `GET /api/view/{name}`
2. if a flush has 2+ calls, SDK sends one `POST /api/view/_batch`

Batch request example:

```json
{
  "calls": [
    { "id": "1", "name": "get_project_board", "params": { "projectId": "p1" } },
    { "id": "2", "name": "get_task_detail", "params": { "taskId": "t1" } }
  ]
}
```

- response:

```json
{
  "results": [
    { "id": "1", "data": {} },
    { "id": "2", "data": {} }
  ]
}
```

The SDK resolves each original `view()` Promise as if it was a separate request.

### Auto-refetch after reducers

When reducer response includes `X-Flop-Writes`, SDK:

1. checks active watched views
2. compares touched tables vs each view's last tracked reads
3. re-fetches only affected views (also batched)

## 6. Watch API for UI

```ts
const stop = client.watch("get_project_board", { projectId: "p1" }, (nextBoard) => {
  renderBoard(nextBoard);
});

await client.reducer("move_task", {
  taskId: "t1",
  status: "in_progress",
  position: 200,
});

// later:
stop();
```

`watch()` automatically benefits from batched re-fetch and reducer-based invalidation.

## 7. Generated TypeScript types

`flop-gen` should generate:

```ts
export interface FlopViews {
  get_project_board: { input: { projectId: string }; output: ProjectBoard };
  get_task_detail: { input: { taskId: string }; output: TaskDetail };
}

export interface FlopReducers {
  move_task: { input: MoveTaskIn; output: Task };
  add_task_comment: { input: AddTaskCommentIn; output: TaskComment };
}
```

App schema adapter:

```ts
type AdaptViews<V> = {
  [K in keyof V]: {
    params: V[K] extends { input: infer I } ? I : never;
    result: V[K] extends { output: infer O } ? O : never;
  };
};

type AdaptReducers<R> = {
  [K in keyof R]: {
    params: R[K] extends { input: infer I } ? I : never;
    result: R[K] extends { output: infer O } ? O : never;
  };
};
```

## 8. Performance notes

1. Separate endpoint functions avoid giant branching handlers.
2. Runtime tracking overhead should stay low with table-id bitsets.
3. Auto-batching reduces network/request overhead in UI-heavy screens with multiple concurrent view reads.
4. Auto-refetch avoids stale UI without manual invalidation glue.

## 8.5 Server wiring

Use default engine mounts so `/api` and `/_` are available without custom routing glue:

```go
mux := http.NewServeMux()
mounts := flop.MountDefaultHandlers(mux, app, db) // mounts API + admin
_ = mounts
```

Then only add app-specific routes you actually need (for example `/api/head` or static assets).

## 9. Recommended app structure

```text
app/
  schema.go
  endpoints/
    views.go
    reducers.go
  server/
    main.go
web/
  src/
    generated/flop.gen.ts
    api/client.ts
    features/kanban/
```

Keep schema, endpoint registration, and endpoint business logic separate.
