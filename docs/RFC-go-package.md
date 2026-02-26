# RFC: Flop as a Go Package (Go-First Runtime)

Status: Draft (Phase 0)

## 1. Summary

This RFC defines the initial public API for turning Flop into a Go package:

- No QuickJS for app logic execution.
- Tables, views, reducers, and pages are defined in Go.
- TypeScript client/router/types are generated from Go metadata.
- React is first-class initially; core route manifest stays framework-agnostic.

## 2. Goals

- Keep app definition simple and explicit.
- Make `AutoTable` the default table-definition API.
- Preserve parity with current TS engine capabilities.
- Support one-port dev and prod serving for API + frontend.
- Keep runtime fast: no reflection in hot paths.

## 3. Non-Goals (Phase 0)

- Full implementation of runtime internals.
- Final production-grade route adapter for non-React frameworks.
- Fully finalized CLI UX for generator flags.

## 4. Proposed Public API (Initial Signatures)

```go
package flop

import "html/template"

// App lifecycle
type Config struct {
	DataDir  string
	SyncMode string
}
type App struct{}
func New(config Config) *App

// Built-in file value type for fileSingle/fileMulti fields.
type FileRef struct {
	Path string `json:"path"`
	URL  string `json:"url"`
	Mime string `json:"mime"`
	Size int64  `json:"size"`
}

// Table registration (default/simple path)
type Table[T any] struct{}
type TableBuilder[T any] struct{}
type FieldBuilder[T any] struct{}

func AutoTable[T any](app *App, name string, configure func(*TableBuilder[T])) *Table[T]

func (tb *TableBuilder[T]) Field(name string) *FieldBuilder[T]

// Field configuration
func (fb *FieldBuilder[T]) Primary() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Required() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Unique() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Default(value any) *FieldBuilder[T]
func (fb *FieldBuilder[T]) DefaultNow() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Autogen(pattern string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) Bcrypt(rounds int) *FieldBuilder[T]
func (fb *FieldBuilder[T]) Roles() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Timestamp() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Ref(other any, field string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) FileSingle(mime ...string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) FileMulti(mime ...string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) HasMany(other any, foreignField string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) BelongsTo(other any, foreignField string) *FieldBuilder[T]
func (fb *FieldBuilder[T]) Virtual() *FieldBuilder[T]
func (fb *FieldBuilder[T]) Index() *FieldBuilder[T]

// Table operations
type Update map[string]any
func Set(field string, value any) Update

func (t *Table[T]) Insert(scope any, row T) (T, error)
func (t *Table[T]) Get(scope any, id string) (*T, error)
func (t *Table[T]) Update(scope any, id string, updates ...Update) error
func (t *Table[T]) Delete(scope any, id string) (bool, error)
func (t *Table[T]) Scan(scope any, limit, offset int) ([]T, error)
func (t *Table[T]) Count(scope any) int

// Access and auth
type AccessPolicy struct {
	Type  string
	Roles []string
}
func Authenticated() AccessPolicy
func Public() AccessPolicy
func Roles(roles ...string) AccessPolicy

type AuthContext struct {
	ID    string
	Email string
	Roles []string
}
type RequestContext struct {
	Auth *AuthContext
}

// Views and reducers
type ViewCtx struct {
	Request RequestContext
}
type ReducerCtx struct {
	Request RequestContext
}
type Tx struct{}

func (ctx *ViewCtx) RequireAuth() (*AuthContext, error)
func (ctx *ReducerCtx) RequireAuth() (*AuthContext, error)

func Transaction[T any](ctx *ReducerCtx, fn func(*Tx) (T, error)) (T, error)

func View[In, Out any](app *App, name string, access AccessPolicy, handler func(*ViewCtx, In) (Out, error))
func Reducer[In, Out any](app *App, name string, access AccessPolicy, handler func(*ReducerCtx, In) (Out, error))

// SSR pages and head
type MetaTag struct {
	Name    string
	Content string
}
type LinkTag struct {
	Rel  string
	Href string
}
type ScriptTag struct {
	Type    string
	Content string
	Src     string
}
type OpenGraph struct {
	Title       string
	Description string
	Type        string
	Image       string
}
type Head struct {
	Title     string
	Charset   string
	Viewport  string
	Meta      []MetaTag
	Link      []LinkTag
	Script    []ScriptTag
	OG        *OpenGraph
	RawHTML   template.HTML
}

type LoaderCtx struct {
	Request RequestContext
}
type HeadCtx[P, D any] struct {
	Params P
	Data   D
}

type LayoutConfig struct {
	Entry       string
	Head        func(*HeadCtx[struct{}, struct{}]) (Head, error)
	RawHeadHTML func(*HeadCtx[struct{}, struct{}]) (template.HTML, error)
}
type PageConfig[P, D any] struct {
	Entry       string
	Loader      func(*LoaderCtx, P) (D, error)
	Head        func(*HeadCtx[P, D]) (Head, error)
	RawHeadHTML func(*HeadCtx[P, D]) (template.HTML, error)
}

func Layout(app *App, path string, cfg LayoutConfig)
func Page[P, D any](app *App, path string, cfg PageConfig[P, D])
```

## 5. Definition Modes

- Default: `AutoTable[T]` + typed builder.
- Optional: struct tags for teams that prefer annotation-heavy schema declaration.
- Both modes must produce identical internal schema metadata.

## 6. Type Generation Outputs

Generator command:

```sh
go run ./cmd/flop-gen
```

Generated files:

- `web/src/flop.gen.ts` (API/client/table/result types)
- `web/src/flop.valibot.gen.ts` (Valibot validators)
- `web/src/routes.gen.ts` (route manifest and route param typing)
- `web/src/flop.react-router.gen.tsx` (React router glue with parallel chunk+loader behavior)

## 7. Routing/SSR Behavior

- Server route match returns route tree and params.
- Loader execution and JS chunk resolution run in parallel.
- Head is computed from typed loader data.
- Head tags are rendered to HTML on server.
- `RawHeadHTML` is escape hatch for advanced/custom tags.

## 8. One-Port Frontend Serving

Dev:

- Go server listens on one port (e.g. `:1985`).
- Vite routes/HMR ws are reverse-proxied by Go.
- SSR shell and API are served from same origin.

Prod:

- Go reads Vite manifest and injects entry/chunk/css preloads.
- Optional embedded assets for single-binary deploy.

## 9. Open Questions

- Exact field selection API for `TableBuilder.Field`:
  - string-based (`"AuthorID"`) initially, typed handles in generator later.
- Default route data transport shape for React adapter.
- Admin panel strategy: keep embedded, generated, or separate package.
- Scope of phase-1 WebSocket parity.

## 10. Phase 0 Deliverables

- This RFC.
- Compileable public scaffolding package for the proposed signatures.
- No behavior guarantees yet; this is contract-first groundwork for implementation phases.

