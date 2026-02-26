# Flop Go Package Migration Plan

## 1. Goal

Move Flop from a TypeScript+QuickJS runtime model to a Go-first package model:

- App logic (tables, reducers, views, routes/pages/head) runs in Go.
- Flop is imported as a Go package inside any Go app.
- TypeScript types and client/router artifacts are generated from Go definitions.
- Frontend and API run on one server/port in both dev and prod.
- Keep API surface very simple (AutoTable-first), with typed relations and minimal boilerplate.

## 2. Design Principles

- Simple by default: `AutoTable` + typed handlers + small route API.
- One source of truth: Go definitions; TS is generated.
- No runtime reflection in hot paths: reflection/tag parsing only at startup or generation time.
- Keep typed head API; render HTML tags server-side.
- Framework-portable route manifest; first-class React adapter first.
- Do not force Zod; use Valibot for generated validators.

## 3. Target Public API (v1)

### 3.1 Tables

- `flop.AutoTable[T](app, "table_name", configureFn)`
- Struct defines field shape.
- `configureFn` only defines constraints/defaults/indexes/relations.
- `flop.FileRef` is built-in package type for file fields.

Example shape:

```go
users := flop.AutoTable[User](app, "users", func(t *flop.TableBuilder[User]) {
  t.Field("ID").Primary().Autogen(`[a-z0-9]{12}`)
  t.Field("Email").Required().Unique()
  t.Field("Password").Bcrypt(10).Required()
  t.Field("Roles").Roles()
})
```

### 3.2 Relations (simple and explicit)

- Preferred: explicit FK field (`AuthorID`) + relation helper.
- Reverse relations are virtual only (not persisted).
- Minimal API:
  - `.Ref(otherTable, "ID")`
  - `.HasMany(otherTable, "AuthorID").Virtual()`
  - `.BelongsTo(otherTable, "AuthorID").Virtual()`

### 3.3 Views and Reducers

- `flop.View[In, Out](app, "name", access, handler)`
- `flop.Reducer[In, Out](app, "name", access, handler)`
- `ctx.RequireAuth()` helper.
- `ctx.Transaction(...)` helper.
- Access policies: authenticated(default), public, roles.

### 3.4 Pages, SSR Head, Loader

- `flop.Layout(path, config)` and `flop.Page[Params, Data](path, config)`
- `Loader` and chunk resolution run in parallel.
- `Head` is typed Go object, rendered to HTML.
- Optional `RawHeadHTML` escape hatch for advanced tags.

### 3.5 Definition Modes (Keep It Simple)

- Primary mode: `AutoTable[T]` with typed builder config.
- Optional mode: struct tags for teams that prefer declarative annotations.
- Both modes compile to the same internal schema metadata.
- `AutoTable` remains the documented default to avoid duplication and weird syntax.

## 4. Runtime Architecture

### 4.1 Go Packages

- `flop/core`: schema, DB engine, transactions, WAL, migrations.
- `flop/app`: table/view/reducer/page registration and metadata.
- `flop/http`: API handlers, auth, SSE, file serving, admin APIs.
- `flop/web`: SSR render pipeline, head rendering, route matching.
- `flop/assets`: dev/prod asset providers (Vite dev, Vite manifest, embed).
- `flop/gen`: code generation for TS types/client/routes/validators.
- `flop/react`: React adapter for generated route manifest.

### 4.2 What Gets Removed

- QuickJS runtime execution for app logic.
- JS bridge/shim for reducers/views execution.
- Runtime JS param/result marshaling for core logic.

### 4.3 What Stays/Reused

- Existing Go storage engine work (page file, WAL, indexes, pubsub, auth, files).
- Existing HTTP/auth/admin concepts (ported to new registration model).

## 5. Feature Parity Checklist (Current TS Engine -> Go Package)

- Tables: schema kinds, defaults, required/unique, autogen, enum, set, vector, json, timestamp.
- Auth table semantics and JWT auth flows.
- References: `refSingle`, `refMulti`, reverse virtual relationships.
- File fields with MIME validation and lifecycle cleanup.
- Views/reducers with access control and transactions.
- Route tree + page component mapping.
- SSR head builder with dynamic data access.
- SSE (single and multiplexed), backpressure-safe fanout.
- WebSocket realtime endpoint (phase 3+ parity).
- Migrations: schema diff, rename/transform, lazy row migration.
- Admin API and admin UI support.
- Backup/restore endpoints and setup flow (first superadmin bootstrap).
- Sync/durability modes (`normal`, `full`, etc.) and checkpoint controls.
- WebSocket/SSE auth token handling parity.
- Typed client SDK generation.

## 6. Type Generation Pipeline

Command:

```sh
go run ./cmd/flop-gen
```

Generated artifacts:

- `web/src/flop.gen.ts`
  - table row types
  - reducer/view input/output types
  - typed `FlopClient` bindings
- `web/src/flop.valibot.gen.ts`
  - valibot schemas for params/payloads
- `web/src/routes.gen.ts`
  - typed route manifest + route param types
- `web/src/flop.react-router.gen.tsx`
  - React Router objects + loader wiring

Generation rules:

- JSON names from `json` tag when present; else stable camelCase conversion.
- Struct tags are optional; configuration API remains source for constraints.
- Use schema snapshot hash to avoid unnecessary regen.

## 7. Frontend Integration (One Port)

Dev mode:

- Go server runs on `:1985`.
- `ViteDevProvider` proxies:
  - `/@vite/*`
  - `/src/*`
  - HMR websocket endpoint
- HTML shell rendered by Go includes Vite client + entry module.

Prod mode:

- `ViteManifestProvider` reads `dist/.vite/manifest.json`.
- Go injects modulepreload + CSS links + entry script.
- Optional `EmbedProvider` serves bundled assets from Go binary.

## 8. React-First Router Design (Framework-Portable Core)

Core manifest (framework-agnostic):

- path pattern
- route id
- component chunk entry key
- loader key
- parent-child structure

React adapter behavior:

- route `lazy()` for chunk loading.
- route `loader()` for data fetch.
- load both in parallel (`Promise.all` semantics).
- typed loader data and params from generated types.

Future Vue/Solid adapters consume same manifest and generated types.

## 9. Validation Strategy

- Go runtime validates request payloads using generated schema metadata.
- TS side uses generated Valibot schemas.
- Keep validator lightweight; no Zod dependency.

## 10. Example Deliverable: `examples/blog-go-react`

Deliver:

- `examples/blog-go-react/app/app.go` (Go app definition)
- `examples/blog-go-react/web/` (React app using generated routes/types)
- `examples/blog-go-react/cmd/server/main.go` (single binary server)
- `examples/blog-go-react/Makefile` targets
  - `make gen`
  - `make dev`
  - `make build`
  - `make run`

Expected dev flow:

```sh
make gen
make dev
# open http://localhost:1985
```

## 11. Execution Phases

### Phase 0 - Foundation and API freeze

- Finalize minimal public API (`AutoTable`, `View`, `Reducer`, `Page`, `Layout`).
- Freeze naming conventions for generated TS outputs.
- Define compatibility policy for future adapters.

Exit criteria:

- API doc approved.
- Tiny sample app compiles with no generator yet.

### Phase 1 - Go app registry + HTTP runtime

- Implement app registry for tables/views/reducers/pages.
- Bind registry to existing storage engine.
- Implement API endpoints from registry metadata.
- Port auth and file routes to package model.

Exit criteria:

- CRUD + views/reducers + auth work in Go-only app.

### Phase 2 - Generator + TS client/types

- Implement `cmd/flop-gen`.
- Emit typed API client and Valibot validators.
- Emit route manifest + typed params.

Exit criteria:

- Type-safe frontend calls generated without manual TS typing.

### Phase 3 - React SSR and asset pipeline

- Implement React adapter and SSR shell integration.
- Implement Vite dev proxy provider on same port.
- Implement prod manifest rendering and preloads.

Exit criteria:

- `examples/blog-go-react` launches in dev/prod with one server port.

### Phase 4 - Realtime and admin parity

- Finalize SSE multiplex and targeted table dependency wiring.
- Add WebSocket parity endpoint.
- Port admin APIs; add simple admin UI integration strategy.

Exit criteria:

- Realtime + admin parity with current engine.

### Phase 5 - Performance hardening

- Benchmark against current finance benchmark.
- Optimize hot paths (serialization, scans, pubsub fanout, WAL batching).
- Add profiling baselines and regression thresholds.

Exit criteria:

- Throughput and latency targets documented and met.

## 12. Risks and Mitigations

- Boilerplate growth risk.
  - Mitigation: keep `AutoTable` defaults strong; generator handles TS boilerplate.
- Reflection/tag complexity risk.
  - Mitigation: parse once at startup/gen; runtime uses compiled metadata.
- Frontend lock-in risk.
  - Mitigation: framework-agnostic route manifest as core contract.
- Dev server complexity risk.
  - Mitigation: isolate in `assets` package with strict interface and tests.

## 13. Immediate Next Steps

- Create `docs/RFC-go-package.md` with finalized API signatures.
- Scaffold `flop/app`, `flop/gen`, `flop/assets`, `flop/react`.
- Start `examples/blog-go-react` with minimal home/about routes.
- Add `cmd/flop-gen` that emits a first `flop.gen.ts` and route manifest.
