# flop

Production release surface is Go-only.

The active engine, tests, generators, and CI entrypoints live at the repository
root. Legacy runtime implementations have been removed so this repository
reflects the code we still support in production.

## What Ships

- repository root: active Flop engine and test suite
- `examples/blog-go-react`: minimal Go-first scaffold
- `examples/movies-go-react`: large-catalog demo
- `examples/twitter-go-react`: richer React demo backed by the Go engine
- `benchmarks/`: comparison harness and supported benchmark servers

## Core Features

- Code-first schema definitions
- Custom page-based storage with WAL durability
- Lazy loading with in-memory indexes
- Schema migration support
- Auth, admin UI, file uploads, and realtime handlers
- Configurable OpenRouter AI moderators with durable review and action history
- Go code generation for app schemas and frontend artifacts

## AI moderation

Enable OpenRouter when creating the app. `OpenRouterAPIKey` falls back to the
`OPENROUTER_API_KEY` environment variable.

```go
app := flop.New(flop.Config{
    DataDir: "./data",
    Moderation: &flop.ModerationConfig{
        // OpenRouterAPIKey: "...",
        Workers: 2,
    },
})
```

Open **AI Moderation** in the admin panel to configure moderators. Each
moderator selects:

- the table, mutation events, and content fields to review;
- an OpenRouter model and optional provider slug;
- whether rows are public while review is pending;
- allowed automatic actions (`review`, `delete`, or `block_user`);
- optional user fields and a cleared-item threshold for new-user review; and
- optional target table fields for report moderation.

Pending rows configured for pre-publication review are hidden from public API
and access-policy reads. Admin reads still expose them for review. Decisions,
categories, reasoning, errors, model/provider details, and actions are retained
as durable moderation runs. Failed runs can be retried, and an administrator
can allow, delete, or block from the review queue.

## Development

Run the Go test suite:

```sh
make test
```

Run the benchmark gate used by CI:

```sh
make pillar-gate
```

Run the complete production qualification locally:

```sh
make release-check
```

This runs formatting, vet, unit, race, reachable-vulnerability, crash-recovery,
and performance gates. `SyncMode` accepts only `full` (the durable default) or
`normal`; invalid values fail database startup.

Launch the maintained demo apps from the repository root:

```sh
make -C examples/blog-go-react dev
make -C examples/movies-go-react dev
make -C examples/twitter-go-react dev
```

## Notes

- [deno.json](/Users/marcisbee/Documents/GitHub/flop/deno.json) remains only for benchmark helper tasks.
- Some React demos ship checked-in browser assets so they can run without a root TypeScript runtime.
- Generated admin HTML under `internal/server/` is refreshed via `go generate`.
