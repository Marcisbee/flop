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
- Configurable OpenRouter AI workflows with durable review and action history
- Go code generation for app schemas and frontend artifacts

## AI workflows

Enable OpenRouter when creating the app. `OpenRouterAPIKey` falls back to the
`OPENROUTER_API_KEY` environment variable.

```go
app := flop.New(flop.Config{
    DataDir: "./data",
    Workflow: &flop.WorkflowConfig{
        // OpenRouterAPIKey: "...",
        Workers: 2,
    },
})
```

Open **AI Workflows** in the admin panel to configure reusable automations.
Workflows support:

- row insert/update, report, Discord, and manual triggers;
- conditions and indexed `get`, index, or full-text search lookups;
- an OpenRouter model, optional provider, prompt, and JSON Schema result;
- automatic image inputs for nested Flop image file references when the selected model supports images;
- a per-workflow provider data-collection policy that defaults to `deny`;
- approve, review queue, delete, archive, block, and create/propose-alias actions;
- per-action human approval, row visibility holds, and automatic retries; and
- durable input, lookup, structured result, reasoning, status, retry, and error history.

The admin includes new-user and reported-content moderation templates plus a
Discord game-reconciliation template that searches game candidates and creates
or proposes an alias. Pending rows configured with `HoldUntilComplete` are
hidden from public API and access-policy reads while admin reads remain
available for review.

Applications can replace those conventional starter templates with definitions
that match their own tables and fields:

```go
Workflow: &flop.WorkflowConfig{
    Templates: []flop.WorkflowTemplate{
        {
            ID:   "app-post-moderator",
            Name: "App post moderator",
            Workflow: flop.Workflow{
                // Application-specific trigger, paths, and actions.
            },
        },
    },
},
```

Set `Templates` to an empty non-nil slice to hide all starters. An `archive`
workflow action uses the table's normal cascade-archive semantics.

Applications can dispatch external events or queue a manual workflow:

```go
err := db.DispatchWorkflowEvent("discord", "activity_mismatch", payload)
run, err := db.RunWorkflow(workflowID, input)
```

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
and performance gates. The performance gate uses the median of three fresh
samples so a transient host stall cannot mask or imitate a sustained regression;
the crash-recovery matrix runs once. `SyncMode` accepts only `full` (the durable
default) or `normal`; invalid values fail database startup.

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
