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

Start with [the development and verification guide](VERIFY.md) for setup,
focused checks, repeatable performance comparisons, and handoff requirements.

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

- [deno.json](deno.json) remains only for benchmark helper tasks.
- Some React demos ship checked-in browser assets so they can run without a root TypeScript runtime.
- Generated admin HTML under `internal/server/` is refreshed via `go generate`.

## Security upgrade notes

Build with Go 1.26.10 or a newer patched release. Applications embedding Flop
must update their dependency and rebuild to receive engine and runtime fixes.

New password hashes use PBKDF2-HMAC-SHA256 with 600,000 iterations, following
[OWASP's password storage guidance](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html).
Existing PBKDF2 and bcrypt passwords remain readable. Built-in user and admin
login upgrade the old 10,000-iteration PBKDF2 hashes after successful password
verification. Dormant accounts retain their old hashes until login or password
reset. Custom authentication using `VerifyPassword` must persist its own upgrades.
An older Flop binary cannot verify the new hash format; take a backup before
upgrading and avoid rolling back to an older authentication implementation.

Email verification, email change, and password reset links are now single-use
and bound to the account's current email, password hash, and verification state.
The engine adds `_auth_used_tokens` automatically and expires consumed-token
records in its existing cleanup worker. Previously issued email action links
must be requested again. If confirmation is interrupted after consuming a link,
request a new one. Existing access and refresh sessions remain usable for their
respective purposes; an access token can no longer renew a session.

Provider sign-in, linking, and consent must start and finish in the same browser.
The start endpoint sets an HttpOnly, Secure, host-only, SameSite=Lax cookie;
loopback development uses a non-Secure cookie. Keep cookies when calling start
and complete, and serve these endpoints on the application's site. Same-site
cross-origin clients also need credentialed fetch and an explicit credentialed
CORS configuration in their host application. Cross-site frontends should proxy
the flow through their own site. Non-browser HTTP clients must preserve the
cookie across start, callback, and complete. In-flight provider flows from an
older version must restart. The in-process provider backend credential APIs are
unchanged.

Admin authentication JSON is limited to 1 MiB and other admin JSON to 32 MiB.
Admin login throttling is per direct peer, independent of email casing or spelling.
User and admin event streams revalidate credentials before delivery and on each
heartbeat; clients must refresh expired access tokens and reconnect.

`FileHandler` retains public-file semantics. Use `PolicyFileHandler` when file
reads must follow table policies; policy-protected responses use `private, no-store`
to avoid reuse after logout or account switching. SSE no longer emits table activity
for updates that are unreadable both before and after the change. Flop retains
exact-case email semantics.
