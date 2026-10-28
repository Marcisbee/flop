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

## Embedded application sessions

Trusted Go backends with their own sign-in flow can use Flop's session lifecycle
for an existing account in the configured auth table:

```go
// Authenticate the account first using your application's sign-in flow.
access, refresh, err := db.CreateSession(accountID)
// Handle err before returning or persisting either token.

// Renew with the refresh token, then persist both replacements together.
access, refresh, err = db.RefreshSession(refresh)

// At logout, use the current access token's validated identity.
auth, err := db.ValidateAccessToken(access)
if err == nil {
    err = db.RevokeSession(auth.ID, auth.SessionID)
}
```

`CreateSession` trusts the caller's authentication decision; never expose it as
an endpoint that accepts an unverified account ID. It loads the current account,
uses Flop's account validation and role normalization, and issues standard user
access and refresh tokens. It does not create an account or link a provider.

Refresh tokens are single-use. Refresh rotates the session and invalidates its
previous access and refresh tokens. Use the replacement session ID for logout.
Revocation checks account ownership, accepts repeated revocation, and leaves
other sessions alone. A trusted backend can retain the validated account and
session IDs to revoke after access-token expiry. No private session-table access
is needed. These methods return errors when user authentication is unconfigured;
superadmin sessions are outside this API.

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

Provider signup uses `POST /api/auth/provider/start` with
`{"provider":"google","intent":"sign_up"}` (and `appId` for multi-app configurations).
After the callback, submit its `completionCode` to `POST /api/auth/provider/complete`.
Flop creates the account, provider identity, optional grant, and authenticated
session atomically and returns the usual access and refresh tokens. Signup
requires a verified, nonempty email, either from the provider or the continuation
below. The auth table must accept the
standard registration fields, defaults, and any application fields selected by
`Config.ProviderSignup` (below). Accounts have no usable password until one is explicitly
set through the existing account recovery flow.

An existing provider identity returns `identity_already_linked`; use `sign_in`.
A unique email collision returns `link_required`; authenticate the existing
account and use the explicitly confirmed `link` flow. Provider email is never
used to select or link an existing account. `sign_in` continues to require a
previously linked identity and never creates accounts.

Applications with required profile fields can configure `ProviderSignup` and send
`signupFields` with the completion code:

```json
{"completionCode":"...","signupFields":{"handle":"new_user","display_name":"New User"}}
```

`Config.ProviderSignup` has the signature
`func(AuthProviderSignupRequest) (map[string]any, error)`. The request contains
`AppID`, the verified provider `Identity`, the verified account `Email`, and
untrusted `Fields`. Explicitly select allowed
profile fields and validate their types, lengths, and format in this callback;
return the selected account fields or an error. For example, an application can
require a handle of 4–15 lowercase letters, digits, or underscores and a display
name of 2–50 characters. It can also generate a handle from the verified identity
when no client fields are needed. Keep the callback side-effect-free: it runs
before the transaction, may run again after a corrected completion, and must not
call provider APIs. Do not use it to create accounts or sessions yourself.

Flop rejects unknown, primary-key, generated, password, role, email, verification,
and account-state fields in both the input and callback output. Without a
callback, nonempty `signupFields` are rejected. These fields are accepted only for
`sign_up`; provider identity claims always come from the verified flow.
Account fields are inserted in the same transaction as the identity and session.
Schema validation and unique constraints still apply. A callback rejection
returns `invalid_signup_fields` (400), invalid account data returns
`account_creation_failed` (400), and a profile uniqueness collision returns
`signup_conflict` (409). No account or session is created on rejection; clients
can correct fields and retry the same browser-bound completion code until it
expires. A successful completion remains single-use. Email collisions retain
`link_required` and never auto-link an account.

When a provider supplies no verified email (for example Steam), completion returns
`verified_email_required` without consuming the flow. Collect an email and send:

```http
POST /api/auth/provider/signup-email
Content-Type: application/json

{"completionCode":"...","email":"user@example.com"}
```

With SMTP configured, Flop returns 202 and sends the `provider-signup` email
template containing a verification token. Ask the user to paste that token into
the signup form, then complete with:

```json
{"completionCode":"...","emailVerificationToken":"token from email","signupFields":{"handle":"new_user","display_name":"New User"}}
```

No account or session exists before successful completion. `Email` in the signup
callback is the verified account address; `Identity.Email` and
`Identity.EmailVerified` remain the provider's original claims. Never treat an
unverified provider email as proof of email ownership.

Both requests require the original browser cookie. The challenge expires with
the completion code, five minutes after the provider callback; email requests
never extend this lifetime. Restart authorization after expiry. Requests are
limited to one per minute per flow, in addition to HTTP auth rate limits.
Requesting another email replaces the previous token and address. Tokens are
random, stored only as hashes, and are never returned by the HTTP API. SMTP
failure returns `email_delivery_failed` (502) and invalidates that token. A wrong
token returns `email_verification_invalid` (400). Corrected profile fields may be
retried with the same token until successful completion or expiry. Successful
completion consumes the flow and token together. An existing account with the
verified email still returns `link_required`; it is never selected or linked.

Provider signup, sign-in, linking, and consent must start and finish in the same browser.
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

## Concurrent explicit transactions

`flop.Transaction` groups durable commits from independent writers. Typed point
reads and writes reserve their rows through commit or rollback. If another
explicit transaction owns a needed row, the operation returns
`flop.ErrTransactionConflict`; callers can use `errors.Is` and retry the entire
transaction with bounded backoff. Callbacks are never replayed automatically.
Keep external side effects outside retried callbacks. Panics roll back buffered
writes before releasing reservations. Point reads through the transaction see its
own latest inserts, updates, and deletes, including repeated writes to one key.

These reservations do not provide snapshot isolation for scans or index queries,
and do not serialize ordinary writers with explicit transactions. Ordinary point
reads return the last committed value while a row update or delete is pending;
pending inserts remain invisible. These before-images exist only until commit or
rollback and are not a persistent read cache.

Explicit transactions persist their complete cross-table redo data in one shared,
checksummed log before acknowledgement. Recovery merges that data with ordinary
per-table WAL records by LSN. A database checkpoint retires the shared log only
after all tables are durable. Existing version-1 transaction journals are read on
upgrade. Opening a database upgrades its journal marker to version 2; older engine
versions reject it rather than silently ignore the new redo log. Keep a backup
before upgrading if you need to return to an older engine.

The
[embedded engine benchmark](benchmarks/engines/README.md) compares Flop,
SQLite, and Turso with equivalent workloads and full durability, including
conflict retries and correctness checks after reopening.
