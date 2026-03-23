# flop

Production release surface is now Go-first.

The active engine, tests, generators, and CI entrypoints live in `go/`. Legacy
`go2` and Deno/TypeScript runtime implementations have been removed so this
repository reflects the code we still support.

## What Ships

- `go/`: active Flop engine and test suite
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
- Go code generation for app schemas and frontend artifacts

## Development

Run the Go test suite:

```sh
cd go
make test
```

Run the benchmark gate used by CI:

```sh
cd go
make pillar-gate
```

Launch the maintained demo apps from the repository root:

```sh
make -C examples/blog-go-react dev
make -C examples/movies-go-react dev
make -C examples/twitter-go-react dev
```

## Notes

- [deno.json](/Users/marcisbee/Documents/GitHub/flop/deno.json) remains only for benchmark helper tasks.
- Some React demos ship checked-in browser assets so they can run without a root TypeScript runtime.
- Generated admin HTML under `go/internal/server/` is refreshed via `go generate`.
