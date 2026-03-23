# blog-go-react

Minimal Go-first Flop scaffold with generated TypeScript artifacts and a launchable demo server.

## Run

From the repository root:

```sh
make -C examples/blog-go-react dev
```

Open `http://localhost:1985`.

Available pages:

- `http://localhost:1985/`
- `http://localhost:1985/about`
- `http://localhost:1985/post/go-first-flop`
- `http://localhost:1985/_` (admin panel, read-only demo)

## Generated files

`make gen` writes:

- `examples/blog-go-react/.flop/spec.json`
- `examples/blog-go-react/app/flop.models.gen.go`
- `examples/blog-go-react/app/flop.tables.gen.go`
- `examples/blog-go-react/web/src/generated/flop.gen.ts`
- `examples/blog-go-react/web/src/generated/flop.valibot.gen.ts`
- `examples/blog-go-react/web/src/generated/routes.gen.ts`
- `examples/blog-go-react/web/src/generated/flop.react-router.gen.tsx`
