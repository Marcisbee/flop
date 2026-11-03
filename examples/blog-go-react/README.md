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

## Development preview login

The example server can show a **Log in to demo** button on the admin login page.
It is off by default and is not part of the Flop library's login page.
Enable it only for a disposable development preview with sample data:

```sh
FLOP_DEMO_LOGIN=1 FLOP_DEMO_EMAIL='demo@example.test' FLOP_DEMO_PASSWORD='demo-password' make run
```

First provision that account through the ordinary admin setup flow. All three
variables are required. The button uses the normal login API; it does not bypass
authentication. Enabling it publishes the supplied demo credentials to preview
visitors. Omit these variables for ordinary runs and deployments.
