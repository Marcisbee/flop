# movies-go-react

Go-first Flop demo app for large movie catalogs.

## Features

- Bulk import endpoint for huge synthetic datasets (`POST /api/import/movies?count=50000`)
- Prefix autocomplete optimized by in-memory sorted index
- Movie detail pages at `/movie/{slug}`
- Full-text indexes on title and overview
- Admin panel at `/_`

## Run

From repository root:

```sh
make -C examples/movies-go-react dev
```

Open `http://localhost:1985`.

Notes:
- `dev` starts server directly (fast path).
- `dev-gen` regenerates spec/artifacts before starting server.
- `build` + `run-bin` gives fastest repeat startup (no `go run` compile on each boot).

## Seed With IMDb Data

IMDb non-commercial datasets can be imported into the demo:

```sh
make -C examples/movies-go-react seed-imdb
```

This downloads:
- `title.basics.tsv.gz`
- `title.ratings.tsv.gz`

Then it imports all `titleType=movie` rows into the `movies` table.

Useful options:

```sh
# import only first 100k movies
GOCACHE=/tmp/go-build-cache go run ./cmd/seed -force -limit 100000

# skip download and use existing files in data/_datasets/imdb
GOCACHE=/tmp/go-build-cache go run ./cmd/seed -force -download=false

# tune buffered write batch size for your machine
GOCACHE=/tmp/go-build-cache go run ./cmd/seed -force -batch 5000
```

Dataset terms and source:
- https://developer.imdb.com/non-commercial-datasets/
- https://datasets.imdbws.com/

## API

- `GET /api/view/get_stats`
- `GET /api/view/list_movies?limit=36&offset=0`
- `GET /api/view/autocomplete_movies?q=blade&limit=10`
- `GET /api/view/get_movie_by_slug?slug=blade-runner-1982`
- `POST /api/import/movies?count=50000`
