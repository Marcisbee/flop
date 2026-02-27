GOCACHE ?= /tmp/go-build-cache

gen:
	GOCACHE=$(GOCACHE) go run ./cmd/gen

dev:
	GOCACHE=$(GOCACHE) go run ./cmd/server

dev-gen: gen dev

build:
	GOCACHE=$(GOCACHE) go build -o movies-server ./cmd/server

run-bin:
	./movies-server

seed-imdb: gen
	GOCACHE=$(GOCACHE) go run ./cmd/seed -force

run: dev
