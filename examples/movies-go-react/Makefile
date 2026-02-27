GOCACHE ?= /tmp/go-build-cache

gen:
	GOCACHE=$(GOCACHE) go run ./cmd/gen

dev: gen
	GOCACHE=$(GOCACHE) go run ./cmd/server

seed-imdb: gen
	GOCACHE=$(GOCACHE) go run ./cmd/seed -force

run: dev
