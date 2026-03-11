GOCACHE ?= /tmp/go-build-cache

dev:
	GOCACHE=$(GOCACHE) go run ./cmd/server

seed:
	GOCACHE=$(GOCACHE) go run ./cmd/seed -force

seed-limit:
	GOCACHE=$(GOCACHE) go run ./cmd/seed -force -limit 10000

build:
	GOCACHE=$(GOCACHE) go build -trimpath -ldflags="-s -w" -o movies-server ./cmd/server

run: dev
