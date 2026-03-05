.PHONY: install build-sdk-js build-js dev-js dev seed build run

GOCACHE ?= /tmp/go-build-cache

install:
	npm install

build-sdk-js:
	npx esbuild ../../ts/client.ts --bundle --outfile=web/assets/flop-client.js \
		--target=es2020 --format=esm --sourcemap --loader:.ts=ts

build-js: build-sdk-js
	npx esbuild web/src/index.tsx --bundle --outfile=web/assets/app.js \
		--jsx=automatic --target=es2020 --format=esm --sourcemap \
		--loader:.tsx=tsx --loader:.ts=ts --loader:.css=css

dev-js:
	npx esbuild web/src/index.tsx --bundle --outfile=web/assets/app.js \
		--jsx=automatic --target=es2020 --format=esm --sourcemap \
		--loader:.tsx=tsx --loader:.ts=ts --loader:.css=css --watch

dev: build-js
	GOCACHE=$(GOCACHE) go run ./cmd/server

seed:
	GOCACHE=$(GOCACHE) go run ./cmd/seed -force

build: build-js
	GOCACHE=$(GOCACHE) go build -trimpath -ldflags="-s -w" -o twitter-server ./cmd/server

run: dev
