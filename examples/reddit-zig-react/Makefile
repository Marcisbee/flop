.PHONY: install build-js dev-js dev build run

install:
	npm install

build-js:
	npx esbuild web/src/index.tsx --bundle --outfile=web/assets/app.js \
		--jsx=automatic --target=es2020 --format=esm --sourcemap \
		--loader:.tsx=tsx --loader:.ts=ts --loader:.css=css

dev-js:
	npx esbuild web/src/index.tsx --bundle --outfile=web/assets/app.js \
		--jsx=automatic --target=es2020 --format=esm --sourcemap \
		--loader:.tsx=tsx --loader:.ts=ts --loader:.css=css --watch

dev: build-js
	cd ../.. && cd examples/reddit-zig-react && zig build run

build: build-js
	cd ../.. && cd examples/reddit-zig-react && zig build -Doptimize=ReleaseFast

run: dev