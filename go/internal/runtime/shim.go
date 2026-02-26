package runtime

import _ "embed"

// ShimSource is the JavaScript shim that replaces the "flop" module.
// When esbuild bundles app.ts, it resolves "flop" to this shim.
// The shim captures all table/view/reducer/route definitions and stores them
// in globalThis.__FLOP_META__ for Go to extract.
//
// Source of truth: shared/shim.js (copied here by go generate)
//
//go:embed shim.js
var ShimSource string
