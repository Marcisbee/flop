package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/evanw/esbuild/pkg/api"
)

var (
	// Match "async" keyword used as function modifier:
	//   async (       -> arrow function
	//   async function -> function declaration
	reAsyncArrow = regexp.MustCompile(`\basync\s+\(`)
	reAsyncFunc  = regexp.MustCompile(`\basync\s+function\b`)
	// Match "await" keyword before an expression.
	reAwait = regexp.MustCompile(`\bawait\s+`)
)

// StripAsync removes async/await keywords from bundled JS code.
// This is safe because all Go host functions (db operations) are synchronous —
// the async/await in user handlers is unnecessary overhead that forces QuickJS
// to use the expensive js_std_await microtask pump on every call.
func StripAsync(code string) string {
	code = reAsyncArrow.ReplaceAllString(code, "(")
	code = reAsyncFunc.ReplaceAllString(code, "function")
	code = reAwait.ReplaceAllString(code, "")
	return code
}

// BundleResult holds the output of bundling the user's app.ts.
type BundleResult struct {
	Code   string
	Errors []string
}

// BundleApp bundles the user's app.ts with the flop shim replacing the "flop" import.
func BundleApp(appPath string) (*BundleResult, error) {
	absPath, err := filepath.Abs(appPath)
	if err != nil {
		return nil, fmt.Errorf("resolve app path: %w", err)
	}

	if _, err := os.Stat(absPath); err != nil {
		return nil, fmt.Errorf("app file not found: %s", absPath)
	}

	// Write shim to a temp file
	shimDir, err := os.MkdirTemp("", "flop-shim-*")
	if err != nil {
		return nil, fmt.Errorf("create shim dir: %w", err)
	}
	defer os.RemoveAll(shimDir)

	shimPath := filepath.Join(shimDir, "mod.js")
	if err := os.WriteFile(shimPath, []byte(ShimSource), 0644); err != nil {
		return nil, fmt.Errorf("write shim: %w", err)
	}

	appDir := filepath.Dir(absPath)

	// Page components (.tsx/.jsx) should be external in the server bundle — they're only used
	// for metadata extraction and will be bundled separately for client-side delivery.
	externalPatterns := []string{
		"react", "react-dom", "react-dom/*", "react/*",
		"*.tsx", "*.jsx",
	}

	result := api.Build(api.BuildOptions{
		EntryPoints: []string{absPath},
		Bundle:      true,
		Format:      api.FormatIIFE,
		GlobalName:  "__FLOP_EXPORTS__",
		Platform:    api.PlatformNeutral,
		Target:      api.ES2020,
		Write:       false,
		Alias: map[string]string{
			"flop": shimPath,
		},
		// Inject import.meta values since IIFE doesn't have import.meta
		Define: map[string]string{
			"import.meta.dirname": fmt.Sprintf("%q", appDir),
			"import.meta.url":     fmt.Sprintf("%q", "file://"+absPath),
		},
		// Page components and React are external — they're bundled separately for client
		External: externalPatterns,
		// Don't tree-shake exports — we need all of them for discovery
		TreeShaking: api.TreeShakingFalse,
		// Treat .ts/.tsx files correctly
		Loader: map[string]api.Loader{
			".ts":  api.LoaderTS,
			".tsx": api.LoaderTSX,
		},
	})

	br := &BundleResult{}

	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			br.Errors = append(br.Errors, e.Text)
		}
		return br, nil
	}

	if len(result.OutputFiles) > 0 {
		br.Code = StripAsync(string(result.OutputFiles[0].Contents))
	}

	return br, nil
}

// BundleClientPages bundles React page components for client-side hydration.
type ClientBundleResult struct {
	JS     []byte
	CSS    []byte
	Errors []string
}

func BundleClientPages(routeTree interface{}, pageRoutes []FlatRoute, staticDir string) (*ClientBundleResult, error) {
	// Generate client entry point that imports all page components
	// and sets up client-side routing
	var imports string
	var routeEntries string

	for i, route := range pageRoutes {
		for j, cp := range route.ComponentPaths {
			varName := fmt.Sprintf("C%d_%d", i, j)
			// Resolve relative to staticDir
			absComponent := cp
			if !filepath.IsAbs(cp) {
				absComponent = filepath.Join(staticDir, cp)
			}
			imports += fmt.Sprintf("import %s from %q;\n", varName, absComponent)
			if j == len(route.ComponentPaths)-1 {
				routeEntries += fmt.Sprintf("  { pattern: %q, component: %s },\n", route.Pattern, varName)
			}
		}
	}

	entryCode := fmt.Sprintf(`
import { createElement, StrictMode } from "react";
import { createRoot } from "react-dom/client";

%s

const routes = [
%s];

const routeData = window.__FLOP_ROUTE__;
if (routeData) {
  const match = routes.find(r => r.pattern === routeData.pattern);
  if (match) {
    const root = createRoot(document.getElementById("root"));
    root.render(createElement(StrictMode, null, createElement(match.component, { params: routeData.params })));
  }
}
`, imports, routeEntries)

	entryDir, err := os.MkdirTemp("", "flop-client-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(entryDir)

	entryPath := filepath.Join(entryDir, "client-entry.jsx")
	if err := os.WriteFile(entryPath, []byte(entryCode), 0644); err != nil {
		return nil, err
	}

	result := api.Build(api.BuildOptions{
		EntryPoints: []string{entryPath},
		Bundle:      true,
		Format:      api.FormatESModule,
		Platform:    api.PlatformBrowser,
		Target:      api.ES2020,
		Write:       false,
		MinifySyntax:      true,
		MinifyWhitespace:  true,
		MinifyIdentifiers: true,
		JSX:               api.JSXAutomatic,
		Loader: map[string]api.Loader{
			".tsx": api.LoaderTSX,
			".ts":  api.LoaderTS,
			".jsx": api.LoaderJSX,
			".css": api.LoaderCSS,
		},
	})

	cbr := &ClientBundleResult{}

	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			cbr.Errors = append(cbr.Errors, e.Text)
		}
		return cbr, nil
	}

	for _, f := range result.OutputFiles {
		if filepath.Ext(f.Path) == ".js" {
			cbr.JS = f.Contents
		} else if filepath.Ext(f.Path) == ".css" {
			cbr.CSS = f.Contents
		}
	}

	return cbr, nil
}
