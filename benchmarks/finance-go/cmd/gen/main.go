package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/marcisbee/flop/benchmarks/finance-go/appschema"
	"github.com/marcisbee/flop/gen"
)

func main() {
	root, err := findModuleRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "finance-go gen: %v\n", err)
		os.Exit(1)
	}

	app := appschema.Build()
	spec := app.Spec()
	specPath := filepath.Join(root, ".flop", "spec.json")
	tsOut := filepath.Join(root, ".flop", "ts")
	goOut := filepath.Join(root, "appschema", "gen")

	if err := app.WriteSpec(specPath); err != nil {
		fmt.Fprintf(os.Stderr, "finance-go gen write spec: %v\n", err)
		os.Exit(1)
	}
	if err := gen.Generate(spec, gen.Options{
		OutDir:    tsOut,
		GoOutDir:  goOut,
		GoPackage: "gen",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "finance-go gen generate: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated spec: %s\n", specPath)
	fmt.Printf("generated ts:   %s\n", tsOut)
	fmt.Printf("generated go:   %s\n", goOut)
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", fmt.Errorf("go.mod not found from %s", dir)
		}
		dir = next
	}
}
