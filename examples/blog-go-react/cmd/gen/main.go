package main

import (
	"fmt"
	"os"
	"path/filepath"

	blog "github.com/marcisbee/flop/examples/blog-go-react/app"
	"github.com/marcisbee/flop/gen"
)

func main() {
	projectRoot, err := findModuleRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "generator error: %v\n", err)
		os.Exit(1)
	}

	application := blog.Build()
	spec := application.Spec()

	specPath := filepath.Join(projectRoot, ".flop", "spec.json")
	outDir := filepath.Join(projectRoot, "web", "src", "generated")
	goOutDir := filepath.Join(projectRoot, "app")

	if err := application.WriteSpec(specPath); err != nil {
		fmt.Fprintf(os.Stderr, "write spec error: %v\n", err)
		os.Exit(1)
	}

	if err := gen.Generate(spec, gen.Options{
		OutDir:    outDir,
		GoOutDir:  goOutDir,
		GoPackage: "app",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "generate error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated spec: %s\n", specPath)
	fmt.Printf("generated ts:   %s\n", outDir)
	fmt.Printf("generated go:   %s\n", goOutDir)
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
