package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/marcisbee/flop/gen"
)

func main() {
	specPath := flag.String("spec", "", "path to app spec json (required)")
	outDir := flag.String("out", ".", "output directory for generated ts files")
	goOutDir := flag.String("go-out", "", "output directory for generated go files (optional)")
	goPkg := flag.String("go-package", "flopgen", "package name for generated go files")
	flag.Parse()

	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "usage: flop-gen -spec <path-to-spec.json> [-out <dir>] [-go-out <dir>] [-go-package <name>]")
		os.Exit(2)
	}

	if err := gen.GenerateFromSpecFile(*specPath, gen.Options{
		OutDir:    *outDir,
		GoOutDir:  *goOutDir,
		GoPackage: *goPkg,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "flop-gen error: %v\n", err)
		os.Exit(1)
	}
}
