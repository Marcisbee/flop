package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	flop "github.com/marcisbee/flop"
	financeapp "github.com/marcisbee/flop/benchmarks/finance-go/appschema"
)

func main() {
	moduleRoot, err := findModuleRoot()
	if err != nil {
		log.Fatalf("find module root: %v", err)
	}
	benchDir, err := findBenchDir(moduleRoot)
	if err != nil {
		log.Fatalf("resolve benchmark dir: %v", err)
	}

	port := flag.Int("port", 1985, "server port")
	dataDir := flag.String("data", filepath.Join(benchDir, "data"), "data directory")
	syncMode := flag.String("sync", "normal", "sync mode: normal|full")
	flag.Parse()

	secret := os.Getenv("FLOP_JWT_SECRET")
	if secret == "" {
		secret = "go-finance-benchmark-secret"
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	app := financeapp.BuildWithOptions(financeapp.BuildOptions{
		DataDir:  *dataDir,
		SyncMode: *syncMode,
	})
	db, err := app.Open()
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	db.SetJWTSecret(secret)

	if err := financeapp.Initialize(db); err != nil {
		log.Fatalf("initialize benchmark state: %v", err)
	}
	stopStatsSnapshot := financeapp.StartStatsSnapshotLoop(db, 2*time.Second)

	mux := http.NewServeMux()
	mounts := flop.MountDefaultHandlers(mux, app, db)

	indexPath := filepath.Join(benchDir, "index.html")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if _, err := os.Stat(indexPath); err != nil {
			http.Error(w, "index.html not found", http.StatusNotFound)
			return
		}
		http.ServeFile(w, r, indexPath)
	})

	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	if err := flop.RunDefaultServer(flop.DefaultServerInfo{
		AppName:    "Go Finance Benchmark",
		Port:       *port,
		DataDir:    *dataDir,
		Engine:     "flop go package",
		AdminPath:  "/_",
		SetupToken: mounts.Admin.SetupToken,
		SetupHint:  setupHint(mounts.Admin.SetupToken),
		Use: []string{
			"deno run --allow-net benchmarks/finance-go/seed.ts",
		},
	}, flop.DefaultServeOptions{
		Server:     srv,
		Checkpoint: db.Checkpoint,
		Close: func() error {
			stopStatsSnapshot()
			return db.Close()
		},
		CheckpointInterval: 30 * time.Second,
	}); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func setupHint(token string) string {
	if token != "" {
		return ""
	}
	return "already configured"
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
			return "", os.ErrNotExist
		}
		dir = next
	}
}

func findBenchDir(moduleRoot string) (string, error) {
	candidates := []string{
		moduleRoot,
		filepath.Join(moduleRoot, "benchmarks", "finance-go"),
		filepath.Join(filepath.Dir(moduleRoot), "benchmarks", "finance-go"),
	}
	for _, dir := range candidates {
		p := filepath.Join(dir, "index.html")
		if _, err := os.Stat(p); err == nil {
			return dir, nil
		}
	}
	return "", fmt.Errorf("index.html not found near module root %q", moduleRoot)
}
