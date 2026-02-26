package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	flop "github.com/marcisbee/flop"
	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/runtime"
	"github.com/marcisbee/flop/internal/server"
)

const (
	defaultPort   = 1985
	defaultSecret = "flop-dev-secret-change-in-production"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: flop <path-to-app.ts>\n")
		fmt.Fprintf(os.Stderr, "  e.g. flop ./app.ts\n")
		os.Exit(1)
	}

	appPath := os.Args[1]
	absAppPath, err := filepath.Abs(appPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving path: %v\n", err)
		os.Exit(1)
	}

	appDir := filepath.Dir(absAppPath)

	// --- Step 1: Bundle app.ts with esbuild ---
	fmt.Println("  Bundling app.ts...")
	bundle, err := runtime.BundleApp(absAppPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Bundle error: %v\n", err)
		os.Exit(1)
	}
	if len(bundle.Errors) > 0 {
		fmt.Fprintf(os.Stderr, "Bundle errors:\n")
		for _, e := range bundle.Errors {
			fmt.Fprintf(os.Stderr, "  %s\n", e)
		}
		os.Exit(1)
	}

	// --- Step 2: Execute in QuickJS to extract metadata ---
	fmt.Println("  Discovering app metadata...")
	vm := runtime.NewVM()
	defer vm.Close()

	meta, err := runtime.DiscoverApp(vm, bundle.Code)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Discovery error: %v\n", err)
		os.Exit(1)
	}

	// Build table definitions
	tableDefs := runtime.BuildTableDefs(meta)
	routes := runtime.BuildRoutes(meta)

	// Resolve data directory
	dataDir := filepath.Join(appDir, "data")
	if meta.Config.DataDir != "" {
		if filepath.IsAbs(meta.Config.DataDir) {
			dataDir = meta.Config.DataDir
		} else {
			dataDir = filepath.Join(appDir, meta.Config.DataDir)
		}
	}

	// --- Step 3: Open database ---
	fmt.Println("  Opening database...")
	syncMode := "full"
	if meta.Config.SyncMode != "" {
		syncMode = meta.Config.SyncMode
	}
	db := engine.NewDatabase(engine.DatabaseConfig{
		DataDir:       dataDir,
		MaxCachePages: 256,
		SyncMode:      syncMode,
	})

	if err := db.Open(tableDefs); err != nil {
		fmt.Fprintf(os.Stderr, "Database error: %v\n", err)
		os.Exit(1)
	}

	// --- Step 4: Create VM pool with bridge host functions ---
	pool, err := runtime.NewHandlerPool(db, bundle.Code)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Pool error: %v\n", err)
		os.Exit(1)
	}

	// --- Step 5: Set up auth ---
	port := defaultPort
	if envPort := os.Getenv("FLOP_PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			port = p
		}
	}

	jwtSecret := os.Getenv("FLOP_JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = defaultSecret
	}

	var authService *server.AuthService
	var setupToken string

	authTable := db.GetAuthTable()
	if authTable != nil {
		authService = server.NewAuthService(authTable, jwtSecret)
		if !authService.HasSuperadmin() {
			setupToken = generateToken(32)
		}
	}

	// --- Step 6: Bundle client pages (if any) ---
	var clientJS, clientCSS []byte
	if len(meta.Routes) > 0 {
		fmt.Println("  Bundling client app...")
		clientBundle, err := runtime.BundleClientPages(nil, meta.Routes, appDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Client bundle error: %v\n", err)
		} else if len(clientBundle.Errors) > 0 {
			fmt.Fprintf(os.Stderr, "Client bundle errors:\n")
			for _, e := range clientBundle.Errors {
				fmt.Fprintf(os.Stderr, "  %s\n", e)
			}
		} else {
			clientJS = clientBundle.JS
			clientCSS = clientBundle.CSS
			if len(clientJS) > 0 {
				fmt.Printf("  Bundle: %.1fKB JS", float64(len(clientJS))/1024)
				if len(clientCSS) > 0 {
					fmt.Printf(" + %.1fKB CSS", float64(len(clientCSS))/1024)
				}
				fmt.Println()
			}
		}
	}

	// --- Step 7: Create HTTP handler ---
	handler := server.NewHandler(
		db,
		pool,
		routes,
		meta.Routes,
		authService,
		server.ServerConfig{
			Port:      port,
			JWTSecret: jwtSecret,
			StaticDir: appDir,
		},
		setupToken,
		clientJS,
		clientCSS,
	)

	// --- Step 8: Print startup info ---
	flop.PrintServerInfo(flop.DefaultServerInfo{
		AppName:    filepath.Base(absAppPath),
		Port:       port,
		DataDir:    dataDir,
		Engine:     "quickjs runtime",
		AdminPath:  "/_",
		SetupToken: setupToken,
		SetupHint:  setupHint(setupToken),
		Extra: []string{
			fmt.Sprintf("Tables:  %d", len(tableDefs)),
			fmt.Sprintf("Routes:  %d", len(routes)),
			fmt.Sprintf("Pages:   %d", len(meta.Routes)),
		},
	})

	for _, route := range routes {
		access := "[public]"
		if route.Access.Type == "roles" {
			access = fmt.Sprintf("[roles: %s]", joinStrings(route.Access.Roles, ","))
		} else if route.Access.Type == "authenticated" {
			access = "[auth]"
		}
		fmt.Printf("  %-5s %s %s\n", route.Method, route.Path, access)
	}

	for _, page := range meta.Routes {
		fmt.Printf("  GET   %s [page]\n", page.Pattern)
	}
	fmt.Println()

	// --- Step 9: Start server ---
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // no timeout for SSE
		IdleTimeout:  120 * time.Second,
	}
	if err := flop.ServeWithDefaults(flop.DefaultServeOptions{
		Server:             srv,
		Checkpoint:         db.Checkpoint,
		Close:              db.Close,
		CheckpointInterval: 30 * time.Second,
		OnShutdown:         pool.Close,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}

func setupHint(token string) string {
	if token != "" {
		return ""
	}
	return "already configured"
}

func generateToken(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	return hex.EncodeToString(b)[:length]
}

func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
