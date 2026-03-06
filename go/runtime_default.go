package flop

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type DefaultServerInfo struct {
	AppName    string
	Port       int
	DataDir    string
	Engine     string
	AdminPath  string
	SetupToken string
	SetupHint  string
	Extra      []string
	Use        []string
}

type DefaultServeOptions struct {
	Server             *http.Server
	Checkpoint         func() error
	Close              func() error
	CheckpointInterval time.Duration
	OnShutdown         func()
}

func RunDefaultServer(info DefaultServerInfo, opts DefaultServeOptions) error {
	PrintServerInfo(info)
	return ServeWithDefaults(opts)
}

func PrintServerInfo(info DefaultServerInfo) {
	appName := strings.TrimSpace(info.AppName)
	if appName == "" {
		appName = "Flop App"
	}

	appURL := localURL(info.Port, "/")
	fmt.Println(appName)
	fmt.Printf("App: %s\n", appURL)

	if strings.TrimSpace(info.AdminPath) != "" {
		fmt.Printf("Admin: %s\n", localURL(info.Port, info.AdminPath))
	}

	if info.SetupToken != "" {
		fmt.Printf("Setup: %s\n", localURL(info.Port, "/_/setup?token="+info.SetupToken))
	} else if strings.TrimSpace(info.SetupHint) != "" {
		fmt.Printf("Setup: %s\n", strings.TrimSpace(info.SetupHint))
	}
	fmt.Println()
}

func ServeWithDefaults(opts DefaultServeOptions) error {
	if opts.Server == nil {
		return errors.New("flop: server is nil")
	}

	interval := opts.CheckpointInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	stopCh := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() { close(stopCh) })
	}

	if opts.Checkpoint != nil {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					if err := opts.Checkpoint(); err != nil {
						if !errors.Is(err, os.ErrNotExist) {
							fmt.Fprintf(os.Stderr, "checkpoint error: %v\n", err)
						}
					}
				}
			}
		}()
	}

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			stop()
			if opts.OnShutdown != nil {
				opts.OnShutdown()
			}
			if opts.Checkpoint != nil {
				if err := opts.Checkpoint(); err != nil {
					if !errors.Is(err, os.ErrNotExist) {
						fmt.Fprintf(os.Stderr, "checkpoint error: %v\n", err)
					}
				}
			}
			if opts.Close != nil {
				if err := opts.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "close error: %v\n", err)
				}
			}
		})
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		fmt.Println("shutting down...")
		_ = opts.Server.Close()
		cleanup()
	}()

	err := opts.Server.ListenAndServe()
	cleanup()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func localURL(port int, path string) string {
	base := fmt.Sprintf("http://localhost:%d", port)
	p := strings.TrimSpace(path)
	if p == "" || p == "/" {
		return base
	}
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		return p
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return base + p
}

func PortFromAddr(addr string, fallback int) int {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return fallback
	}
	if strings.HasPrefix(trimmed, ":") {
		p, err := strconv.Atoi(strings.TrimPrefix(trimmed, ":"))
		if err == nil {
			return p
		}
	}
	return fallback
}
