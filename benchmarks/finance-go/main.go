package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	flop "github.com/marcisbee/flop"
	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/server"
)

type appServer struct {
	db       *engine.Database
	auth     *server.AuthService
	secret   string
	benchDir string
	admin    http.Handler

	users        *engine.TableInstance
	accounts     *engine.TableInstance
	transactions *engine.TableInstance
	ledger       *engine.TableInstance

	accountLocks [1024]sync.Mutex

	stats statsCache

	recentTxMu    sync.RWMutex
	recentTxRing  []map[string]any
	recentTxHead  int
	recentTxSize  int
	recentTxLimit int

	sseBatchSize  int
	sseFlushEvery time.Duration
}

type statsCache struct {
	mu sync.RWMutex

	userCount             int
	accountCount          int
	transactionCount      int
	completedTransactions int
	failedTransactions    int
	totalVolume           float64
	totalBalance          float64
}

type realtimeChange struct {
	Table string         `json:"table"`
	Op    string         `json:"op"`
	RowID string         `json:"rowId"`
	Data  map[string]any `json:"data,omitempty"`
}

var viewTableDeps = map[string][]string{
	"get_stats":               {"users", "accounts", "transactions", "ledger"},
	"get_all_accounts":        {"accounts"},
	"get_accounts":            {"accounts"},
	"get_recent_transactions": {"transactions"},
	"get_transactions":        {"transactions"},
	"get_ledger":              {"ledger"},
}

func main() {
	moduleRoot, err := findModuleRoot()
	if err != nil {
		log.Fatalf("find module root: %v", err)
	}
	repoRoot := filepath.Clean(filepath.Join(moduleRoot, ".."))
	benchDir := filepath.Join(repoRoot, "benchmarks", "finance-go")

	port := flag.Int("port", 1985, "server port")
	dataDir := flag.String("data", filepath.Join(benchDir, "data"), "data directory")
	syncMode := flag.String("sync", "normal", "sync mode: normal|full")
	cachePages := flag.Int("cache-pages", 256, "max in-memory page cache pages (lower = less memory)")
	recentTxLimit := flag.Int("recent-tx-limit", 2048, "recent transaction cache size for fast get_recent_transactions")
	sseBatchSize := flag.Int("sse-batch", 128, "max realtime changes buffered per flush")
	sseFlushMS := flag.Int("sse-flush-ms", 25, "realtime flush interval in milliseconds")
	flag.Parse()

	secret := os.Getenv("FLOP_JWT_SECRET")
	if secret == "" {
		secret = "go-finance-benchmark-secret"
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	db := engine.NewDatabase(engine.DatabaseConfig{
		DataDir:       *dataDir,
		MaxCachePages: *cachePages,
		SyncMode:      *syncMode,
	})
	if err := db.Open(buildTableDefs()); err != nil {
		log.Fatalf("open database: %v", err)
	}

	authTable := db.GetAuthTable()
	if authTable == nil {
		log.Fatalf("auth table not found")
	}

	srv := &appServer{
		db:           db,
		auth:         server.NewAuthService(authTable, secret),
		secret:       secret,
		benchDir:     benchDir,
		users:        db.GetTable("users"),
		accounts:     db.GetTable("accounts"),
		transactions: db.GetTable("transactions"),
		ledger:       db.GetTable("ledger"),

		recentTxLimit: *recentTxLimit,
		sseBatchSize:  maxInt(*sseBatchSize, 16),
		sseFlushEvery: time.Duration(maxInt(*sseFlushMS, 5)) * time.Millisecond,
	}
	setupToken := ""
	if !srv.auth.HasSuperadmin() {
		setupToken = generateToken(32)
	}
	srv.admin = server.NewHandler(
		db,
		nil,
		nil,
		nil,
		srv.auth,
		server.ServerConfig{JWTSecret: secret},
		setupToken,
		nil,
		nil,
	)
	if err := srv.rebuildCaches(); err != nil {
		log.Fatalf("initialize caches: %v", err)
	}

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      srv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	if err := flop.RunDefaultServer(flop.DefaultServerInfo{
		AppName:    "Go Finance Benchmark",
		Port:       *port,
		DataDir:    *dataDir,
		Engine:     "flop internal (pure Go)",
		AdminPath:  "/_",
		SetupToken: setupToken,
		SetupHint:  setupHint(setupToken),
		Extra: []string{
			fmt.Sprintf("Cache:   pages=%d sse=%d/%dms", *cachePages, srv.sseBatchSize, int(srv.sseFlushEvery/time.Millisecond)),
		},
		Use: []string{
			"deno run --allow-net benchmarks/finance-go/seed.ts",
		},
	}, flop.DefaultServeOptions{
		Server:             httpSrv,
		Checkpoint:         db.Checkpoint,
		Close:              db.Close,
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

func buildTableDefs() map[string]*schema.TableDef {
	users := &schema.TableDef{
		Name: "users",
		CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
			{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{15}"},
			{Name: "email", Kind: schema.KindString, Required: true, Unique: true},
			{Name: "password", Kind: schema.KindBcrypt, Required: true, BcryptRounds: 10},
			{Name: "name", Kind: schema.KindString, Required: true},
			{Name: "roles", Kind: schema.KindRoles},
		}),
		Auth: true,
	}

	accounts := &schema.TableDef{
		Name: "accounts",
		CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
			{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{15}"},
			{Name: "ownerId", Kind: schema.KindString, Required: true},
			{Name: "name", Kind: schema.KindString, Required: true},
			{Name: "type", Kind: schema.KindEnum, Required: true, EnumValues: []string{"checking", "savings", "credit"}},
			{Name: "balance", Kind: schema.KindNumber, Required: true},
			{Name: "currency", Kind: schema.KindString, Required: true},
			{Name: "createdAt", Kind: schema.KindTimestamp, DefaultValue: "now"},
		}),
		Indexes: []schema.IndexDef{
			{Fields: []string{"ownerId"}},
		},
	}

	transactions := &schema.TableDef{
		Name: "transactions",
		CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
			{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{15}"},
			{Name: "fromAccountId", Kind: schema.KindString, Required: true},
			{Name: "toAccountId", Kind: schema.KindString, Required: true},
			{Name: "amount", Kind: schema.KindNumber, Required: true},
			{Name: "currency", Kind: schema.KindString, Required: true},
			{Name: "status", Kind: schema.KindEnum, Required: true, EnumValues: []string{"pending", "completed", "failed"}},
			{Name: "description", Kind: schema.KindString},
			{Name: "createdAt", Kind: schema.KindTimestamp, DefaultValue: "now"},
		}),
		Indexes: []schema.IndexDef{
			{Fields: []string{"fromAccountId"}},
			{Fields: []string{"toAccountId"}},
		},
	}

	ledger := &schema.TableDef{
		Name: "ledger",
		CompiledSchema: schema.NewCompiledSchema([]schema.CompiledField{
			{Name: "id", Kind: schema.KindString, Required: true, Unique: true, AutoGenPattern: "[a-z0-9]{15}"},
			{Name: "accountId", Kind: schema.KindString, Required: true},
			{Name: "transactionId", Kind: schema.KindString, Required: true},
			{Name: "amount", Kind: schema.KindNumber, Required: true},
			{Name: "balanceAfter", Kind: schema.KindNumber, Required: true},
			{Name: "type", Kind: schema.KindEnum, Required: true, EnumValues: []string{"debit", "credit"}},
			{Name: "createdAt", Kind: schema.KindTimestamp, DefaultValue: "now"},
		}),
		Indexes: []schema.IndexDef{
			{Fields: []string{"accountId"}},
		},
	}

	return map[string]*schema.TableDef{
		"users":        users,
		"accounts":     accounts,
		"transactions": transactions,
		"ledger":       ledger,
	}
}

func (s *appServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	path := r.URL.Path
	switch {
	case path == "/" && r.Method == http.MethodGet:
		s.serveIndex(w, r)
		return
	case strings.HasPrefix(path, "/_"):
		s.admin.ServeHTTP(w, r)
		return
	case strings.HasPrefix(path, "/api/auth/"):
		s.handleAuth(w, r)
		return
	case path == "/api/sse":
		s.handleMultiplexedSSE(w, r)
		return
	case strings.HasPrefix(path, "/api/view/"):
		s.handleView(w, r)
		return
	case strings.HasPrefix(path, "/api/reduce/"):
		s.handleReduce(w, r)
		return
	case path == "/_schema" || path == "/api/schema":
		jsonResp(w, http.StatusOK, map[string]any{"endpoints": []any{}})
		return
	default:
		jsonResp(w, http.StatusNotFound, map[string]any{"error": "Not found"})
		return
	}
}

func (s *appServer) serveIndex(w http.ResponseWriter, r *http.Request) {
	indexPath := filepath.Join(s.benchDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		jsonResp(w, http.StatusNotFound, map[string]any{"error": "index.html not found"})
		return
	}
	http.ServeFile(w, r, indexPath)
}

func (s *appServer) handleAuth(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/auth/register":
		if r.Method != http.MethodPost {
			jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
			return
		}
		var body struct {
			Email    string `json:"email"`
			Password string `json:"password"`
			Name     string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Invalid JSON"})
			return
		}
		token, authCtx, err := s.auth.Register(body.Email, body.Password, body.Name)
		if err != nil {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		s.statsAddUser()
		jsonResp(w, http.StatusOK, map[string]any{
			"token": token,
			"user":  map[string]any{"id": authCtx.ID, "email": authCtx.Email, "name": body.Name, "roles": authCtx.Roles},
		})
	case "/api/auth/password":
		if r.Method != http.MethodPost {
			jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
			return
		}
		var body struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Invalid JSON"})
			return
		}
		token, refresh, authCtx, err := s.auth.Login(body.Email, body.Password)
		if err != nil {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
			return
		}
		jsonResp(w, http.StatusOK, map[string]any{
			"token":        token,
			"refreshToken": refresh,
			"user":         map[string]any{"id": authCtx.ID, "email": authCtx.Email, "roles": authCtx.Roles},
		})
	case "/api/auth/refresh":
		if r.Method != http.MethodPost {
			jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
			return
		}
		var body struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Invalid JSON"})
			return
		}
		token, err := s.auth.Refresh(body.RefreshToken)
		if err != nil {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
			return
		}
		jsonResp(w, http.StatusOK, map[string]any{"token": token})
	default:
		jsonResp(w, http.StatusNotFound, map[string]any{"error": "Unknown auth endpoint"})
	}
}

func (s *appServer) handleView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/view/")
	auth := s.authFromRequest(r)

	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		s.handleSingleSSE(w, r, name, auth)
		return
	}

	params := make(map[string]string)
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	data, status, err := s.execView(name, params, auth)
	if err != nil {
		jsonResp(w, status, map[string]any{"error": err.Error()})
		return
	}
	jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": data})
}

func (s *appServer) handleReduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/reduce/")
	auth := s.authFromRequest(r)
	if auth == nil {
		jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Authentication required"})
		return
	}

	var params map[string]any
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Invalid JSON"})
		return
	}

	data, status, err := s.execReducer(name, params, auth)
	if err != nil {
		jsonResp(w, status, map[string]any{"error": err.Error()})
		return
	}
	jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": data})
}

func (s *appServer) execView(name string, params map[string]string, auth *schema.AuthContext) (any, int, error) {
	switch name {
	case "get_stats":
		return s.statsSnapshot(), http.StatusOK, nil
	case "get_all_accounts":
		rows, err := s.accounts.Scan(10000, 0)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return nonNilRows(rows), http.StatusOK, nil
	case "get_accounts":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		ptrs := s.accounts.FindAllByIndex([]string{"ownerId"}, auth.ID)
		rows, err := rowsByPointers(s.accounts, ptrs)
		if err == nil {
			return nonNilRows(rows), http.StatusOK, nil
		}
		// Fallback for old data/index states.
		allRows, scanErr := s.accounts.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]map[string]any, 0, len(allRows))
		for _, row := range allRows {
			if toString(row["ownerId"]) == auth.ID {
				filtered = append(filtered, row)
			}
		}
		return nonNilRows(filtered), http.StatusOK, nil
	case "get_recent_transactions":
		limit := atoiOr(params["limit"], 100)
		if limit <= 0 {
			limit = 100
		}
		return s.getRecentTransactions(limit), http.StatusOK, nil
	case "get_transactions":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := params["accountId"]
		fromPtrs := s.transactions.FindAllByIndex([]string{"fromAccountId"}, accountID)
		toPtrs := s.transactions.FindAllByIndex([]string{"toAccountId"}, accountID)
		rows, err := rowsByPointers(s.transactions, append(fromPtrs, toPtrs...))
		if err == nil {
			return nonNilRows(deduplicateRowsByID(rows)), http.StatusOK, nil
		}
		// Fallback for old data/index states.
		allRows, scanErr := s.transactions.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]map[string]any, 0, len(allRows))
		for _, row := range allRows {
			if toString(row["fromAccountId"]) == accountID || toString(row["toAccountId"]) == accountID {
				filtered = append(filtered, row)
			}
		}
		return nonNilRows(filtered), http.StatusOK, nil
	case "get_ledger":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := params["accountId"]
		ptrs := s.ledger.FindAllByIndex([]string{"accountId"}, accountID)
		rows, err := rowsByPointers(s.ledger, ptrs)
		if err == nil {
			return nonNilRows(rows), http.StatusOK, nil
		}
		allRows, scanErr := s.ledger.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]map[string]any, 0, len(allRows))
		for _, row := range allRows {
			if toString(row["accountId"]) == accountID {
				filtered = append(filtered, row)
			}
		}
		return nonNilRows(filtered), http.StatusOK, nil
	default:
		return nil, http.StatusNotFound, errors.New("unknown view")
	}
}

func (s *appServer) execReducer(name string, params map[string]any, auth *schema.AuthContext) (any, int, error) {
	switch name {
	case "create_account":
		accountType := toString(params["type"])
		if accountType != "checking" && accountType != "savings" && accountType != "credit" {
			return nil, http.StatusBadRequest, errors.New("invalid account type")
		}
		row, err := s.accounts.Insert(map[string]any{
			"ownerId":  auth.ID,
			"name":     toString(params["name"]),
			"type":     accountType,
			"balance":  float64(0),
			"currency": defaultString(toString(params["currency"]), "USD"),
		}, nil)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		s.statsAddAccount()
		return row, http.StatusOK, nil
	case "deposit":
		return s.deposit(toString(params["accountId"]), toFloat64(params["amount"]))
	case "transfer":
		return s.transfer(auth.ID, toString(params["fromAccountId"]), toString(params["toAccountId"]), toFloat64(params["amount"]), toString(params["description"]))
	default:
		return nil, http.StatusNotFound, errors.New("unknown reducer")
	}
}

func (s *appServer) deposit(accountID string, amount float64) (any, int, error) {
	if amount <= 0 {
		return nil, http.StatusBadRequest, errors.New("amount must be positive")
	}

	lock := s.lockFor(accountID)
	lock.Lock()
	defer lock.Unlock()

	account, err := s.accounts.Get(accountID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if account == nil {
		return nil, http.StatusBadRequest, errors.New("account not found")
	}

	newBalance := toFloat64(account["balance"]) + amount
	if _, err := s.accounts.Update(accountID, map[string]any{"balance": newBalance}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}

	tx, err := s.transactions.Insert(map[string]any{
		"fromAccountId": "EXTERNAL",
		"toAccountId":   accountID,
		"amount":        amount,
		"currency":      toString(account["currency"]),
		"status":        "completed",
		"description":   "Deposit",
	}, nil)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if _, err := s.ledger.Insert(map[string]any{
		"accountId":     accountID,
		"transactionId": toString(tx["id"]),
		"amount":        amount,
		"balanceAfter":  newBalance,
		"type":          "credit",
	}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}
	s.statsOnDeposit(amount)
	s.pushRecentTx(tx)

	return map[string]any{"balance": newBalance, "transactionId": tx["id"]}, http.StatusOK, nil
}

func (s *appServer) transfer(_userID, fromID, toID string, amount float64, description string) (any, int, error) {
	if amount <= 0 {
		return nil, http.StatusBadRequest, errors.New("amount must be positive")
	}
	if fromID == toID {
		return nil, http.StatusBadRequest, errors.New("cannot transfer to same account")
	}

	i1, i2 := s.lockIndex(fromID), s.lockIndex(toID)
	if i1 == i2 {
		s.accountLocks[i1].Lock()
		defer s.accountLocks[i1].Unlock()
	} else if i1 < i2 {
		s.accountLocks[i1].Lock()
		s.accountLocks[i2].Lock()
		defer s.accountLocks[i2].Unlock()
		defer s.accountLocks[i1].Unlock()
	} else {
		s.accountLocks[i2].Lock()
		s.accountLocks[i1].Lock()
		defer s.accountLocks[i1].Unlock()
		defer s.accountLocks[i2].Unlock()
	}

	from, err := s.accounts.Get(fromID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if from == nil {
		return nil, http.StatusBadRequest, errors.New("source account not found")
	}

	to, err := s.accounts.Get(toID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if to == nil {
		return nil, http.StatusBadRequest, errors.New("destination account not found")
	}

	fromBalance := toFloat64(from["balance"])
	toBalance := toFloat64(to["balance"])
	currency := toString(from["currency"])

	if fromBalance < amount {
		tx, err := s.transactions.Insert(map[string]any{
			"fromAccountId": fromID,
			"toAccountId":   toID,
			"amount":        amount,
			"currency":      currency,
			"status":        "failed",
			"description":   defaultString(description, "Transfer (insufficient funds)"),
		}, nil)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		s.statsOnFailedTransfer()
		s.pushRecentTx(tx)
		return map[string]any{"status": "failed", "reason": "insufficient_funds", "transactionId": tx["id"]}, http.StatusOK, nil
	}

	newFrom := fromBalance - amount
	newTo := toBalance + amount

	if _, err := s.accounts.Update(fromID, map[string]any{"balance": newFrom}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.accounts.Update(toID, map[string]any{"balance": newTo}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}

	tx, err := s.transactions.Insert(map[string]any{
		"fromAccountId": fromID,
		"toAccountId":   toID,
		"amount":        amount,
		"currency":      currency,
		"status":        "completed",
		"description":   defaultString(description, "Transfer"),
	}, nil)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	txID := toString(tx["id"])
	if _, err := s.ledger.Insert(map[string]any{
		"accountId":     fromID,
		"transactionId": txID,
		"amount":        -amount,
		"balanceAfter":  newFrom,
		"type":          "debit",
	}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.ledger.Insert(map[string]any{
		"accountId":     toID,
		"transactionId": txID,
		"amount":        amount,
		"balanceAfter":  newTo,
		"type":          "credit",
	}, nil); err != nil {
		return nil, http.StatusBadRequest, err
	}
	s.statsOnCompletedTransfer(amount)
	s.pushRecentTx(tx)

	return map[string]any{"status": "completed", "transactionId": tx["id"]}, http.StatusOK, nil
}

func (s *appServer) rebuildCaches() error {
	s.stats.mu.Lock()
	s.stats.userCount = s.users.Count()
	s.stats.accountCount = s.accounts.Count()
	s.stats.transactionCount = s.transactions.Count()
	s.stats.completedTransactions = 0
	s.stats.failedTransactions = 0
	s.stats.totalVolume = 0
	s.stats.totalBalance = 0
	s.stats.mu.Unlock()

	accountCount := s.accounts.Count()
	if accountCount > 0 {
		rows, err := s.accounts.Scan(accountCount, 0)
		if err != nil {
			return err
		}
		var totalBalance float64
		for _, row := range rows {
			totalBalance += toFloat64(row["balance"])
		}
		s.stats.mu.Lock()
		s.stats.totalBalance = totalBalance
		s.stats.mu.Unlock()
	}

	txCount := s.transactions.Count()
	if txCount > 0 {
		rows, err := s.transactions.Scan(txCount, 0)
		if err != nil {
			return err
		}
		completed := 0
		failed := 0
		var totalVolume float64
		for _, row := range rows {
			switch toString(row["status"]) {
			case "completed":
				completed++
				totalVolume += toFloat64(row["amount"])
			case "failed":
				failed++
			}
		}
		s.stats.mu.Lock()
		s.stats.completedTransactions = completed
		s.stats.failedTransactions = failed
		s.stats.totalVolume = totalVolume
		s.stats.mu.Unlock()
	}

	s.recentTxMu.Lock()
	if s.recentTxLimit > 0 {
		s.recentTxRing = make([]map[string]any, s.recentTxLimit)
		s.recentTxHead = 0
		s.recentTxSize = 0
		recentCount := minInt(txCount, s.recentTxLimit)
		if recentCount > 0 {
			offset := txCount - recentCount
			rows, err := s.transactions.Scan(recentCount, offset)
			if err != nil {
				s.recentTxMu.Unlock()
				return err
			}
			for _, row := range rows {
				s.recentTxRing[s.recentTxHead] = copyRowMap(row)
				s.recentTxHead = (s.recentTxHead + 1) % s.recentTxLimit
				if s.recentTxSize < s.recentTxLimit {
					s.recentTxSize++
				}
			}
		}
	} else {
		s.recentTxRing = nil
		s.recentTxHead = 0
		s.recentTxSize = 0
	}
	s.recentTxMu.Unlock()

	return nil
}

func (s *appServer) statsSnapshot() map[string]any {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()
	return map[string]any{
		"userCount":             s.stats.userCount,
		"accountCount":          s.stats.accountCount,
		"transactionCount":      s.stats.transactionCount,
		"completedTransactions": s.stats.completedTransactions,
		"failedTransactions":    s.stats.failedTransactions,
		"totalVolume":           s.stats.totalVolume,
		"totalBalance":          s.stats.totalBalance,
	}
}

func (s *appServer) statsAddUser() {
	s.stats.mu.Lock()
	s.stats.userCount++
	s.stats.mu.Unlock()
}

func (s *appServer) statsAddAccount() {
	s.stats.mu.Lock()
	s.stats.accountCount++
	s.stats.mu.Unlock()
}

func (s *appServer) statsOnDeposit(amount float64) {
	s.stats.mu.Lock()
	s.stats.transactionCount++
	s.stats.completedTransactions++
	s.stats.totalVolume += amount
	s.stats.totalBalance += amount
	s.stats.mu.Unlock()
}

func (s *appServer) statsOnFailedTransfer() {
	s.stats.mu.Lock()
	s.stats.transactionCount++
	s.stats.failedTransactions++
	s.stats.mu.Unlock()
}

func (s *appServer) statsOnCompletedTransfer(amount float64) {
	s.stats.mu.Lock()
	s.stats.transactionCount++
	s.stats.completedTransactions++
	s.stats.totalVolume += amount
	s.stats.mu.Unlock()
}

func (s *appServer) pushRecentTx(tx map[string]any) {
	if s.recentTxLimit <= 0 {
		return
	}
	s.recentTxMu.Lock()
	if len(s.recentTxRing) == 0 {
		s.recentTxRing = make([]map[string]any, s.recentTxLimit)
	}
	s.recentTxRing[s.recentTxHead] = copyRowMap(tx)
	s.recentTxHead = (s.recentTxHead + 1) % len(s.recentTxRing)
	if s.recentTxSize < len(s.recentTxRing) {
		s.recentTxSize++
	}
	s.recentTxMu.Unlock()
}

func (s *appServer) getRecentTransactions(limit int) []map[string]any {
	if limit <= 0 {
		limit = 100
	}

	s.recentTxMu.RLock()
	defer s.recentTxMu.RUnlock()

	if s.recentTxSize == 0 || len(s.recentTxRing) == 0 {
		return []map[string]any{}
	}

	n := minInt(limit, s.recentTxSize)
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		idx := (s.recentTxHead - 1 - i + len(s.recentTxRing)) % len(s.recentTxRing)
		row := s.recentTxRing[idx]
		if row == nil {
			continue
		}
		out = append(out, copyRowMap(row))
	}
	return out
}

func (s *appServer) handleMultiplexedSSE(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "SSE not requested"})
		return
	}
	viewNames := strings.Split(r.URL.Query().Get("views"), ",")
	if len(viewNames) == 0 || strings.TrimSpace(viewNames[0]) == "" {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "No views specified. Use ?views=name1,name2"})
		return
	}
	s.streamSSE(w, r, viewNames, s.authFromRequest(r), true)
}

func (s *appServer) handleSingleSSE(w http.ResponseWriter, r *http.Request, viewName string, auth *schema.AuthContext) {
	s.streamSSE(w, r, []string{viewName}, auth, false)
}

func (s *appServer) streamSSE(w http.ResponseWriter, r *http.Request, rawViews []string, auth *schema.AuthContext, namespacedSnapshots bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "SSE not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	viewNames := make([]string, 0, len(rawViews))
	for _, raw := range rawViews {
		name := strings.TrimSpace(raw)
		if name != "" {
			viewNames = append(viewNames, name)
		}
	}
	if len(viewNames) == 0 {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "No views specified"})
		return
	}

	for _, name := range viewNames {
		params := map[string]string{}
		if namespacedSnapshots {
			prefix := name + "."
			for k, v := range r.URL.Query() {
				if strings.HasPrefix(k, prefix) && len(v) > 0 {
					params[k[len(prefix):]] = v[0]
				}
			}
		} else {
			for k, v := range r.URL.Query() {
				if len(v) > 0 {
					params[k] = v[0]
				}
			}
		}

		data, _, err := s.execView(name, params, auth)
		eventName := "snapshot"
		errorName := "error"
		if namespacedSnapshots {
			eventName = "snapshot:" + name
			errorName = "error:" + name
		}
		if err != nil {
			if !writeSSEEvents(w, flusher, errorName, []any{map[string]any{"error": err.Error()}}) {
				return
			}
			continue
		}
		if !writeSSEEvents(w, flusher, eventName, []any{data}) {
			return
		}
	}

	deps := tablesForViews(viewNames)
	changeCh := make(chan engine.ChangeEvent, maxInt(s.sseBatchSize*8, 256))

	var unsubscribe func()
	if len(deps) > 0 {
		unsubscribe = s.db.GetPubSub().Subscribe(deps, func(event engine.ChangeEvent) {
			select {
			case changeCh <- event:
			default:
			}
		})
	} else {
		unsubscribe = s.db.GetPubSub().SubscribeAll(func(event engine.ChangeEvent) {
			select {
			case changeCh <- event:
			default:
			}
		})
	}
	defer unsubscribe()

	flushTicker := time.NewTicker(s.sseFlushEvery)
	defer flushTicker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	pending := make([]any, 0, s.sseBatchSize)
	flushPending := func() bool {
		if len(pending) == 0 {
			return true
		}
		ok := writeSSEEvents(w, flusher, "change", pending)
		pending = pending[:0]
		return ok
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-changeCh:
			pending = append(pending, compactRealtimeChange(event))
			if len(pending) >= s.sseBatchSize && !flushPending() {
				return
			}
		case <-flushTicker.C:
			if !flushPending() {
				return
			}
		case <-heartbeat.C:
			if !flushPending() {
				return
			}
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func rowsByPointers(table *engine.TableInstance, pointers []schema.RowPointer) ([]map[string]any, error) {
	if len(pointers) == 0 {
		return []map[string]any{}, nil
	}
	rows := make([]map[string]any, 0, len(pointers))
	for _, p := range pointers {
		row, err := table.GetByPointer(p)
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func deduplicateRowsByID(rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		id := toString(row["id"])
		if id == "" {
			out = append(out, row)
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, row)
	}
	return out
}

func tablesForViews(viewNames []string) []string {
	set := map[string]struct{}{}
	for _, name := range viewNames {
		for _, table := range viewTableDeps[name] {
			set[table] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for table := range set {
		out = append(out, table)
	}
	return out
}

func compactRealtimeChange(event engine.ChangeEvent) realtimeChange {
	change := realtimeChange{
		Table: event.Table,
		Op:    event.Op,
		RowID: event.RowID,
	}
	if event.Data == nil || event.Op == "delete" {
		return change
	}

	switch event.Table {
	case "accounts":
		change.Data = pickFields(event.Data, "id", "ownerId", "name", "type", "balance", "currency", "createdAt")
	case "transactions":
		change.Data = pickFields(event.Data, "id", "fromAccountId", "toAccountId", "amount", "currency", "status", "description", "createdAt")
	case "ledger":
		change.Data = pickFields(event.Data, "id", "accountId", "transactionId", "amount", "balanceAfter", "type", "createdAt")
	case "users":
		change.Data = pickFields(event.Data, "id")
	default:
		change.Data = copyRowMap(event.Data)
	}

	return change
}

func pickFields(in map[string]any, keys ...string) map[string]any {
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		if v, ok := in[key]; ok {
			out[key] = v
		}
	}
	return out
}

func copyRowMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func writeSSEEvents(w http.ResponseWriter, flusher http.Flusher, eventName string, payloads []any) bool {
	var b strings.Builder
	for _, payload := range payloads {
		data, err := json.Marshal(payload)
		if err != nil {
			continue
		}
		b.WriteString("event: ")
		b.WriteString(eventName)
		b.WriteString("\ndata: ")
		b.Write(data)
		b.WriteString("\n\n")
	}
	if b.Len() == 0 {
		return true
	}
	if _, err := fmt.Fprint(w, b.String()); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func (s *appServer) authFromRequest(r *http.Request) *schema.AuthContext {
	token := server.ExtractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		return nil
	}
	payload := server.VerifyJWT(token, s.secret)
	if payload == nil {
		return nil
	}
	return server.JWTToAuthContext(payload)
}

func (s *appServer) lockIndex(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(s.accountLocks)))
}

func (s *appServer) lockFor(id string) *sync.Mutex {
	return &s.accountLocks[s.lockIndex(id)]
}

func jsonResp(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}

func atoiOr(v string, fallback int) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func generateToken(length int) string {
	if length <= 0 {
		return ""
	}
	buf := make([]byte, length)
	_, _ = rand.Read(buf)
	s := hex.EncodeToString(buf)
	if len(s) > length {
		return s[:length]
	}
	return s
}

func nonNilRows(rows []map[string]any) []map[string]any {
	if rows == nil {
		return []map[string]any{}
	}
	return rows
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
			return "", errors.New("go.mod not found")
		}
		dir = next
	}
}

func shortPath(path string) string {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(filepath.Separator))
	if len(parts) <= 3 {
		return clean
	}
	return filepath.Join(parts[len(parts)-3:]...)
}

// WaitForShutdown is exposed for future benchmark orchestration tools.
func WaitForShutdown(ctx context.Context, srv *http.Server) error {
	<-ctx.Done()
	return srv.Shutdown(context.Background())
}
