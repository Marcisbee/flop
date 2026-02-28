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
	financeapp "github.com/marcisbee/flop/benchmarks/finance-go/appschema"
	fgen "github.com/marcisbee/flop/benchmarks/finance-go/appschema/gen"
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

	users         *engine.TableInstance
	accounts      *engine.TableInstance
	transactions  *engine.TableInstance
	ledger        *engine.TableInstance
	usersT        typedEngineTable[fgen.User]
	accountsT     typedEngineTable[fgen.Account]
	transactionsT typedEngineTable[fgen.Transaction]
	ledgerT       typedEngineTable[fgen.Ledger]

	accountLocks [1024]sync.Mutex

	stats statsCache

	recentTxMu    sync.RWMutex
	recentTxRing  []fgen.Transaction
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

type typedEngineTable[T any] struct {
	raw *engine.TableInstance
}

func newTypedEngineTable[T any](raw *engine.TableInstance) typedEngineTable[T] {
	return typedEngineTable[T]{raw: raw}
}

func (t typedEngineTable[T]) Insert(row T) (T, error) {
	m, err := structToMap(row)
	if err != nil {
		var zero T
		return zero, err
	}
	if def := t.raw.GetDef(); def != nil && def.CompiledSchema != nil {
		for _, f := range def.CompiledSchema.Fields {
			if f.AutoGenPattern == "" && f.AutoIDStrategy == "" {
				continue
			}
			v, ok := m[f.Name]
			if !ok {
				continue
			}
			if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
				delete(m, f.Name)
			}
		}
	}
	out, err := t.raw.Insert(m, nil)
	if err != nil {
		var zero T
		return zero, err
	}
	return mapToStruct[T](out)
}

func (t typedEngineTable[T]) Get(id string) (*T, error) {
	m, err := t.raw.Get(id)
	if err != nil || m == nil {
		return nil, err
	}
	v, err := mapToStruct[T](m)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (t typedEngineTable[T]) Update(id string, fields map[string]any) (T, error) {
	out, err := t.raw.Update(id, fields, nil)
	if err != nil {
		var zero T
		return zero, err
	}
	return mapToStruct[T](out)
}

func (t typedEngineTable[T]) Scan(limit, offset int) ([]T, error) {
	rows, err := t.raw.Scan(limit, offset)
	if err != nil {
		return nil, err
	}
	return mapsToStructs[T](rows)
}

func (t typedEngineTable[T]) FindAllByIndex(field string, value any) ([]T, error) {
	ptrs := t.raw.FindAllByIndex([]string{field}, value)
	out := make([]T, 0, len(ptrs))
	for _, p := range ptrs {
		row, err := t.raw.GetByPointer(p)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		v, err := mapToStruct[T](row)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
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
		users:        db.GetTable(fgen.TableUsers),
		accounts:     db.GetTable(fgen.TableAccounts),
		transactions: db.GetTable(fgen.TableTransactions),
		ledger:       db.GetTable(fgen.TableLedger),

		recentTxLimit: *recentTxLimit,
		sseBatchSize:  maxInt(*sseBatchSize, 16),
		sseFlushEvery: time.Duration(maxInt(*sseFlushMS, 5)) * time.Millisecond,
	}
	srv.usersT = newTypedEngineTable[fgen.User](srv.users)
	srv.accountsT = newTypedEngineTable[fgen.Account](srv.accounts)
	srv.transactionsT = newTypedEngineTable[fgen.Transaction](srv.transactions)
	srv.ledgerT = newTypedEngineTable[fgen.Ledger](srv.ledger)
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
	return financeapp.BuildTableDefs()
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
		rows, err := s.accountsT.Scan(10000, 0)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	case "get_accounts":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		rows, err := s.accountsT.FindAllByIndex("ownerId", auth.ID)
		if err == nil {
			return rows, http.StatusOK, nil
		}
		allRows, scanErr := s.accountsT.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]fgen.Account, 0, len(allRows))
		for _, row := range allRows {
			if row.OwnerID == auth.ID {
				filtered = append(filtered, row)
			}
		}
		return filtered, http.StatusOK, nil
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
		fromRows, fromErr := s.transactionsT.FindAllByIndex("fromAccountId", accountID)
		toRows, toErr := s.transactionsT.FindAllByIndex("toAccountId", accountID)
		if fromErr == nil && toErr == nil {
			return deduplicateTransactionsByID(append(fromRows, toRows...)), http.StatusOK, nil
		}
		allRows, scanErr := s.transactionsT.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]fgen.Transaction, 0, len(allRows))
		for _, row := range allRows {
			if row.FromAccountID == accountID || row.ToAccountID == accountID {
				filtered = append(filtered, row)
			}
		}
		return filtered, http.StatusOK, nil
	case "get_ledger":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := params["accountId"]
		rows, err := s.ledgerT.FindAllByIndex("accountId", accountID)
		if err == nil {
			return rows, http.StatusOK, nil
		}
		allRows, scanErr := s.ledgerT.Scan(10000, 0)
		if scanErr != nil {
			return nil, http.StatusBadRequest, scanErr
		}
		filtered := make([]fgen.Ledger, 0, len(allRows))
		for _, row := range allRows {
			if row.AccountID == accountID {
				filtered = append(filtered, row)
			}
		}
		return filtered, http.StatusOK, nil
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
		row, err := s.accountsT.Insert(fgen.Account{
			OwnerID:  auth.ID,
			Name:     toString(params["name"]),
			Type:     accountType,
			Balance:  0,
			Currency: defaultString(toString(params["currency"]), "USD"),
		})
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

	account, err := s.accountsT.Get(accountID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if account == nil {
		return nil, http.StatusBadRequest, errors.New("account not found")
	}

	newBalance := account.Balance + amount
	if _, err := s.accountsT.Update(accountID, map[string]any{"balance": newBalance}); err != nil {
		return nil, http.StatusBadRequest, err
	}

	desc := "Deposit"
	tx, err := s.transactionsT.Insert(fgen.Transaction{
		FromAccountID: "EXTERNAL",
		ToAccountID:   accountID,
		Amount:        amount,
		Currency:      account.Currency,
		Status:        "completed",
		Description:   &desc,
	})
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if _, err := s.ledgerT.Insert(fgen.Ledger{
		AccountID:     accountID,
		TransactionID: tx.ID,
		Amount:        amount,
		BalanceAfter:  newBalance,
		Type:          "credit",
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	s.statsOnDeposit(amount)
	s.pushRecentTx(tx)

	return map[string]any{"balance": newBalance, "transactionId": tx.ID}, http.StatusOK, nil
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

	from, err := s.accountsT.Get(fromID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if from == nil {
		return nil, http.StatusBadRequest, errors.New("source account not found")
	}

	to, err := s.accountsT.Get(toID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	if to == nil {
		return nil, http.StatusBadRequest, errors.New("destination account not found")
	}

	fromBalance := from.Balance
	toBalance := to.Balance
	currency := from.Currency

	if fromBalance < amount {
		desc := defaultString(description, "Transfer (insufficient funds)")
		tx, err := s.transactionsT.Insert(fgen.Transaction{
			FromAccountID: fromID,
			ToAccountID:   toID,
			Amount:        amount,
			Currency:      currency,
			Status:        "failed",
			Description:   &desc,
		})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		s.statsOnFailedTransfer()
		s.pushRecentTx(tx)
		return map[string]any{"status": "failed", "reason": "insufficient_funds", "transactionId": tx.ID}, http.StatusOK, nil
	}

	newFrom := fromBalance - amount
	newTo := toBalance + amount

	if _, err := s.accountsT.Update(fromID, map[string]any{"balance": newFrom}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.accountsT.Update(toID, map[string]any{"balance": newTo}); err != nil {
		return nil, http.StatusBadRequest, err
	}

	desc := defaultString(description, "Transfer")
	tx, err := s.transactionsT.Insert(fgen.Transaction{
		FromAccountID: fromID,
		ToAccountID:   toID,
		Amount:        amount,
		Currency:      currency,
		Status:        "completed",
		Description:   &desc,
	})
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	txID := tx.ID
	if _, err := s.ledgerT.Insert(fgen.Ledger{
		AccountID:     fromID,
		TransactionID: txID,
		Amount:        -amount,
		BalanceAfter:  newFrom,
		Type:          "debit",
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.ledgerT.Insert(fgen.Ledger{
		AccountID:     toID,
		TransactionID: txID,
		Amount:        amount,
		BalanceAfter:  newTo,
		Type:          "credit",
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	s.statsOnCompletedTransfer(amount)
	s.pushRecentTx(tx)

	return map[string]any{"status": "completed", "transactionId": tx.ID}, http.StatusOK, nil
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
		rows, err := s.accountsT.Scan(accountCount, 0)
		if err != nil {
			return err
		}
		var totalBalance float64
		for _, row := range rows {
			totalBalance += row.Balance
		}
		s.stats.mu.Lock()
		s.stats.totalBalance = totalBalance
		s.stats.mu.Unlock()
	}

	txCount := s.transactions.Count()
	if txCount > 0 {
		rows, err := s.transactionsT.Scan(txCount, 0)
		if err != nil {
			return err
		}
		completed := 0
		failed := 0
		var totalVolume float64
		for _, row := range rows {
			switch row.Status {
			case "completed":
				completed++
				totalVolume += row.Amount
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
		s.recentTxRing = make([]fgen.Transaction, s.recentTxLimit)
		s.recentTxHead = 0
		s.recentTxSize = 0
		recentCount := minInt(txCount, s.recentTxLimit)
		if recentCount > 0 {
			offset := txCount - recentCount
			rows, err := s.transactionsT.Scan(recentCount, offset)
			if err != nil {
				s.recentTxMu.Unlock()
				return err
			}
			for _, row := range rows {
				s.recentTxRing[s.recentTxHead] = row
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

func (s *appServer) pushRecentTx(tx fgen.Transaction) {
	if s.recentTxLimit <= 0 {
		return
	}
	s.recentTxMu.Lock()
	if len(s.recentTxRing) == 0 {
		s.recentTxRing = make([]fgen.Transaction, s.recentTxLimit)
	}
	s.recentTxRing[s.recentTxHead] = tx
	s.recentTxHead = (s.recentTxHead + 1) % len(s.recentTxRing)
	if s.recentTxSize < len(s.recentTxRing) {
		s.recentTxSize++
	}
	s.recentTxMu.Unlock()
}

func (s *appServer) getRecentTransactions(limit int) []fgen.Transaction {
	if limit <= 0 {
		limit = 100
	}

	s.recentTxMu.RLock()
	defer s.recentTxMu.RUnlock()

	if s.recentTxSize == 0 || len(s.recentTxRing) == 0 {
		return []fgen.Transaction{}
	}

	n := minInt(limit, s.recentTxSize)
	out := make([]fgen.Transaction, 0, n)
	for i := 0; i < n; i++ {
		idx := (s.recentTxHead - 1 - i + len(s.recentTxRing)) % len(s.recentTxRing)
		out = append(out, s.recentTxRing[idx])
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

func deduplicateTransactionsByID(rows []fgen.Transaction) []fgen.Transaction {
	if len(rows) == 0 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	out := make([]fgen.Transaction, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			out = append(out, row)
			continue
		}
		if _, ok := seen[row.ID]; ok {
			continue
		}
		seen[row.ID] = struct{}{}
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

func structToMap[T any](v T) (map[string]any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func mapToStruct[T any](m map[string]any) (T, error) {
	var out T
	data, err := json.Marshal(m)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

func mapsToStructs[T any](rows []map[string]any) ([]T, error) {
	if len(rows) == 0 {
		return []T{}, nil
	}
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		item, err := mapToStruct[T](row)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
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
