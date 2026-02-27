package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	flop "github.com/marcisbee/flop"
	_ "modernc.org/sqlite"
)

type appServer struct {
	db       *sql.DB
	benchDir string
	secret   string
	writeMu  sync.Mutex
}

type authCtx struct {
	ID    string
	Email string
	Roles []string
}

func main() {
	rand.Seed(time.Now().UnixNano())

	moduleRoot, err := findModuleRoot()
	if err != nil {
		panic(err)
	}
	repoRoot := filepath.Clean(filepath.Join(moduleRoot, ".."))
	benchDir := filepath.Join(repoRoot, "benchmarks", "finance-turso-go")

	port := flag.Int("port", 1996, "server port")
	dataPath := flag.String("data", filepath.Join(benchDir, "data", "finance.db"), "database path")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*dataPath), 0o755); err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite", *dataPath)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(0)

	if err := initSQLite(db); err != nil {
		panic(err)
	}

	secret := os.Getenv("FLOP_JWT_SECRET")
	if strings.TrimSpace(secret) == "" {
		secret = "turso-go-benchmark-secret"
	}

	srv := &appServer{db: db, benchDir: benchDir, secret: secret}

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      srv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	err = flop.RunDefaultServer(flop.DefaultServerInfo{
		AppName: "Turso Go Finance Benchmark",
		Port:    *port,
		DataDir: *dataPath,
		Engine:  "sqlite-compatible (turso-go baseline)",
		Use: []string{
			"deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:" + strconv.Itoa(*port),
		},
	}, flop.DefaultServeOptions{
		Server: httpSrv,
		Close:  db.Close,
	})
	if err != nil {
		panic(err)
	}
}

func initSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA temp_store = MEMORY",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}

	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		email TEXT NOT NULL UNIQUE,
		password TEXT NOT NULL,
		name TEXT NOT NULL,
		roles TEXT NOT NULL DEFAULT '[]',
		created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
	);

	CREATE TABLE IF NOT EXISTS accounts (
		id TEXT PRIMARY KEY,
		owner_id TEXT NOT NULL,
		name TEXT NOT NULL,
		type TEXT NOT NULL CHECK(type IN ('checking','savings','credit')),
		balance REAL NOT NULL DEFAULT 0,
		currency TEXT NOT NULL DEFAULT 'USD',
		created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
	);

	CREATE TABLE IF NOT EXISTS transactions (
		id TEXT PRIMARY KEY,
		from_account_id TEXT NOT NULL,
		to_account_id TEXT NOT NULL,
		amount REAL NOT NULL,
		currency TEXT NOT NULL,
		status TEXT NOT NULL CHECK(status IN ('pending','completed','failed')),
		description TEXT,
		created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
	);

	CREATE TABLE IF NOT EXISTS ledger (
		id TEXT PRIMARY KEY,
		account_id TEXT NOT NULL,
		transaction_id TEXT NOT NULL,
		amount REAL NOT NULL,
		balance_after REAL NOT NULL,
		type TEXT NOT NULL CHECK(type IN ('debit','credit')),
		created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
	);

	CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts(owner_id);
	CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions(from_account_id);
	CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions(to_account_id);
	CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id);
	`

	_, err := db.Exec(schema)
	return err
}

func (s *appServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	switch {
	case r.URL.Path == "/" && r.Method == http.MethodGet:
		s.serveIndex(w, r)
		return
	case r.URL.Path == "/api/sse" && r.Method == http.MethodGet:
		s.serveSSE(w, r)
		return
	case strings.HasPrefix(r.URL.Path, "/api/auth/"):
		s.handleAuth(w, r)
		return
	case strings.HasPrefix(r.URL.Path, "/api/reduce/"):
		s.handleReduce(w, r)
		return
	case strings.HasPrefix(r.URL.Path, "/api/view/"):
		s.handleView(w, r)
		return
	case (r.URL.Path == "/_schema" || r.URL.Path == "/api/schema") && r.Method == http.MethodGet:
		jsonResp(w, http.StatusOK, map[string]any{})
		return
	default:
		jsonResp(w, http.StatusNotFound, map[string]any{"error": "Not found"})
		return
	}
}

func (s *appServer) serveIndex(w http.ResponseWriter, r *http.Request) {
	p := filepath.Join(s.benchDir, "index.html")
	if _, err := os.Stat(p); err != nil {
		jsonResp(w, http.StatusOK, map[string]any{
			"name":   "turso-go-finance",
			"status": "ok",
		})
		return
	}
	http.ServeFile(w, r, p)
}

func (s *appServer) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonResp(w, http.StatusInternalServerError, map[string]any{"error": "streaming unsupported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	views := strings.Split(strings.TrimSpace(r.URL.Query().Get("views")), ",")
	if len(views) == 1 && strings.TrimSpace(views[0]) == "" {
		views = nil
	}

	for _, view := range views {
		v := strings.TrimSpace(view)
		if v == "" {
			continue
		}
		params := make(map[string]string)
		for k, vals := range r.URL.Query() {
			if len(vals) == 0 {
				continue
			}
			prefix := v + "."
			if strings.HasPrefix(k, prefix) {
				params[strings.TrimPrefix(k, prefix)] = vals[0]
			}
		}
		data, status, err := s.execView(v, params, nil)
		if err != nil {
			if status == http.StatusNotFound {
				continue
			}
			fmt.Fprintf(w, "event: error:%s\ndata: %s\n\n", v, mustJSON(map[string]any{"error": err.Error()}))
			continue
		}
		fmt.Fprintf(w, "event: snapshot:%s\ndata: %s\n\n", v, mustJSON(data))
	}
	flusher.Flush()

	ctx := r.Context()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
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
		if strings.TrimSpace(body.Email) == "" || strings.TrimSpace(body.Password) == "" {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Email and password required"})
			return
		}
		id := genID(15)
		roles := "[]"
		err := withRetry(func() error {
			_, execErr := s.db.Exec(
				"INSERT INTO users (id, email, password, name, roles) VALUES (?, ?, ?, ?, ?)",
				id,
				body.Email,
				hashPassword(body.Password, s.secret),
				body.Name,
				roles,
			)
			return execErr
		})
		if err != nil {
			if isUniqueErr(err) {
				jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Email already registered"})
				return
			}
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		token, err := createJWT(map[string]any{
			"sub":   id,
			"email": body.Email,
			"roles": []string{},
			"exp":   time.Now().Add(1 * time.Hour).UnixMilli(),
		}, s.secret)
		if err != nil {
			jsonResp(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		jsonResp(w, http.StatusOK, map[string]any{
			"token": token,
			"user": map[string]any{
				"id":    id,
				"email": body.Email,
				"name":  body.Name,
				"roles": []string{},
			},
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
		var id string
		var passHash string
		var rolesRaw string
		if err := s.db.QueryRow("SELECT id, password, roles FROM users WHERE email = ?", body.Email).Scan(&id, &passHash, &rolesRaw); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid credentials"})
				return
			}
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if !verifyPassword(body.Password, passHash, s.secret) {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid credentials"})
			return
		}
		roles := parseRoles(rolesRaw)

		token, err := createJWT(map[string]any{
			"sub":   id,
			"email": body.Email,
			"roles": roles,
			"exp":   time.Now().Add(1 * time.Hour).UnixMilli(),
		}, s.secret)
		if err != nil {
			jsonResp(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		refresh, err := createJWT(map[string]any{
			"sub":  id,
			"type": "refresh",
			"exp":  time.Now().Add(24 * time.Hour).UnixMilli(),
		}, s.secret)
		if err != nil {
			jsonResp(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		jsonResp(w, http.StatusOK, map[string]any{
			"token":        token,
			"refreshToken": refresh,
			"user": map[string]any{
				"id":    id,
				"email": body.Email,
				"roles": roles,
			},
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
		claims, err := verifyJWT(body.RefreshToken, s.secret)
		if err != nil {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid refresh token"})
			return
		}
		if toString(claims["type"]) != "refresh" {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid refresh token"})
			return
		}
		id := toString(claims["sub"])
		var email string
		var rolesRaw string
		if err := s.db.QueryRow("SELECT email, roles FROM users WHERE id = ?", id).Scan(&email, &rolesRaw); err != nil {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "User not found"})
			return
		}
		roles := parseRoles(rolesRaw)
		token, err := createJWT(map[string]any{
			"sub":   id,
			"email": email,
			"roles": roles,
			"exp":   time.Now().Add(1 * time.Hour).UnixMilli(),
		}, s.secret)
		if err != nil {
			jsonResp(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		jsonResp(w, http.StatusOK, map[string]any{"token": token})
	default:
		jsonResp(w, http.StatusNotFound, map[string]any{"error": "Unknown auth endpoint"})
	}
}

func (s *appServer) handleReduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
		return
	}
	auth, err := s.authFromRequest(r)
	if err != nil {
		jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Authentication required"})
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Invalid JSON"})
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/reduce/")
	data, status, execErr := s.execReducer(name, body, auth)
	if execErr != nil {
		jsonResp(w, status, map[string]any{"error": execErr.Error()})
		return
	}
	jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": data})
}

func (s *appServer) handleView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonResp(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed"})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/view/")
	params := make(map[string]string)
	for k, vals := range r.URL.Query() {
		if len(vals) == 0 {
			continue
		}
		params[k] = vals[0]
	}
	auth, _ := s.authFromRequest(r)
	data, status, err := s.execView(name, params, auth)
	if err != nil {
		jsonResp(w, status, map[string]any{"error": err.Error()})
		return
	}
	jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": data})
}

func (s *appServer) execReducer(name string, params map[string]any, auth *authCtx) (any, int, error) {
	switch name {
	case "create_account":
		accountType := toString(params["type"])
		if accountType != "checking" && accountType != "savings" && accountType != "credit" {
			return nil, http.StatusBadRequest, errors.New("invalid account type")
		}
		id := genID(15)
		currency := defaultString(toString(params["currency"]), "USD")
		err := withRetry(func() error {
			_, execErr := s.db.Exec(
				"INSERT INTO accounts (id, owner_id, name, type, balance, currency, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
				id,
				auth.ID,
				toString(params["name"]),
				accountType,
				0.0,
				currency,
				time.Now().UnixMilli(),
			)
			return execErr
		})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return map[string]any{
			"id":        id,
			"ownerId":   auth.ID,
			"name":      toString(params["name"]),
			"type":      accountType,
			"balance":   0.0,
			"currency":  currency,
			"createdAt": time.Now().UnixMilli(),
		}, http.StatusOK, nil
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

	var out map[string]any
	err := s.withWriteTx(func(tx *sql.Tx) error {
		var balance float64
		var currency string
		err := tx.QueryRow("SELECT balance, currency FROM accounts WHERE id = ?", accountID).Scan(&balance, &currency)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("account not found")
			}
			return err
		}
		newBalance := balance + amount
		if _, err := tx.Exec("UPDATE accounts SET balance = ? WHERE id = ?", newBalance, accountID); err != nil {
			return err
		}
		txID := genID(15)
		if _, err := tx.Exec(
			"INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			txID, "EXTERNAL", accountID, amount, currency, "completed", "Deposit", time.Now().UnixMilli(),
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			genID(15), accountID, txID, amount, newBalance, "credit", time.Now().UnixMilli(),
		); err != nil {
			return err
		}
		out = map[string]any{"balance": newBalance, "transactionId": txID}
		return nil
	})
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	return out, http.StatusOK, nil
}

func (s *appServer) transfer(_ string, fromID, toID string, amount float64, description string) (any, int, error) {
	if amount <= 0 {
		return nil, http.StatusBadRequest, errors.New("amount must be positive")
	}
	if fromID == toID {
		return nil, http.StatusBadRequest, errors.New("cannot transfer to same account")
	}

	var out map[string]any
	err := s.withWriteTx(func(tx *sql.Tx) error {
		var fromBalance float64
		var fromCurrency string
		if err := tx.QueryRow("SELECT balance, currency FROM accounts WHERE id = ?", fromID).Scan(&fromBalance, &fromCurrency); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("source account not found")
			}
			return err
		}

		var toBalance float64
		if err := tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", toID).Scan(&toBalance); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("destination account not found")
			}
			return err
		}

		txID := genID(15)
		now := time.Now().UnixMilli()
		if fromBalance < amount {
			if _, err := tx.Exec(
				"INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				txID, fromID, toID, amount, fromCurrency, "failed", defaultString(description, "Transfer (insufficient funds)"), now,
			); err != nil {
				return err
			}
			out = map[string]any{"status": "failed", "reason": "insufficient_funds", "transactionId": txID}
			return nil
		}

		newFrom := fromBalance - amount
		newTo := toBalance + amount

		if _, err := tx.Exec("UPDATE accounts SET balance = ? WHERE id = ?", newFrom, fromID); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE accounts SET balance = ? WHERE id = ?", newTo, toID); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT INTO transactions (id, from_account_id, to_account_id, amount, currency, status, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			txID, fromID, toID, amount, fromCurrency, "completed", defaultString(description, "Transfer"), now,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			genID(15), fromID, txID, -amount, newFrom, "debit", now,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT INTO ledger (id, account_id, transaction_id, amount, balance_after, type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			genID(15), toID, txID, amount, newTo, "credit", now,
		); err != nil {
			return err
		}

		out = map[string]any{"status": "completed", "transactionId": txID}
		return nil
	})
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	return out, http.StatusOK, nil
}

func (s *appServer) withWriteTx(fn func(tx *sql.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		s.writeMu.Lock()
		tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			s.writeMu.Unlock()
			if isLockedErr(err) {
				lastErr = err
				time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
				continue
			}
			return err
		}
		err = fn(tx)
		if err != nil {
			_ = tx.Rollback()
			s.writeMu.Unlock()
			if isLockedErr(err) {
				lastErr = err
				time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
				continue
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			if isLockedErr(err) {
				lastErr = err
				time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
				continue
			}
			return err
		}
		s.writeMu.Unlock()
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("write transaction failed")
}

func (s *appServer) execView(name string, params map[string]string, auth *authCtx) (any, int, error) {
	switch name {
	case "get_stats":
		stats, err := s.getStats()
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return stats, http.StatusOK, nil
	case "get_all_accounts":
		rows, err := s.scanAccounts("SELECT id, owner_id, name, type, balance, currency, created_at FROM accounts LIMIT 10000")
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	case "get_accounts":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		rows, err := s.scanAccounts("SELECT id, owner_id, name, type, balance, currency, created_at FROM accounts WHERE owner_id = ? LIMIT 10000", auth.ID)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	case "get_recent_transactions":
		limit := atoiOr(params["limit"], 100)
		if limit <= 0 {
			limit = 100
		}
		rows, err := s.scanTransactions("SELECT id, from_account_id, to_account_id, amount, currency, status, description, created_at FROM transactions ORDER BY created_at DESC LIMIT ?", limit)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	case "get_transactions":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := params["accountId"]
		rows, err := s.scanTransactions("SELECT id, from_account_id, to_account_id, amount, currency, status, description, created_at FROM transactions WHERE from_account_id = ? OR to_account_id = ? ORDER BY created_at DESC LIMIT 10000", accountID, accountID)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	case "get_ledger":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := params["accountId"]
		rows, err := s.scanLedger("SELECT id, account_id, transaction_id, amount, balance_after, type, created_at FROM ledger WHERE account_id = ? ORDER BY created_at DESC LIMIT 10000", accountID)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		return rows, http.StatusOK, nil
	default:
		return nil, http.StatusNotFound, errors.New("unknown view")
	}
}

func (s *appServer) getStats() (map[string]any, error) {
	var userCount int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount); err != nil {
		return nil, err
	}
	var accountCount int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM accounts").Scan(&accountCount); err != nil {
		return nil, err
	}
	var transactionCount int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM transactions").Scan(&transactionCount); err != nil {
		return nil, err
	}

	completed := 0
	failed := 0
	volume := 0.0
	rows, err := s.db.Query("SELECT status, COUNT(*), COALESCE(SUM(amount), 0) FROM transactions GROUP BY status")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var status string
		var count int
		var total float64
		if err := rows.Scan(&status, &count, &total); err != nil {
			rows.Close()
			return nil, err
		}
		if status == "completed" {
			completed = count
			volume = total
		} else if status == "failed" {
			failed = count
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var totalBalance float64
	if err := s.db.QueryRow("SELECT COALESCE(SUM(balance), 0) FROM accounts").Scan(&totalBalance); err != nil {
		return nil, err
	}

	return map[string]any{
		"userCount":             userCount,
		"accountCount":          accountCount,
		"transactionCount":      transactionCount,
		"completedTransactions": completed,
		"failedTransactions":    failed,
		"totalVolume":           volume,
		"totalBalance":          totalBalance,
	}, nil
}

func (s *appServer) scanAccounts(query string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		var id, ownerID, name, typ, currency string
		var balance float64
		var createdAt int64
		if err := rows.Scan(&id, &ownerID, &name, &typ, &balance, &currency, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id":        id,
			"ownerId":   ownerID,
			"name":      name,
			"type":      typ,
			"balance":   balance,
			"currency":  currency,
			"createdAt": createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *appServer) scanTransactions(query string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		var id, fromID, toID, currency, status string
		var description sql.NullString
		var amount float64
		var createdAt int64
		if err := rows.Scan(&id, &fromID, &toID, &amount, &currency, &status, &description, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id":            id,
			"fromAccountId": fromID,
			"toAccountId":   toID,
			"amount":        amount,
			"currency":      currency,
			"status":        status,
			"description":   description.String,
			"createdAt":     createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *appServer) scanLedger(query string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		var id, accountID, transactionID, typ string
		var amount float64
		var balanceAfter float64
		var createdAt int64
		if err := rows.Scan(&id, &accountID, &transactionID, &amount, &balanceAfter, &typ, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id":            id,
			"accountId":     accountID,
			"transactionId": transactionID,
			"amount":        amount,
			"balanceAfter":  balanceAfter,
			"type":          typ,
			"createdAt":     createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *appServer) authFromRequest(r *http.Request) (*authCtx, error) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return nil, errors.New("missing bearer token")
	}
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	claims, err := verifyJWT(token, s.secret)
	if err != nil {
		return nil, err
	}
	id := toString(claims["sub"])
	if id == "" {
		return nil, errors.New("missing subject")
	}
	email := toString(claims["email"])
	roles := parseRolesFromAny(claims["roles"])
	return &authCtx{ID: id, Email: email, Roles: roles}, nil
}

func hashPassword(password, secret string) string {
	sum := sha256.Sum256([]byte(password + secret))
	return hex.EncodeToString(sum[:])
}

func verifyPassword(password, hash, secret string) bool {
	return hashPassword(password, secret) == hash
}

func createJWT(payload map[string]any, secret string) (string, error) {
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	h, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	hEnc := base64.RawURLEncoding.EncodeToString(h)
	pEnc := base64.RawURLEncoding.EncodeToString(p)
	sig := signJWT(hEnc+"."+pEnc, secret)
	return hEnc + "." + pEnc + "." + sig, nil
}

func verifyJWT(token, secret string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid token")
	}
	expected := signJWT(parts[0]+"."+parts[1], secret)
	if !hmac.Equal([]byte(parts[2]), []byte(expected)) {
		return nil, errors.New("invalid signature")
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	claims := make(map[string]any)
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, err
	}
	exp := toInt64(claims["exp"])
	if exp > 0 && time.Now().UnixMilli() > exp {
		return nil, errors.New("token expired")
	}
	return claims, nil
}

func signJWT(input, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(input))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func parseRoles(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	var roles []string
	if err := json.Unmarshal([]byte(raw), &roles); err != nil {
		return []string{}
	}
	if roles == nil {
		return []string{}
	}
	return roles
}

func parseRolesFromAny(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		s := toString(x)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func isUniqueErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unique") || strings.Contains(s, "constraint")
}

func isLockedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "database is locked") || strings.Contains(s, "locked")
}

func withRetry(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isLockedErr(err) {
			return err
		}
		lastErr = err
		time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("retry failed")
}

func genID(length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	if length <= 0 {
		return ""
	}
	b := make([]byte, length)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func jsonResp(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"marshal failed"}`
	}
	return string(b)
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	case nil:
		return ""
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
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	case json.Number:
		i, _ := t.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(t, 10, 64)
		return i
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
