// Finance benchmark server using the go2 flop engine.
// Implements the exact API contract expected by compare/workload.ts:
//   POST /api/auth/register    { email, password, name } → { token }
//   POST /api/auth/password    { email, password }       → { token }
//   POST /api/reduce/{name}    Bearer token, JSON body   → { data: {...} }
//   GET  /api/view/{name}?...  optional Bearer token     → { data: {...} }
package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

// ─── globals ────────────────────────────────────────────────────────────────

var (
	db        *flop.DB
	tokensMu  sync.RWMutex
	tokens    = map[string]uint64{} // token → userID
	jwtSecret = "go2-finance-benchmark-secret"

	// stats counter (only for totalBalance which isn't stored in DB)
	statTotalBalance atomic.Int64 // stored as cents
)

// ─── schemas ────────────────────────────────────────────────────────────────

var userSchema = &flop.Schema{
	Name: "users",
	Fields: []flop.Field{
		{Name: "email", Type: flop.FieldString, Required: true, Unique: true, MaxLen: 255},
		{Name: "password", Type: flop.FieldString, Required: true},
		{Name: "name", Type: flop.FieldString, MaxLen: 100},
	},
}

var accountSchema = &flop.Schema{
	Name: "accounts",
	Fields: []flop.Field{
		{Name: "owner_id", Type: flop.FieldRef, RefTable: "users", Required: true, Indexed: true},
		{Name: "name", Type: flop.FieldString, Required: true, MaxLen: 200},
		{Name: "type", Type: flop.FieldString, Required: true},
		{Name: "currency", Type: flop.FieldString, Required: true},
		{Name: "balance", Type: flop.FieldFloat},
	},
}

var transactionSchema = &flop.Schema{
	Name: "transactions",
	Fields: []flop.Field{
		{Name: "from_account", Type: flop.FieldRef, RefTable: "accounts", Indexed: true},
		{Name: "to_account", Type: flop.FieldRef, RefTable: "accounts", Required: true, Indexed: true},
		{Name: "amount", Type: flop.FieldFloat, Required: true},
		{Name: "type", Type: flop.FieldString, Required: true},
		{Name: "description", Type: flop.FieldString, MaxLen: 500},
		{Name: "status", Type: flop.FieldString, Required: true},
	},
}

// ─── helpers ────────────────────────────────────────────────────────────────

func makeToken(userID uint64) string {
	b := make([]byte, 24)
	rand.Read(b)
	mac := hmac.New(sha256.New, []byte(jwtSecret))
	mac.Write(b)
	mac.Write([]byte(fmt.Sprintf("%d", userID)))
	sig := mac.Sum(nil)
	token := hex.EncodeToString(b) + "." + hex.EncodeToString(sig[:16])

	tokensMu.Lock()
	tokens[token] = userID
	tokensMu.Unlock()

	return token
}

func authFromRequest(r *http.Request) uint64 {
	h := r.Header.Get("Authorization")
	token := strings.TrimPrefix(h, "Bearer ")
	if token == "" || token == h {
		return 0
	}
	tokensMu.RLock()
	uid := tokens[token]
	tokensMu.RUnlock()
	return uid
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func readBody(r *http.Request) map[string]any {
	var m map[string]any
	json.NewDecoder(r.Body).Decode(&m)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

func toStr(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func toF64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	}
	return 0
}

func toID(v any) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case float64:
		return uint64(n)
	case int:
		return uint64(n)
	case int64:
		return uint64(n)
	case string:
		id, _ := strconv.ParseUint(n, 10, 64)
		return id
	}
	return 0
}

// ─── auth handlers ──────────────────────────────────────────────────────────

func handleRegister(w http.ResponseWriter, r *http.Request) {
	body := readBody(r)
	email := toStr(body["email"])
	password := toStr(body["password"])
	name := toStr(body["name"])

	if email == "" || password == "" {
		writeJSON(w, 400, map[string]any{"error": "email and password required"})
		return
	}

	// Simple password hash (fast for benchmarks)
	hash := sha256Hex(password)

	row, err := db.Insert("users", map[string]any{
		"email":    email,
		"password": hash,
		"name":     name,
	})
	if err != nil {
		if strings.Contains(err.Error(), "unique constraint") {
			writeJSON(w, 409, map[string]any{"error": "email already exists"})
			return
		}
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}

	token := makeToken(row.ID)
	writeJSON(w, 200, map[string]any{"token": token})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	body := readBody(r)
	email := toStr(body["email"])
	password := toStr(body["password"])
	hash := sha256Hex(password)

	var found *flop.Row
	db.Table("users").Scan(func(row *flop.Row) bool {
		if toStr(row.Data["email"]) == email && toStr(row.Data["password"]) == hash {
			found = row
			return false
		}
		return true
	})

	if found == nil {
		writeJSON(w, 401, map[string]any{"error": "invalid credentials"})
		return
	}

	token := makeToken(found.ID)
	writeJSON(w, 200, map[string]any{"token": token})
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ─── reduce handlers ────────────────────────────────────────────────────────

func handleReduce(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	uid := authFromRequest(r)
	if uid == 0 {
		writeJSON(w, 401, map[string]any{"error": "unauthorized"})
		return
	}

	body := readBody(r)

	switch name {
	case "create_account":
		handleCreateAccount(w, uid, body)
	case "deposit":
		handleDeposit(w, uid, body)
	case "transfer":
		handleTransfer(w, uid, body)
	case "edit_transfer":
		handleTransfer(w, uid, body) // same logic
	default:
		writeJSON(w, 404, map[string]any{"error": "unknown reducer: " + name})
	}
}

func handleCreateAccount(w http.ResponseWriter, uid uint64, body map[string]any) {
	row, err := db.Insert("accounts", map[string]any{
		"owner_id": uid,
		"name":     toStr(body["name"]),
		"type":     toStr(body["type"]),
		"currency": toStr(body["currency"]),
		"balance":  0.0,
	})
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"data": map[string]any{
		"id": fmt.Sprintf("%d", row.ID),
	}})
}

func handleDeposit(w http.ResponseWriter, uid uint64, body map[string]any) {
	accID := toID(body["accountId"])
	amount := toF64(body["amount"])

	if accID == 0 || amount <= 0 {
		writeJSON(w, 400, map[string]any{"error": "invalid accountId or amount"})
		return
	}

	acc, err := db.Table("accounts").Get(accID)
	if err != nil || acc == nil {
		writeJSON(w, 404, map[string]any{"error": "account not found"})
		return
	}

	newBalance := toF64(acc.Data["balance"]) + amount
	db.Update("accounts", accID, map[string]any{"balance": newBalance})

	tx, _ := db.Insert("transactions", map[string]any{
		"from_account": uint64(0),
		"to_account":   accID,
		"amount":       amount,
		"type":         "deposit",
		"description":  "Deposit",
		"status":       "completed",
	})

	statTotalBalance.Add(int64(amount * 100))

	writeJSON(w, 200, map[string]any{"data": map[string]any{
		"id":      fmt.Sprintf("%d", tx.ID),
		"status":  "completed",
		"balance": newBalance,
	}})
}

func handleTransfer(w http.ResponseWriter, uid uint64, body map[string]any) {
	fromID := toID(body["fromAccountId"])
	toID_ := toID(body["toAccountId"])
	amount := toF64(body["amount"])
	desc := toStr(body["description"])

	if fromID == 0 || toID_ == 0 || amount <= 0 {
		writeJSON(w, 400, map[string]any{"error": "invalid transfer params"})
		return
	}

	fromAcc, _ := db.Table("accounts").Get(fromID)
	toAcc, _ := db.Table("accounts").Get(toID_)
	if fromAcc == nil || toAcc == nil {
		writeJSON(w, 404, map[string]any{"error": "account not found"})
		return
	}

	fromBalance := toF64(fromAcc.Data["balance"])
	if fromBalance < amount {
		// Insufficient funds — still record as failed
		tx, _ := db.Insert("transactions", map[string]any{
			"from_account": fromID,
			"to_account":   toID_,
			"amount":       amount,
			"type":         "transfer",
			"description":  desc,
			"status":       "failed",
		})
		writeJSON(w, 200, map[string]any{"data": map[string]any{
			"id":     fmt.Sprintf("%d", tx.ID),
			"status": "failed",
			"reason": "insufficient_funds",
		}})
		return
	}

	// Debit + credit
	db.Update("accounts", fromID, map[string]any{
		"balance": fromBalance - amount,
	})
	db.Update("accounts", toID_, map[string]any{
		"balance": toF64(toAcc.Data["balance"]) + amount,
	})

	tx, _ := db.Insert("transactions", map[string]any{
		"from_account": fromID,
		"to_account":   toID_,
		"amount":       amount,
		"type":         "transfer",
		"description":  desc,
		"status":       "completed",
	})

	writeJSON(w, 200, map[string]any{"data": map[string]any{
		"id":     fmt.Sprintf("%d", tx.ID),
		"status": "completed",
	}})
}

// ─── view handlers ──────────────────────────────────────────────────────────

func handleView(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	switch name {
	case "get_stats":
		handleGetStats(w, r)
	case "get_all_accounts":
		handleGetAllAccounts(w, r)
	case "get_recent_transactions":
		handleGetRecentTransactions(w, r)
	case "get_transactions":
		handleGetTransactions(w, r)
	default:
		writeJSON(w, 404, map[string]any{"error": "unknown view: " + name})
	}
}

func handleGetStats(w http.ResponseWriter, r *http.Request) {
	userCount, _ := db.Table("users").Count()
	accCount, _ := db.Table("accounts").Count()
	txCount, _ := db.Table("transactions").Count()

	writeJSON(w, 200, map[string]any{"data": map[string]any{
		"users":        userCount,
		"accounts":     accCount,
		"transactions": txCount,
		"totalBalance": math.Round(float64(statTotalBalance.Load()) / 100.0),
	}})
}

func handleGetAllAccounts(w http.ResponseWriter, r *http.Request) {
	uid := authFromRequest(r)
	var accounts []map[string]any

	if uid != 0 {
		// Use secondary index lookup
		db.Table("accounts").ScanByField("owner_id", uid, func(row *flop.Row) bool {
			accounts = append(accounts, map[string]any{
				"id":       fmt.Sprintf("%d", row.ID),
				"name":     row.Data["name"],
				"type":     row.Data["type"],
				"currency": row.Data["currency"],
				"balance":  row.Data["balance"],
			})
			return true
		})
	} else {
		db.Table("accounts").Scan(func(row *flop.Row) bool {
			accounts = append(accounts, map[string]any{
				"id":       fmt.Sprintf("%d", row.ID),
				"name":     row.Data["name"],
				"type":     row.Data["type"],
				"currency": row.Data["currency"],
				"balance":  row.Data["balance"],
			})
			return true
		})
	}

	if accounts == nil {
		accounts = []map[string]any{}
	}
	writeJSON(w, 200, map[string]any{"data": accounts})
}

func handleGetRecentTransactions(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			limit = n
		}
	}

	// Use reverse scan — reads last N rows without full table scan
	txns := make([]map[string]any, 0, limit)
	db.Table("transactions").ScanLast(limit, func(row *flop.Row) bool {
		txns = append(txns, map[string]any{
			"id":          fmt.Sprintf("%d", row.ID),
			"fromAccount": fmt.Sprintf("%d", toID(row.Data["from_account"])),
			"toAccount":   fmt.Sprintf("%d", toID(row.Data["to_account"])),
			"amount":      row.Data["amount"],
			"type":        row.Data["type"],
			"description": row.Data["description"],
			"status":      row.Data["status"],
		})
		return true
	})

	writeJSON(w, 200, map[string]any{"data": txns})
}

func handleGetTransactions(w http.ResponseWriter, r *http.Request) {
	accIDStr := r.URL.Query().Get("accountId")
	accID := toID(accIDStr)

	seen := map[uint64]bool{}
	var txns []map[string]any

	addRow := func(row *flop.Row) bool {
		if seen[row.ID] {
			return true
		}
		seen[row.ID] = true
		txns = append(txns, map[string]any{
			"id":          fmt.Sprintf("%d", row.ID),
			"fromAccount": fmt.Sprintf("%d", toID(row.Data["from_account"])),
			"toAccount":   fmt.Sprintf("%d", toID(row.Data["to_account"])),
			"amount":      row.Data["amount"],
			"type":        row.Data["type"],
			"description": row.Data["description"],
			"status":      row.Data["status"],
		})
		return true
	}

	// Use secondary indexes for both from_account and to_account
	db.Table("transactions").ScanByField("from_account", accID, addRow)
	db.Table("transactions").ScanByField("to_account", accID, addRow)

	if txns == nil {
		txns = []map[string]any{}
	}
	writeJSON(w, 200, map[string]any{"data": txns})
}

// ─── main ───────────────────────────────────────────────────────────────────

func main() {
	port := flag.Int("port", 1985, "HTTP port")
	dataDir := flag.String("data", "benchmarks/finance-go2/data", "data directory")
	flag.Parse()

	if s := os.Getenv("FLOP_JWT_SECRET"); s != "" {
		jwtSecret = s
	}

	var err error
	db, err = flop.OpenDB(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, s := range []*flop.Schema{userSchema, accountSchema, transactionSchema} {
		if _, err := db.CreateTable(s); err != nil {
			log.Fatal(err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/auth/register", handleRegister)
	mux.HandleFunc("POST /api/auth/password", handleLogin)
	mux.HandleFunc("POST /api/reduce/{name}", handleReduce)
	mux.HandleFunc("GET /api/view/{name}", handleView)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		db.Flush()
		srv.Close()
	}()

	log.Printf("flop-go2 finance server on :%d (data=%s)", *port, *dataDir)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
