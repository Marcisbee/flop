package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	flop "github.com/marcisbee/flop"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type appServer struct {
	benchDir string
	secret   string

	client       *mongo.Client
	users        *mongo.Collection
	accounts     *mongo.Collection
	transactions *mongo.Collection
	ledger       *mongo.Collection
}

type authCtx struct {
	ID    string
	Email string
	Roles []string
}

type userDoc struct {
	ID        string   `bson:"id" json:"id"`
	Email     string   `bson:"email" json:"email"`
	Password  string   `bson:"password" json:"-"`
	Name      string   `bson:"name" json:"name"`
	Roles     []string `bson:"roles" json:"roles"`
	CreatedAt int64    `bson:"createdAt" json:"createdAt"`
}

type accountDoc struct {
	ID        string  `bson:"id" json:"id"`
	OwnerID   string  `bson:"ownerId" json:"ownerId"`
	Name      string  `bson:"name" json:"name"`
	Type      string  `bson:"type" json:"type"`
	Balance   float64 `bson:"balance" json:"balance"`
	Currency  string  `bson:"currency" json:"currency"`
	CreatedAt int64   `bson:"createdAt" json:"createdAt"`
}

type transactionDoc struct {
	ID            string  `bson:"id" json:"id"`
	FromAccountID string  `bson:"fromAccountId" json:"fromAccountId"`
	ToAccountID   string  `bson:"toAccountId" json:"toAccountId"`
	Amount        float64 `bson:"amount" json:"amount"`
	Currency      string  `bson:"currency" json:"currency"`
	Status        string  `bson:"status" json:"status"`
	Description   string  `bson:"description,omitempty" json:"description,omitempty"`
	CreatedAt     int64   `bson:"createdAt" json:"createdAt"`
}

type ledgerDoc struct {
	ID            string  `bson:"id" json:"id"`
	AccountID     string  `bson:"accountId" json:"accountId"`
	TransactionID string  `bson:"transactionId" json:"transactionId"`
	Amount        float64 `bson:"amount" json:"amount"`
	BalanceAfter  float64 `bson:"balanceAfter" json:"balanceAfter"`
	Type          string  `bson:"type" json:"type"`
	CreatedAt     int64   `bson:"createdAt" json:"createdAt"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	moduleRoot, err := findModuleRoot()
	if err != nil {
		panic(err)
	}
	repoRoot := filepath.Clean(filepath.Join(moduleRoot, ".."))
	benchDir := filepath.Join(repoRoot, "benchmarks", "finance-mongodb-go")

	port := flag.Int("port", 1998, "server port")
	mongoPort := flag.Int("mongo-port", 0, "local mongod port (defaults to port+1000)")
	mongoURI := flag.String("mongo-uri", "", "MongoDB URI (if empty, local mongod is started)")
	defaultMongodBin := strings.TrimSpace(os.Getenv("MONGOD_BIN"))
	if defaultMongodBin == "" {
		localMongod := filepath.Join(filepath.Dir(benchDir), ".tools", "mongodb", "mongod")
		if _, err := os.Stat(localMongod); err == nil {
			defaultMongodBin = localMongod
		} else {
			defaultMongodBin = "mongod"
		}
	}
	mongodBin := flag.String("mongod-bin", defaultMongodBin, "path to mongod executable")
	mongoDir := flag.String("mongo-dir", filepath.Join(benchDir, "data", "mongod"), "local mongod data dir")
	mongoLog := flag.String("mongo-log", filepath.Join(benchDir, "data", "mongod.log"), "local mongod log path")
	mongoDB := flag.String("mongo-db", "finance_bench", "MongoDB database name")
	flag.Parse()

	if *mongoPort == 0 {
		*mongoPort = *port + 1000
	}

	secret := os.Getenv("FLOP_JWT_SECRET")
	if strings.TrimSpace(secret) == "" {
		secret = "mongodb-go-benchmark-secret"
	}

	if err := os.MkdirAll(*mongoDir, 0o755); err != nil {
		panic(err)
	}

	finalURI := strings.TrimSpace(*mongoURI)
	var mongodCmd *exec.Cmd
	if finalURI == "" {
		finalURI = "mongodb://127.0.0.1:" + strconv.Itoa(*mongoPort)
		cmd, err := startMongod(*mongodBin, *mongoDir, *mongoLog, *mongoPort)
		if err != nil {
			panic(err)
		}
		mongodCmd = cmd
	}

	client, err := connectMongo(finalURI)
	if err != nil {
		if mongodCmd != nil {
			_ = stopMongod(mongodCmd)
		}
		panic(err)
	}
	db := client.Database(*mongoDB)
	if err := ensureIndexes(db); err != nil {
		_ = client.Disconnect(context.Background())
		if mongodCmd != nil {
			_ = stopMongod(mongodCmd)
		}
		panic(err)
	}

	srv := &appServer{
		benchDir:     benchDir,
		secret:       secret,
		client:       client,
		users:        db.Collection("users"),
		accounts:     db.Collection("accounts"),
		transactions: db.Collection("transactions"),
		ledger:       db.Collection("ledger"),
	}

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      srv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	closeFn := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
		if mongodCmd != nil {
			_ = stopMongod(mongodCmd)
		}
		return nil
	}

	err = flop.RunDefaultServer(flop.DefaultServerInfo{
		AppName: "MongoDB Go Finance Benchmark",
		Port:    *port,
		DataDir: *mongoDir,
		Engine:  "mongodb",
		Use: []string{
			"deno run --allow-net benchmarks/compare/seed.ts --host=http://localhost:" + strconv.Itoa(*port),
		},
	}, flop.DefaultServeOptions{
		Server: httpSrv,
		Close:  closeFn,
	})
	if err != nil {
		panic(err)
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
			"name":   "mongodb-go-finance",
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
	flusher.Flush()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
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

		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		if err := s.users.FindOne(ctx, bson.M{"email": body.Email}).Err(); err == nil {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": "Email already registered"})
			return
		} else if !errors.Is(err, mongo.ErrNoDocuments) {
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		id := genID(15)
		doc := userDoc{
			ID:        id,
			Email:     body.Email,
			Password:  hashPassword(body.Password, s.secret),
			Name:      body.Name,
			Roles:     []string{},
			CreatedAt: time.Now().UnixMilli(),
		}
		if _, err := s.users.InsertOne(ctx, doc); err != nil {
			if isDuplicateErr(err) {
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
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		var user userDoc
		if err := s.users.FindOne(ctx, bson.M{"email": body.Email}).Decode(&user); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid credentials"})
				return
			}
			jsonResp(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if !verifyPassword(body.Password, user.Password, s.secret) {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "Invalid credentials"})
			return
		}

		token, err := createJWT(map[string]any{
			"sub":   user.ID,
			"email": user.Email,
			"roles": user.Roles,
			"exp":   time.Now().Add(1 * time.Hour).UnixMilli(),
		}, s.secret)
		if err != nil {
			jsonResp(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		refresh, err := createJWT(map[string]any{
			"sub":  user.ID,
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
				"id":    user.ID,
				"email": user.Email,
				"name":  user.Name,
				"roles": user.Roles,
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
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		var user userDoc
		if err := s.users.FindOne(ctx, bson.M{"id": id}).Decode(&user); err != nil {
			jsonResp(w, http.StatusUnauthorized, map[string]any{"error": "User not found"})
			return
		}
		token, err := createJWT(map[string]any{
			"sub":   user.ID,
			"email": user.Email,
			"roles": user.Roles,
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
	data, status, execErr := s.execReducer(r.Context(), name, body, auth)
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
	data, status, err := s.execView(r.Context(), name, params, auth)
	if err != nil {
		jsonResp(w, status, map[string]any{"error": err.Error()})
		return
	}
	jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": data})
}

func (s *appServer) execReducer(ctx context.Context, name string, params map[string]any, auth *authCtx) (any, int, error) {
	switch name {
	case "create_account":
		accountType := toString(params["type"])
		if accountType != "checking" && accountType != "savings" && accountType != "credit" {
			return nil, http.StatusBadRequest, errors.New("invalid account type")
		}
		now := time.Now().UnixMilli()
		doc := accountDoc{
			ID:        genID(15),
			OwnerID:   auth.ID,
			Name:      toString(params["name"]),
			Type:      accountType,
			Balance:   0,
			Currency:  defaultString(toString(params["currency"]), "USD"),
			CreatedAt: now,
		}
		c, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		if _, err := s.accounts.InsertOne(c, doc); err != nil {
			return nil, http.StatusBadRequest, err
		}
		return doc, http.StatusOK, nil
	case "deposit":
		return s.deposit(ctx, toString(params["accountId"]), toFloat64(params["amount"]))
	case "transfer":
		return s.transfer(ctx, toString(params["fromAccountId"]), toString(params["toAccountId"]), toFloat64(params["amount"]), toString(params["description"]))
	default:
		return nil, http.StatusNotFound, errors.New("unknown reducer")
	}
}

func (s *appServer) deposit(ctx context.Context, accountID string, amount float64) (any, int, error) {
	if amount <= 0 {
		return nil, http.StatusBadRequest, errors.New("amount must be positive")
	}

	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var updated accountDoc
	err := s.accounts.FindOneAndUpdate(
		c,
		bson.M{"id": accountID},
		bson.M{"$inc": bson.M{"balance": amount}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&updated)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, http.StatusBadRequest, errors.New("account not found")
		}
		return nil, http.StatusBadRequest, err
	}

	now := time.Now().UnixMilli()
	txID := genID(15)
	if _, err := s.transactions.InsertOne(c, transactionDoc{
		ID:            txID,
		FromAccountID: "EXTERNAL",
		ToAccountID:   accountID,
		Amount:        amount,
		Currency:      updated.Currency,
		Status:        "completed",
		Description:   "Deposit",
		CreatedAt:     now,
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.ledger.InsertOne(c, ledgerDoc{
		ID:            genID(15),
		AccountID:     accountID,
		TransactionID: txID,
		Amount:        amount,
		BalanceAfter:  updated.Balance,
		Type:          "credit",
		CreatedAt:     now,
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	return map[string]any{"balance": updated.Balance, "transactionId": txID}, http.StatusOK, nil
}

func (s *appServer) transfer(ctx context.Context, fromID, toID string, amount float64, description string) (any, int, error) {
	if amount <= 0 {
		return nil, http.StatusBadRequest, errors.New("amount must be positive")
	}
	if fromID == toID {
		return nil, http.StatusBadRequest, errors.New("cannot transfer to same account")
	}

	c, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	var from accountDoc
	err := s.accounts.FindOneAndUpdate(
		c,
		bson.M{"id": fromID, "balance": bson.M{"$gte": amount}},
		bson.M{"$inc": bson.M{"balance": -amount}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&from)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			var fromExists accountDoc
			if findErr := s.accounts.FindOne(c, bson.M{"id": fromID}).Decode(&fromExists); findErr != nil {
				if errors.Is(findErr, mongo.ErrNoDocuments) {
					return nil, http.StatusBadRequest, errors.New("source account not found")
				}
				return nil, http.StatusBadRequest, findErr
			}
			txID := genID(15)
			if _, txErr := s.transactions.InsertOne(c, transactionDoc{
				ID:            txID,
				FromAccountID: fromID,
				ToAccountID:   toID,
				Amount:        amount,
				Currency:      fromExists.Currency,
				Status:        "failed",
				Description:   defaultString(description, "Transfer (insufficient funds)"),
				CreatedAt:     time.Now().UnixMilli(),
			}); txErr != nil {
				return nil, http.StatusBadRequest, txErr
			}
			return map[string]any{"status": "failed", "reason": "insufficient_funds", "transactionId": txID}, http.StatusOK, nil
		}
		return nil, http.StatusBadRequest, err
	}

	var to accountDoc
	err = s.accounts.FindOneAndUpdate(
		c,
		bson.M{"id": toID},
		bson.M{"$inc": bson.M{"balance": amount}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&to)
	if err != nil {
		_, _ = s.accounts.UpdateOne(c, bson.M{"id": fromID}, bson.M{"$inc": bson.M{"balance": amount}})
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, http.StatusBadRequest, errors.New("destination account not found")
		}
		return nil, http.StatusBadRequest, err
	}

	txID := genID(15)
	now := time.Now().UnixMilli()
	if _, err := s.transactions.InsertOne(c, transactionDoc{
		ID:            txID,
		FromAccountID: fromID,
		ToAccountID:   toID,
		Amount:        amount,
		Currency:      from.Currency,
		Status:        "completed",
		Description:   defaultString(description, "Transfer"),
		CreatedAt:     now,
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	if _, err := s.ledger.InsertMany(c, []any{
		ledgerDoc{
			ID:            genID(15),
			AccountID:     fromID,
			TransactionID: txID,
			Amount:        -amount,
			BalanceAfter:  from.Balance,
			Type:          "debit",
			CreatedAt:     now,
		},
		ledgerDoc{
			ID:            genID(15),
			AccountID:     toID,
			TransactionID: txID,
			Amount:        amount,
			BalanceAfter:  to.Balance,
			Type:          "credit",
			CreatedAt:     now,
		},
	}); err != nil {
		return nil, http.StatusBadRequest, err
	}
	return map[string]any{"status": "completed", "transactionId": txID}, http.StatusOK, nil
}

func (s *appServer) execView(ctx context.Context, name string, params map[string]string, auth *authCtx) (any, int, error) {
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	switch name {
	case "get_stats":
		userCount, err := s.users.CountDocuments(c, bson.M{})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		accountCount, err := s.accounts.CountDocuments(c, bson.M{})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		transactionCount, err := s.transactions.CountDocuments(c, bson.M{})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}

		completed := int64(0)
		failed := int64(0)
		totalVolume := float64(0)
		cur, err := s.transactions.Aggregate(c, mongo.Pipeline{
			{{Key: "$group", Value: bson.M{
				"_id":   "$status",
				"count": bson.M{"$sum": 1},
				"total": bson.M{"$sum": "$amount"},
			}}},
		})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		for cur.Next(c) {
			var row struct {
				ID    string  `bson:"_id"`
				Count int64   `bson:"count"`
				Total float64 `bson:"total"`
			}
			if err := cur.Decode(&row); err != nil {
				_ = cur.Close(c)
				return nil, http.StatusBadRequest, err
			}
			if row.ID == "completed" {
				completed = row.Count
				totalVolume = row.Total
			} else if row.ID == "failed" {
				failed = row.Count
			}
		}
		_ = cur.Close(c)

		totalBalance := float64(0)
		balCur, err := s.accounts.Aggregate(c, mongo.Pipeline{
			{{Key: "$group", Value: bson.M{"_id": nil, "total": bson.M{"$sum": "$balance"}}}},
		})
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		if balCur.Next(c) {
			var row struct {
				Total float64 `bson:"total"`
			}
			if err := balCur.Decode(&row); err == nil {
				totalBalance = row.Total
			}
		}
		_ = balCur.Close(c)

		return map[string]any{
			"userCount":             userCount,
			"accountCount":          accountCount,
			"transactionCount":      transactionCount,
			"completedTransactions": completed,
			"failedTransactions":    failed,
			"totalVolume":           totalVolume,
			"totalBalance":          totalBalance,
		}, http.StatusOK, nil

	case "get_all_accounts":
		return s.scanAccounts(c, bson.M{}, 10000)

	case "get_accounts":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		return s.scanAccounts(c, bson.M{"ownerId": auth.ID}, 10000)

	case "get_recent_transactions":
		limit := toInt64(params["limit"])
		if limit <= 0 {
			limit = 100
		}
		return s.scanTransactions(c, bson.M{}, limit)

	case "get_transactions":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := strings.TrimSpace(params["accountId"])
		return s.scanTransactions(c, bson.M{"$or": []bson.M{
			{"fromAccountId": accountID},
			{"toAccountId": accountID},
		}}, 10000)

	case "get_ledger":
		if auth == nil {
			return nil, http.StatusUnauthorized, errors.New("authentication required")
		}
		accountID := strings.TrimSpace(params["accountId"])
		cur, err := s.ledger.Find(
			c,
			bson.M{"accountId": accountID},
			options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(10000),
		)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		defer cur.Close(c)
		out := make([]ledgerDoc, 0, 128)
		for cur.Next(c) {
			var row ledgerDoc
			if err := cur.Decode(&row); err != nil {
				return nil, http.StatusBadRequest, err
			}
			out = append(out, row)
		}
		return out, http.StatusOK, nil
	}

	return nil, http.StatusNotFound, errors.New("unknown view")
}

func (s *appServer) scanAccounts(ctx context.Context, filter bson.M, limit int64) (any, int, error) {
	cur, err := s.accounts.Find(
		ctx,
		filter,
		options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(limit),
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	defer cur.Close(ctx)
	out := make([]accountDoc, 0, 256)
	for cur.Next(ctx) {
		var row accountDoc
		if err := cur.Decode(&row); err != nil {
			return nil, http.StatusBadRequest, err
		}
		out = append(out, row)
	}
	return out, http.StatusOK, nil
}

func (s *appServer) scanTransactions(ctx context.Context, filter bson.M, limit int64) (any, int, error) {
	cur, err := s.transactions.Find(
		ctx,
		filter,
		options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(limit),
	)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	defer cur.Close(ctx)
	out := make([]transactionDoc, 0, 256)
	for cur.Next(ctx) {
		var row transactionDoc
		if err := cur.Decode(&row); err != nil {
			return nil, http.StatusBadRequest, err
		}
		out = append(out, row)
	}
	return out, http.StatusOK, nil
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

func startMongod(bin, dataDir, logPath string, port int) (*exec.Cmd, error) {
	cmd := exec.Command(
		bin,
		"--bind_ip", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--dbpath", dataDir,
		"--logpath", logPath,
		"--quiet",
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func connectMongo(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMaxPoolSize(256))
	if err != nil {
		return nil, err
	}
	for i := 0; i < 120; i++ {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err = client.Ping(pingCtx, readpref.Primary())
		pingCancel()
		if err == nil {
			return client, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	_ = client.Disconnect(context.Background())
	return nil, fmt.Errorf("mongodb startup timeout: %w", err)
}

func ensureIndexes(db *mongo.Database) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := db.Collection("users").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return err
	}
	_, err = db.Collection("accounts").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "ownerId", Value: 1}}},
	})
	if err != nil {
		return err
	}
	_, err = db.Collection("transactions").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "fromAccountId", Value: 1}}},
		{Keys: bson.D{{Key: "toAccountId", Value: 1}}},
		{Keys: bson.D{{Key: "createdAt", Value: -1}}},
	})
	if err != nil {
		return err
	}
	_, err = db.Collection("ledger").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "accountId", Value: 1}}},
		{Keys: bson.D{{Key: "createdAt", Value: -1}}},
	})
	return err
}

func stopMongod(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	case <-done:
	}
	return nil
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

func isDuplicateErr(err error) bool {
	var we mongo.WriteException
	if errors.As(err, &we) {
		for _, e := range we.WriteErrors {
			if e.Code == 11000 {
				return true
			}
		}
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "duplicate key")
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
	case uint:
		return float64(t)
	case uint64:
		return float64(t)
	case uint32:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
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
		i, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return i
	default:
		return 0
	}
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
