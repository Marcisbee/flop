package appschema

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"

	flop "github.com/marcisbee/flop"
)

const benchmarkStatsID = "singleton"

var (
	accountLocks [1024]sync.Mutex
	statsMu      sync.Mutex
)

type tableStore interface {
	Table(name string) *flop.TableInstance
}

type GetRecentTransactionsIn struct {
	Limit int `json:"limit"`
}

type GetTransactionsIn struct {
	AccountID string `json:"accountId"`
}

type GetLedgerIn struct {
	AccountID string `json:"accountId"`
}

type CreateAccountIn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Currency string `json:"currency"`
}

type DepositIn struct {
	AccountID string  `json:"accountId"`
	Amount    float64 `json:"amount"`
}

type TransferIn struct {
	FromAccountID string  `json:"fromAccountId"`
	ToAccountID   string  `json:"toAccountId"`
	Amount        float64 `json:"amount"`
	Description   string  `json:"description"`
}

type statsDelta struct {
	AccountCount          int
	TransactionCount      int
	CompletedTransactions int
	FailedTransactions    int
	TotalVolume           float64
	TotalBalance          float64
}

func RegisterEndpoints(app *flop.App) {
	flop.View(app, "get_stats", flop.Public(), GetStatsView)
	flop.View(app, "get_all_accounts", flop.Public(), GetAllAccountsView)
	flop.View(app, "get_accounts", flop.Authenticated(), GetAccountsView)
	flop.View(app, "get_recent_transactions", flop.Public(), GetRecentTransactionsView)
	flop.View(app, "get_transactions", flop.Authenticated(), GetTransactionsView)
	flop.View(app, "get_ledger", flop.Authenticated(), GetLedgerView)

	flop.Reducer(app, "create_account", flop.Authenticated(), CreateAccountReducer)
	flop.Reducer(app, "deposit", flop.Authenticated(), DepositReducer)
	flop.Reducer(app, "transfer", flop.Authenticated(), TransferReducer)
}

func Initialize(db *flop.Database) error {
	if db == nil {
		return errors.New("database is nil")
	}
	stats, err := rebuildStats(db)
	if err != nil {
		return err
	}
	statsTable := db.Table("benchmark_stats")
	if statsTable == nil {
		return errors.New("benchmark_stats table not found")
	}
	if row, _ := statsTable.Get(benchmarkStatsID); row == nil {
		_, err = statsTable.Insert(stats)
		return err
	}
	_, err = statsTable.Update(benchmarkStatsID, stats)
	return err
}

func GetStatsView(ctx *flop.ViewCtx, _ struct{}) (map[string]any, error) {
	users := ctx.DB.Table("users")
	statsTable := ctx.DB.Table("benchmark_stats")
	if users == nil || statsTable == nil {
		return nil, errors.New("required tables not found")
	}

	row, err := statsTable.Get(benchmarkStatsID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		row, err = rebuildStats(ctx.DB)
		if err != nil {
			return nil, err
		}
		if _, err := statsTable.Insert(row); err != nil {
			return nil, err
		}
	}

	return map[string]any{
		"userCount":             users.Count(),
		"accountCount":          intValue(row["accountCount"]),
		"transactionCount":      intValue(row["transactionCount"]),
		"completedTransactions": intValue(row["completedTransactions"]),
		"failedTransactions":    intValue(row["failedTransactions"]),
		"totalVolume":           floatValue(row["totalVolume"]),
		"totalBalance":          floatValue(row["totalBalance"]),
	}, nil
}

func GetAllAccountsView(ctx *flop.ViewCtx, _ struct{}) ([]map[string]any, error) {
	accounts := ctx.DB.Table("accounts")
	if accounts == nil {
		return nil, errors.New("accounts table not found")
	}
	return accounts.Scan(10000, 0)
}

func GetAccountsView(ctx *flop.ViewCtx, _ struct{}) ([]map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	accounts := ctx.DB.Table("accounts")
	if accounts == nil {
		return nil, errors.New("accounts table not found")
	}
	return accounts.FindByIndex("ownerId", auth.ID)
}

func GetRecentTransactionsView(ctx *flop.ViewCtx, in GetRecentTransactionsIn) ([]map[string]any, error) {
	tx := ctx.DB.Table("transactions")
	if tx == nil {
		return nil, errors.New("transactions table not found")
	}
	limit := clampInt(in.Limit, 1, 1000, 100)
	total := tx.Count()
	if total == 0 {
		return []map[string]any{}, nil
	}
	offset := total - limit
	if offset < 0 {
		offset = 0
	}
	rows, err := tx.Scan(limit, offset)
	if err != nil {
		return nil, err
	}
	// Return newest first to preserve previous behavior.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows, nil
}

func GetTransactionsView(ctx *flop.ViewCtx, in GetTransactionsIn) ([]map[string]any, error) {
	if strings.TrimSpace(in.AccountID) == "" {
		return nil, errors.New("accountId is required")
	}
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	tx := ctx.DB.Table("transactions")
	if tx == nil {
		return nil, errors.New("transactions table not found")
	}

	fromRows, err := tx.FindByIndex("fromAccountId", in.AccountID)
	if err != nil {
		return nil, err
	}
	toRows, err := tx.FindByIndex("toAccountId", in.AccountID)
	if err != nil {
		return nil, err
	}

	rows := deduplicateRowsByID(append(fromRows, toRows...))
	sortByCreatedAtDesc(rows)
	return rows, nil
}

func GetLedgerView(ctx *flop.ViewCtx, in GetLedgerIn) ([]map[string]any, error) {
	if strings.TrimSpace(in.AccountID) == "" {
		return nil, errors.New("accountId is required")
	}
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	ledger := ctx.DB.Table("ledger")
	if ledger == nil {
		return nil, errors.New("ledger table not found")
	}
	rows, err := ledger.FindByIndex("accountId", in.AccountID)
	if err != nil {
		return nil, err
	}
	sortByCreatedAtDesc(rows)
	return rows, nil
}

func CreateAccountReducer(ctx *flop.ReducerCtx, in CreateAccountIn) (map[string]any, error) {
	auth, err := ctx.RequireAuth()
	if err != nil {
		return nil, err
	}
	accountType := strings.TrimSpace(in.Type)
	if accountType != "checking" && accountType != "savings" && accountType != "credit" {
		return nil, errors.New("invalid account type")
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, errors.New("name is required")
	}
	currency := strings.TrimSpace(in.Currency)
	if currency == "" {
		currency = "USD"
	}
	accounts := ctx.DB.Table("accounts")
	if accounts == nil {
		return nil, errors.New("accounts table not found")
	}
	row, err := accounts.Insert(map[string]any{
		"ownerId":  auth.ID,
		"name":     name,
		"type":     accountType,
		"balance":  0.0,
		"currency": currency,
	})
	if err != nil {
		return nil, err
	}
	if err := applyStatsDelta(ctx.DB, statsDelta{AccountCount: 1}); err != nil {
		return nil, err
	}
	return row, nil
}

func DepositReducer(ctx *flop.ReducerCtx, in DepositIn) (map[string]any, error) {
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	accountID := strings.TrimSpace(in.AccountID)
	if accountID == "" {
		return nil, errors.New("accountId is required")
	}
	if in.Amount <= 0 {
		return nil, errors.New("amount must be positive")
	}
	lockFor(accountID).Lock()
	defer lockFor(accountID).Unlock()

	accounts := ctx.DB.Table("accounts")
	transactions := ctx.DB.Table("transactions")
	ledger := ctx.DB.Table("ledger")
	if accounts == nil || transactions == nil || ledger == nil {
		return nil, errors.New("required tables not found")
	}

	account, err := accounts.Get(accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, errors.New("account not found")
	}

	newBalance := floatValue(account["balance"]) + in.Amount
	if _, err := accounts.Update(accountID, map[string]any{"balance": newBalance}); err != nil {
		return nil, err
	}

	tx, err := transactions.Insert(map[string]any{
		"fromAccountId": "EXTERNAL",
		"toAccountId":   accountID,
		"amount":        in.Amount,
		"currency":      defaultString(stringValue(account["currency"]), "USD"),
		"status":        "completed",
		"description":   "Deposit",
	})
	if err != nil {
		return nil, err
	}

	if _, err := ledger.Insert(map[string]any{
		"accountId":     accountID,
		"transactionId": stringValue(tx["id"]),
		"amount":        in.Amount,
		"balanceAfter":  newBalance,
		"type":          "credit",
	}); err != nil {
		return nil, err
	}

	if err := applyStatsDelta(ctx.DB, statsDelta{
		TransactionCount:      1,
		CompletedTransactions: 1,
		TotalVolume:           in.Amount,
		TotalBalance:          in.Amount,
	}); err != nil {
		return nil, err
	}

	return map[string]any{
		"balance":       newBalance,
		"transactionId": stringValue(tx["id"]),
	}, nil
}

func TransferReducer(ctx *flop.ReducerCtx, in TransferIn) (map[string]any, error) {
	if _, err := ctx.RequireAuth(); err != nil {
		return nil, err
	}
	fromID := strings.TrimSpace(in.FromAccountID)
	toID := strings.TrimSpace(in.ToAccountID)
	if fromID == "" || toID == "" {
		return nil, errors.New("fromAccountId and toAccountId are required")
	}
	if fromID == toID {
		return nil, errors.New("cannot transfer to same account")
	}
	if in.Amount <= 0 {
		return nil, errors.New("amount must be positive")
	}

	lockPair(fromID, toID)
	defer unlockPair(fromID, toID)

	accounts := ctx.DB.Table("accounts")
	transactions := ctx.DB.Table("transactions")
	ledger := ctx.DB.Table("ledger")
	if accounts == nil || transactions == nil || ledger == nil {
		return nil, errors.New("required tables not found")
	}

	from, err := accounts.Get(fromID)
	if err != nil {
		return nil, err
	}
	if from == nil {
		return nil, errors.New("source account not found")
	}
	to, err := accounts.Get(toID)
	if err != nil {
		return nil, err
	}
	if to == nil {
		return nil, errors.New("destination account not found")
	}

	fromBalance := floatValue(from["balance"])
	toBalance := floatValue(to["balance"])
	currency := defaultString(stringValue(from["currency"]), "USD")
	description := defaultString(strings.TrimSpace(in.Description), "Transfer")

	if fromBalance < in.Amount {
		tx, err := transactions.Insert(map[string]any{
			"fromAccountId": fromID,
			"toAccountId":   toID,
			"amount":        in.Amount,
			"currency":      currency,
			"status":        "failed",
			"description":   description,
		})
		if err != nil {
			return nil, err
		}
		if err := applyStatsDelta(ctx.DB, statsDelta{
			TransactionCount:   1,
			FailedTransactions: 1,
		}); err != nil {
			return nil, err
		}
		return map[string]any{
			"status":        "failed",
			"reason":        "insufficient_funds",
			"transactionId": stringValue(tx["id"]),
		}, nil
	}

	newFrom := fromBalance - in.Amount
	newTo := toBalance + in.Amount

	if _, err := accounts.Update(fromID, map[string]any{"balance": newFrom}); err != nil {
		return nil, err
	}
	if _, err := accounts.Update(toID, map[string]any{"balance": newTo}); err != nil {
		return nil, err
	}

	tx, err := transactions.Insert(map[string]any{
		"fromAccountId": fromID,
		"toAccountId":   toID,
		"amount":        in.Amount,
		"currency":      currency,
		"status":        "completed",
		"description":   description,
	})
	if err != nil {
		return nil, err
	}
	txID := stringValue(tx["id"])

	if _, err := ledger.Insert(map[string]any{
		"accountId":     fromID,
		"transactionId": txID,
		"amount":        -in.Amount,
		"balanceAfter":  newFrom,
		"type":          "debit",
	}); err != nil {
		return nil, err
	}
	if _, err := ledger.Insert(map[string]any{
		"accountId":     toID,
		"transactionId": txID,
		"amount":        in.Amount,
		"balanceAfter":  newTo,
		"type":          "credit",
	}); err != nil {
		return nil, err
	}

	if err := applyStatsDelta(ctx.DB, statsDelta{
		TransactionCount:      1,
		CompletedTransactions: 1,
		TotalVolume:           in.Amount,
	}); err != nil {
		return nil, err
	}

	return map[string]any{
		"status":        "completed",
		"transactionId": txID,
	}, nil
}

func applyStatsDelta(db tableStore, d statsDelta) error {
	statsMu.Lock()
	defer statsMu.Unlock()

	statsTable := db.Table("benchmark_stats")
	if statsTable == nil {
		return errors.New("benchmark_stats table not found")
	}
	row, err := statsTable.Get(benchmarkStatsID)
	if err != nil {
		return err
	}
	if row == nil {
		row, err = rebuildStats(db)
		if err != nil {
			return err
		}
		if _, err := statsTable.Insert(row); err != nil {
			return err
		}
	}

	next := map[string]any{
		"accountCount":          intValue(row["accountCount"]) + d.AccountCount,
		"transactionCount":      intValue(row["transactionCount"]) + d.TransactionCount,
		"completedTransactions": intValue(row["completedTransactions"]) + d.CompletedTransactions,
		"failedTransactions":    intValue(row["failedTransactions"]) + d.FailedTransactions,
		"totalVolume":           floatValue(row["totalVolume"]) + d.TotalVolume,
		"totalBalance":          floatValue(row["totalBalance"]) + d.TotalBalance,
	}
	_, err = statsTable.Update(benchmarkStatsID, next)
	return err
}

func rebuildStats(db tableStore) (map[string]any, error) {
	accounts := db.Table("accounts")
	transactions := db.Table("transactions")
	if accounts == nil || transactions == nil {
		return nil, errors.New("required tables not found")
	}

	accountCount := accounts.Count()
	totalBalance := 0.0
	if accountCount > 0 {
		rows, err := accounts.Scan(accountCount, 0)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			totalBalance += floatValue(row["balance"])
		}
	}

	transactionCount := transactions.Count()
	completed := 0
	failed := 0
	totalVolume := 0.0
	if transactionCount > 0 {
		rows, err := transactions.Scan(transactionCount, 0)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			status := stringValue(row["status"])
			switch status {
			case "completed":
				completed++
				totalVolume += floatValue(row["amount"])
			case "failed":
				failed++
			}
		}
	}

	return map[string]any{
		"id":                    benchmarkStatsID,
		"accountCount":          accountCount,
		"transactionCount":      transactionCount,
		"completedTransactions": completed,
		"failedTransactions":    failed,
		"totalVolume":           totalVolume,
		"totalBalance":          totalBalance,
	}, nil
}

func deduplicateRowsByID(rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		id := stringValue(row["id"])
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

func sortByCreatedAtDesc(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		return floatValue(rows[i]["createdAt"]) > floatValue(rows[j]["createdAt"])
	})
}

func lockIndex(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(accountLocks)))
}

func lockFor(id string) *sync.Mutex {
	return &accountLocks[lockIndex(id)]
}

func lockPair(a, b string) {
	i1 := lockIndex(a)
	i2 := lockIndex(b)
	if i1 == i2 {
		accountLocks[i1].Lock()
		return
	}
	if i1 < i2 {
		accountLocks[i1].Lock()
		accountLocks[i2].Lock()
		return
	}
	accountLocks[i2].Lock()
	accountLocks[i1].Lock()
}

func unlockPair(a, b string) {
	i1 := lockIndex(a)
	i2 := lockIndex(b)
	if i1 == i2 {
		accountLocks[i1].Unlock()
		return
	}
	if i1 < i2 {
		accountLocks[i2].Unlock()
		accountLocks[i1].Unlock()
		return
	}
	accountLocks[i1].Unlock()
	accountLocks[i2].Unlock()
}

func clampInt(v, lo, hi, fallback int) int {
	if v == 0 {
		v = fallback
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func intValue(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func floatValue(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case string:
		var f float64
		_, _ = fmt.Sscanf(t, "%f", &f)
		return f
	default:
		return 0
	}
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
