package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	flop "github.com/marcisbee/flop/go2"
)

func main() {
	dir, _ := os.MkdirTemp("", "flop-bench-*")
	defer os.RemoveAll(dir)

	db, err := flop.OpenDB(dir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Define schemas
	accountSchema := &flop.Schema{
		Name: "accounts",
		Fields: []flop.Field{
			{Name: "name", Type: flop.FieldString, Required: true, MaxLen: 100},
			{Name: "email", Type: flop.FieldString, Required: true, Unique: true},
			{Name: "balance", Type: flop.FieldFloat, Required: true},
			{Name: "currency", Type: flop.FieldString, Required: true},
			{Name: "status", Type: flop.FieldString, EnumValues: []string{"active", "frozen", "closed"}},
		},
	}

	transactionSchema := &flop.Schema{
		Name: "transactions",
		Fields: []flop.Field{
			{Name: "from_account", Type: flop.FieldRef, RefTable: "accounts", Required: true},
			{Name: "to_account", Type: flop.FieldRef, RefTable: "accounts", Required: true},
			{Name: "amount", Type: flop.FieldFloat, Required: true},
			{Name: "currency", Type: flop.FieldString, Required: true},
			{Name: "description", Type: flop.FieldString, Searchable: true, MaxLen: 500},
			{Name: "category", Type: flop.FieldString},
			{Name: "status", Type: flop.FieldString, EnumValues: []string{"pending", "completed", "failed"}},
		},
	}

	ledgerSchema := &flop.Schema{
		Name: "ledger",
		Fields: []flop.Field{
			{Name: "account_id", Type: flop.FieldRef, RefTable: "accounts", Required: true},
			{Name: "transaction_id", Type: flop.FieldRef, RefTable: "transactions", Required: true},
			{Name: "debit", Type: flop.FieldFloat},
			{Name: "credit", Type: flop.FieldFloat},
			{Name: "running_balance", Type: flop.FieldFloat},
		},
	}

	db.CreateTable(accountSchema)
	db.CreateTable(transactionSchema)
	db.CreateTable(ledgerSchema)

	currencies := []string{"USD", "EUR", "GBP", "JPY", "CHF"}
	categories := []string{"transfer", "payment", "refund", "fee", "interest", "salary", "purchase"}

	fmt.Println("=== FLOP Finance Benchmark ===")
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Println()

	// Benchmark 1: Insert accounts
	numAccounts := 10000
	fmt.Printf("--- Insert %d accounts ---\n", numAccounts)
	start := time.Now()
	var accountIDs []uint64

	for i := 0; i < numAccounts; i++ {
		row, err := db.Insert("accounts", map[string]any{
			"name":     fmt.Sprintf("Account-%d", i),
			"email":    fmt.Sprintf("user%d@bank.com", i),
			"balance":  float64(rand.Intn(1000000)) / 100.0,
			"currency": currencies[rand.Intn(len(currencies))],
			"status":   "active",
		})
		if err != nil {
			panic(err)
		}
		accountIDs = append(accountIDs, row.ID)
	}

	elapsed := time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f inserts/sec\n", float64(numAccounts)/elapsed.Seconds())
	printMemUsage()

	// Benchmark 2: Insert transactions
	numTxns := 50000
	fmt.Printf("\n--- Insert %d transactions ---\n", numTxns)
	start = time.Now()
	var txnIDs []uint64

	for i := 0; i < numTxns; i++ {
		from := accountIDs[rand.Intn(len(accountIDs))]
		to := accountIDs[rand.Intn(len(accountIDs))]
		for to == from {
			to = accountIDs[rand.Intn(len(accountIDs))]
		}
		amount := float64(rand.Intn(100000)) / 100.0

		row, err := db.Insert("transactions", map[string]any{
			"from_account": from,
			"to_account":   to,
			"amount":       amount,
			"currency":     currencies[rand.Intn(len(currencies))],
			"description":  fmt.Sprintf("Payment %d from account %d to %d for services", i, from, to),
			"category":     categories[rand.Intn(len(categories))],
			"status":       "completed",
		})
		if err != nil {
			panic(err)
		}
		txnIDs = append(txnIDs, row.ID)
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f inserts/sec\n", float64(numTxns)/elapsed.Seconds())
	printMemUsage()

	// Benchmark 3: Insert ledger entries (2 per transaction)
	numLedger := numTxns * 2
	fmt.Printf("\n--- Insert %d ledger entries ---\n", numLedger)
	start = time.Now()

	for i := 0; i < numTxns; i++ {
		amount := float64(rand.Intn(100000)) / 100.0
		from := accountIDs[rand.Intn(len(accountIDs))]
		to := accountIDs[rand.Intn(len(accountIDs))]

		db.Insert("ledger", map[string]any{
			"account_id":      from,
			"transaction_id":  txnIDs[i],
			"debit":           amount,
			"credit":          0.0,
			"running_balance": float64(rand.Intn(1000000)) / 100.0,
		})
		db.Insert("ledger", map[string]any{
			"account_id":      to,
			"transaction_id":  txnIDs[i],
			"debit":           0.0,
			"credit":          amount,
			"running_balance": float64(rand.Intn(1000000)) / 100.0,
		})
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f inserts/sec\n", float64(numLedger)/elapsed.Seconds())
	printMemUsage()

	// Benchmark 4: Point reads
	numReads := 100000
	fmt.Printf("\n--- %d point reads (random accounts) ---\n", numReads)
	start = time.Now()

	for i := 0; i < numReads; i++ {
		id := accountIDs[rand.Intn(len(accountIDs))]
		_, err := db.Table("accounts").Get(id)
		if err != nil {
			panic(err)
		}
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f reads/sec\n", float64(numReads)/elapsed.Seconds())

	// Benchmark 5: View (filtered scan)
	fmt.Printf("\n--- View: USD transactions > $500 ---\n")
	start = time.Now()

	view := &flop.ViewDef{
		Name:  "large_usd_txns",
		Table: "transactions",
		Filters: []flop.Filter{
			{Field: "currency", Op: flop.OpEq, Value: "USD"},
			{Field: "amount", Op: flop.OpGt, Value: 500.0},
		},
		OrderBy: "amount",
		Order:   flop.Desc,
		Limit:   100,
	}

	result, err := db.ExecuteView(view)
	if err != nil {
		panic(err)
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Results: %d (total matching: %d)\n", len(result.Rows), result.Total)

	// Benchmark 6: Full-text search
	fmt.Printf("\n--- Full-text search: 'payment services' ---\n")
	start = time.Now()

	searchResults, err := db.Search("transactions", "payment services", 20)
	if err != nil {
		panic(err)
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Results: %d\n", len(searchResults))

	// Benchmark 7: Updates
	numUpdates := 10000
	fmt.Printf("\n--- %d random account balance updates ---\n", numUpdates)
	start = time.Now()

	for i := 0; i < numUpdates; i++ {
		id := accountIDs[rand.Intn(len(accountIDs))]
		_, err := db.Update("accounts", id, map[string]any{
			"balance": float64(rand.Intn(1000000)) / 100.0,
		})
		if err != nil {
			panic(err)
		}
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f updates/sec\n", float64(numUpdates)/elapsed.Seconds())

	// Benchmark 8: Deletes (soft-delete + archive)
	numDeletes := 1000
	fmt.Printf("\n--- %d soft deletes ---\n", numDeletes)
	start = time.Now()

	for i := 0; i < numDeletes; i++ {
		idx := rand.Intn(len(txnIDs))
		err := db.Delete("transactions", txnIDs[idx])
		if err != nil {
			// May already be deleted
			continue
		}
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f deletes/sec\n", float64(numDeletes)/elapsed.Seconds())

	// Benchmark 9: Flush to disk
	fmt.Printf("\n--- Flush all to disk ---\n")
	start = time.Now()

	err = db.Flush()
	if err != nil {
		panic(err)
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)

	// Benchmark 10: Backup
	backupDir, _ := os.MkdirTemp("", "flop-backup-*")
	defer os.RemoveAll(backupDir)

	fmt.Printf("\n--- Backup database ---\n")
	start = time.Now()

	err = db.Backup(backupDir)
	if err != nil {
		panic(err)
	}

	elapsed = time.Since(start)
	fmt.Printf("  Time: %v\n", elapsed)

	// Print database stats
	fmt.Println("\n=== Database Stats ===")
	for _, table := range []string{"accounts", "transactions", "ledger"} {
		count, _ := db.Table(table).Count()
		fmt.Printf("  %s: %d rows\n", table, count)
	}
	printMemUsage()

	// Print disk usage
	var totalSize int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			totalSize += info.Size()
		}
	}
	fmt.Printf("  Disk usage: %.2f MB\n", float64(totalSize)/(1024*1024))
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("  Memory: Alloc=%.2f MB, Sys=%.2f MB, HeapObjects=%d\n",
		float64(m.Alloc)/(1024*1024),
		float64(m.Sys)/(1024*1024),
		m.HeapObjects)
}
