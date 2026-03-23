package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
)

type metric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type gitMeta struct {
	Commit string `json:"commit,omitempty"`
	Branch string `json:"branch,omitempty"`
	Dirty  bool   `json:"dirty,omitempty"`
}

type report struct {
	CreatedAt string   `json:"createdAt"`
	GoVersion string   `json:"goVersion"`
	Git       gitMeta  `json:"git"`
	DataDir   string   `json:"dataDir"`
	Rows      int      `json:"rows"`
	Lookups   int      `json:"lookups"`
	Searches  int      `json:"searches"`
	SyncMode  string   `json:"syncMode"`
	Metrics   []metric `json:"metrics"`
}

func main() {
	var (
		rows        = flag.Int("rows", 50000, "number of rows to seed")
		lookups     = flag.Int("lookups", 20000, "number of unique-index lookups per benchmark run")
		searches    = flag.Int("searches", 4000, "number of full-text queries per benchmark run")
		batch       = flag.Int("batch", 2000, "bulk insert WAL flush size")
		searchLimit = flag.Int("search-limit", 8, "search result limit")
		syncMode    = flag.String("sync-mode", "normal", "sync mode: normal or full")
		seed        = flag.Int64("seed", 42, "rng seed for repeatable query mixes")
		dataDir     = flag.String("data-dir", "", "existing/target data dir (defaults to temporary)")
		keepData    = flag.Bool("keep-data", false, "keep generated data dir when using a temporary dir")
		jsonOut     = flag.Bool("json", false, "print JSON report in addition to RESULT lines")
		outPath     = flag.String("out", "", "optional file path to save JSON report")
		warmTimeout = flag.Duration("warm-timeout", 45*time.Second, "timeout waiting for async secondary indexes")
	)
	flag.Parse()

	if *rows <= 0 || *lookups <= 0 || *searches <= 0 || *batch <= 0 || *searchLimit <= 0 {
		fmt.Fprintln(os.Stderr, "rows/lookups/searches/batch/search-limit must be > 0")
		os.Exit(2)
	}

	benchDir := *dataDir
	tempDir := false
	if benchDir == "" {
		base, err := os.MkdirTemp("", "flop-quickbench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
			os.Exit(1)
		}
		benchDir = filepath.Join(base, "data")
		tempDir = true
	}
	if err := os.MkdirAll(benchDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir data dir: %v\n", err)
		os.Exit(1)
	}
	if tempDir && !*keepData {
		defer os.RemoveAll(filepath.Dir(benchDir))
	}

	defs := benchmarkTableDefs()
	metrics := make([]metric, 0, 32)
	addMetric := func(name string, value float64, unit string) {
		metrics = append(metrics, metric{Name: name, Value: value, Unit: unit})
	}
	addDurationMs := func(name string, d time.Duration) {
		addMetric(name, float64(d.Milliseconds()), "ms")
	}
	addDurationUs := func(name string, d time.Duration) {
		addMetric(name, float64(d.Microseconds()), "us")
	}

	lookupKeys := buildLookupKeys(*lookups, *rows, *seed)
	searchQueries := buildSearchQueries(*searches, *seed)

	seedOpenStart := time.Now()
	seedDB, seedTable, err := openDatabase(benchDir, *syncMode, false, defs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open seed db: %v\n", err)
		os.Exit(1)
	}
	addDurationMs("seed_open_ms", time.Since(seedOpenStart))

	insertStart := time.Now()
	inserted, err := seedRows(seedTable, *rows, *batch)
	if err != nil {
		_ = seedDB.Close()
		fmt.Fprintf(os.Stderr, "seed rows: %v\n", err)
		os.Exit(1)
	}
	insertDur := time.Since(insertStart)
	addDurationMs("seed_insert_ms", insertDur)
	addMetric("seed_insert_rows_per_sec", float64(inserted)/insertDur.Seconds(), "rows/s")
	if err := seedDB.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close seed db: %v\n", err)
		os.Exit(1)
	}

	syncOpenStart := time.Now()
	syncDB, syncTable, err := openDatabase(benchDir, *syncMode, false, defs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open sync db: %v\n", err)
		os.Exit(1)
	}
	syncOpenDur := time.Since(syncOpenStart)
	addDurationMs("open_sync_ms", syncOpenDur)

	syncLookupDur, syncLookupHits, err := runLookupBench(syncTable, lookupKeys)
	if err != nil {
		_ = syncDB.Close()
		fmt.Fprintf(os.Stderr, "sync lookup bench: %v\n", err)
		os.Exit(1)
	}
	if syncLookupHits == 0 {
		_ = syncDB.Close()
		fmt.Fprintln(os.Stderr, "sync lookup bench: zero hits")
		os.Exit(1)
	}
	addDurationMs("lookup_sync_total_ms", syncLookupDur)
	addMetric("lookup_sync_avg_us", float64(syncLookupDur.Microseconds())/float64(len(lookupKeys)), "us/op")

	syncSearchDur, syncSearchHits, err := runSearchBench(syncTable, searchQueries, *searchLimit)
	if err != nil {
		_ = syncDB.Close()
		fmt.Fprintf(os.Stderr, "sync search bench: %v\n", err)
		os.Exit(1)
	}
	addDurationMs("search_sync_total_ms", syncSearchDur)
	addMetric("search_sync_avg_us", float64(syncSearchDur.Microseconds())/float64(len(searchQueries)), "us/op")
	addMetric("search_sync_total_hits", float64(syncSearchHits), "rows")
	if err := syncDB.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close sync db: %v\n", err)
		os.Exit(1)
	}

	asyncOpenStart := time.Now()
	asyncDB, asyncTable, err := openDatabase(benchDir, *syncMode, true, defs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open async db: %v\n", err)
		os.Exit(1)
	}
	asyncOpenDur := time.Since(asyncOpenStart)
	addDurationMs("open_async_ms", asyncOpenDur)

	firstLookupStart := time.Now()
	_, ok := asyncTable.FindByIndex([]string{"slug"}, lookupKeys[0])
	firstLookupDur := time.Since(firstLookupStart)
	if !ok {
		_ = asyncDB.Close()
		fmt.Fprintln(os.Stderr, "async first lookup did not find a row")
		os.Exit(1)
	}
	addDurationUs("lookup_async_first_us", firstLookupDur)

	firstSearchStart := time.Now()
	firstSearchRows, err := asyncTable.SearchFullText([]string{"title"}, searchQueries[0], *searchLimit)
	firstSearchDur := time.Since(firstSearchStart)
	if err != nil {
		_ = asyncDB.Close()
		fmt.Fprintf(os.Stderr, "async first search: %v\n", err)
		os.Exit(1)
	}
	addDurationUs("search_async_first_us", firstSearchDur)
	addMetric("search_async_first_hits", float64(len(firstSearchRows)), "rows")

	warmDur, ready := waitUntilReady(asyncTable, *warmTimeout)
	if ready {
		addDurationMs("async_warmup_ms", warmDur)
	} else {
		addDurationMs("async_warmup_timeout_ms", warmDur)
	}

	asyncLookupDur, asyncLookupHits, err := runLookupBench(asyncTable, lookupKeys)
	if err != nil {
		_ = asyncDB.Close()
		fmt.Fprintf(os.Stderr, "async lookup bench: %v\n", err)
		os.Exit(1)
	}
	if asyncLookupHits == 0 {
		_ = asyncDB.Close()
		fmt.Fprintln(os.Stderr, "async lookup bench: zero hits")
		os.Exit(1)
	}
	addDurationMs("lookup_async_total_ms", asyncLookupDur)
	addMetric("lookup_async_avg_us", float64(asyncLookupDur.Microseconds())/float64(len(lookupKeys)), "us/op")

	asyncSearchDur, asyncSearchHits, err := runSearchBench(asyncTable, searchQueries, *searchLimit)
	if err != nil {
		_ = asyncDB.Close()
		fmt.Fprintf(os.Stderr, "async search bench: %v\n", err)
		os.Exit(1)
	}
	addDurationMs("search_async_total_ms", asyncSearchDur)
	addMetric("search_async_avg_us", float64(asyncSearchDur.Microseconds())/float64(len(searchQueries)), "us/op")
	addMetric("search_async_total_hits", float64(asyncSearchHits), "rows")
	if err := asyncDB.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close async db: %v\n", err)
		os.Exit(1)
	}

	if asyncOpenDur > 0 {
		addMetric("open_sync_over_async_ratio", float64(syncOpenDur)/float64(asyncOpenDur), "x")
	}
	if asyncLookupDur > 0 {
		addMetric("lookup_sync_over_async_ratio", float64(syncLookupDur)/float64(asyncLookupDur), "x")
	}
	if asyncSearchDur > 0 {
		addMetric("search_sync_over_async_ratio", float64(syncSearchDur)/float64(asyncSearchDur), "x")
	}

	fmt.Printf("INFO data_dir=%s rows=%d lookups=%d searches=%d sync_mode=%s\n", benchDir, *rows, *lookups, *searches, *syncMode)
	for _, m := range metrics {
		fmt.Printf("RESULT %s=%.3f %s\n", m.Name, m.Value, m.Unit)
	}

	reportData := report{
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		Git:       detectGitMeta(),
		DataDir:   benchDir,
		Rows:      *rows,
		Lookups:   *lookups,
		Searches:  *searches,
		SyncMode:  *syncMode,
		Metrics:   metrics,
	}

	if *outPath != "" {
		if err := writeReportFile(*outPath, reportData); err != nil {
			fmt.Fprintf(os.Stderr, "write report file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("INFO report_file=%s\n", *outPath)
	}

	if *jsonOut {
		out, err := json.MarshalIndent(reportData, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "json marshal: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func benchmarkTableDefs() map[string]*schema.TableDef {
	fields := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true},
		{Name: "slug", Kind: schema.KindString, Required: true},
		{Name: "title", Kind: schema.KindString, Required: true},
		{Name: "genre", Kind: schema.KindString},
		{Name: "overview", Kind: schema.KindString},
	}
	indexes := []schema.IndexDef{
		{Fields: []string{"slug"}, Unique: true, Type: schema.IndexTypeHash},
		{Fields: []string{"genre"}, Unique: false, Type: schema.IndexTypeHash},
		{Fields: []string{"title"}, Unique: false, Type: schema.IndexTypeFullText},
	}
	return map[string]*schema.TableDef{
		"movies": {
			Name:           "movies",
			CompiledSchema: schema.NewCompiledSchema(fields),
			Indexes:        indexes,
		},
	}
}

func openDatabase(dataDir, syncMode string, async bool, defs map[string]*schema.TableDef) (*engine.Database, *engine.TableInstance, error) {
	db := engine.NewDatabase(engine.DatabaseConfig{
		DataDir:               dataDir,
		SyncMode:              syncMode,
		AsyncSecondaryIndexes: async,
	})
	if err := db.Open(defs); err != nil {
		return nil, nil, err
	}
	ti := db.GetTable("movies")
	if ti == nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("movies table missing")
	}
	return db, ti, nil
}

func seedRows(ti *engine.TableInstance, rows, batch int) (int, error) {
	chunkSize := batch * 4
	if chunkSize < 1000 {
		chunkSize = 1000
	}
	chunk := make([]map[string]interface{}, 0, chunkSize)
	inserted := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		n, err := ti.BulkInsert(chunk, batch)
		inserted += n
		chunk = chunk[:0]
		return err
	}
	for i := 0; i < rows; i++ {
		chunk = append(chunk, benchmarkRow(i))
		if len(chunk) == cap(chunk) {
			if err := flush(); err != nil {
				return inserted, err
			}
		}
	}
	if err := flush(); err != nil {
		return inserted, err
	}
	return inserted, nil
}

func benchmarkRow(i int) map[string]interface{} {
	genre := []string{"action", "drama", "thriller", "comedy", "fantasy"}[i%5]
	return map[string]interface{}{
		"id":       fmt.Sprintf("id-%06d", i),
		"slug":     fmt.Sprintf("slug-%06d", i),
		"title":    fmt.Sprintf("Movie %06d Galactic Saga Chapter %03d", i, i%1000),
		"genre":    genre,
		"overview": fmt.Sprintf("Overview for movie %06d with galactic saga chapter %03d", i, i%1000),
	}
}

func buildLookupKeys(count, rows int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = fmt.Sprintf("slug-%06d", rng.Intn(rows))
	}
	return out
}

func buildSearchQueries(count int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed + 991))
	out := make([]string, count)
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			out[i] = "galactic saga"
		} else {
			out[i] = fmt.Sprintf("chapter %03d", rng.Intn(1000))
		}
	}
	return out
}

func runLookupBench(ti *engine.TableInstance, slugs []string) (time.Duration, int, error) {
	start := time.Now()
	hits := 0
	for _, slug := range slugs {
		ptr, ok := ti.FindByIndex([]string{"slug"}, slug)
		if !ok {
			continue
		}
		row, err := ti.GetByPointer(ptr)
		if err != nil {
			return 0, hits, err
		}
		if row != nil {
			hits++
		}
	}
	return time.Since(start), hits, nil
}

func runSearchBench(ti *engine.TableInstance, queries []string, limit int) (time.Duration, int, error) {
	start := time.Now()
	hits := 0
	for _, q := range queries {
		rows, err := ti.SearchFullText([]string{"title"}, q, limit)
		if err != nil {
			return 0, hits, err
		}
		hits += len(rows)
	}
	return time.Since(start), hits, nil
}

func waitUntilReady(ti *engine.TableInstance, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		if ti.SecondaryIndexesReady() {
			return time.Since(start), true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Since(start), false
}

func writeReportFile(path string, rep report) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("empty output path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func detectGitMeta() gitMeta {
	meta := gitMeta{}
	if v, ok := runGit("rev-parse", "--short", "HEAD"); ok {
		meta.Commit = v
	}
	if v, ok := runGit("rev-parse", "--abbrev-ref", "HEAD"); ok {
		meta.Branch = v
	}
	if v, ok := runGit("status", "--porcelain"); ok && strings.TrimSpace(v) != "" {
		meta.Dirty = true
	}
	return meta
}

func runGit(args ...string) (string, bool) {
	cmd := exec.Command("git", args...)
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}
