package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcisbee/flop/internal/engine"
	"github.com/marcisbee/flop/internal/schema"
)

type metric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type crashResult struct {
	Scenario       string `json:"scenario"`
	Run            int    `json:"run"`
	WorkerExitCode int    `json:"workerExitCode"`
	RecoveryMS     int64  `json:"recoveryMs"`
	Consistent     bool   `json:"consistent"`
	Rows           int    `json:"rows"`
	Error          string `json:"error,omitempty"`
}

type report struct {
	CreatedAt    string        `json:"createdAt"`
	Mode         string        `json:"mode"`
	SyncMode     string        `json:"syncMode"`
	DataDir      string        `json:"dataDir"`
	Rows         int           `json:"rows"`
	Ops          int           `json:"ops"`
	Workers      int           `json:"workers"`
	Seed         int64         `json:"seed"`
	Metrics      []metric      `json:"metrics,omitempty"`
	CrashResults []crashResult `json:"crashResults,omitempty"`
}

type latencySeries struct {
	mu   sync.Mutex
	vals []float64
}

func (s *latencySeries) add(d time.Duration) {
	s.mu.Lock()
	s.vals = append(s.vals, float64(d.Microseconds()))
	s.mu.Unlock()
}

func (s *latencySeries) summary(prefix string, out *[]metric) {
	s.mu.Lock()
	vals := append([]float64(nil), s.vals...)
	s.mu.Unlock()
	if len(vals) == 0 {
		*out = append(*out,
			metric{Name: prefix + "_count", Value: 0, Unit: "ops"},
		)
		return
	}
	sort.Float64s(vals)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	*out = append(*out,
		metric{Name: prefix + "_count", Value: float64(len(vals)), Unit: "ops"},
		metric{Name: prefix + "_avg_us", Value: sum / float64(len(vals)), Unit: "us"},
		metric{Name: prefix + "_p50_us", Value: percentile(vals, 0.50), Unit: "us"},
		metric{Name: prefix + "_p95_us", Value: percentile(vals, 0.95), Unit: "us"},
		metric{Name: prefix + "_p99_us", Value: percentile(vals, 0.99), Unit: "us"},
	)
}

type keyState struct {
	mu     sync.RWMutex
	keys   []string
	keySet map[string]struct{}
	nextID atomic.Int64
}

func newKeyState(initial []string) *keyState {
	set := make(map[string]struct{}, len(initial))
	for _, k := range initial {
		set[k] = struct{}{}
	}
	s := &keyState{keys: append([]string(nil), initial...), keySet: set}
	s.nextID.Store(int64(len(initial)))
	return s
}

func (s *keyState) randomKey(r *rand.Rand) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.keys) == 0 {
		return "", false
	}
	return s.keys[r.Intn(len(s.keys))], true
}

func (s *keyState) add(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keySet[k]; exists {
		return
	}
	s.keySet[k] = struct{}{}
	s.keys = append(s.keys, k)
}

func (s *keyState) remove(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keySet[k]; !exists {
		return
	}
	delete(s.keySet, k)
	for i := range s.keys {
		if s.keys[i] == k {
			s.keys[i] = s.keys[len(s.keys)-1]
			s.keys = s.keys[:len(s.keys)-1]
			break
		}
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func main() {
	var (
		mode           = flag.String("mode", "all", "baseline | crash | crash-worker | all")
		syncMode       = flag.String("sync-mode", "full", "sync mode: normal or full")
		rows           = flag.Int("rows", 10000, "rows to seed before workload")
		ops            = flag.Int("ops", 30000, "operations in mixed workload")
		workers        = flag.Int("workers", 4, "concurrent workers for mixed workload")
		seed           = flag.Int64("seed", 42, "rng seed")
		dataDir        = flag.String("data-dir", "", "data directory (default temporary)")
		keepData       = flag.Bool("keep-data", false, "keep temporary data directory")
		jsonOut        = flag.Bool("json", true, "print json report")
		outPath        = flag.String("out", "", "optional file path for JSON report")
		crashRuns      = flag.Int("crash-runs", 1, "number of runs per crash scenario")
		crashScenarios = flag.String("crash-scenarios", strings.Join(defaultCrashScenarios, ","), "comma-separated failpoint names")
	)
	flag.Parse()

	if *mode == "crash-worker" {
		if err := runCrashWorker(*syncMode, *dataDir, *rows, *ops, *seed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *rows <= 0 || *ops <= 0 || *workers <= 0 {
		fmt.Fprintln(os.Stderr, "rows, ops, workers must be > 0")
		os.Exit(2)
	}

	dir, tempDir, err := resolveDataDir(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve data dir: %v\n", err)
		os.Exit(1)
	}
	if tempDir && !*keepData {
		defer os.RemoveAll(filepath.Dir(dir))
	}

	rep := report{
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Mode:      *mode,
		SyncMode:  *syncMode,
		DataDir:   dir,
		Rows:      *rows,
		Ops:       *ops,
		Workers:   *workers,
		Seed:      *seed,
	}

	switch *mode {
	case "baseline":
		rep.Metrics, err = runBaseline(dir, *syncMode, *rows, *ops, *workers, *seed)
	case "crash":
		rep.CrashResults, err = runCrashMatrix(dir, *syncMode, *rows, *ops, *seed, *crashRuns, splitCSV(*crashScenarios))
	case "all":
		rep.Metrics, err = runBaseline(dir, *syncMode, *rows, *ops, *workers, *seed)
		if err == nil {
			rep.CrashResults, err = runCrashMatrix(filepath.Join(dir, "crash"), *syncMode, max(200, *rows/10), max(600, *ops/10), *seed+17, *crashRuns, splitCSV(*crashScenarios))
		}
	default:
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if *outPath != "" {
		if err := writeReport(*outPath, rep); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("INFO report_file=%s\n", *outPath)
	}

	for _, m := range rep.Metrics {
		fmt.Printf("RESULT %s=%.3f %s\n", m.Name, m.Value, m.Unit)
	}
	for _, cr := range rep.CrashResults {
		status := "ok"
		if !cr.Consistent {
			status = "inconsistent"
		}
		fmt.Printf("CRASH scenario=%s run=%d exit=%d recovery_ms=%d rows=%d status=%s\n", cr.Scenario, cr.Run, cr.WorkerExitCode, cr.RecoveryMS, cr.Rows, status)
	}

	if *jsonOut {
		payload, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal report: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(payload))
	}
}

var defaultCrashScenarios = []string{
	"insert_after_wal_record",
	"insert_after_page_write",
	"insert_after_index_update",
	"insert_before_commit",
	"update_after_wal_record",
	"update_after_page_write",
	"update_after_index_update",
	"update_before_commit",
	"delete_after_wal_record",
	"delete_after_page_write",
	"delete_after_index_update",
	"delete_before_commit",
	"checkpoint_after_table_flush",
	"checkpoint_after_index_write",
	"checkpoint_after_wal_fsync",
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func resolveDataDir(dataDir string) (string, bool, error) {
	if strings.TrimSpace(dataDir) != "" {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return "", false, err
		}
		return dataDir, false, nil
	}
	base, err := os.MkdirTemp("", "flop-pillarbench-*")
	if err != nil {
		return "", false, err
	}
	dir := filepath.Join(base, "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, err
	}
	return dir, true, nil
}

func runBaseline(dataDir, syncMode string, rows, ops, workers int, seed int64) ([]metric, error) {
	if err := os.RemoveAll(dataDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}

	db, table, err := openEngineDB(dataDir, syncMode)
	if err != nil {
		return nil, err
	}

	metrics := make([]metric, 0, 48)
	add := func(name string, value float64, unit string) {
		metrics = append(metrics, metric{Name: name, Value: value, Unit: unit})
	}

	seedStart := time.Now()
	if err := seedTable(table, rows); err != nil {
		_ = db.Close()
		return nil, err
	}
	seedDur := time.Since(seedStart)
	add("seed_ms", float64(seedDur.Milliseconds()), "ms")
	add("seed_rows_per_sec", float64(rows)/seedDur.Seconds(), "rows/s")

	state := newKeyState(makeSeedKeys(rows))
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	workStart := time.Now()
	workStats, err := runMixedWorkload(table, state, ops, workers, seed)
	workDur := time.Since(workStart)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)
	allocBytes := float64(m2.TotalAlloc - m1.TotalAlloc)
	add("workload_ms", float64(workDur.Milliseconds()), "ms")
	add("workload_tps", float64(ops)/workDur.Seconds(), "ops/s")
	add("workload_alloc_bytes", allocBytes, "bytes")
	add("workload_alloc_per_op", allocBytes/float64(max(ops, 1)), "bytes/op")

	add("ops_get", float64(workStats.getOps.Load()), "ops")
	add("ops_update", float64(workStats.updateOps.Load()), "ops")
	add("ops_insert", float64(workStats.insertOps.Load()), "ops")
	add("ops_delete", float64(workStats.deleteOps.Load()), "ops")
	add("ops_scan", float64(workStats.scanOps.Load()), "ops")

	workStats.getLatency.summary("get", &metrics)
	workStats.updateLatency.summary("update", &metrics)
	workStats.insertLatency.summary("insert", &metrics)
	workStats.deleteLatency.summary("delete", &metrics)
	workStats.scanLatency.summary("scan", &metrics)

	checkpointStart := time.Now()
	if err := table.Checkpoint(); err != nil {
		_ = db.Close()
		return nil, err
	}
	checkpointDur := time.Since(checkpointStart)
	add("checkpoint_ms", float64(checkpointDur.Milliseconds()), "ms")

	closeStart := time.Now()
	if err := db.Close(); err != nil {
		return nil, err
	}
	add("close_ms", float64(time.Since(closeStart).Milliseconds()), "ms")

	recoveryStart := time.Now()
	db2, table2, err := openEngineDB(dataDir, syncMode)
	if err != nil {
		return nil, err
	}
	recoveryDur := time.Since(recoveryStart)
	add("recovery_open_ms", float64(recoveryDur.Milliseconds()), "ms")

	consistent, rowCount, cErr := verifyConsistency(table2)
	add("recovery_consistent", boolToFloat(consistent), "bool")
	add("recovery_rows", float64(rowCount), "rows")
	if cErr != nil {
		_ = db2.Close()
		return nil, cErr
	}
	if err := db2.Close(); err != nil {
		return nil, err
	}

	return metrics, nil
}

type workloadStats struct {
	getOps    atomic.Int64
	updateOps atomic.Int64
	insertOps atomic.Int64
	deleteOps atomic.Int64
	scanOps   atomic.Int64

	getLatency    latencySeries
	updateLatency latencySeries
	insertLatency latencySeries
	deleteLatency latencySeries
	scanLatency   latencySeries
}

func runMixedWorkload(table *engine.TableInstance, state *keyState, ops, workers int, seed int64) (*workloadStats, error) {
	stats := &workloadStats{}
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)

	basePerWorker := ops / workers
	extra := ops % workers

	for w := 0; w < workers; w++ {
		workerOps := basePerWorker
		if w < extra {
			workerOps++
		}
		workerSeed := seed + int64(w*104729)
		go func(workerIdx, count int, localSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(localSeed))
			for i := 0; i < count; i++ {
				p := rng.Intn(100)
				switch {
				case p < 45:
					key, ok := state.randomKey(rng)
					if !ok {
						continue
					}
					start := time.Now()
					_, err := table.Get(key)
					stats.getLatency.add(time.Since(start))
					stats.getOps.Add(1)
					if err != nil {
						errCh <- err
						return
					}
				case p < 70:
					key, ok := state.randomKey(rng)
					if !ok {
						continue
					}
					start := time.Now()
					_, err := table.Update(key, map[string]interface{}{
						"value": float64(rng.Intn(1_000_000)),
						"title": fmt.Sprintf("Updated %s %d", key, rng.Intn(1000)),
					}, nil)
					stats.updateLatency.add(time.Since(start))
					stats.updateOps.Add(1)
					if err != nil {
						errCh <- err
						return
					}
				case p < 85:
					idNum := state.nextID.Add(1) - 1
					id := fmt.Sprintf("id-%08d", idNum)
					start := time.Now()
					_, err := table.Insert(map[string]interface{}{
						"id":    id,
						"slug":  id,
						"title": fmt.Sprintf("Inserted %d", idNum),
						"value": float64(rng.Intn(1_000_000)),
					}, nil)
					stats.insertLatency.add(time.Since(start))
					stats.insertOps.Add(1)
					if err != nil {
						errCh <- err
						return
					}
					state.add(id)
				case p < 95:
					key, ok := state.randomKey(rng)
					if !ok {
						continue
					}
					start := time.Now()
					deleted, err := table.Delete(key, nil)
					stats.deleteLatency.add(time.Since(start))
					stats.deleteOps.Add(1)
					if err != nil {
						errCh <- err
						return
					}
					if deleted {
						state.remove(key)
					}
				default:
					offset := rng.Intn(max(1, int(state.nextID.Load())))
					start := time.Now()
					_, err := table.Scan(50, offset)
					stats.scanLatency.add(time.Since(start))
					stats.scanOps.Add(1)
					if err != nil {
						errCh <- err
						return
					}
				}
				if i > 0 && i%2000 == 0 && workerIdx == 0 {
					if err := table.Checkpoint(); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(w, workerOps, workerSeed)
	}

	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		return nil, <-errCh
	}
	return stats, nil
}

func runCrashMatrix(baseDir, syncMode string, rows, ops int, seed int64, runs int, scenarios []string) ([]crashResult, error) {
	if runs <= 0 {
		runs = 1
	}
	if len(scenarios) == 0 {
		scenarios = defaultCrashScenarios
	}
	results := make([]crashResult, 0, len(scenarios)*runs)

	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	for _, scenario := range scenarios {
		for i := 0; i < runs; i++ {
			runDir := filepath.Join(baseDir, strings.ReplaceAll(scenario, ",", "_"), fmt.Sprintf("run-%02d", i+1))
			if err := os.RemoveAll(runDir); err != nil {
				return nil, err
			}
			if err := os.MkdirAll(runDir, 0o755); err != nil {
				return nil, err
			}
			if err := prepareCrashSeed(runDir, syncMode, rows); err != nil {
				return nil, err
			}

			args := []string{
				"-mode=crash-worker",
				"-sync-mode=" + syncMode,
				"-data-dir=" + runDir,
				"-rows=0",
				fmt.Sprintf("-ops=%d", ops),
				fmt.Sprintf("-seed=%d", seed+int64(i)),
			}
			cmd := exec.Command(exe, args...)
			cmd.Env = append(os.Environ(),
				"FLOP_FAILPOINT="+scenario,
				"FLOP_FAILPOINT_HIT=1",
				"FLOP_FAILPOINT_MODE=exit",
			)
			err := cmd.Run()
			exitCode := 0
			if err != nil {
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					exitCode = exitErr.ExitCode()
				} else {
					return nil, err
				}
			}

			recoverStart := time.Now()
			db, table, err := openEngineDB(runDir, syncMode)
			if err != nil {
				results = append(results, crashResult{
					Scenario:       scenario,
					Run:            i + 1,
					WorkerExitCode: exitCode,
					RecoveryMS:     time.Since(recoverStart).Milliseconds(),
					Consistent:     false,
					Error:          err.Error(),
				})
				continue
			}
			consistent, rowCount, verr := verifyConsistency(table)
			closeErr := db.Close()
			recoveryMS := time.Since(recoverStart).Milliseconds()
			if verr != nil {
				results = append(results, crashResult{
					Scenario:       scenario,
					Run:            i + 1,
					WorkerExitCode: exitCode,
					RecoveryMS:     recoveryMS,
					Consistent:     false,
					Rows:           rowCount,
					Error:          verr.Error(),
				})
				continue
			}
			if closeErr != nil {
				results = append(results, crashResult{
					Scenario:       scenario,
					Run:            i + 1,
					WorkerExitCode: exitCode,
					RecoveryMS:     recoveryMS,
					Consistent:     false,
					Rows:           rowCount,
					Error:          closeErr.Error(),
				})
				continue
			}
			results = append(results, crashResult{
				Scenario:       scenario,
				Run:            i + 1,
				WorkerExitCode: exitCode,
				RecoveryMS:     recoveryMS,
				Consistent:     consistent,
				Rows:           rowCount,
			})
		}
	}
	return results, nil
}

func runCrashWorker(syncMode, dataDir string, rows, ops int, seed int64) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("-data-dir is required in crash-worker mode")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	db, table, err := openEngineDB(dataDir, syncMode)
	if err != nil {
		return err
	}
	if rows > 0 {
		if err := seedTable(table, rows); err != nil {
			_ = db.Close()
			return err
		}
	}

	rng := rand.New(rand.NewSource(seed))
	keys := makeSeedKeys(table.Count())
	if len(keys) == 0 {
		if scan, err := table.Scan(2_000_000, 0); err == nil {
			keys = keys[:0]
			for _, row := range scan {
				keys = append(keys, fmt.Sprintf("%v", row["id"]))
			}
		}
	}
	if len(keys) == 0 {
		_ = db.Close()
		return fmt.Errorf("no keys available for crash workload")
	}
	nextInsertID := len(keys)

	for i := 0; i < ops; i++ {
		switch i % 5 {
		case 0:
			key := keys[rng.Intn(len(keys))]
			if _, err := table.Get(key); err != nil {
				_ = db.Close()
				return err
			}
		case 1:
			key := keys[rng.Intn(len(keys))]
			if _, err := table.Update(key, map[string]interface{}{"value": float64(i), "title": fmt.Sprintf("up-%d", i)}, nil); err != nil {
				_ = db.Close()
				return err
			}
		case 2:
			id := fmt.Sprintf("id-%08d", nextInsertID)
			nextInsertID++
			if _, err := table.Insert(map[string]interface{}{
				"id":    id,
				"slug":  id,
				"title": fmt.Sprintf("crash-%d", i),
				"value": float64(i),
			}, nil); err != nil {
				_ = db.Close()
				return err
			}
			keys = append(keys, id)
		case 3:
			if len(keys) > 0 {
				idx := rng.Intn(len(keys))
				key := keys[idx]
				deleted, err := table.Delete(key, nil)
				if err != nil {
					_ = db.Close()
					return err
				}
				if deleted {
					keys[idx] = keys[len(keys)-1]
					keys = keys[:len(keys)-1]
				}
			}
		default:
			if _, err := table.Scan(25, rng.Intn(max(1, len(keys)))); err != nil {
				_ = db.Close()
				return err
			}
		}
		if i > 0 && i%50 == 0 {
			if err := table.Checkpoint(); err != nil {
				_ = db.Close()
				return err
			}
		}
	}
	return db.Close()
}

func prepareCrashSeed(dataDir, syncMode string, rows int) error {
	db, table, err := openEngineDB(dataDir, syncMode)
	if err != nil {
		return err
	}
	if rows > 0 {
		if err := seedTable(table, rows); err != nil {
			_ = db.Close()
			return err
		}
	}
	return db.Close()
}

func verifyConsistency(table *engine.TableInstance) (bool, int, error) {
	rows, err := table.Scan(2_000_000, 0)
	if err != nil {
		return false, 0, err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		id := fmt.Sprintf("%v", row["id"])
		if id == "" {
			return false, len(rows), fmt.Errorf("row without id")
		}
		if _, dup := seen[id]; dup {
			return false, len(rows), fmt.Errorf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
		got, err := table.Get(id)
		if err != nil {
			return false, len(rows), err
		}
		if got == nil {
			return false, len(rows), fmt.Errorf("index points missing for id %q", id)
		}
	}
	if count := table.Count(); count != len(rows) {
		return false, len(rows), fmt.Errorf("count mismatch index=%d scan=%d", count, len(rows))
	}
	return true, len(rows), nil
}

func seedTable(table *engine.TableInstance, rows int) error {
	chunk := make([]map[string]interface{}, 0, 1000)
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		_, err := table.BulkInsert(chunk, 1000)
		chunk = chunk[:0]
		return err
	}
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("id-%08d", i)
		chunk = append(chunk, map[string]interface{}{
			"id":    id,
			"slug":  id,
			"title": fmt.Sprintf("Title %08d", i),
			"value": float64(i),
		})
		if len(chunk) == cap(chunk) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func makeSeedKeys(rows int) []string {
	out := make([]string, rows)
	for i := 0; i < rows; i++ {
		out[i] = fmt.Sprintf("id-%08d", i)
	}
	return out
}

func openEngineDB(dataDir, syncMode string) (*engine.Database, *engine.TableInstance, error) {
	fields := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true},
		{Name: "slug", Kind: schema.KindString, Required: true},
		{Name: "title", Kind: schema.KindString, Required: true},
		{Name: "value", Kind: schema.KindNumber},
	}
	defs := map[string]*schema.TableDef{
		"items": {
			Name:           "items",
			CompiledSchema: schema.NewCompiledSchema(fields),
			Indexes: []schema.IndexDef{
				{Fields: []string{"slug"}, Unique: true, Type: schema.IndexTypeHash},
				{Fields: []string{"title"}, Unique: false, Type: schema.IndexTypeFullText},
			},
		},
	}
	db := engine.NewDatabase(engine.DatabaseConfig{
		DataDir:               dataDir,
		SyncMode:              syncMode,
		AsyncSecondaryIndexes: false,
	})
	if err := db.Open(defs); err != nil {
		return nil, nil, err
	}
	table := db.GetTable("items")
	if table == nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("items table missing")
	}
	return db, table, nil
}

func writeReport(path string, rep report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
