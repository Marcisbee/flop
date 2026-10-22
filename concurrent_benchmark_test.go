package flop

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/storage"
)

// BenchmarkConcurrentDurable measures scheduled arrival through acknowledgement,
// including worker queueing. Run with -benchtime=1x; use FLOP_CONCURRENT_OPS and
// FLOP_CONCURRENT_RATE to set a fixed open-loop load, shared by every case.
func BenchmarkConcurrentDurable(b *testing.B) {
	for _, writers := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, tx := range []bool{false, true} {
			for _, shared := range []bool{false, true} {
				b.Run(fmt.Sprintf("writers=%d/tx=%t/shared=%t", writers, tx, shared), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						runConcurrentDurable(b, writers, tx, shared)
					}
				})
			}
		}
	}
}

type concurrentRow struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

func runConcurrentDurable(b *testing.B, writers int, explicit, shared bool) {
	b.Helper()
	setting := func(name string, fallback int) int {
		if s := os.Getenv(name); s != "" {
			n, e := strconv.Atoi(s)
			if e != nil || n <= 0 {
				b.Fatalf("invalid %s", name)
			}
			return n
		}
		return fallback
	}
	n, rate := setting("FLOP_CONCURRENT_OPS", 10000), setting("FLOP_CONCURRENT_RATE", 1000)
	dataDir := b.TempDir()
	app := New(Config{DataDir: dataDir, SyncMode: "full"})
	tables := make([]*Table[concurrentRow], 2)
	for i := range tables {
		tables[i] = AutoTable[concurrentRow](app, fmt.Sprintf("bench%d", i), func(t *TableBuilder[concurrentRow]) { t.Field("ID").Primary(); t.Field("Value").Required() })
	}
	db, err := app.Open()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	}()
	for _, table := range tables {
		for w := 0; w < writers; w++ {
			if _, e := table.Insert(db, concurrentRow{ID: strconv.Itoa(w), Value: "00000000"}); e != nil {
				b.Fatal(e)
			}
		}
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	arrivals := make([]time.Duration, n)
	rng := rand.New(rand.NewSource(42))
	var elapsed float64
	for i := range arrivals {
		elapsed += rng.ExpFloat64() / float64(rate)
		arrivals[i] = time.Duration(elapsed * float64(time.Second))
	}
	latencies := make([]float64, n)
	succeeded := make([]bool, n)
	reads := make([]float64, 0)
	checkpoints := make([]float64, 0)
	publications := make([]float64, 0)
	readMissing := 0
	var next atomic.Int64
	var errors atomic.Int64
	var firstErr error
	var errOnce sync.Once
	fail := func(e error) {
		if e != nil {
			errors.Add(1)
			errOnce.Do(func() { firstErr = e })
		}
	}
	var peakHeap atomic.Uint64
	stop := make(chan struct{})
	var observers sync.WaitGroup
	start := time.Now().Add(10 * time.Millisecond)
	observers.Add(1)
	go func() {
		defer observers.Done()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peakHeap.Load() {
					peakHeap.Store(m.HeapAlloc)
				}
			}
		}
	}()
	if os.Getenv("FLOP_CONCURRENT_CHECKPOINT") == "1" {
		manifest, err := storage.ReadLatestCheckpointManifest(dataDir, "bench0")
		if err != nil || manifest == nil {
			b.Fatalf("initial checkpoint manifest: %v", err)
		}
		generation := manifest.Generation
		observers.Add(2)
		go func() {
			defer observers.Done()
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					s := time.Now()
					fail(db.Checkpoint())
					elapsed := float64(time.Since(s)) / 1e6
					checkpoints = append(checkpoints, elapsed)
					// Inspect the durable generation after timing the call: a
					// successful request may defer publication under write load.
					manifest, err := storage.ReadLatestCheckpointManifest(dataDir, "bench0")
					if err != nil {
						fail(err)
						return
					}
					if manifest == nil {
						fail(fmt.Errorf("checkpoint manifest missing"))
						return
					}
					if manifest.Generation > generation {
						publications = append(publications, elapsed)
					}
					generation = manifest.Generation
				}
			}
		}()
		go func() {
			defer observers.Done()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case scheduled := <-tick.C:
					row, e := tables[0].Get(db, "0")
					if row == nil {
						readMissing++
					}
					fail(e)
					reads = append(reads, float64(time.Since(scheduled))/1e6)
				}
			}
		}()
	}
	var workers sync.WaitGroup
	for w := 0; w < writers; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			key := strconv.Itoa(w)
			if shared {
				key = "0"
			}
			ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				scheduled := start.Add(arrivals[i])
				time.Sleep(time.Until(scheduled))
				value := fmt.Sprintf("%08d", i)
				var operationErr error
				if explicit {
					_, e := Transaction(ctx, func(tx *Tx) (bool, error) {
						for _, table := range tables {
							if e := table.Update(tx, key, Set("value", value)); e != nil {
								return false, e
							}
						}
						return true, nil
					})
					operationErr = e
				} else {
					for _, table := range tables {
						if e := table.Update(db, key, Set("value", value)); e != nil {
							operationErr = e
							break
						}
					}
				}
				fail(operationErr)
				succeeded[i] = operationErr == nil
				latencies[i] = float64(time.Since(scheduled)) / 1e6
			}
		}(w)
	}
	workers.Wait()
	duration := time.Since(start).Seconds()
	close(stop)
	observers.Wait()
	quantiles := func(v []float64) []float64 {
		if len(v) == 0 {
			return nil
		}
		sort.Float64s(v)
		out := make([]float64, 3)
		for i, p := range []float64{.5, .99, .999} {
			out[i] = v[int(math.Ceil(p*float64(len(v))))-1]
		}
		return out
	}
	successLatencies := make([]float64, 0, n)
	for i, ok := range succeeded {
		if ok {
			successLatencies = append(successLatencies, latencies[i])
		}
	}
	report := map[string]any{"writers": writers, "transaction": explicit, "shared": shared, "operations": n, "rate": rate, "attempt_ms": quantiles(latencies), "write_ms": quantiles(successLatencies), "read_ms": quantiles(reads), "checkpoint_ms": quantiles(checkpoints), "checkpoint_calls": len(checkpoints), "checkpoint_publications": len(publications), "published_checkpoint_ms": quantiles(publications), "read_calls": len(reads), "read_missing": readMissing, "throughput": float64(len(successLatencies)) / duration, "checkpoint": os.Getenv("FLOP_CONCURRENT_CHECKPOINT") == "1", "peak_heap_bytes": peakHeap.Load(), "errors": errors.Load(), "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	data, _ := json.Marshal(report)
	fmt.Printf("CONCURRENT %s\n", data)
	if firstErr != nil {
		if !shared {
			b.Fatalf("workload failed: %v (%d errors)", firstErr, errors.Load())
		}
		b.Logf("shared-key workload errors: %v (%d errors); failed attempts excluded from throughput and commit latency", firstErr, errors.Load())
	}
}
