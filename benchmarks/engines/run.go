package engines

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"

	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Engine interface {
	Seed(int) error
	Op(int, string, string, string) error
	Read(string, string) (string, error)
	Checkpoint() error
	Close() error
	Count() (int, error)
	Reopen() error
	RetryCount() int64
}
type Config struct {
	Engine      string
	Revision    string
	Workload    string
	Workers     int
	Shared      bool
	Operations  int
	Rate        int
	Seconds     float64
	Checkpoints bool
	Rows        int
}
type Result struct {
	Retries           int64     `json:"retries"`
	ReopenVerified    bool      `json:"reopen_verified"`
	EngineVersion     string    `json:"engine_version"`
	Config            Config    `json:"config"`
	Success           int       `json:"success"`
	Errors            int       `json:"errors"`
	FirstError        string    `json:"first_error,omitempty"`
	Seconds           float64   `json:"seconds"`
	Throughput        float64   `json:"throughput"`
	Latency           []float64 `json:"latency_ms"`
	Attempts          []float64 `json:"attempt_ms"`
	PeakRSS           uint64    `json:"peak_rss_bytes"`
	Go                string    `json:"go"`
	CPUs              int       `json:"gomaxprocs"`
	ReadLatency       []float64 `json:"read_ms,omitempty"`
	ReadErrors        int       `json:"read_errors"`
	CheckpointLatency []float64 `json:"checkpoint_request_ms,omitempty"`
	CheckpointErrors  int       `json:"checkpoint_errors"`
	CheckpointCalls   int       `json:"checkpoint_calls"`
	Verification      string    `json:"verification"`
}

// Fixed-size logarithmic histograms bound recorder memory independently of speed.
// Reported values are upper bucket bounds, with at most 1% quantization error.
type histogram struct {
	bins  [3200]int64
	count int
}

func (h *histogram) add(ms float64) {
	ns := math.Max(ms*1e6, 1)
	i := int(math.Ceil(math.Log(ns) / math.Log(1.01)))
	if i >= len(h.bins) {
		i = len(h.bins) - 1
	}
	h.bins[i]++
	h.count++
}
func (h *histogram) merge(o *histogram) {
	for i, n := range o.bins {
		h.bins[i] += n
	}
	h.count += o.count
}
func (h *histogram) quantiles() []float64 {
	if h.count == 0 {
		return nil
	}
	out := make([]float64, 3)
	for j, p := range []float64{.5, .99, .999} {
		target := int64(math.Ceil(p * float64(h.count)))
		var n int64
		for i, v := range h.bins {
			n += v
			if n >= target {
				out[j] = math.Pow(1.01, float64(i)) / 1e6
				break
			}
		}
	}
	return out
}
func RSS() uint64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, s := range strings.Split(string(b), "\n") {
		f := strings.Fields(s)
		if len(f) > 1 && f[0] == "VmHWM:" {
			n, _ := strconv.ParseUint(f[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
func Run(e Engine, c Config) (Result, error) {
	r := Result{Config: c, Go: runtime.Version(), CPUs: runtime.GOMAXPROCS(0)}
	if err := e.Seed(c.Rows); err != nil {
		return r, err
	}
	if err := e.Checkpoint(); err != nil {
		return r, err
	}
	var next atomic.Int64
	start := time.Now().Add(10 * time.Millisecond)
	end := start.Add(time.Duration(c.Seconds * float64(time.Second)))
	arrivals := make([]time.Duration, c.Operations)
	rng := rand.New(rand.NewSource(42))
	var at float64
	if c.Rate > 0 {
		for i := range arrivals {
			at += rng.ExpFloat64() / float64(c.Rate)
			arrivals[i] = time.Duration(at * float64(time.Second))
		}
	}
	type sample struct {
		ok, all            histogram
		errors             int
		first              string
		lastKey, lastValue string
	}
	samples := make([]sample, c.Workers)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var observers sync.WaitGroup
	var readTimes, checkpointTimes histogram
	readErrors, cpErrors := 0, 0
	if c.Checkpoints {
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
					err := e.Checkpoint()
					checkpointTimes.add(float64(time.Since(s)) / 1e6)
					if err != nil {
						cpErrors++
					}
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
				case s := <-tick.C:
					_, err := e.Read("leftrows", "0")
					readTimes.add(float64(time.Since(s)) / 1e6)
					if err != nil {
						readErrors++
					}
				}
			}
		}()
	}
	for w := 0; w < c.Workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s := &samples[w]
			time.Sleep(time.Until(start))
			for {
				i := int(next.Add(1) - 1)
				if c.Rate > 0 && i >= c.Operations {
					return
				}
				if c.Rate == 0 && time.Now().After(end) {
					return
				}
				scheduled := time.Now()
				if c.Rate > 0 {
					scheduled = start.Add(arrivals[i])
					time.Sleep(time.Until(scheduled))
				}
				key := strconv.Itoa(w)
				if c.Shared {
					key = "0"
				}
				kind := c.Workload
				if kind == "read" {
					key = strconv.Itoa(i % c.Rows)
				}
				if kind == "mixed" {
					if i%5 == 0 {
						kind = "update"
					} else {
						kind = "read"
					}
					key = strconv.Itoa(w)
				}
				if kind == "insert" {
					key = "new" + strconv.Itoa(i)
				}
				if kind == "delete" {
					if i >= c.Rows {
						return
					}
					key = strconv.Itoa(i)
				}
				value := fmt.Sprintf("%08d", i+1)
				err := e.Op(w, kind, key, value)
				lat := float64(time.Since(scheduled)) / 1e6
				s.all.add(lat)
				if err != nil {
					s.errors++
					if s.first == "" {
						s.first = err.Error()
					}
				} else {
					s.ok.add(lat)
					if kind == "update" || kind == "tx" {
						s.lastKey = key
						s.lastValue = value
					}
				}
			}
		}(w)
	}
	wg.Wait()
	r.Seconds = time.Since(start).Seconds()
	close(stop)
	observers.Wait()
	var ok, all histogram
	for _, s := range samples {
		ok.merge(&s.ok)
		all.merge(&s.all)
		r.Errors += s.errors
		if r.FirstError == "" {
			r.FirstError = s.first
		}
	}
	r.Success = ok.count
	r.Throughput = float64(r.Success) / r.Seconds
	r.Latency = ok.quantiles()
	r.Attempts = all.quantiles()
	r.ReadLatency = readTimes.quantiles()
	r.ReadErrors = readErrors
	r.CheckpointLatency = checkpointTimes.quantiles()
	r.CheckpointErrors = cpErrors
	r.CheckpointCalls = checkpointTimes.count
	r.Verification = "passed"
	verify := func() error {
		count, err := e.Count()
		want := c.Rows
		if c.Workload == "insert" {
			want += r.Success
		}
		if c.Workload == "delete" {
			want -= r.Success
		}
		if err != nil || count != want {
			return fmt.Errorf("row count got %d want %d: %v", count, want, err)
		}
		if c.Workload == "tx" || c.Workload == "update" {
			for _, s := range samples {
				if s.lastKey == "" {
					continue
				}
				v, err := e.Read("leftrows", s.lastKey)
				if err != nil || (!c.Shared && v != s.lastValue) {
					return fmt.Errorf("final value mismatch: %s %v", s.lastKey, err)
				}
				if c.Workload == "tx" {
					other, err := e.Read("rightrows", s.lastKey)
					if err != nil || v != other {
						return fmt.Errorf("transaction tables diverged: %s", s.lastKey)
					}
				}
			}
		}
		return nil
	}
	if err := verify(); err != nil {
		r.Verification = err.Error()
	}
	r.Retries = e.RetryCount()
	if sql, ok := e.(*SQL); ok {
		r.EngineVersion = sql.Version
	}
	if err := e.Reopen(); err != nil {
		r.Verification = "reopen failed: " + err.Error()
	} else if err := verify(); err != nil {
		r.Verification = "after reopen: " + err.Error()
	} else {
		r.ReopenVerified = true
	}
	if r.Errors > 0 || r.ReadErrors > 0 || r.CheckpointErrors > 0 {
		r.Verification = "failed operations; not eligible for speed ranking; " + r.Verification
	}
	r.PeakRSS = RSS()
	return r, nil
}
func Print(r Result) { b, _ := json.Marshal(r); fmt.Printf("ENGINE_RESULT %s\n", b) }
