package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

type report struct {
	Metrics      []metric      `json:"metrics"`
	CrashResults []crashResult `json:"crashResults"`
}

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
	Error          string `json:"error"`
}

func main() {
	var (
		reportPath      = flag.String("report", "", "path to pillarbench JSON report")
		minWorkloadTPS  = flag.Float64("min-workload-tps", 3000, "minimum workload_tps")
		maxInsertP99US  = flag.Float64("max-insert-p99-us", 5000, "maximum insert_p99_us")
		maxUpdateP99US  = flag.Float64("max-update-p99-us", 10000, "maximum update_p99_us")
		maxDeleteP99US  = flag.Float64("max-delete-p99-us", 5000, "maximum delete_p99_us")
		maxAllocPerOp   = flag.Float64("max-alloc-per-op", 12000, "maximum workload_alloc_per_op")
		maxRecoveryMS   = flag.Int64("max-recovery-ms", 2000, "maximum crash recoveryMs per scenario")
		requireExitCode = flag.Int("require-exit-code", 197, "required crash-worker exit code; set <0 to disable")
		minCrashScens   = flag.Int("min-crash-scenarios", 1, "minimum number of crash results")
	)
	flag.Parse()

	if strings.TrimSpace(*reportPath) == "" {
		fail("missing -report path")
	}

	payload, err := os.ReadFile(*reportPath)
	if err != nil {
		failf("read report: %v", err)
	}

	var rep report
	if err := json.Unmarshal(payload, &rep); err != nil {
		failf("decode report: %v", err)
	}

	metrics := make(map[string]float64, len(rep.Metrics))
	for _, m := range rep.Metrics {
		metrics[m.Name] = m.Value
	}

	var failures []string
	expectMin(metrics, "workload_tps", *minWorkloadTPS, &failures)
	expectMax(metrics, "insert_p99_us", *maxInsertP99US, &failures)
	expectMax(metrics, "update_p99_us", *maxUpdateP99US, &failures)
	expectMax(metrics, "delete_p99_us", *maxDeleteP99US, &failures)
	expectMax(metrics, "workload_alloc_per_op", *maxAllocPerOp, &failures)
	expectMin(metrics, "recovery_consistent", 1, &failures)

	if len(rep.CrashResults) < *minCrashScens {
		failures = append(failures, fmt.Sprintf("crash results: got %d want >= %d", len(rep.CrashResults), *minCrashScens))
	}
	for _, cr := range rep.CrashResults {
		id := fmt.Sprintf("%s#%d", cr.Scenario, cr.Run)
		if !cr.Consistent {
			failures = append(failures, fmt.Sprintf("%s inconsistent", id))
		}
		if cr.Error != "" {
			failures = append(failures, fmt.Sprintf("%s error=%q", id, cr.Error))
		}
		if *requireExitCode >= 0 && cr.WorkerExitCode != *requireExitCode {
			failures = append(failures, fmt.Sprintf("%s exit=%d want=%d", id, cr.WorkerExitCode, *requireExitCode))
		}
		if cr.RecoveryMS > *maxRecoveryMS {
			failures = append(failures, fmt.Sprintf("%s recovery_ms=%d exceeds %d", id, cr.RecoveryMS, *maxRecoveryMS))
		}
	}

	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "PILLAR_GATE_FAIL %s\n", f)
		}
		os.Exit(1)
	}
	fmt.Println("PILLAR_GATE_OK")
}

func expectMin(metrics map[string]float64, name string, want float64, failures *[]string) {
	got, ok := metrics[name]
	if !ok {
		*failures = append(*failures, fmt.Sprintf("metric %q missing", name))
		return
	}
	if got < want {
		*failures = append(*failures, fmt.Sprintf("%s=%.3f below %.3f", name, got, want))
	}
}

func expectMax(metrics map[string]float64, name string, want float64, failures *[]string) {
	got, ok := metrics[name]
	if !ok {
		*failures = append(*failures, fmt.Sprintf("metric %q missing", name))
		return
	}
	if got > want {
		*failures = append(*failures, fmt.Sprintf("%s=%.3f above %.3f", name, got, want))
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func failf(format string, args ...interface{}) {
	fail(fmt.Sprintf(format, args...))
}
