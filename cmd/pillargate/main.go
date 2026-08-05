package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
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

type reportPaths []string

func (p *reportPaths) String() string {
	return strings.Join(*p, ",")
}

func (p *reportPaths) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("report path cannot be empty")
	}
	*p = append(*p, value)
	return nil
}

type thresholds struct {
	minWorkloadTPS  float64
	maxInsertP99US  float64
	maxUpdateP99US  float64
	maxDeleteP99US  float64
	maxAllocPerOp   float64
	maxRecoveryMS   int64
	requireExitCode int
	minCrashScens   int
}

func main() {
	var paths reportPaths
	flag.Var(&paths, "report", "path to pillarbench JSON report; repeat for median performance sampling")
	var (
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

	if len(paths) == 0 {
		fail("missing -report path")
	}

	reports := make([]report, 0, len(paths))
	for _, path := range paths {
		payload, err := os.ReadFile(path)
		if err != nil {
			failf("read report %q: %v", path, err)
		}

		var rep report
		if err := json.Unmarshal(payload, &rep); err != nil {
			failf("decode report %q: %v", path, err)
		}
		reports = append(reports, rep)
	}

	failures := evaluateReports(reports, thresholds{
		minWorkloadTPS:  *minWorkloadTPS,
		maxInsertP99US:  *maxInsertP99US,
		maxUpdateP99US:  *maxUpdateP99US,
		maxDeleteP99US:  *maxDeleteP99US,
		maxAllocPerOp:   *maxAllocPerOp,
		maxRecoveryMS:   *maxRecoveryMS,
		requireExitCode: *requireExitCode,
		minCrashScens:   *minCrashScens,
	})
	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "PILLAR_GATE_FAIL %s\n", f)
		}
		os.Exit(1)
	}
	fmt.Printf("PILLAR_GATE_OK samples=%d\n", len(reports))
}

func evaluateReports(reports []report, limits thresholds) []string {
	var failures []string
	expectMedianMin(reports, "workload_tps", limits.minWorkloadTPS, &failures)
	expectMedianMax(reports, "insert_p99_us", limits.maxInsertP99US, &failures)
	expectMedianMax(reports, "update_p99_us", limits.maxUpdateP99US, &failures)
	expectMedianMax(reports, "delete_p99_us", limits.maxDeleteP99US, &failures)
	expectMedianMax(reports, "workload_alloc_per_op", limits.maxAllocPerOp, &failures)
	expectAllMin(reports, "recovery_consistent", 1, &failures)

	crashResults := make([]crashResult, 0)
	for _, rep := range reports {
		crashResults = append(crashResults, rep.CrashResults...)
	}
	if len(crashResults) < limits.minCrashScens {
		failures = append(failures, fmt.Sprintf("crash results: got %d want >= %d", len(crashResults), limits.minCrashScens))
	}
	for _, cr := range crashResults {
		id := fmt.Sprintf("%s#%d", cr.Scenario, cr.Run)
		if !cr.Consistent {
			failures = append(failures, fmt.Sprintf("%s inconsistent", id))
		}
		if cr.Error != "" {
			failures = append(failures, fmt.Sprintf("%s error=%q", id, cr.Error))
		}
		if limits.requireExitCode >= 0 && cr.WorkerExitCode != limits.requireExitCode {
			failures = append(failures, fmt.Sprintf("%s exit=%d want=%d", id, cr.WorkerExitCode, limits.requireExitCode))
		}
		if cr.RecoveryMS > limits.maxRecoveryMS {
			failures = append(failures, fmt.Sprintf("%s recovery_ms=%d exceeds %d", id, cr.RecoveryMS, limits.maxRecoveryMS))
		}
	}

	return failures
}

func expectMedianMin(reports []report, name string, want float64, failures *[]string) {
	got, ok := medianMetric(reports, name, failures)
	if !ok {
		return
	}
	if got < want {
		*failures = append(*failures, fmt.Sprintf("%s median=%.3f below %.3f", name, got, want))
	}
}

func expectMedianMax(reports []report, name string, want float64, failures *[]string) {
	got, ok := medianMetric(reports, name, failures)
	if !ok {
		return
	}
	if got > want {
		*failures = append(*failures, fmt.Sprintf("%s median=%.3f above %.3f", name, got, want))
	}
}

func expectAllMin(reports []report, name string, want float64, failures *[]string) {
	for i, rep := range reports {
		got, ok := reportMetric(rep, name)
		if !ok {
			*failures = append(*failures, fmt.Sprintf("report %d metric %q missing", i+1, name))
			continue
		}
		if got < want {
			*failures = append(*failures, fmt.Sprintf("report %d %s=%.3f below %.3f", i+1, name, got, want))
		}
	}
}

func medianMetric(reports []report, name string, failures *[]string) (float64, bool) {
	values := make([]float64, 0, len(reports))
	for i, rep := range reports {
		value, ok := reportMetric(rep, name)
		if !ok {
			*failures = append(*failures, fmt.Sprintf("report %d metric %q missing", i+1, name))
			return 0, false
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		*failures = append(*failures, fmt.Sprintf("metric %q missing", name))
		return 0, false
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle], true
	}
	return (values[middle-1] + values[middle]) / 2, true
}

func reportMetric(rep report, name string) (float64, bool) {
	for _, m := range rep.Metrics {
		if m.Name == name {
			return m.Value, true
		}
	}
	return 0, false
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func failf(format string, args ...interface{}) {
	fail(fmt.Sprintf(format, args...))
}
