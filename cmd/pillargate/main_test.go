package main

import (
	"strings"
	"testing"
)

func TestEvaluateReportsIgnoresSinglePerformanceOutlier(t *testing.T) {
	reports := []report{
		testReport(950, 89_224, 128_201, 89_409, 2_998),
		testReport(6_100, 2_100, 5_500, 2_200, 2_970),
		testReport(5_900, 2_000, 5_300, 2_100, 2_960),
	}
	reports[0].CrashResults = []crashResult{{Scenario: "insert_before_commit", Run: 1, WorkerExitCode: 197, Consistent: true}}

	if failures := evaluateReports(reports, testThresholds()); len(failures) != 0 {
		t.Fatalf("unexpected failures: %v", failures)
	}
}

func TestEvaluateReportsRejectsSustainedPerformanceRegression(t *testing.T) {
	reports := []report{
		testReport(950, 89_224, 128_201, 89_409, 2_998),
		testReport(940, 88_000, 127_000, 87_000, 3_010),
		testReport(960, 90_000, 129_000, 91_000, 2_990),
	}
	reports[0].CrashResults = []crashResult{{Scenario: "insert_before_commit", Run: 1, WorkerExitCode: 197, Consistent: true}}

	failures := evaluateReports(reports, testThresholds())
	joined := strings.Join(failures, "\n")
	for _, want := range []string{"insert_p99_us median=", "update_p99_us median=", "delete_p99_us median="} {
		if !strings.Contains(joined, want) {
			t.Errorf("failures %q do not contain %q", joined, want)
		}
	}
}

func TestEvaluateReportsRequiresEveryPerformanceMetric(t *testing.T) {
	reports := []report{
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
	}
	reports[0].CrashResults = []crashResult{{Scenario: "insert_before_commit", Run: 1, WorkerExitCode: 197, Consistent: true}}
	reports[1].Metrics = reports[1].Metrics[1:]

	failures := evaluateReports(reports, testThresholds())
	if got := strings.Join(failures, "\n"); !strings.Contains(got, `report 2 metric "workload_tps" missing`) {
		t.Fatalf("unexpected failures: %v", failures)
	}
}

func TestEvaluateReportsDoesNotMaskCrashFailure(t *testing.T) {
	reports := []report{
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
	}
	reports[0].CrashResults = []crashResult{{Scenario: "insert_before_commit", Run: 1, WorkerExitCode: 1, Consistent: false}}

	failures := evaluateReports(reports, testThresholds())
	joined := strings.Join(failures, "\n")
	if !strings.Contains(joined, "insert_before_commit#1 inconsistent") || !strings.Contains(joined, "exit=1 want=197") {
		t.Fatalf("unexpected failures: %v", failures)
	}
}

func TestEvaluateReportsDoesNotMaskConsistencyFailure(t *testing.T) {
	reports := []report{
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
		testReport(6_000, 2_000, 5_000, 2_000, 3_000),
	}
	reports[0].CrashResults = []crashResult{{Scenario: "insert_before_commit", Run: 1, WorkerExitCode: 197, Consistent: true}}
	reports[1].Metrics[len(reports[1].Metrics)-1].Value = 0

	failures := evaluateReports(reports, testThresholds())
	if got := strings.Join(failures, "\n"); !strings.Contains(got, "report 2 recovery_consistent=0.000 below 1.000") {
		t.Fatalf("unexpected failures: %v", failures)
	}
}

func testReport(tps, insertP99, updateP99, deleteP99, allocPerOp float64) report {
	return report{Metrics: []metric{
		{Name: "workload_tps", Value: tps},
		{Name: "insert_p99_us", Value: insertP99},
		{Name: "update_p99_us", Value: updateP99},
		{Name: "delete_p99_us", Value: deleteP99},
		{Name: "workload_alloc_per_op", Value: allocPerOp},
		{Name: "recovery_consistent", Value: 1},
	}}
}

func testThresholds() thresholds {
	return thresholds{
		minWorkloadTPS:  250,
		maxInsertP99US:  50_000,
		maxUpdateP99US:  120_000,
		maxDeleteP99US:  50_000,
		maxAllocPerOp:   20_000,
		maxRecoveryMS:   5_000,
		requireExitCode: 197,
		minCrashScens:   1,
	}
}
