package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcisbee/flop/internal/quickbenchreport"
)

func series(samples ...float64) []report {
	var out []report
	for _, value := range samples {
		out = append(out, report{
			GoVersion: "go1.26.5", Rows: 100, Lookups: 50, Searches: 10, SyncMode: "full",
			Git:        quickbenchreport.GitMeta{Commit: "abc"},
			Comparison: &quickbenchreport.Config{Version: 1, Batch: 10, SearchLimit: 8, Host: "host", CPUs: 8, Procs: 8},
			Metrics:    []metric{{Name: "lookup", Value: value, Unit: "us/op"}, {Name: "search_hits", Value: 80, Unit: "rows"}},
		})
	}
	return out
}

func TestComparisonSignals(t *testing.T) {
	for _, tc := range []struct {
		name      string
		old, next []report
		want      string
	}{
		{"faster", series(100, 101, 102, 103, 104), series(80, 81, 82, 83, 84), "pct=-19.61% improvement-signal"},
		{"slower", series(80, 81, 82, 83, 84), series(100, 101, 102, 103, 104), "regression-signal"},
		{"overlap", series(100, 101, 102, 103, 104), series(90, 91, 92, 93, 101), "inconclusive"},
		{"single sample", series(100), series(10), "inconclusive"},
		{"zero baseline", series(0, 0, 0, 0, 0), series(1, 1, 1, 1, 1), "pct=n/a regression-signal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := compare(&out, tc.old, tc.next); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("wanted %q in %s", tc.want, out.String())
			}
		})
	}
	a, b := series(100, 101, 102, 103, 104), series(80, 81, 82, 83, 84)
	for _, group := range [][]report{a, b} {
		for i := range group {
			group[i].Metrics[0].Unit = "rows/s"
		}
	}
	var out bytes.Buffer
	if err := compare(&out, a, b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "regression-signal") {
		t.Fatal(out.String())
	}
}

func TestRejectInvalidComparisons(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*report)
		want   string
	}{
		{"legacy", func(r *report) { r.Comparison = nil }, "recapture"},
		{"rows", func(r *report) { r.Rows++ }, "mismatch"},
		{"lookups", func(r *report) { r.Lookups++ }, "mismatch"},
		{"searches", func(r *report) { r.Searches++ }, "mismatch"},
		{"durability", func(r *report) { r.SyncMode = "normal" }, "mismatch"},
		{"seed", func(r *report) { r.Comparison.Seed++ }, "mismatch"},
		{"batch", func(r *report) { r.Comparison.Batch++ }, "mismatch"},
		{"limit", func(r *report) { r.Comparison.SearchLimit++ }, "mismatch"},
		{"host", func(r *report) { r.Comparison.Host = "other" }, "mismatch"},
		{"toolchain", func(r *report) { r.GoVersion = "go1.27" }, "mismatch"},
		{"procs", func(r *report) { r.Comparison.Procs++ }, "mismatch"},
		{"unit", func(r *report) { r.Metrics[0].Unit = "ms" }, "unit differs"},
		{"missing metric", func(r *report) { r.Metrics = r.Metrics[:1] }, "metric set"},
		{"duplicate", func(r *report) { r.Metrics[1] = r.Metrics[0] }, "duplicate"},
		{"counts", func(r *report) { r.Metrics[1].Value++ }, "count changed"},
		{"timeout", func(r *report) { r.Metrics[0].Name = "async_warmup_timeout_ms" }, "timed out"},
		{"revision", func(r *report) { r.Git.Commit = "def" }, "mixed revisions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, next := series(1, 2, 3, 4, 5), series(1, 2, 3, 4, 5)
			tc.change(&next[3])
			var out bytes.Buffer
			err := compare(&out, old, next)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
			if out.Len() != 0 {
				t.Fatal("printed comparison before validation finished")
			}
		})
	}
}

func TestReadSeries(t *testing.T) {
	dir := t.TempDir()
	if _, err := readSeries(dir); err == nil {
		t.Fatal("accepted empty directory")
	}
	samples := series(10, 11, 12, 13, 14)
	for i, r := range samples {
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, string(rune('a'+i))+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := readSeries(dir)
	if err != nil || len(got) != 5 {
		t.Fatalf("got %d reports, %v", len(got), err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSeries(dir); err == nil {
		t.Fatal("ignored corrupt report")
	}
}
