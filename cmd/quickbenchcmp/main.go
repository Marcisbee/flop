package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/marcisbee/flop/internal/quickbenchreport"
)

type report = quickbenchreport.Report
type metric = quickbenchreport.Metric

func main() {
	oldPath := flag.String("old", "", "baseline JSON report or series directory")
	newPath := flag.String("new", "", "candidate JSON report or series directory")
	flag.Parse()
	if *oldPath == "" || *newPath == "" {
		fmt.Fprintln(os.Stderr, "usage: quickbenchcmp -old <baseline.json|directory> -new <candidate.json|directory>")
		os.Exit(2)
	}
	old, err := readSeries(*oldPath)
	if err == nil {
		var next []report
		next, err = readSeries(*newPath)
		if err == nil {
			err = compare(os.Stdout, old, next)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func readSeries(path string) ([]report, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	paths := []string{path}
	if info.IsDir() {
		paths, err = filepath.Glob(filepath.Join(path, "*.json"))
		if err != nil {
			return nil, err
		}
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no JSON reports in %s", path)
	}
	var reports []report
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var r report
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		reports = append(reports, r)
	}
	return reports, nil
}

func validate(r report) error {
	c := r.Comparison
	if c == nil || c.Version != 1 {
		return fmt.Errorf("report lacks supported comparison metadata; recapture baseline and candidate with current quickbench")
	}
	if r.Rows <= 0 || r.Lookups <= 0 || r.Searches <= 0 || c.Batch <= 0 || c.SearchLimit <= 0 || c.Host == "" || r.GoVersion == "" || c.CPUs <= 0 || c.Procs <= 0 {
		return fmt.Errorf("report has incomplete workload/environment metadata")
	}
	if r.SyncMode != "full" && r.SyncMode != "normal" {
		return fmt.Errorf("invalid sync mode")
	}
	if len(r.Metrics) == 0 {
		return fmt.Errorf("report has no metrics")
	}
	seen := map[string]bool{}
	for _, m := range r.Metrics {
		if m.Name == "" || m.Unit == "" || seen[m.Name] || math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0 {
			return fmt.Errorf("invalid or duplicate metric %q", m.Name)
		}
		if m.Name == "async_warmup_timeout_ms" {
			return fmt.Errorf("async index warmup timed out; fix readiness before comparing")
		}
		seen[m.Name] = true
	}
	return nil
}

func compatible(a, b report) bool {
	return a.GoVersion == b.GoVersion && a.Rows == b.Rows && a.Lookups == b.Lookups && a.Searches == b.Searches && a.SyncMode == b.SyncMode && *a.Comparison == *b.Comparison
}

func compare(w io.Writer, old, next []report) error {
	if len(old) == 0 || len(next) == 0 {
		return fmt.Errorf("both series need reports")
	}
	reference := old[0]
	if err := validate(reference); err != nil {
		return err
	}
	units := map[string]string{}
	for _, m := range reference.Metrics {
		units[m.Name] = m.Unit
	}
	values := make([]map[string][]float64, 2)
	for group, series := range [][]report{old, next} {
		values[group] = map[string][]float64{}
		for _, r := range series {
			if err := validate(r); err != nil {
				return err
			}
			if !compatible(reference, r) {
				return fmt.Errorf("workload or environment mismatch; use identical settings and host for all samples")
			}
			if r.Git != series[0].Git {
				return fmt.Errorf("mixed revisions within a series; recapture without editing between samples")
			}
			if len(r.Metrics) != len(units) {
				return fmt.Errorf("metric set differs; results are not comparable")
			}
			for _, m := range r.Metrics {
				if units[m.Name] != m.Unit {
					return fmt.Errorf("metric set or unit differs for %s", m.Name)
				}
				values[group][m.Name] = append(values[group][m.Name], m.Value)
			}
		}
	}
	// Hit counts are observable behavior, not performance. Fail before printing a
	// speed comparison if either revision or any sample changes those results.
	for name, unit := range units {
		if unit != "rows" {
			continue
		}
		for _, group := range values {
			for _, v := range group[name] {
				if v != values[0][name][0] {
					return fmt.Errorf("result count changed for %s; verify correctness before comparing speed", name)
				}
			}
		}
	}
	fmt.Fprintf(w, "INFO old commit=%s dirty=%t n=%d; new commit=%s dirty=%t n=%d\n", old[0].Git.Commit, old[0].Git.Dirty, len(old), next[0].Git.Commit, next[0].Git.Dirty, len(next))
	fmt.Fprintln(w, "INFO ranges are observed min/max, not confidence intervals; signals require >=5 samples per side and non-overlapping ranges. Recheck signals in reverse run order. This is not a release gate.")
	names := make([]string, 0, len(units))
	for name := range units {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		a, b := values[0][name], values[1][name]
		sort.Float64s(a)
		sort.Float64s(b)
		am, bm := median(a), median(b)
		pct := "n/a"
		if am != 0 {
			pct = fmt.Sprintf("%+.2f%%", (bm/am-1)*100)
		}
		verdict := "inconclusive"
		switch units[name] {
		case "rows":
			verdict = "unchanged-count"
		case "x":
			verdict = "descriptive-ratio"
		case "ms", "us", "us/op", "rows/s":
			if len(a) >= 5 && len(b) >= 5 {
				faster, slower := b[len(b)-1] < a[0], b[0] > a[len(a)-1]
				if units[name] == "rows/s" {
					faster, slower = slower, faster
				}
				if faster {
					verdict = "improvement-signal"
				}
				if slower {
					verdict = "regression-signal"
				}
			}
		}
		fmt.Fprintf(w, "CMP %s old=%.3f [%.3f,%.3f] new=%.3f [%.3f,%.3f] unit=%s pct=%s %s\n", name, am, a[0], a[len(a)-1], bm, b[0], b[len(b)-1], units[name], pct, verdict)
	}
	return nil
}

func median(v []float64) float64 {
	n := len(v)
	if n%2 == 0 {
		return (v[n/2-1] + v[n/2]) / 2
	}
	return v[n/2]
}
