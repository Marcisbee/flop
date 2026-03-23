package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
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
	oldPath := flag.String("old", "", "baseline quickbench JSON report path")
	newPath := flag.String("new", "", "new quickbench JSON report path")
	flag.Parse()

	if *oldPath == "" || *newPath == "" {
		fmt.Fprintln(os.Stderr, "usage: quickbenchcmp -old <baseline.json> -new <candidate.json>")
		os.Exit(2)
	}

	oldRep, err := readReport(*oldPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read old report: %v\n", err)
		os.Exit(1)
	}
	newRep, err := readReport(*newPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read new report: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("INFO old=%s commit=%s created=%s rows=%d\n", *oldPath, oldRep.Git.Commit, oldRep.CreatedAt, oldRep.Rows)
	fmt.Printf("INFO new=%s commit=%s created=%s rows=%d\n", *newPath, newRep.Git.Commit, newRep.CreatedAt, newRep.Rows)

	oldMap := metricsByName(oldRep.Metrics)
	newMap := metricsByName(newRep.Metrics)

	common := make([]string, 0, len(newMap))
	for name := range newMap {
		if _, ok := oldMap[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)

	for _, name := range common {
		ov := oldMap[name]
		nv := newMap[name]
		unit := nv.Unit
		if unit == "" {
			unit = ov.Unit
		}
		delta := nv.Value - ov.Value
		pct := math.NaN()
		if ov.Value != 0 {
			pct = (delta / ov.Value) * 100
		}
		if math.IsNaN(pct) {
			fmt.Printf("CMP %s old=%.3f new=%.3f delta=%.3f unit=%s pct=n/a\n", name, ov.Value, nv.Value, delta, unit)
			continue
		}
		fmt.Printf("CMP %s old=%.3f new=%.3f delta=%.3f unit=%s pct=%+.2f%%\n", name, ov.Value, nv.Value, delta, unit, pct)
	}
}

func readReport(path string) (report, error) {
	var rep report
	data, err := os.ReadFile(path)
	if err != nil {
		return rep, err
	}
	if err := json.Unmarshal(data, &rep); err != nil {
		return rep, err
	}
	return rep, nil
}

func metricsByName(metrics []metric) map[string]metric {
	out := make(map[string]metric, len(metrics))
	for _, m := range metrics {
		out[m.Name] = m
	}
	return out
}
