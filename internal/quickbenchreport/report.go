// Package quickbenchreport defines the saved quickbench measurement format.
package quickbenchreport

// Config identifies the workload and execution environment, excluding the code
// revision being compared. Keep it comparable so mismatches cannot be ignored.
type Config struct {
	Version     int    `json:"version"`
	Batch       int    `json:"batch"`
	SearchLimit int    `json:"searchLimit"`
	Seed        int64  `json:"seed"`
	WarmTimeout string `json:"warmTimeout"`
	Host        string `json:"host"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	CPUs        int    `json:"cpus"`
	Procs       int    `json:"procs"`
	TempDir     string `json:"tempDir"`
	GOFLAGS     string `json:"goFlags"`
	GODEBUG     string `json:"goDebug"`
}

type Metric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type GitMeta struct {
	Commit string `json:"commit,omitempty"`
	Branch string `json:"branch,omitempty"`
	Dirty  bool   `json:"dirty,omitempty"`
}

type Report struct {
	CreatedAt  string   `json:"createdAt"`
	GoVersion  string   `json:"goVersion"`
	Git        GitMeta  `json:"git"`
	DataDir    string   `json:"dataDir"`
	Rows       int      `json:"rows"`
	Lookups    int      `json:"lookups"`
	Searches   int      `json:"searches"`
	SyncMode   string   `json:"syncMode"`
	Comparison *Config  `json:"comparison,omitempty"`
	Metrics    []Metric `json:"metrics"`
}
