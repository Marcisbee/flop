//go:build enginecompare

package flop

import (
	"fmt"
	"github.com/marcisbee/flop/benchmarks/engines"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type comparisonRow struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}
type comparisonFlop struct {
	retries atomic.Int64

	app    *App
	db     *Database
	tables []*Table[comparisonRow]
	ctx    []*ReducerCtx
}

func newComparisonFlop(dir string, workers int) (*comparisonFlop, error) {
	a := New(Config{DataDir: dir, SyncMode: "full"})
	s := &comparisonFlop{app: a}
	for _, name := range []string{"leftrows", "rightrows"} {
		s.tables = append(s.tables, AutoTable[comparisonRow](a, name, func(t *TableBuilder[comparisonRow]) { t.Field("ID").Primary(); t.Field("Value").Required() }))
	}
	d, e := a.Open()
	if e != nil {
		return nil, e
	}
	s.db = d
	for i := 0; i < workers; i++ {
		s.ctx = append(s.ctx, &ReducerCtx{DB: d.trackedAccessor(nil, nil)})
	}
	return s, nil
}
func (s *comparisonFlop) Seed(n int) error {
	_, err := Transaction(s.ctx[0], func(tx *Tx) (bool, error) {
		for _, t := range s.tables {
			for i := 0; i < n; i++ {
				if _, e := t.Insert(tx, comparisonRow{ID: strconv.Itoa(i), Value: "00000000"}); e != nil {
					return false, e
				}
			}
		}
		return true, nil
	})
	return err
}
func (s *comparisonFlop) Op(w int, kind, key, value string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := s.op(w, kind, key, value)
		if err == nil {
			return nil
		}
		// Exact error text keeps the same adapter buildable against pre-conflict revisions.
		if err.Error() != "transaction row conflict; retry transaction" || time.Now().After(deadline) {
			return err
		}
		s.retries.Add(1)
		time.Sleep(time.Millisecond)
	}
}
func (s *comparisonFlop) op(w int, kind, key, value string) error {
	switch kind {
	case "read":
		_, e := s.Read("leftrows", key)
		return e
	case "insert":
		_, e := s.tables[0].Insert(s.db, comparisonRow{ID: key, Value: value})
		return e
	case "delete":
		ok, e := s.tables[0].Delete(s.db, key)
		if e == nil && !ok {
			return fmt.Errorf("missing row")
		}
		return e
	case "tx":
		_, e := Transaction(s.ctx[w], func(tx *Tx) (bool, error) {
			for _, t := range s.tables {
				if e := t.Update(tx, key, Set("value", value)); e != nil {
					return false, e
				}
			}
			return true, nil
		})
		return e
	default:
		return s.tables[0].Update(s.db, key, Set("value", value))
	}
}
func (s *comparisonFlop) Read(table, key string) (string, error) {
	i := 0
	if table == "rightrows" {
		i = 1
	}
	r, e := s.tables[i].Get(s.db, key)
	if e != nil {
		return "", e
	}
	if r == nil {
		return "", fmt.Errorf("missing row")
	}
	return r.Value, nil
}
func (s *comparisonFlop) Checkpoint() error { return s.db.Checkpoint() }
func (s *comparisonFlop) Close() error      { return s.db.Close() }
func TestEngineComparison(t *testing.T) {
	integer := func(k string, d int) int {
		v := os.Getenv(k)
		if v == "" {
			return d
		}
		n, e := strconv.Atoi(v)
		if e != nil || n < 0 {
			t.Fatalf("invalid %s", k)
		}
		return n
	}
	kind := os.Getenv("EC_ENGINE")
	if kind == "" {
		kind = "flop"
	}
	work := os.Getenv("EC_WORKLOAD")
	if work == "" {
		work = "tx"
	}
	if kind != "flop" && kind != "sqlite" && kind != "turso" {
		t.Fatalf("unknown engine %q", kind)
	}
	switch work {
	case "read", "update", "insert", "delete", "tx", "mixed":
	default:
		t.Fatalf("unknown workload %q", work)
	}
	c := engines.Config{Engine: kind, Revision: os.Getenv("EC_REVISION"), Workload: work, Workers: integer("EC_WORKERS", 8), Shared: os.Getenv("EC_SHARED") == "1", Operations: integer("EC_OPS", 1000), Rate: integer("EC_RATE", 1000), Seconds: float64(integer("EC_SECONDS", 2)), Checkpoints: os.Getenv("EC_CHECKPOINT") == "1", Rows: integer("EC_ROWS", 64)}
	if c.Workers < 1 || c.Rows < c.Workers || c.Operations < 1 {
		t.Fatal("invalid dimensions")
	}
	var e engines.Engine
	var err error
	if kind == "flop" {
		e, err = newComparisonFlop(t.TempDir(), c.Workers)
	} else {
		e, err = engines.OpenSQL(kind, t.TempDir(), c.Workers)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	r, err := engines.Run(e, c)
	if err != nil {
		t.Fatal(err)
	}
	engines.Print(r)
}

func (s *comparisonFlop) Count() (int, error) { return int(s.db.Table("leftrows").Count()), nil }
func (s *comparisonFlop) Reopen() error {
	if err := s.db.Close(); err != nil {
		return err
	}
	d, err := s.app.Open()
	if err != nil {
		return err
	}
	s.db = d
	return nil
}
func (s *comparisonFlop) RetryCount() int64 { return s.retries.Load() }
