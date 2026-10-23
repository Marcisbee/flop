package engines

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	_ "turso.tech/database/tursogo"
)

type SQL struct {
	DB      *sql.DB
	Kind    string
	Version string
	Workers []*sql.Conn
	Dir     string
	Retries atomic.Int64
}

func OpenSQL(kind, dir string, workers int) (*SQL, error) {
	driver, dsn := "sqlite3", filepath.Join(dir, "database.db")+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=0"
	if kind == "turso" {
		driver = "turso"
		dsn = filepath.Join(dir, "database.db")
	}
	db, e := sql.Open(driver, dsn)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(workers + 3)
	s := &SQL{DB: db, Kind: kind, Dir: dir}
	if kind == "turso" {
		if _, e = db.Exec("PRAGMA journal_mode=mvcc"); e != nil {
			return nil, e
		}
	}
	if _, e = db.Exec("PRAGMA synchronous=FULL"); e != nil {
		return nil, e
	}
	if e = db.QueryRow("SELECT sqlite_version()").Scan(&s.Version); e != nil {
		return nil, e
	}
	for _, t := range []string{"leftrows", "rightrows"} {
		if _, e = db.Exec("CREATE TABLE IF NOT EXISTS " + t + " (id TEXT PRIMARY KEY, value TEXT NOT NULL)"); e != nil {
			return nil, e
		}
	}
	for w := 0; w < workers; w++ {
		c, e := db.Conn(context.Background())
		if e != nil {
			return nil, e
		}
		if _, e = c.ExecContext(context.Background(), "PRAGMA synchronous=FULL"); e != nil {
			return nil, e
		}
		var sync int
		if e = c.QueryRowContext(context.Background(), "PRAGMA synchronous").Scan(&sync); e != nil || sync != 2 {
			return nil, fmt.Errorf("FULL sync not active: %d %v", sync, e)
		}
		s.Workers = append(s.Workers, c)
	}
	return s, nil
}
func (s *SQL) Seed(n int) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, t := range []string{"leftrows", "rightrows"} {
		st, e := tx.Prepare("INSERT INTO " + t + " VALUES (?,?)")
		if e != nil {
			return e
		}
		for i := 0; i < n; i++ {
			if _, e = st.Exec(fmt.Sprint(i), "00000000"); e != nil {
				return e
			}
		}
		st.Close()
	}
	return tx.Commit()
}
func (s *SQL) Op(w int, kind, key, value string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := s.op(w, kind, key, value)
		if err == nil {
			return nil
		}
		msg := strings.ToLower(err.Error())
		if !(strings.Contains(msg, "busy") || strings.Contains(msg, "locked") || strings.Contains(msg, "write-write conflict") || strings.Contains(msg, "commit dependency aborted")) || time.Now().After(deadline) {
			return err
		}
		s.Retries.Add(1)
		time.Sleep(time.Millisecond)
	}
}
func (s *SQL) op(w int, kind, key, value string) error {
	c := s.Workers[w]
	ctx := context.Background()
	if kind == "read" {
		var v string
		return c.QueryRowContext(ctx, "SELECT value FROM leftrows WHERE id=?", key).Scan(&v)
	}
	if kind == "tx" {
		begin := "BEGIN IMMEDIATE"
		if s.Kind == "turso" {
			begin = "BEGIN CONCURRENT"
		}
		if _, e := c.ExecContext(ctx, begin); e != nil {
			return e
		}
		for _, t := range []string{"leftrows", "rightrows"} {
			r, e := c.ExecContext(ctx, "UPDATE "+t+" SET value=? WHERE id=?", value, key)
			if e == nil {
				n, x := r.RowsAffected()
				if x != nil {
					e = x
				} else if n != 1 {
					e = fmt.Errorf("missing row")
				}
			}
			if e != nil {
				c.ExecContext(ctx, "ROLLBACK")
				return e
			}
		}
		_, e := c.ExecContext(ctx, "COMMIT")
		if e != nil {
			c.ExecContext(ctx, "ROLLBACK")
		}
		return e
	}
	q := "UPDATE leftrows SET value=? WHERE id=?"
	args := []any{value, key}
	if kind == "insert" {
		q = "INSERT INTO leftrows (id,value) VALUES (?,?)"
		args = []any{key, value}
	}
	if kind == "delete" {
		q = "DELETE FROM leftrows WHERE id=?"
		args = []any{key}
	}
	r, e := c.ExecContext(ctx, q, args...)
	if e != nil {
		return e
	}
	n, e := r.RowsAffected()
	if e == nil && n != 1 {
		e = fmt.Errorf("affected %d rows", n)
	}
	return e
}
func (s *SQL) Read(table, key string) (string, error) {
	var v string
	e := s.DB.QueryRow("SELECT value FROM "+table+" WHERE id=?", key).Scan(&v)
	return v, e
}
func (s *SQL) Checkpoint() error { _, e := s.DB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); return e }
func (s *SQL) Close() error {
	for _, c := range s.Workers {
		c.Close()
	}
	return s.DB.Close()
}

func (s *SQL) Count() (int, error) {
	var n int
	err := s.DB.QueryRow("SELECT COUNT(*) FROM leftrows").Scan(&n)
	return n, err
}
func (s *SQL) Reopen() error {
	kind, dir, n := s.Kind, s.Dir, len(s.Workers)
	if err := s.Close(); err != nil {
		return err
	}
	other, err := OpenSQL(kind, dir, n)
	if err != nil {
		return err
	}
	s.DB = other.DB
	s.Workers = other.Workers
	return nil
}
func (s *SQL) RetryCount() int64 { return s.Retries.Load() }
