package flop

import (
	"fmt"
	"testing"
)

// Sparse visibility crosses the old physical chunk boundaries. Deletion leaves
// a storage hole; pagination must count visible rows rather than physical slots.
func TestScopedScanPagination(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	var raw *TableInstance
	nestedWrite := false
	Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Boolean("visible")
		s.String("secret").Access(FieldAccess{Read: func(*TableReadCtx) bool { return false }})
		s.Access(TableAccess{Read: func(c *TableReadCtx) bool {
			if nestedWrite {
				nestedWrite = false
				if _, err := raw.Update("p0000", map[string]any{"secret": "changed"}); err != nil {
					t.Fatal(err)
				}
			}
			return c.Row["visible"] == true
		}})
	})
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	raw = db.Table("posts")
	var want []string
	rows := make([]map[string]any, 0, 1200)
	for i := 0; i < 1200; i++ {
		id := fmt.Sprintf("p%04d", i)
		visible := i%17 == 0
		rows = append(rows, map[string]any{"id": id, "visible": visible, "secret": "hidden"})
		if visible && i != 51 {
			want = append(want, id)
		}
	}
	if _, err := raw.InsertMany(rows, 1000); err != nil {
		t.Fatal(err)
	}
	if ok, err := raw.Delete("p0051"); err != nil || !ok {
		t.Fatalf("delete=%v err=%v", ok, err)
	}
	scoped := db.trackedAccessor(nil, nil).Table("posts")
	nestedWrite = true
	if n := scoped.Count(); n != len(want) {
		t.Fatalf("count=%d want=%d", n, len(want))
	}
	for _, tc := range []struct{ limit, offset int }{{7, 0}, {9, 30}, {100, 0}, {10, 68}, {10, 100}, {1, -1}, {0, 0}, {-1, 0}} {
		t.Run(fmt.Sprintf("limit%d_offset%d", tc.limit, tc.offset), func(t *testing.T) {
			nestedWrite = true
			got, err := scoped.Scan(tc.limit, tc.offset)
			if err != nil {
				t.Fatal(err)
			}
			start := max(0, min(tc.offset, len(want)))
			end := min(len(want), start+max(tc.limit, 0))
			if len(got) != end-start {
				t.Fatalf("rows=%d want=%d", len(got), end-start)
			}
			for i, row := range got {
				if row["id"] != want[start+i] {
					t.Fatalf("row %d=%v want=%s", i, row["id"], want[start+i])
				}
				if _, ok := row["secret"]; ok {
					t.Fatal("protected field leaked")
				}
			}
		})
	}
}

func TestScopedTraversalSkipsPendingRowsAcrossPages(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	posts := Define(app, "posts", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.Access(TableAccess{Read: func(*TableReadCtx) bool { return true }})
	})
	db, err := app.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scoped := db.trackedAccessor(nil, nil).Table("posts")
	ctx := &ReducerCtx{DB: db.trackedAccessor(nil, nil)}
	_, err = Transaction(ctx, func(tx *Tx) (bool, error) {
		if _, err := posts.Insert(tx, map[string]any{"id": "pending"}); err != nil {
			return false, err
		}
		rows := make([]map[string]any, 600)
		for i := range rows {
			rows[i] = map[string]any{"id": fmt.Sprintf("p%04d", i)}
		}
		if _, err := db.Table("posts").InsertMany(rows, 600); err != nil {
			return false, err
		}
		if n := scoped.Count(); n != 600 {
			t.Errorf("count while first row is pending=%d want=600", n)
		}
		got, err := scoped.Scan(10, 590)
		if err != nil {
			return false, err
		}
		if len(got) != 10 {
			t.Errorf("page length=%d want=10", len(got))
		}
		for i, row := range got {
			if want := fmt.Sprintf("p%04d", 590+i); row["id"] != want {
				t.Errorf("page row %d=%v want=%s", i, row["id"], want)
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := scoped.Count(); n != 601 {
		t.Fatalf("committed count=%d want=601", n)
	}
	got, err := scoped.Scan(1, 0)
	if err != nil || len(got) != 1 || got[0]["id"] != "pending" {
		t.Fatalf("committed first page=%v err=%v", got, err)
	}
}
