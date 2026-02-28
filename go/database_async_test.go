package flop

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func buildMovieApp(dataDir string, async bool) *App {
	app := New(Config{
		DataDir:               dataDir,
		SyncMode:              "normal",
		AsyncSecondaryIndexes: async,
	})
	Define(app, "movies", func(s *SchemaBuilder) {
		s.String("id").Primary().Required()
		s.String("slug").Required().Unique()
		s.String("title").Required().FullText()
		s.String("genre").Index()
	})
	return app
}

func buildMovieRows(count int) []map[string]any {
	rows := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		genre := "action"
		if i%2 == 1 {
			genre = "drama"
		}
		rows = append(rows, map[string]any{
			"id":    fmt.Sprintf("id-%06d", i),
			"slug":  fmt.Sprintf("slug-%06d", i),
			"title": fmt.Sprintf("Movie %06d Galactic Saga", i),
			"genre": genre,
		})
	}
	return rows
}

func TestDatabaseAsyncIndexesImmediateSearchAndLookup(t *testing.T) {
	dataDir := t.TempDir()
	rows := buildMovieRows(5000)

	seedApp := buildMovieApp(dataDir, false)
	seedDB, err := seedApp.Open()
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	seedTable := seedDB.Table("movies")
	if seedTable == nil {
		t.Fatal("movies table missing")
	}
	inserted, err := seedTable.InsertMany(rows, 1000)
	if err != nil {
		t.Fatalf("seed insert many: %v", err)
	}
	if inserted != len(rows) {
		t.Fatalf("inserted rows mismatch: got %d want %d", inserted, len(rows))
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	asyncApp := buildMovieApp(dataDir, true)
	asyncDB, err := asyncApp.Open()
	if err != nil {
		t.Fatalf("async open: %v", err)
	}
	defer func() { _ = asyncDB.Close() }()

	table := asyncDB.Table("movies")
	if table == nil {
		t.Fatal("movies table missing (async)")
	}

	row, ok := table.FindByUniqueIndex("slug", "slug-004321")
	if !ok {
		t.Fatal("expected immediate unique index lookup result")
	}
	if got := fmt.Sprintf("%v", row["id"]); got != "id-004321" {
		t.Fatalf("unexpected lookup id: got %q", got)
	}

	found, err := table.SearchFullText([]string{"title"}, "galactic saga", 8)
	if err != nil {
		t.Fatalf("immediate full-text search: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("expected full-text results")
	}
}

func TestDatabaseAsyncIndexesKeepUniqueConstraints(t *testing.T) {
	app := buildMovieApp(t.TempDir(), true)
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	table := db.Table("movies")
	if table == nil {
		t.Fatal("movies table missing")
	}

	_, err = table.Insert(map[string]any{
		"id":    "id-1",
		"slug":  "same-slug",
		"title": "Movie One",
		"genre": "action",
	})
	if err != nil {
		t.Fatalf("insert first row: %v", err)
	}
	_, err = table.Insert(map[string]any{
		"id":    "id-2",
		"slug":  "same-slug",
		"title": "Movie Two",
		"genre": "drama",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate unique constraint") {
		t.Fatalf("expected duplicate unique constraint, got: %v", err)
	}
}

func TestPrimaryStrategyAutogen(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "ids", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("kind").Required().Unique()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	table := db.Table("ids")
	if table == nil {
		t.Fatal("ids table missing")
	}
	row, err := table.Insert(map[string]any{"kind": "uuid"})
	if err != nil {
		t.Fatalf("insert uuid row: %v", err)
	}
	uuid := fmt.Sprintf("%v", row["id"])
	uuidV7 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidV7.MatchString(uuid) {
		t.Fatalf("expected uuidv7 format, got %q", uuid)
	}

	app2 := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app2, "ids", func(s *SchemaBuilder) {
		s.String("id").Primary("ulid")
		s.String("kind").Required().Unique()
	})
	db2, err := app2.Open()
	if err != nil {
		t.Fatalf("open ulid db: %v", err)
	}
	defer func() { _ = db2.Close() }()
	row2, err := db2.Table("ids").Insert(map[string]any{"kind": "ulid"})
	if err != nil {
		t.Fatalf("insert ulid row: %v", err)
	}
	ulid := fmt.Sprintf("%v", row2["id"])
	ulidRe := regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	if !ulidRe.MatchString(ulid) {
		t.Fatalf("expected ulid format, got %q", ulid)
	}

	app3 := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app3, "ids", func(s *SchemaBuilder) {
		s.String("id").Primary("nanoid")
		s.String("kind").Required().Unique()
	})
	db3, err := app3.Open()
	if err != nil {
		t.Fatalf("open nanoid db: %v", err)
	}
	defer func() { _ = db3.Close() }()
	row3, err := db3.Table("ids").Insert(map[string]any{"kind": "nanoid"})
	if err != nil {
		t.Fatalf("insert nanoid row: %v", err)
	}
	nanoid := fmt.Sprintf("%v", row3["id"])
	nanoidRe := regexp.MustCompile(`^[0-9A-Za-z_-]{21}$`)
	if !nanoidRe.MatchString(nanoid) {
		t.Fatalf("expected nanoid format, got %q", nanoid)
	}
}

func TestPrimaryStrategyAutoIncrement(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})
	Define(app, "events", func(s *SchemaBuilder) {
		s.Number("id").Primary("autoincrement")
		s.String("kind").Required().Unique()
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	table := db.Table("events")
	if table == nil {
		t.Fatal("events table missing")
	}
	r1, err := table.Insert(map[string]any{"kind": "a"})
	if err != nil {
		t.Fatalf("insert #1: %v", err)
	}
	r2, err := table.Insert(map[string]any{"kind": "b"})
	if err != nil {
		t.Fatalf("insert #2: %v", err)
	}
	if got := fmt.Sprintf("%v", r1["id"]); got != "1" {
		t.Fatalf("expected id=1, got %q", got)
	}
	if got := fmt.Sprintf("%v", r2["id"]); got != "2" {
		t.Fatalf("expected id=2, got %q", got)
	}
	rExplicit, err := table.Insert(map[string]any{"id": float64(10), "kind": "manual"})
	if err != nil {
		t.Fatalf("insert manual id: %v", err)
	}
	if got := fmt.Sprintf("%v", rExplicit["id"]); got != "10" {
		t.Fatalf("expected manual id=10, got %q", got)
	}
	r4, err := table.Insert(map[string]any{"kind": "d"})
	if err != nil {
		t.Fatalf("insert after manual id: %v", err)
	}
	if got := fmt.Sprintf("%v", r4["id"]); got != "11" {
		t.Fatalf("expected id=11 after manual id, got %q", got)
	}
	_ = db.Close()

	db2, err := app.Open()
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	r3, err := db2.Table("events").Insert(map[string]any{"kind": "c"})
	if err != nil {
		t.Fatalf("insert after reopen: %v", err)
	}
	if got := fmt.Sprintf("%v", r3["id"]); got != "12" {
		t.Fatalf("expected id=12 after reopen, got %q", got)
	}
}
