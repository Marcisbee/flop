package flop

import (
	"fmt"
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
