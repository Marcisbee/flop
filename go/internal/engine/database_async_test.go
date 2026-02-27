package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/schema"
)

func testMovieTableDef(withIndexes bool) *schema.TableDef {
	fields := []schema.CompiledField{
		{Name: "id", Kind: schema.KindString, Required: true},
		{Name: "slug", Kind: schema.KindString, Required: true},
		{Name: "title", Kind: schema.KindString, Required: true},
		{Name: "genre", Kind: schema.KindString},
	}
	indexes := []schema.IndexDef{}
	if withIndexes {
		indexes = append(indexes,
			schema.IndexDef{Fields: []string{"slug"}, Unique: true, Type: schema.IndexTypeHash},
			schema.IndexDef{Fields: []string{"genre"}, Unique: false, Type: schema.IndexTypeHash},
			schema.IndexDef{Fields: []string{"title"}, Unique: false, Type: schema.IndexTypeFullText},
		)
	}
	return &schema.TableDef{
		Name:           "movies",
		CompiledSchema: schema.NewCompiledSchema(fields),
		Indexes:        indexes,
	}
}

func openTestDB(t *testing.T, dataDir string, async, withIndexes bool) *Database {
	t.Helper()
	db := NewDatabase(DatabaseConfig{
		DataDir:               dataDir,
		SyncMode:              "normal",
		AsyncSecondaryIndexes: async,
	})
	if err := db.Open(map[string]*schema.TableDef{"movies": testMovieTableDef(withIndexes)}); err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func mustTable(t *testing.T, db *Database) *TableInstance {
	t.Helper()
	ti := db.GetTable("movies")
	if ti == nil {
		t.Fatal("movies table missing")
	}
	return ti
}

func seedMovies(t *testing.T, ti *TableInstance, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		genre := "action"
		if i%2 == 1 {
			genre = "drama"
		}
		title := fmt.Sprintf("Movie %06d Galactic Saga", i)
		if i%250 == 0 {
			title = fmt.Sprintf("Sherlock Holmes: A Game of Shadows %d", i)
		}
		_, err := ti.Insert(map[string]interface{}{
			"id":    fmt.Sprintf("id-%06d", i),
			"slug":  fmt.Sprintf("slug-%06d", i),
			"title": title,
			"genre": genre,
		}, nil)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
}

func waitForIndexesReady(t *testing.T, ti *TableInstance, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ti.secondaryIndexesReady() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("secondary indexes were not ready within %s", timeout)
}

func TestSecondaryIndexFallbackFindAndSearch(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)
	seedMovies(t, ti, 400)

	ti.setIndexesReady(false)

	ptr, ok := ti.FindByIndex([]string{"slug"}, "slug-000123")
	if !ok {
		t.Fatal("expected pointer from fallback unique index lookup")
	}
	row, err := ti.GetByPointer(ptr)
	if err != nil {
		t.Fatalf("get by pointer: %v", err)
	}
	if got := toString(row["id"]); got != "id-000123" {
		t.Fatalf("unexpected id: got %q", got)
	}

	ptrs := ti.FindAllByIndex([]string{"genre"}, "action")
	if len(ptrs) != 200 {
		t.Fatalf("unexpected action pointer count: got %d want %d", len(ptrs), 200)
	}

	rows, err := ti.SearchFullText([]string{"title"}, "the galactic saga", 10)
	if err != nil {
		t.Fatalf("full-text fallback search: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected full-text fallback matches")
	}
}

func TestSecondaryIndexFallbackUniqueConstraints(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)
	seedMovies(t, ti, 6)
	ti.setIndexesReady(false)

	_, err := ti.Insert(map[string]interface{}{
		"id":    "id-999999",
		"slug":  "slug-000001",
		"title": "Duplicate Slug Insert",
		"genre": "drama",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate unique constraint") {
		t.Fatalf("expected duplicate unique constraint on insert, got: %v", err)
	}

	_, err = ti.Update("id-000004", map[string]interface{}{
		"slug": "slug-000001",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate unique constraint") {
		t.Fatalf("expected duplicate unique constraint on update, got: %v", err)
	}
}

func TestOpenAsyncSecondaryIndexesEventuallyReady(t *testing.T) {
	dataDir := t.TempDir()

	seedDB := openTestDB(t, dataDir, false, false)
	seedTI := mustTable(t, seedDB)
	seedMovies(t, seedTI, 12000)
	if err := seedDB.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	asyncDB := openTestDB(t, dataDir, true, true)
	t.Cleanup(func() { _ = asyncDB.Close() })
	ti := mustTable(t, asyncDB)

	if ti.indexBuildDone == nil {
		t.Fatal("expected async index builder channel to be initialized")
	}

	ptr, ok := ti.FindByIndex([]string{"slug"}, "slug-000999")
	if !ok {
		t.Fatal("expected immediate lookup result during async index build")
	}
	row, err := ti.GetByPointer(ptr)
	if err != nil {
		t.Fatalf("read immediate lookup row: %v", err)
	}
	if got := toString(row["id"]); got != "id-000999" {
		t.Fatalf("unexpected row id: got %q", got)
	}

	rows, err := ti.SearchFullText([]string{"title"}, "game shadows", 3)
	if err != nil {
		t.Fatalf("immediate full-text search: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected immediate full-text matches while indexes warm")
	}

	waitForIndexesReady(t, ti, 8*time.Second)
}

func TestCloseWaitsForAsyncSecondaryIndexBuild(t *testing.T) {
	dataDir := t.TempDir()

	seedDB := openTestDB(t, dataDir, false, false)
	seedTI := mustTable(t, seedDB)
	seedMovies(t, seedTI, 14000)
	if err := seedDB.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	asyncDB := openTestDB(t, dataDir, true, true)
	if err := asyncDB.Close(); err != nil {
		t.Fatalf("close async db: %v", err)
	}

	verifyDB := openTestDB(t, dataDir, false, true)
	defer func() { _ = verifyDB.Close() }()
	verifyTI := mustTable(t, verifyDB)
	ptr, ok := verifyTI.FindByIndex([]string{"slug"}, "slug-000100")
	if !ok {
		t.Fatal("expected lookup result after close/reopen")
	}
	row, err := verifyTI.GetByPointer(ptr)
	if err != nil {
		t.Fatalf("verify get by pointer: %v", err)
	}
	if got := toString(row["id"]); got != "id-000100" {
		t.Fatalf("unexpected verify id: got %q", got)
	}
}
