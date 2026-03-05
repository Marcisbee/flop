package engine

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

func TestConcurrentInsertsPreserveAllRows(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	const workers = 8
	const perWorker = 120

	start := make(chan struct{})
	errCh := make(chan error, workers*perWorker)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				id := fmt.Sprintf("id-%02d-%04d", worker, i)
				slug := fmt.Sprintf("slug-%02d-%04d", worker, i)
				_, err := ti.Insert(map[string]interface{}{
					"id":    id,
					"slug":  slug,
					"title": fmt.Sprintf("Movie %s", id),
					"genre": "action",
				}, nil)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent insert error: %v", err)
	}

	want := workers * perWorker
	if got := ti.Count(); got != want {
		t.Fatalf("unexpected row count: got %d want %d", got, want)
	}
}

func TestConcurrentInsertHonorsUniqueConstraint(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	const contenders = 48
	start := make(chan struct{})
	var success atomic.Int32
	var duplicate atomic.Int32
	errCh := make(chan error, contenders)
	var wg sync.WaitGroup

	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := ti.Insert(map[string]interface{}{
				"id":    fmt.Sprintf("id-race-%03d", i),
				"slug":  "slug-race-shared",
				"title": fmt.Sprintf("Race Title %03d", i),
				"genre": "drama",
			}, nil)
			if err == nil {
				success.Add(1)
				return
			}
			if strings.Contains(err.Error(), "duplicate unique constraint") {
				duplicate.Add(1)
				return
			}
			errCh <- err
		}(i)
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected insert error: %v", err)
	}

	if got := success.Load(); got != 1 {
		t.Fatalf("expected exactly one success, got %d", got)
	}
	if got := duplicate.Load(); got != contenders-1 {
		t.Fatalf("expected %d duplicate errors, got %d", contenders-1, got)
	}
	if got := ti.Count(); got != 1 {
		t.Fatalf("unexpected row count: got %d want 1", got)
	}
}

func TestScanHidesPendingBufferedInsertUntilCommit(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	txBuf := make(map[string]*walBufEntry)
	if _, err := ti.Insert(map[string]interface{}{
		"id":    "id-pending-1",
		"slug":  "slug-pending-1",
		"title": "Pending Insert",
		"genre": "drama",
	}, txBuf); err != nil {
		t.Fatalf("buffered insert: %v", err)
	}

	rows, err := ti.Scan(10, 0)
	if err != nil {
		t.Fatalf("scan while pending: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected pending row hidden from scan, got %d rows", len(rows))
	}

	if err := db.EnqueueCommit(txBuf); err != nil {
		t.Fatalf("commit tx buffer: %v", err)
	}

	rows, err = ti.Scan(10, 0)
	if err != nil {
		t.Fatalf("scan after commit: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected committed row visible, got %d rows", len(rows))
	}
	if got := toString(rows[0]["id"]); got != "id-pending-1" {
		t.Fatalf("unexpected row id after commit: %q", got)
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
