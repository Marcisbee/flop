package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcisbee/flop/internal/reqtrace"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
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

func TestForceRebuildSecondaryIndexesPersistsMultiIndexFiles(t *testing.T) {
	dataDir := t.TempDir()
	db := openTestDB(t, dataDir, false, true)
	ti := mustTable(t, db)
	seedMovies(t, ti, 20)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	_ = db.Close()

	stale := storage.NewMultiIndex()
	stale.Add("action", schema.RowPointer{PageNumber: 1, SlotIndex: 0})
	indexPath := secondaryIndexDiskPath(dataDir, "movies", "genre", true)
	if err := storage.WriteMappedMultiIndexFile(indexPath, stale); err != nil {
		t.Fatalf("write stale index: %v", err)
	}

	db = openTestDB(t, dataDir, false, true)
	defer db.Close()
	ti = mustTable(t, db)

	before, err := storage.ReadMappedMultiIndexFile(indexPath)
	if err != nil {
		t.Fatalf("read stale index: %v", err)
	}
	if got := len(before.GetAll("drama")); got != 0 {
		t.Fatalf("expected stale on-disk drama postings to be empty, got %d", got)
	}

	if err := ti.ForceRebuildSecondaryIndexes(); err != nil {
		t.Fatalf("force rebuild secondary indexes: %v", err)
	}

	after, err := storage.ReadMappedMultiIndexFile(indexPath)
	if err != nil {
		t.Fatalf("read rebuilt index: %v", err)
	}
	if got := len(after.GetAll("drama")); got == 0 {
		t.Fatalf("expected rebuilt index file to contain drama postings")
	}

	if _, err := os.Stat(filepath.Join(dataDir, "movies.67656e7265.smidx")); err != nil {
		t.Fatalf("expected persisted mapped secondary index file: %v", err)
	}
}

func TestFreshMultiIndexEmptyLookupSkipsFallbackScan(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(DatabaseConfig{DataDir: dir, AsyncSecondaryIndexes: false})
	def := testMovieTableDef(true)
	if err := db.Open(map[string]*schema.TableDef{"movies": def}); err != nil {
		t.Fatalf("open db: %v", err)
	}
	ti := db.GetTable("movies")
	if ti == nil {
		t.Fatal("missing movies table")
	}
	if _, err := ti.Insert(map[string]interface{}{
		"id":    "1",
		"slug":  "movie-1",
		"title": "Movie One",
		"genre": "action",
	}, nil); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if err := ti.ForceRebuildSecondaryIndexes(); err != nil {
		t.Fatalf("force rebuild secondary indexes: %v", err)
	}

	tc := reqtrace.Start()
	rows := ti.FindAllByIndex([]string{"genre"}, "strategy")
	tc.End()
	if len(rows) != 0 {
		t.Fatalf("expected zero rows, got %d", len(rows))
	}
	spans := tc.Spans()
	if len(spans) == 0 {
		t.Fatal("expected trace spans")
	}
	last := spans[len(spans)-1]
	if note, _ := last["note"].(string); strings.Contains(note, "fallback scan") {
		t.Fatalf("expected no fallback scan, got note %q", note)
	}
	if note, _ := last["note"].(string); note != "multi-index empty" {
		t.Fatalf("expected multi-index empty note, got %q", note)
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

func TestUniqueInsertRepairsStaleHashIndexEntry(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)

	row, err := ti.Insert(map[string]interface{}{
		"id":    "id-1",
		"slug":  "real-slug",
		"title": "Movie One",
		"genre": "action",
	}, nil)
	if err != nil {
		t.Fatalf("insert first row: %v", err)
	}

	pointer, ok := ti.primaryIndex.Get(fmt.Sprintf("%v", row["id"]))
	if !ok {
		t.Fatal("expected primary pointer for seeded row")
	}

	indexDef := ti.def.Indexes[0]
	indexKey := secondaryIndexKey(indexDef)
	hi, ok := ti.secondaryIdxs[indexKey].(*storage.HashIndex)
	if !ok {
		t.Fatal("expected unique slug index to be a hash index")
	}

	hi.Set("ghost-slug", pointer)

	if _, err := ti.Insert(map[string]interface{}{
		"id":    "id-2",
		"slug":  "ghost-slug",
		"title": "Movie Two",
		"genre": "drama",
	}, nil); err != nil {
		t.Fatalf("expected stale unique key to self-heal on insert, got: %v", err)
	}

	ptr, ok := ti.FindByIndex([]string{"slug"}, "ghost-slug")
	if !ok {
		t.Fatal("expected inserted row to be indexed by repaired unique key")
	}
	got, err := ti.GetByPointer(ptr)
	if err != nil {
		t.Fatalf("get by pointer: %v", err)
	}
	if fmt.Sprintf("%v", got["id"]) != "id-2" {
		t.Fatalf("expected repaired row id-2, got %v", got["id"])
	}
}

func TestRepairIndexesRebuildsCorruptSecondaryIndexes(t *testing.T) {
	db := openTestDB(t, t.TempDir(), false, true)
	t.Cleanup(func() { _ = db.Close() })
	ti := mustTable(t, db)
	seedMovies(t, ti, 4)

	row, err := ti.Get("id-000000")
	if err != nil || row == nil {
		t.Fatalf("get seeded row: %v row=%v", err, row)
	}
	ptr, ok := ti.primaryIndex.Get("id-000000")
	if !ok {
		t.Fatal("expected pointer for seeded row")
	}

	slugIndexKey := secondaryIndexKey(ti.def.Indexes[0])
	slugIdx := ti.secondaryIdxs[slugIndexKey].(*storage.HashIndex)
	slugIdx.Delete("slug-000000")
	slugIdx.Set("ghost-slug", ptr)

	genreIndexKey := secondaryIndexKey(ti.def.Indexes[1])
	genreIdx := ti.secondaryIdxs[genreIndexKey].(*storage.MultiIndex)
	genreIdx.Delete("action", ptr)
	genreIdx.Add("ghost-genre", ptr)

	if err := ti.RepairIndexesIfNeeded(); err != nil {
		t.Fatalf("repair indexes: %v", err)
	}

	ptr, ok = ti.FindByIndex([]string{"slug"}, "slug-000000")
	if !ok {
		t.Fatal("expected slug index to be repaired")
	}
	got, err := ti.GetByPointer(ptr)
	if err != nil {
		t.Fatalf("get repaired slug row: %v", err)
	}
	if fmt.Sprintf("%v", got["id"]) != "id-000000" {
		t.Fatalf("expected repaired slug row id-000000, got %v", got["id"])
	}
	if _, ok := ti.FindByIndex([]string{"slug"}, "ghost-slug"); ok {
		t.Fatal("expected ghost slug entry to be removed")
	}

	actionPtrs := ti.FindAllByIndex([]string{"genre"}, "action")
	if len(actionPtrs) != 2 {
		t.Fatalf("expected repaired action postings, got %d", len(actionPtrs))
	}
	if ghostPtrs := ti.FindAllByIndex([]string{"genre"}, "ghost-genre"); len(ghostPtrs) != 0 {
		t.Fatalf("expected ghost genre postings to be removed, got %d", len(ghostPtrs))
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
