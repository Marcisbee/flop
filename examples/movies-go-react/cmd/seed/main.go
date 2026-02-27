package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/marcisbee/flop"
	"github.com/marcisbee/flop/examples/movies-go-react/app"
)

const (
	imdbBasicsURL  = "https://datasets.imdbws.com/title.basics.tsv.gz"
	imdbRatingsURL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
)

type ratingInfo struct {
	avg   float64
	votes int
}

func main() {
	var (
		force      bool
		limit      int
		download   bool
		timeoutSec int
		batchSize  int
	)
	flag.BoolVar(&force, "force", false, "remove existing data directory before import")
	flag.IntVar(&limit, "limit", 0, "optional max number of movie rows to import (0 = all)")
	flag.BoolVar(&download, "download", true, "download IMDb files before import")
	flag.IntVar(&timeoutSec, "timeout", 900, "http timeout in seconds for dataset downloads")
	flag.IntVar(&batchSize, "batch", 2000, "number of rows per buffered write flush")
	flag.Parse()

	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatalf("seed: find module root: %v", err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	datasetsDir := filepath.Join(dataDir, "_datasets", "imdb")
	basicsPath := filepath.Join(datasetsDir, "title.basics.tsv.gz")
	ratingsPath := filepath.Join(datasetsDir, "title.ratings.tsv.gz")

	if force {
		log.Printf("seed: resetting data dir %s (preserving downloaded datasets)", dataDir)
		if err := resetDataDirPreserveIMDb(dataDir, datasetsDir); err != nil {
			log.Fatalf("seed: reset data dir: %v", err)
		}
	}

	if err := os.MkdirAll(datasetsDir, 0o755); err != nil {
		log.Fatalf("seed: create datasets dir: %v", err)
	}

	client := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	if download {
		if err := downloadFile(client, imdbBasicsURL, basicsPath); err != nil {
			log.Fatalf("seed: download basics: %v", err)
		}
		if err := downloadFile(client, imdbRatingsURL, ratingsPath); err != nil {
			log.Fatalf("seed: download ratings: %v", err)
		}
	}

	application := app.BuildWithDataDir(dataDir)
	db, err := application.Open()
	if err != nil {
		log.Fatalf("seed: open database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	movies := db.Table("movies")
	if movies == nil {
		log.Fatal("seed: movies table not found")
	}

	if !force && movies.Count() > 0 {
		log.Fatalf("seed: movies table already has %d rows (use -force to reset)", movies.Count())
	}

	log.Printf("seed: loading ratings from %s", ratingsPath)
	ratings, err := parseRatings(ratingsPath)
	if err != nil {
		log.Fatalf("seed: parse ratings: %v", err)
	}
	log.Printf("seed: loaded %d rating rows", len(ratings))

	log.Printf("seed: importing movies from %s", basicsPath)
	imported, err := importMovies(movies, basicsPath, ratings, limit, batchSize)
	if err != nil {
		log.Fatalf("seed: import movies: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		log.Fatalf("seed: checkpoint: %v", err)
	}

	log.Printf("seed: done. imported=%d total=%d", imported, movies.Count())
}

func downloadFile(client *http.Client, sourceURL, targetPath string) error {
	if info, err := os.Stat(targetPath); err == nil && info.Size() > 0 {
		log.Printf("seed: using existing file %s", targetPath)
		return nil
	}

	log.Printf("seed: downloading %s", sourceURL)
	resp, err := client.Get(sourceURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	tmp := targetPath + ".partial"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err := os.Rename(tmp, targetPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func parseRatings(path string) (map[string]ratingInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("ratings file is empty")
	}
	header := strings.Split(strings.TrimRight(scanner.Text(), "\r"), "\t")

	index := fieldIndexMap(header)
	iTconst := index["tconst"]
	iAvg := index["averageRating"]
	iVotes := index["numVotes"]
	if iTconst < 0 || iAvg < 0 || iVotes < 0 {
		return nil, fmt.Errorf("ratings header missing required fields")
	}

	out := make(map[string]ratingInfo, 1_000_000)
	for scanner.Scan() {
		record := strings.Split(strings.TrimRight(scanner.Text(), "\r"), "\t")

		if iTconst >= len(record) {
			continue
		}
		tconst := record[iTconst]
		if tconst == "" || tconst == "\\N" {
			continue
		}

		avg := 0.0
		if iAvg < len(record) && record[iAvg] != "" && record[iAvg] != "\\N" {
			avg, _ = strconv.ParseFloat(record[iAvg], 64)
		}

		votes := 0
		if iVotes < len(record) && record[iVotes] != "" && record[iVotes] != "\\N" {
			votes, _ = strconv.Atoi(record[iVotes])
		}

		out[tconst] = ratingInfo{avg: avg, votes: votes}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func importMovies(table *flop.TableInstance, basicsPath string, ratings map[string]ratingInfo, limit int, batchSize int) (int, error) {
	file, err := os.Open(basicsPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("basics file is empty")
	}
	header := strings.Split(strings.TrimRight(scanner.Text(), "\r"), "\t")

	index := fieldIndexMap(header)
	iTconst := index["tconst"]
	iType := index["titleType"]
	iPrimaryTitle := index["primaryTitle"]
	iStartYear := index["startYear"]
	iRuntime := index["runtimeMinutes"]
	iGenres := index["genres"]
	if iTconst < 0 || iType < 0 || iPrimaryTitle < 0 || iStartYear < 0 {
		return 0, fmt.Errorf("basics header missing required fields")
	}

	imported := 0
	seen := 0
	started := time.Now()
	nextLog := 5000
	if batchSize <= 0 {
		batchSize = 2000
	}
	batch := make([]map[string]any, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := table.InsertMany(batch, len(batch))
		imported += n
		batch = batch[:0]
		if err != nil {
			return err
		}
		for imported >= nextLog {
			elapsed := time.Since(started)
			log.Printf("seed: imported %d movies (seen %d rows, elapsed %s)", nextLog, seen, elapsed.Round(time.Second))
			nextLog += 5000
		}
		return nil
	}

	for scanner.Scan() {
		record := strings.Split(strings.TrimRight(scanner.Text(), "\r"), "\t")
		seen++

		if iType >= len(record) || record[iType] != "movie" {
			continue
		}
		if iStartYear >= len(record) || record[iStartYear] == "" || record[iStartYear] == "\\N" {
			continue
		}

		year, err := strconv.Atoi(record[iStartYear])
		if err != nil {
			continue
		}
		title := strings.TrimSpace(record[iPrimaryTitle])
		if title == "" || title == "\\N" {
			continue
		}
		tconst := record[iTconst]
		if tconst == "" || tconst == "\\N" {
			continue
		}

		row := map[string]any{
			"id":    strings.ToLower(tconst),
			"slug":  slugify(title) + "-" + strings.ToLower(tconst),
			"title": title,
			"year":  year,
		}

		if iRuntime < len(record) && record[iRuntime] != "" && record[iRuntime] != "\\N" {
			if runtime, err := strconv.Atoi(record[iRuntime]); err == nil && runtime > 0 {
				row["runtimeMinutes"] = runtime
			}
		}

		if iGenres < len(record) && record[iGenres] != "" && record[iGenres] != "\\N" {
			parts := strings.Split(record[iGenres], ",")
			genres := make([]any, 0, len(parts))
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" || part == "\\N" {
					continue
				}
				genres = append(genres, part)
			}
			if len(genres) > 0 {
				row["genres"] = genres
			}
		}

		if rating, ok := ratings[tconst]; ok {
			if rating.avg > 0 {
				row["rating"] = rating.avg
			}
			if rating.votes > 0 {
				row["votes"] = rating.votes
			}
		}

		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return imported, err
			}
		}
		if limit > 0 && imported+len(batch) >= limit {
			if limit-imported < len(batch) {
				batch = batch[:limit-imported]
			}
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return imported, err
	}

	if err := flushBatch(); err != nil {
		return imported, err
	}

	return imported, nil
}

func fieldIndexMap(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, field := range header {
		index[field] = i
	}
	return index
}

var nonSlugChars = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(in string) string {
	s := strings.ToLower(strings.TrimSpace(in))
	s = nonSlugChars.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		return "movie"
	}
	return s
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			return "", fmt.Errorf("go.mod not found from %s", dir)
		}
		dir = next
	}
}

func resetDataDirPreserveIMDb(dataDir, datasetsDir string) error {
	backup := ""
	if info, err := os.Stat(datasetsDir); err == nil && info.IsDir() {
		backup = filepath.Join(os.TempDir(), fmt.Sprintf("flop-imdb-cache-%d", time.Now().UnixNano()))
		if err := os.MkdirAll(filepath.Dir(backup), 0o755); err != nil {
			return err
		}
		if err := os.Rename(datasetsDir, backup); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	if backup != "" {
		if err := os.MkdirAll(filepath.Dir(datasetsDir), 0o755); err != nil {
			return err
		}
		if err := os.Rename(backup, datasetsDir); err != nil {
			return err
		}
	}
	return nil
}
