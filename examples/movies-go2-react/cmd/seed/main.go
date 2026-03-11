package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	flop "github.com/marcisbee/flop/go2"
	"github.com/marcisbee/flop/examples/movies-go2-react/app"
)

func main() {
	force := flag.Bool("force", false, "remove existing data before import")
	limit := flag.Int("limit", 0, "import only first N movies (0 = all)")
	download := flag.Bool("download", true, "download IMDb files")
	batch := flag.Int("batch", 2000, "rows per buffered write")
	flag.Parse()

	projectRoot, err := findModuleRoot()
	if err != nil {
		log.Fatal(err)
	}

	dataDir := filepath.Join(projectRoot, "data")
	datasetsDir := filepath.Join(dataDir, "_datasets", "imdb")

	if *force {
		// Remove data files but keep datasets
		entries, _ := os.ReadDir(dataDir)
		for _, e := range entries {
			if e.Name() == "_datasets" {
				continue
			}
			os.RemoveAll(filepath.Join(dataDir, e.Name()))
		}
	}

	basicsPath := filepath.Join(datasetsDir, "title.basics.tsv.gz")
	ratingsPath := filepath.Join(datasetsDir, "title.ratings.tsv.gz")

	if *download {
		downloadFile("https://datasets.imdbws.com/title.basics.tsv.gz", basicsPath)
		downloadFile("https://datasets.imdbws.com/title.ratings.tsv.gz", ratingsPath)
	}

	// Open database
	db, err := flop.OpenDB(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.CreateTable(app.MoviesSchema); err != nil {
		log.Fatal(err)
	}

	// Load ratings into memory
	log.Println("Loading ratings...")
	ratings := loadRatings(ratingsPath)
	log.Printf("Loaded %d ratings", len(ratings))

	// Import movies
	log.Println("Importing movies...")
	imported := importMovies(db, basicsPath, ratings, *limit, *batch)
	log.Printf("Imported %d movies", imported)

	log.Println("Flushing to disk...")
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}
	log.Println("Done!")
}

type rating struct {
	avg   float64
	votes int64
}

func loadRatings(path string) map[string]rating {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	ratings := make(map[string]rating, 1_500_000)
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // skip header

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 3 {
			continue
		}
		avg, _ := strconv.ParseFloat(fields[1], 64)
		votes, _ := strconv.ParseInt(fields[2], 10, 64)
		ratings[fields[0]] = rating{avg: avg, votes: votes}
	}
	return ratings
}

func importMovies(db *flop.DB, basicsPath string, ratings map[string]rating, limit, batch int) int {
	f, err := os.Open(basicsPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // skip header

	count := 0
	batchCount := 0
	start := time.Now()

	for scanner.Scan() {
		if limit > 0 && count >= limit {
			break
		}

		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 9 {
			continue
		}

		tconst := fields[0]
		titleType := fields[1]
		title := fields[2]
		yearStr := fields[5]
		runtimeStr := fields[7]
		genresStr := fields[8]

		// Only movies
		if titleType != "movie" {
			continue
		}
		if title == "" || title == "\\N" {
			continue
		}
		year, err := strconv.ParseInt(yearStr, 10, 64)
		if err != nil || year == 0 {
			continue
		}

		slug := slugify(title) + "-" + tconst

		data := map[string]any{
			"slug":   slug,
			"title":  title,
			"year":   year,
			"genres": app.ParseGenres(genresStr),
		}

		if runtimeStr != "\\N" {
			if rt, err := strconv.ParseInt(runtimeStr, 10, 64); err == nil {
				data["runtime_minutes"] = rt
			}
		}

		if r, ok := ratings[tconst]; ok {
			data["rating"] = r.avg
			data["votes"] = r.votes
		}

		if _, err := db.Insert("movies", data); err != nil {
			// Skip duplicates
			if strings.Contains(err.Error(), "unique constraint") {
				continue
			}
			log.Printf("Insert error: %v", err)
			continue
		}

		count++
		batchCount++

		if batchCount >= batch {
			if err := db.Flush(); err != nil {
				log.Printf("Flush error: %v", err)
			}
			batchCount = 0
			elapsed := time.Since(start)
			rate := float64(count) / elapsed.Seconds()
			log.Printf("  %d movies (%.0f/sec)", count, rate)
		}
	}

	return count
}

func slugify(s string) string {
	var b strings.Builder
	prev := false
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			prev = false
		} else if !prev && b.Len() > 0 {
			b.WriteByte('-')
			prev = true
		}
	}
	result := b.String()
	if len(result) > 0 && result[len(result)-1] == '-' {
		result = result[:len(result)-1]
	}
	if len(result) > 80 {
		result = result[:80]
	}
	return result
}

func downloadFile(url, dest string) {
	if _, err := os.Stat(dest); err == nil {
		log.Printf("Using cached %s", filepath.Base(dest))
		return
	}

	os.MkdirAll(filepath.Dir(dest), 0755)
	log.Printf("Downloading %s...", url)

	client := &http.Client{Timeout: 15 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	f, err := os.Create(dest)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Downloaded %s (%d MB)", filepath.Base(dest), n/1024/1024)
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
			return "", os.ErrNotExist
		}
		dir = next
	}
}
