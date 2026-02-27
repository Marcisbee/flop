package app

import (
	"fmt"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcisbee/flop"
)

type HeadMeta struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type HeadPayload struct {
	Title string     `json:"title"`
	Meta  []HeadMeta `json:"meta,omitempty"`
}

type MovieIndexEntry struct {
	ID    string `json:"id"`
	Slug  string `json:"slug"`
	Title string `json:"title"`
	Year  int    `json:"year"`
}

// Build creates the Flop app with schema definitions.
func Build() *flop.App {
	return BuildWithDataDir("./data")
}

// BuildWithDataDir creates the Flop app with an explicit data directory.
func BuildWithDataDir(dataDir string) *flop.App {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "./data"
	}

	application := flop.New(flop.Config{
		DataDir:  dataDir,
		SyncMode: "normal",
	})

	flop.Define(application, "movies", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Autogen(`[a-z0-9]{12}`)
		s.String("slug").Required().Unique()
		s.String("title").Required()
		s.Integer("year").Required()
		s.Integer("runtimeMinutes")
		s.Number("rating")
		s.Integer("votes")
		s.Set("genres")
		s.String("overview")
		s.Timestamp("createdAt").DefaultNow()
	})

	return application
}

// Seed inserts starter movies if the table is empty.
func Seed(db *flop.Database) {
	movies := db.Table("movies")
	if movies == nil || movies.Count() > 0 {
		return
	}

	now := time.Now().UnixMilli()
	seedRows := []map[string]any{
		{"slug": "the-godfather", "title": "The Godfather", "year": 1972, "runtimeMinutes": 175, "rating": 9.2, "votes": 2050000, "genres": []any{"Crime", "Drama"}, "overview": "The aging patriarch of a crime dynasty transfers control to his reluctant son.", "createdAt": float64(now - 900000)},
		{"slug": "spirited-away", "title": "Spirited Away", "year": 2001, "runtimeMinutes": 125, "rating": 8.6, "votes": 870000, "genres": []any{"Animation", "Fantasy"}, "overview": "A young girl enters a spirit world and must save her parents.", "createdAt": float64(now - 800000)},
		{"slug": "parasite", "title": "Parasite", "year": 2019, "runtimeMinutes": 132, "rating": 8.5, "votes": 980000, "genres": []any{"Thriller", "Drama"}, "overview": "Class tensions explode when two families become entangled.", "createdAt": float64(now - 700000)},
		{"slug": "arrival", "title": "Arrival", "year": 2016, "runtimeMinutes": 116, "rating": 7.9, "votes": 780000, "genres": []any{"Sci-Fi", "Drama"}, "overview": "A linguist races to decode an alien language before conflict starts.", "createdAt": float64(now - 600000)},
		{"slug": "mad-max-fury-road", "title": "Mad Max: Fury Road", "year": 2015, "runtimeMinutes": 120, "rating": 8.1, "votes": 1100000, "genres": []any{"Action", "Adventure"}, "overview": "In a wasteland, rebels stage a relentless convoy escape.", "createdAt": float64(now - 500000)},
		{"slug": "blade-runner-2049", "title": "Blade Runner 2049", "year": 2017, "runtimeMinutes": 164, "rating": 8.0, "votes": 710000, "genres": []any{"Sci-Fi", "Neo-Noir"}, "overview": "A new blade runner uncovers a secret that could reshape society.", "createdAt": float64(now - 400000)},
	}

	for _, row := range seedRows {
		_, _ = movies.Insert(row)
	}
}

func AllMovieIndexEntries(db *flop.Database) ([]MovieIndexEntry, error) {
	movies := db.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}

	count := movies.Count()
	if count == 0 {
		return []MovieIndexEntry{}, nil
	}

	rows, err := movies.Scan(count, 0)
	if err != nil {
		return nil, err
	}

	entries := make([]MovieIndexEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, MovieIndexEntry{
			ID:    toString(row["id"]),
			Slug:  toString(row["slug"]),
			Title: toString(row["title"]),
			Year:  toInt(row["year"]),
		})
	}
	return entries, nil
}

func ListMovies(db *flop.Database, limit, offset int) ([]map[string]any, error) {
	movies := db.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	if limit <= 0 {
		limit = 24
	}
	rows, err := movies.Scan(limit, offset)
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool {
		yi, yj := toInt(rows[i]["year"]), toInt(rows[j]["year"])
		if yi == yj {
			return toString(rows[i]["title"]) < toString(rows[j]["title"])
		}
		return yi > yj
	})
	return rows, nil
}

func ImportGeneratedMovies(db *flop.Database, count int) ([]MovieIndexEntry, error) {
	movies := db.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	if count <= 0 {
		return []MovieIndexEntry{}, nil
	}

	start := movies.Count()
	imported := make([]MovieIndexEntry, 0, count)

	for i := 0; i < count; i++ {
		seq := start + i + 1
		title := generatedTitle(seq)
		year := 1950 + (seq % 76)
		genres := generatedGenres(seq)
		overview := generatedOverview(seq, title, genres)
		slug := slugify(title) + "-" + strconv.FormatInt(int64(seq), 36)
		runtime := 82 + (seq * 17 % 79)
		rating := float64(55+(seq*13)%45) / 10.0
		votes := 500 + (seq*seq)%1200000

		row, err := movies.Insert(map[string]any{
			"slug":           slug,
			"title":          title,
			"year":           year,
			"runtimeMinutes": runtime,
			"rating":         rating,
			"votes":          votes,
			"genres":         genres,
			"overview":       overview,
		})
		if err != nil {
			return imported, err
		}

		imported = append(imported, MovieIndexEntry{
			ID:    toString(row["id"]),
			Slug:  slug,
			Title: title,
			Year:  year,
		})
	}

	return imported, nil
}

func ResolveHead(db *flop.Database, path string) HeadPayload {
	switch {
	case path == "/":
		count := 0
		if movies := db.Table("movies"); movies != nil {
			count = movies.Count()
		}
		return HeadPayload{
			Title: "Flop Movies",
			Meta: []HeadMeta{
				{Name: "description", Content: fmt.Sprintf("Search %d movies with instant autocomplete", count)},
			},
		}
	case strings.HasPrefix(path, "/movie/"):
		raw := strings.TrimPrefix(path, "/movie/")
		slug, _ := url.PathUnescape(raw)
		movie := FindMovieBySlug(db, slug)
		if movie == nil {
			return HeadPayload{Title: "Movie Not Found", Meta: []HeadMeta{{Name: "description", Content: "Requested movie was not found"}}}
		}
		title := toString(movie["title"])
		year := toInt(movie["year"])
		overview := toString(movie["overview"])
		if overview == "" {
			overview = "Movie details"
		}
		return HeadPayload{
			Title: fmt.Sprintf("%s (%d)", title, year),
			Meta:  []HeadMeta{{Name: "description", Content: overview}},
		}
	default:
		return HeadPayload{Title: "Movie Not Found", Meta: []HeadMeta{{Name: "description", Content: "Page not found"}}}
	}
}

func FindMovieBySlug(db *flop.Database, slug string) map[string]any {
	movies := db.Table("movies")
	if movies == nil {
		return nil
	}
	row, ok := movies.FindByUniqueIndex("slug", slug)
	if !ok {
		return nil
	}
	return row
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

func generatedTitle(seq int) string {
	adjectives := []string{"Silent", "Electric", "Broken", "Golden", "Neon", "Hidden", "Wild", "Last", "Crimson", "Frozen", "Velvet", "Burning", "Lunar", "Feral", "Atomic", "Obsidian"}
	nouns := []string{"Empire", "Memory", "Signal", "Frontier", "Echo", "Labyrinth", "Protocol", "Voyage", "Reckoning", "District", "Orbit", "Harbor", "Machine", "Mirage", "Dynasty", "Ritual"}
	suffix := []string{"of Tomorrow", "at Midnight", "Beyond Time", "and Dust", "in Reverse", "Under Glass", "Without Mercy", "for the Crown", "on Fire", "of the North", "in Exile", "from Zero"}

	a := adjectives[seq%len(adjectives)]
	n := nouns[(seq/len(adjectives))%len(nouns)]
	x := suffix[(seq/11)%len(suffix)]

	if seq%5 == 0 {
		return fmt.Sprintf("%s %s", a, n)
	}
	return fmt.Sprintf("%s %s %s", a, n, x)
}

func generatedGenres(seq int) []any {
	pairs := [][]string{
		{"Drama", "Thriller"},
		{"Sci-Fi", "Adventure"},
		{"Crime", "Mystery"},
		{"Action", "Adventure"},
		{"Fantasy", "Drama"},
		{"Animation", "Family"},
		{"Horror", "Mystery"},
		{"Romance", "Drama"},
	}
	p := pairs[seq%len(pairs)]
	return []any{p[0], p[1]}
}

func generatedOverview(seq int, title string, genres []any) string {
	lead := []string{
		"A disgraced investigator",
		"An ambitious smuggler",
		"A brilliant archivist",
		"Two rival siblings",
		"A vanished astronaut",
		"A restless mayor",
		"An exhausted stunt pilot",
		"A fugitive composer",
	}
	stakes := []string{
		"must decode a conspiracy before sunrise",
		"gets trapped in a city-wide blackout",
		"uncovers a map to a forbidden archive",
		"is forced to choose between truth and loyalty",
		"returns with a warning nobody believes",
		"chases a signal broadcasting from the future",
		"protects a witness who remembers impossible events",
		"risks everything to stop a staged revolution",
	}

	g := "drama"
	if len(genres) > 0 {
		g = strings.ToLower(toString(genres[0]))
	}
	return fmt.Sprintf("%s in %s must act when %s. %q mixes %s energy with big-screen spectacle.", lead[seq%len(lead)], yearLabel(1950+(seq%76)), stakes[(seq/7)%len(stakes)], title, g)
}

func yearLabel(year int) string {
	decade := int(math.Floor(float64(year)/10.0) * 10)
	return fmt.Sprintf("the %ds", decade)
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toInt(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case float32:
		return int(val)
	default:
		return 0
	}
}
