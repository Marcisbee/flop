package app

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

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
		DataDir:               dataDir,
		SyncMode:              "normal",
		AsyncSecondaryIndexes: true,
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
