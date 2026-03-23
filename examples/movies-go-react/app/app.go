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

type GetStatsIn struct{}

type ListMoviesIn struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type GetMovieBySlugIn struct {
	Slug string `json:"slug"`
}

type AutocompleteMoviesIn struct {
	Q     string `json:"q"`
	Limit int    `json:"limit"`
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
		s.String("id").Primary("uuidv7")
		s.String("slug").Required().Unique()
		s.String("title").Required().FullText()
		s.Integer("year").Required()
		s.Integer("runtimeMinutes")
		s.Number("rating")
		s.Integer("votes")
		s.Set("genres")
		s.String("overview")
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.View(application, "get_stats", flop.Public(), GetStatsView)
	flop.View(application, "list_movies", flop.Public(), ListMoviesView)
	flop.View(application, "get_movie_by_slug", flop.Public(), GetMovieBySlugView)
	flop.View(application, "autocomplete_movies", flop.Public(), AutocompleteMoviesView)

	return application
}

func GetStatsView(ctx *flop.ViewCtx, _ GetStatsIn) (map[string]any, error) {
	movies := ctx.DB.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	return map[string]any{
		"movies": movies.Count(),
	}, nil
}

func ListMoviesView(ctx *flop.ViewCtx, in ListMoviesIn) ([]map[string]any, error) {
	movies := ctx.DB.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 24
	}
	if limit > 200 {
		limit = 200
	}
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}
	rows, err := movies.Scan(limit, offset)
	if err != nil {
		return nil, err
	}
	sortMovies(rows)
	return rows, nil
}

func GetMovieBySlugView(ctx *flop.ViewCtx, in GetMovieBySlugIn) (map[string]any, error) {
	slug := strings.TrimSpace(in.Slug)
	if slug == "" {
		return nil, fmt.Errorf("slug is required")
	}
	movies := ctx.DB.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	row, ok := movies.FindByUniqueIndex("slug", slug)
	if !ok {
		return nil, nil
	}
	return row, nil
}

func AutocompleteMoviesView(ctx *flop.ViewCtx, in AutocompleteMoviesIn) ([]map[string]any, error) {
	movies := ctx.DB.Table("movies")
	if movies == nil {
		return nil, fmt.Errorf("movies table not found")
	}
	q := strings.TrimSpace(in.Q)
	if q == "" {
		return []map[string]any{}, nil
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 20 {
		limit = 20
	}
	rows, err := movies.SearchFullText([]string{"title"}, q, limit)
	if err != nil {
		return []map[string]any{}, nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"slug":  row["slug"],
			"title": row["title"],
			"year":  row["year"],
		})
	}
	return out, nil
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
	sortMovies(rows)
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

func sortMovies(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		yi, yj := toInt(rows[i]["year"]), toInt(rows[j]["year"])
		if yi == yj {
			return toString(rows[i]["title"]) < toString(rows[j]["title"])
		}
		return yi > yj
	})
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
