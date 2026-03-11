package app

import (
	"encoding/json"
	"strings"

	flop "github.com/marcisbee/flop/go2"
)

var MoviesSchema = &flop.Schema{
	Name: "movies",
	Fields: []flop.Field{
		{Name: "slug", Type: flop.FieldString, Required: true, Unique: true},
		{Name: "title", Type: flop.FieldString, Required: true, Searchable: true},
		{Name: "year", Type: flop.FieldInt, Required: true},
		{Name: "runtime_minutes", Type: flop.FieldInt},
		{Name: "rating", Type: flop.FieldFloat},
		{Name: "votes", Type: flop.FieldInt},
		{Name: "genres", Type: flop.FieldString}, // JSON array stored as string
		{Name: "overview", Type: flop.FieldString},
	},
}

func Setup(db *flop.DB, srv *flop.Server) {
	// Stats view
	srv.RegisterView("/api/stats", &flop.ViewDef{
		Name:  "get_stats",
		Table: "movies",
		Limit: 1,
	})

	// List movies (paginated, sorted by year desc)
	srv.RegisterView("/api/movies", &flop.ViewDef{
		Name:    "list_movies",
		Table:   "movies",
		OrderBy: "year",
		Order:   flop.Desc,
		Limit:   36,
	})

	// Autocomplete search
	srv.RegisterView("/api/movies/search", &flop.ViewDef{
		Name:    "autocomplete_movies",
		Table:   "movies",
		Limit:   10,
		SearchQ: "", // populated from ?q= parameter
	})

	// Get movie by slug
	srv.RegisterView("/api/movies/by-slug/{slug}", &flop.ViewDef{
		Name:  "get_movie_by_slug",
		Table: "movies",
		Filters: []flop.Filter{
			{Field: "slug", Op: flop.OpEq},
		},
		Limit: 1,
	})
}

// ParseGenres parses a pipe-separated genre string into a JSON array string.
func ParseGenres(s string) string {
	if s == "" || s == "\\N" {
		return "[]"
	}
	parts := strings.Split(s, ",")
	b, _ := json.Marshal(parts)
	return string(b)
}
