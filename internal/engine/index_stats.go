package engine

import (
	"sort"
	"time"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

type IndexStatsReport struct {
	GeneratedAtUnixMilli  int64             `json:"generatedAtUnixMilli"`
	TableCount            int               `json:"tableCount"`
	PrimaryIndexCount     int               `json:"primaryIndexCount"`
	SecondaryIndexCount   int               `json:"secondaryIndexCount"`
	EstimatedPayloadBytes uint64            `json:"estimatedPayloadBytes"`
	Tables                []TableIndexStats `json:"tables"`
}

type TableIndexStats struct {
	Table                 string       `json:"table"`
	Rows                  int          `json:"rows"`
	PrimaryKeyField       string       `json:"primaryKeyField"`
	PrimaryKeys           int          `json:"primaryKeys"`
	PrimaryEstimatedBytes uint64       `json:"primaryEstimatedBytes"`
	SecondaryIndexes      []IndexStats `json:"secondaryIndexes"`
	TotalEstimatedBytes   uint64       `json:"totalEstimatedBytes"`
	SecondaryReady        bool         `json:"secondaryReady"`
}

type IndexStats struct {
	Name                  string   `json:"name"`
	Type                  string   `json:"type"`
	Fields                []string `json:"fields"`
	Unique                bool     `json:"unique"`
	KeyCount              int      `json:"keyCount"`
	EntryCount            int      `json:"entryCount"`
	EstimatedPayloadBytes uint64   `json:"estimatedPayloadBytes"`
}

func (db *Database) IndexStatsReport() IndexStatsReport {
	tableNames := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	out := IndexStatsReport{
		GeneratedAtUnixMilli:  time.Now().UnixMilli(),
		TableCount:            len(tableNames),
		PrimaryIndexCount:     len(tableNames),
		Tables:                make([]TableIndexStats, 0, len(tableNames)),
		SecondaryIndexCount:   0,
		EstimatedPayloadBytes: 0,
	}

	for _, name := range tableNames {
		ti := db.Tables[name]
		if ti == nil {
			continue
		}
		t := ti.indexStats()
		out.Tables = append(out.Tables, t)
		out.SecondaryIndexCount += len(t.SecondaryIndexes)
		out.EstimatedPayloadBytes += t.TotalEstimatedBytes
	}

	return out
}

func (ti *TableInstance) indexStats() TableIndexStats {
	primary := ti.primaryIndex.Stats()
	secondary := make([]IndexStats, 0, len(ti.def.Indexes))

	for _, def := range ti.def.Indexes {
		key := secondaryIndexKey(def)
		idx := ti.secondaryIdxs[key]
		stats := IndexStats{
			Name:   key,
			Type:   indexTypeLabel(def),
			Fields: append([]string(nil), def.Fields...),
			Unique: def.Unique,
		}

		switch typed := idx.(type) {
		case *storage.HashIndex:
			s := typed.Stats()
			stats.KeyCount = s.Keys
			stats.EntryCount = s.Keys
			stats.EstimatedPayloadBytes = s.EstimatedPayloadBytes
		case *storage.MultiIndex:
			s := typed.Stats()
			stats.KeyCount = s.Keys
			stats.EntryCount = s.Entries
			stats.EstimatedPayloadBytes = s.EstimatedPayloadBytes
		case *storage.FullTextIndex:
			s := typed.Stats()
			stats.KeyCount = s.TokenCount
			stats.EntryCount = s.PostingEntries
			stats.EstimatedPayloadBytes = s.EstimatedPayloadBytes
		}

		secondary = append(secondary, stats)
	}

	sort.Slice(secondary, func(i, j int) bool {
		return secondary[i].Name < secondary[j].Name
	})

	total := primary.EstimatedPayloadBytes
	for _, s := range secondary {
		total += s.EstimatedPayloadBytes
	}

	return TableIndexStats{
		Table:                 ti.Name,
		Rows:                  ti.Count(),
		PrimaryKeyField:       ti.primaryKeyField(),
		PrimaryKeys:           primary.Keys,
		PrimaryEstimatedBytes: primary.EstimatedPayloadBytes,
		SecondaryIndexes:      secondary,
		TotalEstimatedBytes:   total,
		SecondaryReady:        ti.SecondaryIndexesReady(),
	}
}

func indexTypeLabel(def schema.IndexDef) string {
	switch normalizeIndexType(def.Type) {
	case schema.IndexTypeFullText:
		return "fullText"
	default:
		if def.Unique {
			return "unique"
		}
		return "hash"
	}
}
