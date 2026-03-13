package flop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/marcisbee/flop/internal/images"
	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/storage"
)

const mediaIndexVersion = 2

type mediaIndex struct {
	Version  int                          `json:"version"`
	Complete bool                         `json:"complete"`
	Files    map[string]*mediaIndexRecord `json:"files"`
}

type mediaIndexRecord struct {
	Path       string            `json:"path"`
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	Mime       string            `json:"mime"`
	RefSize    int64             `json:"refSize"`
	DiskSize   int64             `json:"diskSize"`
	ThumbCount int               `json:"thumbCount"`
	ThumbBytes int64             `json:"thumbBytes"`
	Width      int               `json:"width,omitempty"`
	Height     int               `json:"height,omitempty"`
	Thumbs     []string          `json:"thumbs,omitempty"`
	Usages     []AdminMediaUsage `json:"usages,omitempty"`
}

type mediaIndexOp func(*mediaIndex, *Database) error

func newMediaIndex() *mediaIndex {
	return &mediaIndex{
		Version:  mediaIndexVersion,
		Complete: false,
		Files:    map[string]*mediaIndexRecord{},
	}
}

func mediaIndexFilePath(dataDir string) string {
	return filepath.Join(dataDir, "_system", "media_index.json")
}

func loadMediaIndex(dataDir string) (*mediaIndex, error) {
	path := mediaIndexFilePath(dataDir)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var idx mediaIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, err
	}
	if idx.Version != mediaIndexVersion || idx.Files == nil {
		return nil, fmt.Errorf("unsupported media index version")
	}
	return &idx, nil
}

func saveMediaIndex(dataDir string, idx *mediaIndex) error {
	if idx == nil {
		idx = newMediaIndex()
	}
	idx.Version = mediaIndexVersion
	if idx.Files == nil {
		idx.Files = map[string]*mediaIndexRecord{}
	}
	path := mediaIndexFilePath(dataDir)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, "media_index-*.tmp")
	if err != nil {
		return err
	}
	tmp := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Database) EnsureMediaIndex() error {
	if d == nil || d.db == nil {
		return fmt.Errorf("database not available")
	}
	d.mediaIndexMu.Lock()
	defer d.mediaIndexMu.Unlock()
	idx, err := loadMediaIndex(d.db.GetDataDir())
	if err == nil && idx.Complete {
		return nil
	}
	return d.rebuildMediaIndexLocked()
}

func (d *Database) ensureMediaIndexBackground() {
	if d == nil || d.db == nil {
		return
	}
	d.mediaIndexMu.Lock()
	if d.mediaIndexRebuild {
		d.mediaIndexMu.Unlock()
		return
	}
	d.mediaIndexRebuild = true
	d.mediaIndexMu.Unlock()

	go func() {
		d.mediaIndexMu.Lock()
		err := d.rebuildMediaIndexLocked()
		d.mediaIndexRebuild = false
		d.mediaIndexMu.Unlock()
		if err != nil {
			// Best effort only; admin can retry later.
			fmt.Printf("flop: media index rebuild failed: %v\n", err)
		}
	}()
}

func (d *Database) RebuildMediaIndex() error {
	if d == nil || d.db == nil {
		return fmt.Errorf("database not available")
	}
	d.mediaIndexMu.Lock()
	defer d.mediaIndexMu.Unlock()
	d.mediaIndexRebuild = true
	defer func() { d.mediaIndexRebuild = false }()
	return d.rebuildMediaIndexLocked()
}

func (d *Database) rebuildMediaIndexLocked() error {
	idx := newMediaIndex()
	dataDir := d.db.GetDataDir()
	scanMediaIndexFilesOnDisk(dataDir, idx, d)

	tableNames := make([]string, 0, len(d.db.Tables))
	for name := range d.db.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		table := d.db.Tables[tableName]
		if table == nil {
			continue
		}
		def := table.GetDef()
		rows, err := table.Scan(1_000_000, 0)
		if err != nil {
			return fmt.Errorf("scan media rows for %s: %w", tableName, err)
		}
		for _, row := range rows {
			if err := mediaIndexSyncRow(idx, d, tableName, def, row); err != nil {
				return err
			}
		}
	}

	idx.Complete = true
	return saveMediaIndex(dataDir, idx)
}

func (d *Database) applyMediaIndexOps(ops ...mediaIndexOp) error {
	if d == nil || d.db == nil || len(ops) == 0 {
		return nil
	}
	d.mediaIndexMu.Lock()
	defer d.mediaIndexMu.Unlock()

	idx, err := loadMediaIndex(d.db.GetDataDir())
	if err != nil {
		idx = newMediaIndex()
	}
	for _, op := range ops {
		if op == nil {
			continue
		}
		if err := op(idx, d); err != nil {
			return err
		}
	}
	return saveMediaIndex(d.db.GetDataDir(), idx)
}

func mediaIndexSyncRowOp(tableName string, row map[string]any) mediaIndexOp {
	rowCopy := cloneRow(row)
	return func(idx *mediaIndex, d *Database) error {
		table := d.db.GetTable(tableName)
		if table == nil {
			return fmt.Errorf("table not found: %s", tableName)
		}
		return mediaIndexSyncRow(idx, d, tableName, table.GetDef(), rowCopy)
	}
}

func mediaIndexRemoveRowOp(tableName, rowID string) mediaIndexOp {
	return func(idx *mediaIndex, d *Database) error {
		removeMediaUsagesForRow(idx, tableName, rowID)
		removeMediaFilesWithPrefix(idx, mediaIndexRowPathPrefix(tableName, rowID))
		return nil
	}
}

func mediaIndexSyncTableOp(tableName string, rows []map[string]any) mediaIndexOp {
	rowsCopy := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		rowsCopy = append(rowsCopy, cloneRow(row))
	}
	return func(idx *mediaIndex, d *Database) error {
		table := d.db.GetTable(tableName)
		if table == nil {
			return fmt.Errorf("table not found: %s", tableName)
		}
		removeMediaUsagesForTable(idx, tableName)
		removeMediaFilesWithPrefix(idx, "_files/"+tableName+"/")
		scanTableDiskFilesIntoIndex(idx, d, tableName)
		for _, row := range rowsCopy {
			if err := mediaIndexSyncRow(idx, d, tableName, table.GetDef(), row); err != nil {
				return err
			}
		}
		return nil
	}
}

func mediaIndexUpsertFileOp(ref schema.FileRef) mediaIndexOp {
	return func(idx *mediaIndex, d *Database) error {
		_, _, _, _, ok := mediaIndexPathParts(ref.Path)
		if !ok {
			return nil
		}
		_, err := ensureMediaIndexRecord(idx, d, ref)
		return err
	}
}

func mediaIndexRowPathPrefix(tableName, rowID string) string {
	return "_files/" + tableName + "/" + rowID + "/"
}

func mediaIndexSyncRow(idx *mediaIndex, d *Database, tableName string, def *schema.TableDef, row map[string]any) error {
	if idx == nil || d == nil || def == nil || len(def.CompiledSchema.Fields) == 0 || row == nil {
		return nil
	}
	pkField := def.CompiledSchema.Fields[0].Name
	rowID := fmt.Sprint(row[pkField])
	if strings.TrimSpace(rowID) == "" {
		return nil
	}

	removeMediaUsagesForRow(idx, tableName, rowID)
	if err := syncRowDiskFilesIntoIndex(idx, d, tableName, rowID); err != nil {
		return err
	}

	for _, field := range def.CompiledSchema.Fields {
		if !adminFieldCanContainMedia(field.Kind, row[field.Name]) {
			continue
		}
		refs := adminCollectMediaRefs(row[field.Name], field.Kind)
		for _, ref := range refs {
			record, err := ensureMediaIndexRecord(idx, d, ref)
			if err != nil || record == nil {
				continue
			}
			record.Thumbs = append([]string(nil), field.ThumbSizes...)
			addMediaUsage(record, AdminMediaUsage{
				TableName: tableName,
				RowID:     rowID,
				FieldName: field.Name,
				Multi:     field.Kind == schema.KindFileMulti,
			})
		}
	}
	return nil
}

func scanMediaIndexFilesOnDisk(dataDir string, idx *mediaIndex, d *Database) {
	filesRoot := filepath.Join(dataDir, "_files")
	_ = filepath.Walk(filesRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return nil
		}
		ref := schema.FileRef{
			Path: filepath.ToSlash(rel),
			Name: filepath.Base(path),
			URL:  "/api/files/" + strings.TrimPrefix(filepath.ToSlash(rel), "_files/"),
			Size: info.Size(),
			Mime: storage.MimeFromExtension(path),
		}
		_, _ = ensureMediaIndexRecord(idx, d, ref)
		return nil
	})
}

func syncRowDiskFilesIntoIndex(idx *mediaIndex, d *Database, tableName, rowID string) error {
	rowDir := filepath.Join(d.db.GetDataDir(), "_files", tableName, rowID)
	prefix := mediaIndexRowPathPrefix(tableName, rowID)
	seen := map[string]bool{}
	_ = filepath.Walk(rowDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(d.db.GetDataDir(), path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, prefix) {
			return nil
		}
		seen[rel] = true
		ref := schema.FileRef{
			Path: rel,
			Name: filepath.Base(path),
			URL:  "/api/files/" + strings.TrimPrefix(rel, "_files/"),
			Size: info.Size(),
			Mime: storage.MimeFromExtension(path),
		}
		_, _ = ensureMediaIndexRecord(idx, d, ref)
		return nil
	})
	for path := range idx.Files {
		if strings.HasPrefix(path, prefix) && !seen[path] {
			delete(idx.Files, path)
		}
	}
	return nil
}

func scanTableDiskFilesIntoIndex(idx *mediaIndex, d *Database, tableName string) {
	tableDir := filepath.Join(d.db.GetDataDir(), "_files", tableName)
	_ = filepath.Walk(tableDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(d.db.GetDataDir(), path)
		if err != nil {
			return nil
		}
		ref := schema.FileRef{
			Path: filepath.ToSlash(rel),
			Name: filepath.Base(path),
			URL:  "/api/files/" + strings.TrimPrefix(filepath.ToSlash(rel), "_files/"),
			Size: info.Size(),
			Mime: storage.MimeFromExtension(path),
		}
		_, _ = ensureMediaIndexRecord(idx, d, ref)
		return nil
	})
}

func ensureMediaIndexRecord(idx *mediaIndex, d *Database, ref schema.FileRef) (*mediaIndexRecord, error) {
	ref.Path = strings.TrimSpace(ref.Path)
	if ref.Path == "" || !strings.HasPrefix(ref.Path, "_files/") {
		return nil, nil
	}
	fullPath := filepath.Join(d.db.GetDataDir(), filepath.FromSlash(ref.Path))
	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			delete(idx.Files, ref.Path)
			return nil, nil
		}
		return nil, err
	}

	record := idx.Files[ref.Path]
	if record == nil {
		record = &mediaIndexRecord{Path: ref.Path}
		idx.Files[ref.Path] = record
	}
	record.Name = ref.Name
	if record.Name == "" {
		record.Name = filepath.Base(fullPath)
	}
	record.URL = ref.URL
	if record.URL == "" {
		record.URL = "/api/files/" + strings.TrimPrefix(ref.Path, "_files/")
	}
	record.Mime = ref.Mime
	if record.Mime == "" {
		record.Mime = storage.MimeFromExtension(fullPath)
	}
	record.RefSize = ref.Size
	record.DiskSize = stat.Size()
	record.Width = 0
	record.Height = 0
	if strings.HasPrefix(record.Mime, "image/") || adminLooksLikeImagePath(record.Path) {
		if w, h, err := images.ReadDimensions(fullPath); err == nil {
			record.Width = w
			record.Height = h
		}
	}
	record.ThumbCount = 0
	record.ThumbBytes = 0
	tableName, rowID, fieldName, filename, ok := mediaIndexPathParts(ref.Path)
	if ok {
		if field := d.mediaFieldSpec(tableName, fieldName); field != nil {
			record.Thumbs = append([]string(nil), field.ThumbSizes...)
		}
		thumbDir := filepath.Join(d.db.GetDataDir(), "_thumbs", tableName, rowID, fieldName)
		matches, err := filepath.Glob(filepath.Join(thumbDir, "*_"+filename))
		if err == nil {
			record.ThumbCount = len(matches)
			for _, match := range matches {
				if thumbStat, err := os.Stat(match); err == nil {
					record.ThumbBytes += thumbStat.Size()
				}
			}
		}
	}
	return record, nil
}

func (d *Database) mediaFieldSpec(tableName, fieldName string) *schema.CompiledField {
	ti := d.db.GetTable(tableName)
	if ti == nil {
		return nil
	}
	return ti.GetDef().CompiledSchema.FieldMap[fieldName]
}

func mediaIndexPathParts(path string) (tableName, rowID, fieldName, filename string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(path, "_files/"), "/")
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func removeMediaUsagesForRow(idx *mediaIndex, tableName, rowID string) {
	for path, record := range idx.Files {
		if record == nil {
			delete(idx.Files, path)
			continue
		}
		filtered := record.Usages[:0]
		for _, usage := range record.Usages {
			if usage.TableName == tableName && usage.RowID == rowID {
				continue
			}
			filtered = append(filtered, usage)
		}
		record.Usages = filtered
	}
}

func removeMediaUsagesForTable(idx *mediaIndex, tableName string) {
	for path, record := range idx.Files {
		if record == nil {
			delete(idx.Files, path)
			continue
		}
		filtered := record.Usages[:0]
		for _, usage := range record.Usages {
			if usage.TableName == tableName {
				continue
			}
			filtered = append(filtered, usage)
		}
		record.Usages = filtered
	}
}

func removeMediaFilesWithPrefix(idx *mediaIndex, prefix string) {
	for path := range idx.Files {
		if strings.HasPrefix(path, prefix) {
			delete(idx.Files, path)
		}
	}
}

func addMediaUsage(record *mediaIndexRecord, usage AdminMediaUsage) {
	if record == nil {
		return
	}
	for _, existing := range record.Usages {
		if existing.TableName == usage.TableName && existing.RowID == usage.RowID && existing.FieldName == usage.FieldName {
			return
		}
	}
	record.Usages = append(record.Usages, usage)
}

func flattenMediaIndexRows(idx *mediaIndex) []AdminMediaRow {
	if idx == nil || len(idx.Files) == 0 {
		return []AdminMediaRow{}
	}
	rows := make([]AdminMediaRow, 0, len(idx.Files))
	for _, record := range idx.Files {
		if record == nil {
			continue
		}
		validUsages := make([]AdminMediaUsage, 0, len(record.Usages))
		for _, usage := range record.Usages {
			if mediaUsageLooksLikeFileField(usage.FieldName, record.Path) {
				validUsages = append(validUsages, usage)
			}
		}
		if len(validUsages) == 0 {
			rows = append(rows, AdminMediaRow{
				Path:       record.Path,
				Name:       record.Name,
				URL:        record.URL,
				Mime:       record.Mime,
				RefSize:    record.RefSize,
				DiskSize:   record.DiskSize,
				ThumbCount: record.ThumbCount,
				ThumbBytes: record.ThumbBytes,
				Width:      record.Width,
				Height:     record.Height,
				Orphaned:   true,
				Thumbs:     append([]string(nil), record.Thumbs...),
			})
			continue
		}
		for _, usage := range validUsages {
			rows = append(rows, AdminMediaRow{
				Path:       record.Path,
				Name:       record.Name,
				URL:        record.URL,
				Mime:       record.Mime,
				RefSize:    record.RefSize,
				DiskSize:   record.DiskSize,
				ThumbCount: record.ThumbCount,
				ThumbBytes: record.ThumbBytes,
				Width:      record.Width,
				Height:     record.Height,
				Orphaned:   false,
				TableName:  usage.TableName,
				RowID:      usage.RowID,
				FieldName:  usage.FieldName,
				Thumbs:     append([]string(nil), record.Thumbs...),
			})
		}
	}
	return rows
}

func mediaUsageLooksLikeFileField(fieldName, path string) bool {
	if strings.TrimSpace(fieldName) == "" {
		return false
	}
	_, _, pathField, _, ok := mediaIndexPathParts(path)
	if !ok {
		return false
	}
	return fieldName == pathField
}
