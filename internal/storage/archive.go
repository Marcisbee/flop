package storage

import "encoding/json"

// ArchivedRow stores a soft-deleted row plus metadata required for restore.
type ArchivedRow struct {
	ArchiveID        string `json:"archiveId"`
	OriginalPK       string `json:"originalPk"`
	DeletedAtUnixMs  int64  `json:"deletedAtUnixMs"`
	DeletedBy        string `json:"deletedBy,omitempty"`
	CascadeGroupID   string `json:"cascadeGroupId,omitempty"`
	CascadeRootTable string `json:"cascadeRootTable,omitempty"`
	CascadeRootPK    string `json:"cascadeRootPk,omitempty"`
	CascadeDepth     int    `json:"cascadeDepth,omitempty"`
	RowData          []byte `json:"rowData"`
}

func SerializeArchivedRow(row *ArchivedRow) ([]byte, error) {
	if row == nil {
		return []byte("null"), nil
	}
	return json.Marshal(row)
}

func DeserializeArchivedRow(data []byte) (*ArchivedRow, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var row ArchivedRow
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	return &row, nil
}
