package flop

import (
	"fmt"
)

// ReducerDef defines a write operation (reducer).
type ReducerDef struct {
	Name     string
	Table    string
	Action   ReducerAction
	Validate func(data map[string]any) error
	PermCheck func(auth any, data map[string]any) bool
	Transform     func(data map[string]any) map[string]any              // pre-insert/update transform
	AuthTransform func(auth any, data map[string]any) map[string]any // transform with auth context
}

type ReducerAction int

const (
	ActionInsert ReducerAction = iota
	ActionUpdate
	ActionDelete
	ActionUpsert
	ActionCustom
)

// ReducerResult holds the result of executing a reducer.
type ReducerResult struct {
	Row     *Row
	Rows    []*Row
	Deleted bool
}

// ExecuteReducer runs a reducer against the database.
func (db *DB) ExecuteReducer(r *ReducerDef, data map[string]any, auth any) (*ReducerResult, error) {
	// Permission check
	if r.PermCheck != nil && !r.PermCheck(auth, data) {
		return nil, fmt.Errorf("permission denied")
	}

	// Validate input
	if r.Validate != nil {
		if err := r.Validate(data); err != nil {
			return nil, fmt.Errorf("validation: %w", err)
		}
	}

	// Transform
	if r.Transform != nil {
		data = r.Transform(data)
	}
	if r.AuthTransform != nil {
		data = r.AuthTransform(auth, data)
	}

	switch r.Action {
	case ActionInsert:
		row, err := db.Insert(r.Table, data)
		if err != nil {
			return nil, err
		}
		return &ReducerResult{Row: row}, nil

	case ActionUpdate:
		id := toUint64(data["id"])
		if id == 0 {
			return nil, fmt.Errorf("update requires 'id' field")
		}
		delete(data, "id")
		row, err := db.Update(r.Table, id, data)
		if err != nil {
			return nil, err
		}
		return &ReducerResult{Row: row}, nil

	case ActionDelete:
		id := toUint64(data["id"])
		if id == 0 {
			return nil, fmt.Errorf("delete requires 'id' field")
		}
		if err := db.Delete(r.Table, id); err != nil {
			return nil, err
		}
		return &ReducerResult{Deleted: true}, nil

	case ActionUpsert:
		// Try update first, then insert
		id := toUint64(data["id"])
		if id > 0 {
			table := db.Table(r.Table)
			if table != nil {
				existing, _ := table.Get(id)
				if existing != nil {
					delete(data, "id")
					row, err := db.Update(r.Table, id, data)
					if err != nil {
						return nil, err
					}
					return &ReducerResult{Row: row}, nil
				}
			}
		}
		row, err := db.Insert(r.Table, data)
		if err != nil {
			return nil, err
		}
		return &ReducerResult{Row: row}, nil

	default:
		return nil, fmt.Errorf("unsupported action: %d", r.Action)
	}
}
