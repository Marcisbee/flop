package schema

import "fmt"

// SchemaChange describes a difference between two schema versions.
type SchemaChange struct {
	Type    string // "added", "removed", "typeChanged"
	Field   string
	OldType string
	NewType string
}

// DiffSchemas compares a stored schema with the current compiled schema.
func DiffSchemas(stored *StoredSchema, current *CompiledSchema) []SchemaChange {
	var changes []SchemaChange

	storedMap := make(map[string]*StoredColumnDef)
	for i := range stored.Columns {
		storedMap[stored.Columns[i].Name] = &stored.Columns[i]
	}

	currentMap := make(map[string]*CompiledField)
	for i := range current.Fields {
		currentMap[current.Fields[i].Name] = &current.Fields[i]
	}

	// Removed fields
	for name := range storedMap {
		if _, ok := currentMap[name]; !ok {
			changes = append(changes, SchemaChange{Type: "removed", Field: name})
		}
	}

	// Added or changed fields
	for name, field := range currentMap {
		old, exists := storedMap[name]
		if !exists {
			changes = append(changes, SchemaChange{Type: "added", Field: name})
		} else if old.Type != string(field.Kind) {
			changes = append(changes, SchemaChange{
				Type:    "typeChanged",
				Field:   name,
				OldType: old.Type,
				NewType: string(field.Kind),
			})
		}
	}

	return changes
}

// SchemasEqual checks if a stored schema matches the current compiled schema.
func SchemasEqual(stored *StoredSchema, current *CompiledSchema) bool {
	if len(stored.Columns) != len(current.Fields) {
		return false
	}
	for i := range stored.Columns {
		if stored.Columns[i].Name != current.Fields[i].Name {
			return false
		}
		if stored.Columns[i].Type != string(current.Fields[i].Kind) {
			return false
		}
	}
	return true
}

// ValidateMigration checks schema changes against provided migration steps.
func ValidateMigration(changes []SchemaChange, migrations []MigrationStep, targetVersion int) []string {
	var errors []string

	var migration *MigrationStep
	for i := range migrations {
		if migrations[i].Version == targetVersion {
			migration = &migrations[i]
			break
		}
	}

	renames := make(map[string]string)
	hasTransform := false
	if migration != nil {
		renames = migration.Rename
		// Transform is handled on JS side; we just check if migration exists
		hasTransform = true
	}

	renameOldNames := make(map[string]bool)
	renameNewNames := make(map[string]bool)
	for old, new_ := range renames {
		renameOldNames[old] = true
		renameNewNames[new_] = true
	}

	for _, change := range changes {
		switch change.Type {
		case "added":
			if renameNewNames[change.Field] {
				continue
			}
		case "removed":
			if renameOldNames[change.Field] {
				continue
			}
		case "typeChanged":
			if !hasTransform {
				errors = append(errors, fmt.Sprintf(
					`Field "%s" changed type from "%s" to "%s" but no transform function provided in migration version %d`,
					change.Field, change.OldType, change.NewType, targetVersion,
				))
			}
		}
	}

	return errors
}
