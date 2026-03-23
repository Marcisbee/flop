package schema

// MigrationChainStep describes one version transition.
type MigrationChainStep struct {
	FromVersion   int
	ToVersion     int
	Rename        map[string]string
	AddedFields   []string
	RemovedFields []string
	TargetSchema  *StoredSchema
}

// MigrationChain applies a series of migration steps to a row.
type MigrationChain struct {
	Steps []MigrationChainStep
}

// Migrate applies all chain steps to transform a row from old schema to current.
// Note: transform functions are handled on the JS side. Go only handles
// renames, added fields (null defaults), and removed fields.
func (mc *MigrationChain) Migrate(row map[string]interface{}) map[string]interface{} {
	current := make(map[string]interface{}, len(row))
	for k, v := range row {
		current[k] = v
	}

	for _, step := range mc.Steps {
		// Apply renames
		if step.Rename != nil {
			for oldName, newName := range step.Rename {
				if v, ok := current[oldName]; ok {
					current[newName] = v
					delete(current, oldName)
				}
			}
		}

		// Add new fields with null defaults
		for _, field := range step.AddedFields {
			if _, ok := current[field]; !ok {
				current[field] = nil
			}
		}

		// Remove old fields
		for _, field := range step.RemovedFields {
			delete(current, field)
		}
	}

	return current
}

// BuildMigrationChain builds the chain from fromVersion to toVersion.
func BuildMigrationChain(
	fromVersion, toVersion int,
	migrations []MigrationStep,
	schemas map[int]*StoredSchema,
) *MigrationChain {
	var steps []MigrationChainStep

	for v := fromVersion + 1; v <= toVersion; v++ {
		var migration *MigrationStep
		for i := range migrations {
			if migrations[i].Version == v {
				migration = &migrations[i]
				break
			}
		}

		prevSchema := schemas[v-1]
		targetSchema := schemas[v]
		if targetSchema == nil {
			continue
		}

		prevFieldNames := make(map[string]bool)
		if prevSchema != nil {
			for _, c := range prevSchema.Columns {
				prevFieldNames[c.Name] = true
			}
		}

		targetFieldNames := make(map[string]bool)
		for _, c := range targetSchema.Columns {
			targetFieldNames[c.Name] = true
		}

		renameOldNames := make(map[string]bool)
		renameNewNames := make(map[string]bool)
		var renames map[string]string
		if migration != nil {
			renames = migration.Rename
			for old, new_ := range renames {
				renameOldNames[old] = true
				renameNewNames[new_] = true
			}
		}

		var addedFields []string
		for name := range targetFieldNames {
			if !prevFieldNames[name] && !renameNewNames[name] {
				addedFields = append(addedFields, name)
			}
		}

		var removedFields []string
		for name := range prevFieldNames {
			if !targetFieldNames[name] && !renameOldNames[name] {
				removedFields = append(removedFields, name)
			}
		}

		steps = append(steps, MigrationChainStep{
			FromVersion:   v - 1,
			ToVersion:     v,
			Rename:        renames,
			AddedFields:   addedFields,
			RemovedFields: removedFields,
			TargetSchema:  targetSchema,
		})
	}

	return &MigrationChain{Steps: steps}
}

// DeserializeWithSchema maps positional field values to a named row using a stored schema.
func DeserializeWithSchema(values []interface{}, stored *StoredSchema) map[string]interface{} {
	row := make(map[string]interface{}, len(stored.Columns))
	for i := 0; i < len(stored.Columns) && i < len(values); i++ {
		row[stored.Columns[i].Name] = values[i]
	}
	return row
}
