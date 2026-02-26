// Schema migration — lazy per-row migration on read

import type { MigrationStep, StoredSchema, CompiledSchema } from "../types.ts";

interface MigrationChainStep {
  fromVersion: number;
  toVersion: number;
  rename?: Record<string, string>;
  transform?: (row: Record<string, unknown>) => Record<string, unknown>;
  addedFields: string[];
  removedFields: string[];
  targetSchema: StoredSchema;
}

export class MigrationChain {
  readonly steps: MigrationChainStep[];

  constructor(steps: MigrationChainStep[]) {
    this.steps = steps;
  }

  migrate(row: Record<string, unknown>): Record<string, unknown> {
    let current = { ...row };

    for (const step of this.steps) {
      // Apply renames
      if (step.rename) {
        for (const [oldName, newName] of Object.entries(step.rename)) {
          if (oldName in current) {
            current[newName] = current[oldName];
            delete current[oldName];
          }
        }
      }

      // Apply custom transform
      if (step.transform) {
        current = step.transform(current);
      }

      // Add new fields with null defaults
      for (const field of step.addedFields) {
        if (!(field in current)) {
          current[field] = null;
        }
      }

      // Remove old fields
      for (const field of step.removedFields) {
        delete current[field];
      }
    }

    return current;
  }
}

export function buildMigrationChain(
  fromVersion: number,
  toVersion: number,
  migrations: MigrationStep[],
  schemas: Record<number, StoredSchema>,
): MigrationChain {
  const steps: MigrationChainStep[] = [];

  for (let v = fromVersion + 1; v <= toVersion; v++) {
    const migration = migrations.find((m) => m.version === v);
    const prevSchema = schemas[v - 1];
    const targetSchema = schemas[v];

    if (!targetSchema) {
      throw new Error(`Missing schema definition for version ${v}`);
    }

    const prevFieldNames = new Set(prevSchema?.columns.map((c) => c.name) ?? []);
    const targetFieldNames = new Set(targetSchema.columns.map((c) => c.name));

    // Determine renames from migration hints
    const renameOldNames = new Set(Object.keys(migration?.rename ?? {}));
    const renameNewNames = new Set(Object.values(migration?.rename ?? {}));

    // Added: in target but not in prev, and not a rename target
    const addedFields = [...targetFieldNames].filter(
      (f) => !prevFieldNames.has(f) && !renameNewNames.has(f),
    );

    // Removed: in prev but not in target, and not a rename source
    const removedFields = [...prevFieldNames].filter(
      (f) => !targetFieldNames.has(f) && !renameOldNames.has(f),
    );

    steps.push({
      fromVersion: v - 1,
      toVersion: v,
      rename: migration?.rename,
      transform: migration?.transform,
      addedFields,
      removedFields,
      targetSchema,
    });
  }

  return new MigrationChain(steps);
}

export function deserializeWithSchema(
  fieldValues: unknown[],
  schema: StoredSchema,
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (let i = 0; i < schema.columns.length && i < fieldValues.length; i++) {
    row[schema.columns[i].name] = fieldValues[i];
  }
  return row;
}
