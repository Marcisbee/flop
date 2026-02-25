// Structural schema comparison — detect changes between stored and code schemas

import type { CompiledSchema, StoredSchema, StoredColumnDef, CompiledField } from "../types.ts";

export interface SchemaChange {
  type: "added" | "removed" | "typeChanged" | "requireChanged";
  field: string;
  oldType?: string;
  newType?: string;
}

export function compiledToStored(schema: CompiledSchema): StoredSchema {
  return {
    columns: schema.fields.map((f) => ({
      name: f.name,
      type: f.kind,
      required: f.required || undefined,
      unique: f.unique || undefined,
    })),
  };
}

export function diffSchemas(
  stored: StoredSchema,
  current: CompiledSchema,
): SchemaChange[] {
  const changes: SchemaChange[] = [];

  const storedMap = new Map<string, StoredColumnDef>();
  for (const col of stored.columns) {
    storedMap.set(col.name, col);
  }

  const currentMap = new Map<string, CompiledField>();
  for (const field of current.fields) {
    currentMap.set(field.name, field);
  }

  // Check for removed fields
  for (const [name] of storedMap) {
    if (!currentMap.has(name)) {
      changes.push({ type: "removed", field: name });
    }
  }

  // Check for added or changed fields
  for (const [name, field] of currentMap) {
    const old = storedMap.get(name);
    if (!old) {
      changes.push({ type: "added", field: name });
    } else if (old.type !== field.kind) {
      changes.push({
        type: "typeChanged",
        field: name,
        oldType: old.type,
        newType: field.kind,
      });
    }
  }

  return changes;
}

export function schemasEqual(stored: StoredSchema, current: CompiledSchema): boolean {
  if (stored.columns.length !== current.fields.length) return false;

  for (let i = 0; i < stored.columns.length; i++) {
    const s = stored.columns[i];
    const c = current.fields[i];
    if (s.name !== c.name || s.type !== c.kind) return false;
  }

  return true;
}

export function validateMigration(
  changes: SchemaChange[],
  migrations: { version: number; rename?: Record<string, string>; transform?: unknown }[],
  targetVersion: number,
): string[] {
  const errors: string[] = [];
  const migration = migrations.find((m) => m.version === targetVersion);
  const renames = migration?.rename ?? {};
  const hasTransform = !!migration?.transform;

  // Renamed fields are expected to show as "removed old" + "added new"
  const renameOldNames = new Set(Object.keys(renames));
  const renameNewNames = new Set(Object.values(renames));

  for (const change of changes) {
    if (change.type === "added") {
      // If it's a rename target, that's fine
      if (renameNewNames.has(change.field)) continue;
      // New field must not be required without a default
      // (this check would need the actual field info — done at table level)
    } else if (change.type === "removed") {
      // If it's a rename source, that's fine
      if (renameOldNames.has(change.field)) continue;
      // Removing fields is auto-migratable
    } else if (change.type === "typeChanged") {
      if (!hasTransform) {
        errors.push(
          `Field "${change.field}" changed type from "${change.oldType}" to "${change.newType}" ` +
            `but no transform function provided in migration version ${targetVersion}`
        );
      }
    }
  }

  return errors;
}
