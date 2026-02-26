type Dict = Record<string, unknown>;

const MASK_SENTINEL = "••••••••";

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function split_path(path_value: string): string[] {
  if (typeof path_value !== "string") return [];
  return path_value
    .split(".")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function deep_clone(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => deep_clone(item));
  if (is_plain_object(value)) {
    const out: Dict = {};
    for (const [key, child] of Object.entries(value)) {
      out[key] = deep_clone(child);
    }
    return out;
  }
  return value;
}

export function deepMerge<T extends Dict>(base: T, patch: Record<string, unknown>): T {
  const left = (deep_clone(base) as Dict) ?? {};
  const right = (deep_clone(patch) as Dict) ?? {};
  const out: Dict = { ...left };

  for (const [key, value] of Object.entries(right)) {
    const current = out[key];
    if (is_plain_object(current) && is_plain_object(value)) {
      out[key] = deepMerge(current, value);
      continue;
    }
    out[key] = deep_clone(value);
  }

  return out as T;
}

export function getByPath(obj: unknown, dotted_path: string): unknown {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return obj;

  let cursor: unknown = obj;
  for (const key of parts) {
    if (!is_plain_object(cursor)) return undefined;
    cursor = cursor[key];
  }
  return cursor;
}

export function setByPath(obj: Dict, dotted_path: string, value: unknown): Dict {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return obj;

  let cursor: Dict = obj;
  for (let idx = 0; idx < parts.length - 1; idx += 1) {
    const key = parts[idx];
    const current = cursor[key];
    if (!is_plain_object(current)) {
      cursor[key] = {};
    }
    cursor = cursor[key] as Dict;
  }

  cursor[parts[parts.length - 1]] = deep_clone(value);
  return obj;
}

export function maskSensitive(
  settings: Record<string, unknown>,
  sensitive_paths: string[]
): { maskedSettings: Record<string, unknown>; maskedMap: Record<string, boolean> } {
  const masked_settings = deep_clone(settings) as Record<string, unknown>;
  const masked_map: Record<string, boolean> = {};

  for (const path_value of sensitive_paths) {
    const current = getByPath(masked_settings, path_value);
    if (current === undefined || current === null) continue;
    if (typeof current === "string" && current.length === 0) continue;
    if (Array.isArray(current) && current.length === 0) continue;
    setByPath(masked_settings, path_value, MASK_SENTINEL);
    masked_map[path_value] = true;
  }

  return {
    maskedSettings: masked_settings,
    maskedMap: masked_map
  };
}

export function applyPatchWithMaskHandling(
  existing: Record<string, unknown>,
  patch: Record<string, unknown>,
  sensitive_paths: string[]
): Record<string, unknown> {
  const patch_copy = deep_clone(patch) as Dict;

  for (const path_value of sensitive_paths) {
    const next = getByPath(patch_copy, path_value);
    if (next === MASK_SENTINEL) {
      const original = getByPath(existing, path_value);
      if (original !== undefined) {
        setByPath(patch_copy, path_value, original);
      } else {
        deleteByPath(patch_copy, path_value);
      }
    }
  }

  return deepMerge(existing, patch_copy);
}

function deleteByPath(obj: Dict, dotted_path: string): void {
  const parts = split_path(dotted_path);
  if (parts.length === 0) return;

  let cursor: unknown = obj;
  for (let idx = 0; idx < parts.length - 1; idx += 1) {
    if (!is_plain_object(cursor)) return;
    cursor = cursor[parts[idx]];
  }
  if (!is_plain_object(cursor)) return;
  delete cursor[parts[parts.length - 1]];
}
