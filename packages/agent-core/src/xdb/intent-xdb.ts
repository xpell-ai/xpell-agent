import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const INTENTS_ENTITY_ID = "agent.intents";

export type AgentIntentXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedIntentConfigRecord = {
  _id: string;
  _app_id: string;
  _env: string;
  _enabled: boolean;
  _priority: number;
  _roles_allowed: string[];
  _channels_allowed: string[];
  _synonyms: string[];
  _examples: string[];
  _default_params_json?: string;
  _created_at: number;
  _updated_at: number;
};

type IntentXdb = {
  _intents: XDBEntity;
};

const _xdb_by_scope = new Map<string, IntentXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<IntentXdb>>();

let _xdb_ready: Promise<void> | null = null;
let _xdb_ready_resolved = false;
let _resolve_xdb_ready: (() => void) | null = null;

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function to_text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function to_ts(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return Math.floor(value);
  const parsed = Date.parse(String(value ?? ""));
  if (Number.isNaN(parsed)) return Date.now();
  return parsed;
}

function normalize_string_array(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const entry of value) {
    if (typeof entry !== "string") continue;
    const trimmed = entry.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out;
}

function ensure_xdb_result<T>(res: XResponseData, label: string): T {
  if (res?._ok !== true) {
    const err = (res as any)?._error ?? (res as any)?._result ?? "unknown";
    let message = "";
    if (typeof err === "string") message = err;
    else if (is_plain_object(err)) {
      message = to_text(err._message) || to_text(err.message);
      if (!message) {
        try {
          message = JSON.stringify(err);
        } catch {
          message = String(err);
        }
      }
    } else {
      message = String(err);
    }
    throw new Error(`[agent-intents:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentIntentXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function intent_entity_name(scope: AgentIntentXdbScope): string {
  return `${INTENTS_ENTITY_ID}::${scope_key(scope)}`;
}

async function ensure_entity_loaded(entity: any): Promise<void> {
  if (!entity) return;
  if (entity._loaded === true) return;
  if (typeof entity.ensureLoaded === "function") return entity.ensureLoaded();
  if (typeof entity.load === "function") return entity.load();
  if (typeof entity.init === "function") return entity.init();
  if (typeof entity.loadData === "function") return entity.loadData();
}

function ensure_xdb_ready(): Promise<void> {
  if (_xdb_ready) return _xdb_ready;

  _xdb_ready = new Promise<void>((resolve) => {
    _resolve_xdb_ready = resolve;
  });

  if (typeof (XDB as any)?.ready === "boolean" && XDB.ready) {
    _xdb_ready_resolved = true;
    _resolve_xdb_ready?.();
  }

  _xem.on(
    "xdb-ready",
    () => {
      if (_xdb_ready_resolved) return;
      _xdb_ready_resolved = true;
      _resolve_xdb_ready?.();
    },
    { _once: true }
  );

  return _xdb_ready;
}

async function ensure_intents_xdb(scope: AgentIntentXdbScope): Promise<IntentXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const intents = XDB.create({
      _type: "xdb-entity",
      _name: intent_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _enabled: { _type: "Boolean", _required: true },
        _priority: { _type: "Number", _required: true, _index: true },
        _roles_allowed: { _type: "Array", _required: true },
        _channels_allowed: { _type: "Array" },
        _synonyms: { _type: "Array" },
        _examples: { _type: "Array" },
        _default_params_json: { _type: "String" }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(intents);

    const out = { _intents: intents };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-intents:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_intent_config(value: unknown): PersistedIntentConfigRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _id = to_text(value._id);
  if (!_id) return undefined;

  return {
    _id,
    _app_id: to_text(value._app_id),
    _env: to_text(value._env),
    _enabled: value._enabled === true,
    _priority: typeof value._priority === "number" && Number.isFinite(value._priority) ? Math.floor(value._priority) : 100,
    _roles_allowed: normalize_string_array(value._roles_allowed),
    _channels_allowed: normalize_string_array(value._channels_allowed),
    _synonyms: normalize_string_array(value._synonyms),
    _examples: normalize_string_array(value._examples),
    ...(to_text(value._default_params_json) ? { _default_params_json: to_text(value._default_params_json) } : {}),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export async function init_intents_xdb(scope: AgentIntentXdbScope): Promise<void> {
  await ensure_intents_xdb(scope);
}

export async function create_intents_entity_xdb(scope: AgentIntentXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_intents_xdb(scope);
  return xdb._intents;
}

export async function list_intent_configs_xdb(scope: AgentIntentXdbScope): Promise<PersistedIntentConfigRecord[]> {
  const xdb = await ensure_intents_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(xdb._intents.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000), "list_intents")._data;
  const out: PersistedIntentConfigRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_intent_config(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_intent_config_xdb(
  scope: AgentIntentXdbScope,
  intent_id: string
): Promise<PersistedIntentConfigRecord | undefined> {
  const xdb = await ensure_intents_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._intents.find({ _app_id: scope._app_id, _env: scope._env, _id: intent_id }, 0, 1),
    "get_intent"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_intent_config(rows[0]);
}

export async function upsert_intent_config_xdb(
  scope: AgentIntentXdbScope,
  record: PersistedIntentConfigRecord
): Promise<void> {
  const xdb = await ensure_intents_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _id: record._id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._intents.find(filter, 0, 1), "find_intent")._data;

  const row = {
    _id: record._id,
    _app_id: scope._app_id,
    _env: scope._env,
    _enabled: record._enabled === true,
    _priority: Math.floor(record._priority),
    _roles_allowed: [...record._roles_allowed],
    _channels_allowed: [...record._channels_allowed],
    _synonyms: [...record._synonyms],
    _examples: [...record._examples],
    _default_params_json: record._default_params_json ?? ""
  };

  if (existing.length > 0) {
    await xdb._intents.update(filter, row, true);
    return;
  }

  await xdb._intents.add(row, true, true);
}

export async function delete_intent_config_xdb(scope: AgentIntentXdbScope, intent_id: string): Promise<void> {
  const xdb = await ensure_intents_xdb(scope);
  await xdb._intents.delete({ _app_id: scope._app_id, _env: scope._env, _id: intent_id });
}
