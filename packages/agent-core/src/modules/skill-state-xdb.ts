import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const SKILL_STATE_ENTITY_ID = "agent.skill_state";

export type AgentSkillStateXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedSkillStateRecord = {
  _ukey: string;
  _skill_id: string;
  _key: string;
  _value: Record<string, unknown>;
  _created_at: number;
  _updated_at: number;
};

type SkillStateXdb = {
  _state: XDBEntity;
};

const _xdb_by_scope = new Map<string, SkillStateXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<SkillStateXdb>>();

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
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = Date.parse(String(value ?? ""));
  if (Number.isNaN(parsed)) return Date.now();
  return parsed;
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
    throw new Error(`[agent-skill-state:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentSkillStateXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function skill_state_entity_name(scope: AgentSkillStateXdbScope): string {
  return `${SKILL_STATE_ENTITY_ID}::${scope_key(scope)}`;
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

async function ensure_skill_state_xdb(scope: AgentSkillStateXdbScope): Promise<SkillStateXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const state = XDB.create({
      _type: "xdb-entity",
      _name: skill_state_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _ukey: { _type: "String", _required: true, _index: true },
        _skill_id: { _type: "String", _required: true, _index: true },
        _key: { _type: "String", _required: true, _index: true },
        _value: { _type: "Object", _required: true }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(state);

    const out = { _state: state };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-skill-state:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_skill_state(value: unknown): PersistedSkillStateRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _ukey = to_text(value._ukey || value._id);
  const _skill_id = to_text(value._skill_id);
  const _key = to_text(value._key);
  if (!_ukey || !_skill_id || !_key || !is_plain_object(value._value)) return undefined;

  return {
    _ukey,
    _skill_id,
    _key,
    _value: { ...value._value },
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export function build_skill_state_ukey(skill_id: string, key: string): string {
  return `${skill_id}:${key}`;
}

export async function init_skill_state_xdb(scope: AgentSkillStateXdbScope): Promise<void> {
  await ensure_skill_state_xdb(scope);
}

export async function create_skill_state_entity_xdb(scope: AgentSkillStateXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_skill_state_xdb(scope);
  return xdb._state;
}

export async function list_skill_state_xdb(scope: AgentSkillStateXdbScope): Promise<PersistedSkillStateRecord[]> {
  const xdb = await ensure_skill_state_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(xdb._state.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000), "list_skill_state")
    ._data;
  const out: PersistedSkillStateRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_skill_state(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_skill_state_xdb(
  scope: AgentSkillStateXdbScope,
  ukey: string
): Promise<PersistedSkillStateRecord | undefined> {
  const xdb = await ensure_skill_state_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._state.find({ _app_id: scope._app_id, _env: scope._env, _ukey: ukey }, 0, 1),
    "find_skill_state"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_skill_state(rows[0]);
}

export async function upsert_skill_state_xdb(
  scope: AgentSkillStateXdbScope,
  record: PersistedSkillStateRecord
): Promise<void> {
  const xdb = await ensure_skill_state_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _ukey: record._ukey };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._state.find(filter, 0, 1), "find_skill_state")._data;

  const row = {
    _id: record._ukey,
    _app_id: scope._app_id,
    _env: scope._env,
    _ukey: record._ukey,
    _skill_id: record._skill_id,
    _key: record._key,
    _value: { ...record._value }
  };

  if (existing.length > 0) {
    await xdb._state.update(filter, row, true);
    return;
  }

  await xdb._state.add(row, true, true);
}
