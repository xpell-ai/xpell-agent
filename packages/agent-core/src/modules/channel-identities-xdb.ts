import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const CHANNEL_IDENTITIES_ENTITY_ID = "agent.channel_identities";

export type AgentChannelIdentitiesXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedChannelIdentityRecord = {
  _key: string;
  _channel_id: string;
  _external_user_id: string;
  _user_id: string;
  _display_name?: string;
  _meta?: Record<string, unknown>;
  _created_at: number;
  _updated_at: number;
};

type ChannelIdentitiesXdb = {
  _identities: XDBEntity;
};

const _xdb_by_scope = new Map<string, ChannelIdentitiesXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<ChannelIdentitiesXdb>>();

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
    throw new Error(`[agent-channel-identities:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentChannelIdentitiesXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function channel_identities_entity_name(scope: AgentChannelIdentitiesXdbScope): string {
  return `${CHANNEL_IDENTITIES_ENTITY_ID}::${scope_key(scope)}`;
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

async function ensure_channel_identities_xdb(scope: AgentChannelIdentitiesXdbScope): Promise<ChannelIdentitiesXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const identities = XDB.create({
      _type: "xdb-entity",
      _name: channel_identities_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _key: { _type: "String", _required: true, _index: true },
        _channel_id: { _type: "String", _required: true, _index: true },
        _external_user_id: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _display_name: { _type: "String" },
        _meta: { _type: "Object" }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(identities);

    const out = { _identities: identities };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-channel-identities:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_channel_identity(value: unknown): PersistedChannelIdentityRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _key = to_text(value._key || value._id);
  const _channel_id = to_text(value._channel_id).toLowerCase();
  const _external_user_id = to_text(value._external_user_id);
  const _user_id = to_text(value._user_id);
  if (!_key || !_channel_id || !_external_user_id || !_user_id) return undefined;

  return {
    _key,
    _channel_id,
    _external_user_id,
    _user_id,
    ...(to_text(value._display_name) ? { _display_name: to_text(value._display_name) } : {}),
    ...(is_plain_object(value._meta) ? { _meta: { ...value._meta } } : {}),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export function build_channel_identity_key(channel_id: string, external_user_id: string): string {
  return `${channel_id}:${external_user_id}`;
}

export async function init_channel_identities_xdb(scope: AgentChannelIdentitiesXdbScope): Promise<void> {
  await ensure_channel_identities_xdb(scope);
}

export async function create_channel_identities_entity_xdb(scope: AgentChannelIdentitiesXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_channel_identities_xdb(scope);
  return xdb._identities;
}

export async function list_channel_identities_xdb(
  scope: AgentChannelIdentitiesXdbScope
): Promise<PersistedChannelIdentityRecord[]> {
  const xdb = await ensure_channel_identities_xdb(scope);
  const res = xdb._identities.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_channel_identities")._data;

  const out: PersistedChannelIdentityRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_channel_identity(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_channel_identity_xdb(
  scope: AgentChannelIdentitiesXdbScope,
  key: string
): Promise<PersistedChannelIdentityRecord | undefined> {
  const xdb = await ensure_channel_identities_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._identities.find({ _app_id: scope._app_id, _env: scope._env, _key: key }, 0, 1),
    "find_channel_identity"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_channel_identity(rows[0]);
}

export async function upsert_channel_identity_xdb(
  scope: AgentChannelIdentitiesXdbScope,
  record: PersistedChannelIdentityRecord
): Promise<void> {
  const xdb = await ensure_channel_identities_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _key: record._key };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._identities.find(filter, 0, 1), "find_channel_identity")
    ._data;

  const row = {
    _id: record._key,
    _app_id: scope._app_id,
    _env: scope._env,
    _key: record._key,
    _channel_id: record._channel_id,
    _external_user_id: record._external_user_id,
    _user_id: record._user_id,
    _display_name: record._display_name ?? "",
    _meta: is_plain_object(record._meta) ? { ...record._meta } : {}
  };

  if (existing.length > 0) {
    await xdb._identities.update(filter, row, true);
    return;
  }

  await xdb._identities.add(row, true, true);
}
