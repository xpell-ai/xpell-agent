import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export type AgentBroadcastsXdbScope = {
  _app_id: string;
  _env: string;
};

export type BroadcastStatus = "draft" | "queued" | "sending" | "done" | "failed";
export type BroadcastChannel = "telegram" | "all";
export type BroadcastAudienceRole = "customer" | "all";
export type BroadcastDeliveryStatus = "queued" | "sent" | "failed";

export type PersistedBroadcastRecord = {
  _id: string;
  _app_id: string;
  _env: string;
  _status: BroadcastStatus;
  _channel: BroadcastChannel;
  _audience_role: BroadcastAudienceRole;
  _message_text: string;
  _created_at: number;
  _updated_at: number;
  _created_by_user_id: string;
  _stats: {
    total: number;
    sent: number;
    failed: number;
  };
};

export type PersistedBroadcastDeliveryRecord = {
  _id: string;
  _broadcast_id: string;
  _user_id: string;
  _channel: string;
  _status: BroadcastDeliveryStatus;
  _created_at: number;
  _updated_at: number;
  _thread_id?: string;
  _error?: string;
};

type BroadcastsXdb = {
  _broadcasts: XDBEntity;
  _deliveries: XDBEntity;
};

const ENTITY_BROADCASTS_PREFIX = "agent.broadcasts";
const ENTITY_DELIVERIES_PREFIX = "agent.broadcast_deliveries";

const _xdb_by_scope = new Map<string, BroadcastsXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<BroadcastsXdb>>();

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

function to_non_negative_int(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return Math.max(0, Math.floor(value));
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) return 0;
  return Math.floor(parsed);
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
    throw new Error(`[agent-broadcasts:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentBroadcastsXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function broadcasts_entity_name(scope: AgentBroadcastsXdbScope): string {
  return `${ENTITY_BROADCASTS_PREFIX}::${scope_key(scope)}`;
}

export function broadcast_deliveries_entity_name(scope: AgentBroadcastsXdbScope): string {
  return `${ENTITY_DELIVERIES_PREFIX}::${scope_key(scope)}`;
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

async function ensure_broadcasts_xdb(scope: AgentBroadcastsXdbScope): Promise<BroadcastsXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const broadcasts = XDB.create({
      _type: "xdb-entity",
      _name: broadcasts_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _status: { _type: "String", _required: true, _index: true },
        _channel: { _type: "String", _required: true, _index: true },
        _audience_role: { _type: "String", _required: true, _index: true },
        _message_text: { _type: "String", _required: true },
        _created_by_user_id: { _type: "String", _required: true, _index: true },
        _stats: { _type: "Object" }
      }
    }) as XDBEntity;

    const deliveries = XDB.create({
      _type: "xdb-entity",
      _name: broadcast_deliveries_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _broadcast_id: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _channel: { _type: "String", _required: true, _index: true },
        _status: { _type: "String", _required: true, _index: true },
        _thread_id: { _type: "String" },
        _error: { _type: "String" }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(broadcasts);
    await ensure_entity_loaded(deliveries);

    const out = { _broadcasts: broadcasts, _deliveries: deliveries };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-broadcasts:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_stats(value: unknown): PersistedBroadcastRecord["_stats"] {
  const obj = is_plain_object(value) ? value : {};
  return {
    total: to_non_negative_int(obj.total),
    sent: to_non_negative_int(obj.sent),
    failed: to_non_negative_int(obj.failed)
  };
}

function normalize_broadcast(value: unknown): PersistedBroadcastRecord | undefined {
  if (!is_plain_object(value)) return undefined;
  const _id = to_text(value._id);
  const _status = to_text(value._status).toLowerCase() as BroadcastStatus;
  const _channel = to_text(value._channel).toLowerCase() as BroadcastChannel;
  const _audience_role = to_text(value._audience_role).toLowerCase() as BroadcastAudienceRole;
  const _message_text = to_text(value._message_text);
  const _created_by_user_id = to_text(value._created_by_user_id);
  if (!_id || !_status || !_channel || !_audience_role || !_message_text || !_created_by_user_id) return undefined;
  return {
    _id,
    _app_id: to_text(value._app_id),
    _env: to_text(value._env),
    _status,
    _channel,
    _audience_role,
    _message_text,
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at),
    _created_by_user_id,
    _stats: normalize_stats(value._stats)
  };
}

function normalize_delivery(value: unknown): PersistedBroadcastDeliveryRecord | undefined {
  if (!is_plain_object(value)) return undefined;
  const _id = to_text(value._id);
  const _broadcast_id = to_text(value._broadcast_id);
  const _user_id = to_text(value._user_id);
  const _channel = to_text(value._channel);
  const _status = to_text(value._status).toLowerCase() as BroadcastDeliveryStatus;
  if (!_id || !_broadcast_id || !_user_id || !_channel || !_status) return undefined;
  return {
    _id,
    _broadcast_id,
    _user_id,
    _channel,
    _status,
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at),
    ...(to_text(value._thread_id) ? { _thread_id: to_text(value._thread_id) } : {}),
    ...(to_text(value._error) ? { _error: to_text(value._error) } : {})
  };
}

export async function init_broadcasts_xdb(scope: AgentBroadcastsXdbScope): Promise<void> {
  await ensure_broadcasts_xdb(scope);
}

export async function save_broadcast_xdb(scope: AgentBroadcastsXdbScope, record: PersistedBroadcastRecord): Promise<void> {
  const xdb = await ensure_broadcasts_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _id: record._id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._broadcasts.find(filter, 0, 1), "find_broadcast")._data;
  const row = {
    _id: record._id,
    _app_id: scope._app_id,
    _env: scope._env,
    _status: record._status,
    _channel: record._channel,
    _audience_role: record._audience_role,
    _message_text: record._message_text,
    _created_by_user_id: record._created_by_user_id,
    _stats: {
      total: to_non_negative_int(record._stats.total),
      sent: to_non_negative_int(record._stats.sent),
      failed: to_non_negative_int(record._stats.failed)
    }
  };
  if (existing.length > 0) {
    await xdb._broadcasts.update(filter, row, true);
    return;
  }
  await xdb._broadcasts.add(row, true, true);
}

export async function get_broadcast_xdb(
  scope: AgentBroadcastsXdbScope,
  broadcast_id: string
): Promise<PersistedBroadcastRecord | undefined> {
  const xdb = await ensure_broadcasts_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._broadcasts.find({ _app_id: scope._app_id, _env: scope._env, _id: broadcast_id }, 0, 1),
    "get_broadcast"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_broadcast(rows[0]);
}

export async function list_broadcasts_xdb(
  scope: AgentBroadcastsXdbScope,
  skip = 0,
  limit = 20
): Promise<PersistedBroadcastRecord[]> {
  const xdb = await ensure_broadcasts_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._broadcasts.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000),
    "list_broadcasts"
  )._data;
  const out: PersistedBroadcastRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_broadcast(row);
    if (normalized) out.push(normalized);
  }
  out.sort((left, right) => (left._created_at !== right._created_at ? right._created_at - left._created_at : left._id.localeCompare(right._id)));
  const safe_skip = Number.isFinite(skip) && skip > 0 ? Math.floor(skip) : 0;
  const safe_limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 20;
  return out.slice(safe_skip, safe_skip + safe_limit);
}

export async function save_broadcast_delivery_xdb(
  scope: AgentBroadcastsXdbScope,
  record: PersistedBroadcastDeliveryRecord
): Promise<void> {
  const xdb = await ensure_broadcasts_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _id: record._id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._deliveries.find(filter, 0, 1), "find_delivery")._data;
  const row = {
    _id: record._id,
    _app_id: scope._app_id,
    _env: scope._env,
    _broadcast_id: record._broadcast_id,
    _user_id: record._user_id,
    _channel: record._channel,
    _status: record._status,
    _thread_id: record._thread_id ?? "",
    _error: record._error ?? ""
  };
  if (existing.length > 0) {
    await xdb._deliveries.update(filter, row, true);
    return;
  }
  await xdb._deliveries.add(row, true, true);
}

export async function list_broadcast_deliveries_xdb(
  scope: AgentBroadcastsXdbScope,
  broadcast_id: string
): Promise<PersistedBroadcastDeliveryRecord[]> {
  const xdb = await ensure_broadcasts_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._deliveries.find({ _app_id: scope._app_id, _env: scope._env, _broadcast_id: broadcast_id }, 0, 100000),
    "list_deliveries"
  )._data;
  const out: PersistedBroadcastDeliveryRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_delivery(row);
    if (normalized) out.push(normalized);
  }
  out.sort((left, right) => (left._created_at !== right._created_at ? left._created_at - right._created_at : left._id.localeCompare(right._id)));
  return out;
}
