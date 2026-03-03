import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const CONV_PARTICIPANTS_ENTITY_ID = "agent.conv_participants";

export type AgentConvParticipantsXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedConvParticipantRecord = {
  _key: string;
  _thread_id: string;
  _user_id: string;
  _role: "owner" | "admin" | "customer" | "system";
  _channel_id?: string;
  _last_seen_at?: number;
  _created_at: number;
  _updated_at: number;
};

type ConvParticipantsXdb = {
  _participants: XDBEntity;
};

const _xdb_by_scope = new Map<string, ConvParticipantsXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<ConvParticipantsXdb>>();

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
    throw new Error(`[agent-conv-participants:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentConvParticipantsXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function conv_participants_entity_name(scope: AgentConvParticipantsXdbScope): string {
  return `${CONV_PARTICIPANTS_ENTITY_ID}::${scope_key(scope)}`;
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

async function ensure_conv_participants_xdb(scope: AgentConvParticipantsXdbScope): Promise<ConvParticipantsXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const participants = XDB.create({
      _type: "xdb-entity",
      _name: conv_participants_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _key: { _type: "String", _required: true, _index: true },
        _thread_id: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _role: { _type: "String", _required: true },
        _channel_id: { _type: "String", _index: true },
        _last_seen_at: { _type: "Date" }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(participants);

    const out = { _participants: participants };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-conv-participants:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_participant(value: unknown): PersistedConvParticipantRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _key = to_text(value._key || value._id);
  const _thread_id = to_text(value._thread_id);
  const _user_id = to_text(value._user_id);
  const role = to_text(value._role);
  if (!_key || !_thread_id || !_user_id) return undefined;
  if (role !== "owner" && role !== "admin" && role !== "customer" && role !== "system") return undefined;

  return {
    _key,
    _thread_id,
    _user_id,
    _role: role,
    ...(to_text(value._channel_id) ? { _channel_id: to_text(value._channel_id) } : {}),
    ...(value._last_seen_at !== undefined && value._last_seen_at !== null ? { _last_seen_at: to_ts(value._last_seen_at) } : {}),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export function build_conv_participant_key(thread_id: string, user_id: string): string {
  return `${thread_id}:${user_id}`;
}

export async function init_conv_participants_xdb(scope: AgentConvParticipantsXdbScope): Promise<void> {
  await ensure_conv_participants_xdb(scope);
}

export async function create_conv_participants_entity_xdb(scope: AgentConvParticipantsXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_conv_participants_xdb(scope);
  return xdb._participants;
}

export async function list_conv_participants_xdb(
  scope: AgentConvParticipantsXdbScope,
  thread_id?: string
): Promise<PersistedConvParticipantRecord[]> {
  const xdb = await ensure_conv_participants_xdb(scope);
  const filter: Record<string, unknown> = { _app_id: scope._app_id, _env: scope._env };
  if (thread_id) filter._thread_id = thread_id;
  const rows = ensure_xdb_result<{ _data: unknown[] }>(xdb._participants.find(filter, 0, 100000), "list_conv_participants")
    ._data;

  const out: PersistedConvParticipantRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_participant(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_conv_participant_xdb(
  scope: AgentConvParticipantsXdbScope,
  key: string
): Promise<PersistedConvParticipantRecord | undefined> {
  const xdb = await ensure_conv_participants_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._participants.find({ _app_id: scope._app_id, _env: scope._env, _key: key }, 0, 1),
    "find_conv_participant"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_participant(rows[0]);
}

export async function upsert_conv_participant_xdb(
  scope: AgentConvParticipantsXdbScope,
  record: PersistedConvParticipantRecord
): Promise<void> {
  const xdb = await ensure_conv_participants_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _key: record._key };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._participants.find(filter, 0, 1), "find_conv_participant")
    ._data;

  const row = {
    _id: record._key,
    _app_id: scope._app_id,
    _env: scope._env,
    _key: record._key,
    _thread_id: record._thread_id,
    _user_id: record._user_id,
    _role: record._role,
    _channel_id: record._channel_id ?? "",
    ...(record._last_seen_at !== undefined ? { _last_seen_at: new Date(record._last_seen_at).toISOString() } : {})
  };

  if (existing.length > 0) {
    await xdb._participants.update(filter, row, true);
    return;
  }

  await xdb._participants.add(row, true, true);
}

export async function delete_conv_participant_xdb(scope: AgentConvParticipantsXdbScope, key: string): Promise<void> {
  const xdb = await ensure_conv_participants_xdb(scope);
  await xdb._participants.delete({ _app_id: scope._app_id, _env: scope._env, _key: key }, true);
}
