import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export type AgentChannelsXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedChannelRegistration = {
  channel: string;
  connector_module: string;
  config: Record<string, unknown>;
  created_at: number;
  updated_at: number;
};

type ChannelsXdb = {
  _registrations: XDBEntity;
};

const ENTITY_REGISTRATIONS_PREFIX = "agent.channels";

const _xdb_by_scope = new Map<string, ChannelsXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<ChannelsXdb>>();

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

function to_json(value: unknown): string {
  return JSON.stringify(value ?? {});
}

function parse_json(raw: unknown): unknown {
  if (typeof raw !== "string" || !raw.trim()) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
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
    throw new Error(`[agent-channels:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentChannelsXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

function registrations_entity_name(scope: AgentChannelsXdbScope): string {
  return `${ENTITY_REGISTRATIONS_PREFIX}::${scope_key(scope)}`;
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

async function ensure_channels_xdb(scope: AgentChannelsXdbScope): Promise<ChannelsXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const registrations = XDB.create({
      _type: "xdb-entity",
      _name: registrations_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _channel: { _type: "String", _required: true, _index: true },
        _connector_module: { _type: "String", _required: true },
        _config_json: { _type: "String" },
        _created_at: { _type: "Number", _required: true, _index: true },
        _updated_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(registrations);

    const out = { _registrations: registrations };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-channels:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_registration(value: unknown): PersistedChannelRegistration | undefined {
  if (!is_plain_object(value)) return undefined;

  const channel = to_text(value._channel || value._id).toLowerCase();
  const connector_module = to_text(value._connector_module);
  if (!channel || !connector_module) return undefined;

  const config = parse_json(value._config_json);
  return {
    channel,
    connector_module,
    config: is_plain_object(config) ? config : {},
    created_at: to_ts(value._created_at),
    updated_at: to_ts(value._updated_at)
  };
}

export async function init_channels_xdb(scope: AgentChannelsXdbScope): Promise<void> {
  await ensure_channels_xdb(scope);
}

export async function list_registrations_xdb(scope: AgentChannelsXdbScope): Promise<PersistedChannelRegistration[]> {
  const xdb = await ensure_channels_xdb(scope);
  const res = xdb._registrations.find({ _app_id: scope._app_id, _env: scope._env }, 0, 10000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_registrations")._data;

  const out: PersistedChannelRegistration[] = [];
  for (const row of rows) {
    const normalized = normalize_registration(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function upsert_registration_xdb(
  scope: AgentChannelsXdbScope,
  record: PersistedChannelRegistration
): Promise<void> {
  const xdb = await ensure_channels_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _channel: record.channel };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._registrations.find(filter, 0, 1),
    "find_registration"
  )._data;

  const row = {
    _id: record.channel,
    _app_id: scope._app_id,
    _env: scope._env,
    _channel: record.channel,
    _connector_module: record.connector_module,
    _config_json: to_json(record.config),
    _created_at: record.created_at,
    _updated_at: record.updated_at
  };

  if (existing.length > 0) {
    await xdb._registrations.update(filter, row, true);
    return;
  }

  await xdb._registrations.add(row, true, true);
}
