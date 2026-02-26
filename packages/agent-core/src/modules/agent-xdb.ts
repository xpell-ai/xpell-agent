import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export type AgentTaskXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedTaskRecord = {
  _task_id: string;
  _task_type: string;
  _payload: unknown;
  _created_at: number;
};

type AgentTaskXdb = {
  _tasks: XDBEntity;
};

const ENTITY_TASKS_PREFIX = "agent.tasks";

const _xdb_by_scope = new Map<string, AgentTaskXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<AgentTaskXdb>>();

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
  return JSON.stringify(value ?? null);
}

function parse_json(raw: unknown): unknown {
  if (typeof raw !== "string" || !raw.trim()) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
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
    throw new Error(`[agent-task:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentTaskXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

function tasks_entity_name(scope: AgentTaskXdbScope): string {
  return `${ENTITY_TASKS_PREFIX}::${scope_key(scope)}`;
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

async function ensure_task_xdb(scope: AgentTaskXdbScope): Promise<AgentTaskXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const tasks = XDB.create({
      _type: "xdb-entity",
      _name: tasks_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _task_id: { _type: "String", _required: true, _index: true },
        _task_type: { _type: "String", _required: true, _index: true },
        _payload_json: { _type: "String" },
        _created_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(tasks);

    const out = { _tasks: tasks };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-task:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_task(value: unknown): PersistedTaskRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const task_id = to_text(value._task_id || value._id);
  const task_type = to_text(value._task_type);
  if (!task_id || !task_type) return undefined;

  return {
    _task_id: task_id,
    _task_type: task_type,
    _payload: parse_json(value._payload_json),
    _created_at: to_ts(value._created_at)
  };
}

export async function init_agent_tasks_xdb(scope: AgentTaskXdbScope): Promise<void> {
  await ensure_task_xdb(scope);
}

export async function list_tasks_xdb(scope: AgentTaskXdbScope): Promise<PersistedTaskRecord[]> {
  const xdb = await ensure_task_xdb(scope);
  const res = xdb._tasks.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_tasks")._data;

  const out: PersistedTaskRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_task(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function add_task_xdb(scope: AgentTaskXdbScope, record: PersistedTaskRecord): Promise<void> {
  const xdb = await ensure_task_xdb(scope);
  const row = {
    _id: record._task_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _task_id: record._task_id,
    _task_type: record._task_type,
    _payload_json: to_json(record._payload),
    _created_at: record._created_at
  };
  await xdb._tasks.add(row, true, true);
}
