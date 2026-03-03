import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const KB_SOURCES_ENTITY_ID = "agent.kb_sources";
export const KB_DOCS_ENTITY_ID = "agent.kb_docs";

export type AgentKbXdbScope = {
  _app_id: string;
  _env: string;
};

export type PersistedKbSourceRecord = {
  _source_id: string;
  _title: string;
  _enabled: boolean;
  _paths: string[];
  _lang?: string;
  _notes?: string;
  _created_at: number;
  _updated_at: number;
};

export type PersistedKbDocRecord = {
  _key: string;
  _source_id: string;
  _doc_id: string;
  _path: string;
  _title?: string;
  _lang?: string;
  _content: string;
  _content_sha1: string;
  _created_at: number;
  _updated_at: number;
};

type KbXdb = {
  _sources: XDBEntity;
  _docs: XDBEntity;
};

const _xdb_by_scope = new Map<string, KbXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<KbXdb>>();

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

function normalize_string_array(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  for (const item of value) {
    const text = to_text(item);
    if (!text) continue;
    out.push(text);
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
    throw new Error(`[agent-kb:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentKbXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function kb_sources_entity_name(scope: AgentKbXdbScope): string {
  return `${KB_SOURCES_ENTITY_ID}::${scope_key(scope)}`;
}

export function kb_docs_entity_name(scope: AgentKbXdbScope): string {
  return `${KB_DOCS_ENTITY_ID}::${scope_key(scope)}`;
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

async function ensure_kb_xdb(scope: AgentKbXdbScope): Promise<KbXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const sources = XDB.create({
      _type: "xdb-entity",
      _name: kb_sources_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _source_id: { _type: "String", _required: true, _index: true },
        _title: { _type: "String", _required: true },
        _enabled: { _type: "Boolean", _required: true },
        _paths: { _type: "Array", _required: true },
        _lang: { _type: "String" },
        _notes: { _type: "String" }
      }
    }) as XDBEntity;

    const docs = XDB.create({
      _type: "xdb-entity",
      _name: kb_docs_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _key: { _type: "String", _required: true, _index: true },
        _source_id: { _type: "String", _required: true, _index: true },
        _doc_id: { _type: "String", _required: true, _index: true },
        _path: { _type: "String", _required: true },
        _title: { _type: "String" },
        _lang: { _type: "String" },
        _content: { _type: "String", _required: true },
        _content_sha1: { _type: "String", _required: true, _index: true }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(sources);
    await ensure_entity_loaded(docs);

    const out = { _sources: sources, _docs: docs };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-kb:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_kb_source(value: unknown): PersistedKbSourceRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _source_id = to_text(value._source_id || value._id);
  const _title = to_text(value._title);
  if (!_source_id || !_title) return undefined;

  return {
    _source_id,
    _title,
    _enabled: value._enabled !== false,
    _paths: normalize_string_array(value._paths),
    ...(to_text(value._lang) ? { _lang: to_text(value._lang) } : {}),
    ...(to_text(value._notes) ? { _notes: to_text(value._notes) } : {}),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

function normalize_kb_doc(value: unknown): PersistedKbDocRecord | undefined {
  if (!is_plain_object(value)) return undefined;

  const _key = to_text(value._key || value._id);
  const _source_id = to_text(value._source_id);
  const _doc_id = to_text(value._doc_id);
  const _path = to_text(value._path);
  const _content = typeof value._content === "string" ? value._content : "";
  const _content_sha1 = to_text(value._content_sha1);
  if (!_key || !_source_id || !_doc_id || !_path || !_content || !_content_sha1) return undefined;

  return {
    _key,
    _source_id,
    _doc_id,
    _path,
    ...(to_text(value._title) ? { _title: to_text(value._title) } : {}),
    ...(to_text(value._lang) ? { _lang: to_text(value._lang) } : {}),
    _content,
    _content_sha1,
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export function build_kb_doc_key(source_id: string, doc_id: string): string {
  return `${source_id}:${doc_id}`;
}

export async function init_kb_xdb(scope: AgentKbXdbScope): Promise<void> {
  await ensure_kb_xdb(scope);
}

export async function create_kb_sources_entity_xdb(scope: AgentKbXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_kb_xdb(scope);
  return xdb._sources;
}

export async function create_kb_docs_entity_xdb(scope: AgentKbXdbScope): Promise<XDBEntity> {
  const xdb = await ensure_kb_xdb(scope);
  return xdb._docs;
}

export async function list_kb_sources_xdb(scope: AgentKbXdbScope): Promise<PersistedKbSourceRecord[]> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(xdb._sources.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000), "list_kb_sources")
    ._data;
  const out: PersistedKbSourceRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_kb_source(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_kb_source_xdb(
  scope: AgentKbXdbScope,
  source_id: string
): Promise<PersistedKbSourceRecord | undefined> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._sources.find({ _app_id: scope._app_id, _env: scope._env, _source_id: source_id }, 0, 1),
    "find_kb_source"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_kb_source(rows[0]);
}

export async function upsert_kb_source_xdb(scope: AgentKbXdbScope, record: PersistedKbSourceRecord): Promise<void> {
  const xdb = await ensure_kb_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _source_id: record._source_id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._sources.find(filter, 0, 1), "find_kb_source")._data;

  const row = {
    _id: record._source_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _source_id: record._source_id,
    _title: record._title,
    _enabled: record._enabled,
    _paths: [...record._paths],
    _lang: record._lang ?? "",
    _notes: record._notes ?? ""
  };

  if (existing.length > 0) {
    await xdb._sources.update(filter, row, true);
    return;
  }

  await xdb._sources.add(row, true, true);
}

export async function list_kb_docs_xdb(scope: AgentKbXdbScope, source_id?: string): Promise<PersistedKbDocRecord[]> {
  const xdb = await ensure_kb_xdb(scope);
  const filter: Record<string, unknown> = { _app_id: scope._app_id, _env: scope._env };
  if (source_id) filter._source_id = source_id;
  const rows = ensure_xdb_result<{ _data: unknown[] }>(xdb._docs.find(filter, 0, 200000), "list_kb_docs")._data;
  const out: PersistedKbDocRecord[] = [];
  for (const row of rows) {
    const normalized = normalize_kb_doc(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function get_kb_doc_xdb(scope: AgentKbXdbScope, key: string): Promise<PersistedKbDocRecord | undefined> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._docs.find({ _app_id: scope._app_id, _env: scope._env, _key: key }, 0, 1),
    "find_kb_doc"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_kb_doc(rows[0]);
}

export async function upsert_kb_doc_xdb(scope: AgentKbXdbScope, record: PersistedKbDocRecord): Promise<void> {
  const xdb = await ensure_kb_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _key: record._key };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._docs.find(filter, 0, 1), "find_kb_doc")._data;

  const row = {
    _id: record._key,
    _app_id: scope._app_id,
    _env: scope._env,
    _key: record._key,
    _source_id: record._source_id,
    _doc_id: record._doc_id,
    _path: record._path,
    _title: record._title ?? "",
    _lang: record._lang ?? "",
    _content: record._content,
    _content_sha1: record._content_sha1
  };

  if (existing.length > 0) {
    await xdb._docs.update(filter, row, true);
    return;
  }

  await xdb._docs.add(row, true, true);
}
