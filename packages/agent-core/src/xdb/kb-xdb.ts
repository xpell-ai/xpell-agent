import { randomUUID } from "node:crypto";

import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

type Dict = Record<string, unknown>;

export const KB_DOCS_ENTITY_ID = "agent.kb_docs";
export const KB_AUDIT_ENTITY_ID = "agent.kb_audit";
export const ADMIN_PENDING_ACTIONS_ENTITY_ID = "agent.admin_pending_actions";

export type AgentKbXdbScope = {
  _app_id: string;
  _env: string;
};

export type KbActor = {
  _actor_user_id: string;
  _actor_role: "admin" | "owner";
};

export type PersistedKbDocRecord = {
  _kb_id: string;
  _lang: string;
  _format: "md";
  _content: string;
  _source?: string;
  _created_at: number;
  _updated_at: number;
};

export type PersistedKbAuditRecord = {
  _audit_id: string;
  _actor_user_id: string;
  _actor_role: string;
  _op: string;
  _kb_id: string;
  _lang: string;
  _summary?: string;
  _diff_preview?: string;
  _payload_json?: string;
  _created_at: number;
};

export type PersistedAdminPendingActionRecord = {
  _action_id: string;
  _actor_user_id: string;
  _kind: string;
  _kb_id: string;
  _lang: string;
  _proposal_json: string;
  _expires_at: number;
  _created_at: number;
};

type KbXdb = {
  _docs: XDBEntity;
  _audit: XDBEntity;
  _pending: XDBEntity;
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

function to_number(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return Math.floor(value);
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
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
    throw new Error(`[agent-kb:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentKbXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

function kb_docs_entity_name(scope: AgentKbXdbScope): string {
  return `${KB_DOCS_ENTITY_ID}::${scope_key(scope)}`;
}

function kb_audit_entity_name(scope: AgentKbXdbScope): string {
  return `${KB_AUDIT_ENTITY_ID}::${scope_key(scope)}`;
}

function admin_pending_entity_name(scope: AgentKbXdbScope): string {
  return `${ADMIN_PENDING_ACTIONS_ENTITY_ID}::${scope_key(scope)}`;
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

    const docs = XDB.create({
      _type: "xdb-entity",
      _name: kb_docs_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _kb_id: { _type: "String", _required: true, _index: true },
        _lang: { _type: "String", _required: true, _index: true },
        _format: { _type: "String", _required: true },
        _content: { _type: "String", _required: true },
        _source: { _type: "String" },
        _created_at: { _type: "Number", _required: true, _index: true },
        _updated_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    const audit = XDB.create({
      _type: "xdb-entity",
      _name: kb_audit_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _audit_id: { _type: "String", _required: true, _index: true },
        _actor_user_id: { _type: "String", _required: true, _index: true },
        _actor_role: { _type: "String", _required: true, _index: true },
        _op: { _type: "String", _required: true, _index: true },
        _kb_id: { _type: "String", _required: true, _index: true },
        _lang: { _type: "String", _required: true, _index: true },
        _summary: { _type: "String" },
        _diff_preview: { _type: "String" },
        _payload_json: { _type: "String" },
        _created_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    const pending = XDB.create({
      _type: "xdb-entity",
      _name: admin_pending_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _action_id: { _type: "String", _required: true, _index: true },
        _actor_user_id: { _type: "String", _required: true, _index: true },
        _kind: { _type: "String", _required: true, _index: true },
        _kb_id: { _type: "String", _required: true },
        _lang: { _type: "String", _required: true },
        _proposal_json: { _type: "String", _required: true },
        _expires_at: { _type: "Number", _required: true, _index: true },
        _created_at: { _type: "Number", _required: true, _index: true }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(docs);
    await ensure_entity_loaded(audit);
    await ensure_entity_loaded(pending);

    const out = { _docs: docs, _audit: audit, _pending: pending };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-kb:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_doc_record(value: unknown): PersistedKbDocRecord | undefined {
  if (!is_plain_object(value)) return undefined;
  const _kb_id = to_text(value._kb_id);
  const _lang = to_text(value._lang);
  const _format = to_text(value._format) || "md";
  if (!_kb_id || !_lang) return undefined;
  return {
    _kb_id,
    _lang,
    _format: _format === "md" ? "md" : "md",
    _content: typeof value._content === "string" ? value._content : "",
    ...(to_text(value._source) ? { _source: to_text(value._source) } : {}),
    _created_at: to_number(value._created_at, Date.now()),
    _updated_at: to_number(value._updated_at, Date.now())
  };
}

function normalize_audit_record(value: unknown): PersistedKbAuditRecord | undefined {
  if (!is_plain_object(value)) return undefined;
  const _audit_id = to_text(value._audit_id || value._id);
  const _actor_user_id = to_text(value._actor_user_id);
  const _actor_role = to_text(value._actor_role);
  const _op = to_text(value._op);
  const _kb_id = to_text(value._kb_id);
  const _lang = to_text(value._lang);
  if (!_audit_id || !_actor_user_id || !_actor_role || !_op || !_kb_id || !_lang) return undefined;
  return {
    _audit_id,
    _actor_user_id,
    _actor_role,
    _op,
    _kb_id,
    _lang,
    ...(to_text(value._summary) ? { _summary: to_text(value._summary) } : {}),
    ...(to_text(value._diff_preview) ? { _diff_preview: to_text(value._diff_preview) } : {}),
    ...(to_text(value._payload_json) ? { _payload_json: to_text(value._payload_json) } : {}),
    _created_at: to_number(value._created_at, Date.now())
  };
}

function normalize_pending_record(value: unknown): PersistedAdminPendingActionRecord | undefined {
  if (!is_plain_object(value)) return undefined;
  const _action_id = to_text(value._action_id || value._id);
  const _actor_user_id = to_text(value._actor_user_id);
  const _kind = to_text(value._kind);
  const _kb_id = to_text(value._kb_id);
  const _lang = to_text(value._lang);
  const _proposal_json = to_text(value._proposal_json);
  if (!_action_id || !_actor_user_id || !_kind || !_kb_id || !_lang || !_proposal_json) return undefined;
  return {
    _action_id,
    _actor_user_id,
    _kind,
    _kb_id,
    _lang,
    _proposal_json,
    _expires_at: to_number(value._expires_at, 0),
    _created_at: to_number(value._created_at, Date.now())
  };
}

function build_diff_preview(previous: string, next: string): string {
  if (previous === next) return "(no-op)";
  const prev = previous.trim();
  const curr = next.trim();
  const prev_head = prev.length > 240 ? `${prev.slice(0, 240)}...` : prev;
  const next_head = curr.length > 240 ? `${curr.slice(0, 240)}...` : curr;
  return `- ${prev_head || "(empty)"}\n+ ${next_head || "(empty)"}`;
}

function safe_json_stringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return "{}";
  }
}

async function add_kb_audit_xdb(
  scope: AgentKbXdbScope,
  record: PersistedKbAuditRecord
): Promise<void> {
  const xdb = await ensure_kb_xdb(scope);
  await xdb._audit.add(
    {
      _id: record._audit_id,
      _audit_id: record._audit_id,
      _app_id: scope._app_id,
      _env: scope._env,
      _actor_user_id: record._actor_user_id,
      _actor_role: record._actor_role,
      _op: record._op,
      _kb_id: record._kb_id,
      _lang: record._lang,
      _summary: record._summary ?? "",
      _diff_preview: record._diff_preview ?? "",
      _payload_json: record._payload_json ?? "",
      _created_at: record._created_at
    },
    true,
    true
  );
}

export async function init_kb_xdb(scope: AgentKbXdbScope): Promise<void> {
  await ensure_kb_xdb(scope);
}

export async function kb_get_doc(
  scope: AgentKbXdbScope,
  kb_id: string,
  lang: string
): Promise<PersistedKbDocRecord | undefined> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._docs.find({ _app_id: scope._app_id, _env: scope._env, _kb_id: kb_id, _lang: lang }, 0, 1),
    "kb_get_doc"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_doc_record(rows[0]);
}

export async function kb_set_doc(
  scope: AgentKbXdbScope,
  kb_id: string,
  lang: string,
  content: string,
  source: string,
  actor: KbActor,
  meta?: { _op?: string; _summary?: string; _payload?: unknown; _previous_content?: string }
): Promise<PersistedKbDocRecord> {
  const xdb = await ensure_kb_xdb(scope);
  const existing = await kb_get_doc(scope, kb_id, lang);
  const now = Date.now();
  const row = {
    _app_id: scope._app_id,
    _env: scope._env,
    _kb_id: kb_id,
    _lang: lang,
    _format: "md",
    _content: content,
    _source: source,
    _created_at: existing?._created_at ?? now,
    _updated_at: now
  };
  const filter = { _app_id: scope._app_id, _env: scope._env, _kb_id: kb_id, _lang: lang };
  if (existing) {
    await xdb._docs.update(filter, row, true);
  } else {
    await xdb._docs.add({ _id: `${kb_id}:${lang}`, ...row }, true, true);
  }

  await add_kb_audit_xdb(scope, {
    _audit_id: `kbaud_${randomUUID().replace(/-/g, "")}`,
    _actor_user_id: actor._actor_user_id,
    _actor_role: actor._actor_role,
    _op: meta?._op ?? "kb.set",
    _kb_id: kb_id,
    _lang: lang,
    ...(meta?._summary ? { _summary: meta._summary } : {}),
    _diff_preview: build_diff_preview(meta?._previous_content ?? existing?._content ?? "", content),
    _payload_json: safe_json_stringify(meta?._payload ?? { _kb_id: kb_id, _lang: lang, _source: source }),
    _created_at: now
  });

  return {
    _kb_id: kb_id,
    _lang: lang,
    _format: "md",
    _content: content,
    ...(source ? { _source: source } : {}),
    _created_at: existing?._created_at ?? now,
    _updated_at: now
  };
}

export async function kb_append(
  scope: AgentKbXdbScope,
  kb_id: string,
  lang: string,
  md_block: string,
  actor: KbActor
): Promise<PersistedKbDocRecord> {
  const existing = await kb_get_doc(scope, kb_id, lang);
  const current = existing?._content ?? "";
  const block = md_block.trim();
  const next = current.trim().length > 0 ? `${current.trimEnd()}\n\n---\n\n${block}\n` : `${block}\n`;
  return kb_set_doc(scope, kb_id, lang, next, "chat", actor, {
    _op: "kb.append",
    _summary: "Append markdown block",
    _payload: { _kb_id: kb_id, _lang: lang, _md: md_block },
    _previous_content: current
  });
}

function find_section_bounds(content: string, section_title: string): { start: number; end: number } | undefined {
  const lines = content.split(/\r?\n/g);
  const normalized_target = section_title.trim().toLowerCase();
  let start = -1;
  let end = lines.length;
  let level = 0;
  for (let idx = 0; idx < lines.length; idx += 1) {
    const line = lines[idx];
    const match = line.match(/^(#{2,3})\s+(.+?)\s*$/);
    if (!match) continue;
    const heading_level = match[1].length;
    const heading_title = match[2].trim().toLowerCase();
    if (start < 0 && heading_title === normalized_target) {
      start = idx;
      level = heading_level;
      continue;
    }
    if (start >= 0 && heading_level <= level) {
      end = idx;
      break;
    }
  }
  if (start < 0) return undefined;
  return { start, end };
}

export async function kb_patch_section(
  scope: AgentKbXdbScope,
  kb_id: string,
  lang: string,
  section_title: string,
  md_replacement: string,
  actor: KbActor
): Promise<PersistedKbDocRecord> {
  const existing = await kb_get_doc(scope, kb_id, lang);
  const current = existing?._content ?? "";
  const bounds = find_section_bounds(current, section_title);
  if (!bounds) {
    throw new Error(`Section not found: ${section_title}`);
  }
  const lines = current.split(/\r?\n/g);
  const heading = lines[bounds.start];
  const replacement_lines = [heading, md_replacement.trim()];
  const next = [...lines.slice(0, bounds.start), ...replacement_lines, ...lines.slice(bounds.end)].join("\n");
  return kb_set_doc(scope, kb_id, lang, next, "chat", actor, {
    _op: "kb.patch",
    _summary: `Patch section ${section_title}`,
    _payload: { _kb_id: kb_id, _lang: lang, _section_title: section_title, _md: md_replacement },
    _previous_content: current
  });
}

export async function kb_delete_section(
  scope: AgentKbXdbScope,
  kb_id: string,
  lang: string,
  section_title: string,
  actor: KbActor
): Promise<PersistedKbDocRecord> {
  const existing = await kb_get_doc(scope, kb_id, lang);
  const current = existing?._content ?? "";
  const bounds = find_section_bounds(current, section_title);
  if (!bounds) {
    throw new Error(`Section not found: ${section_title}`);
  }
  const lines = current.split(/\r?\n/g);
  const next = [...lines.slice(0, bounds.start), ...lines.slice(bounds.end)].join("\n").trim();
  const normalized = next ? `${next}\n` : "";
  return kb_set_doc(scope, kb_id, lang, normalized, "chat", actor, {
    _op: "kb.delete_section",
    _summary: `Delete section ${section_title}`,
    _payload: { _kb_id: kb_id, _lang: lang, _section_title: section_title },
    _previous_content: current
  });
}

export async function kb_list_audit(
  scope: AgentKbXdbScope,
  since_ts = 0,
  limit = 50
): Promise<PersistedKbAuditRecord[]> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._audit.find({ _app_id: scope._app_id, _env: scope._env }, 0, Math.max(1, limit * 4)),
    "kb_list_audit"
  )._data;
  const items = rows
    .map((row) => normalize_audit_record(row))
    .filter((row): row is PersistedKbAuditRecord => row !== undefined)
    .filter((row) => row._created_at >= Math.max(0, since_ts));
  items.sort((left, right) => right._created_at - left._created_at || right._audit_id.localeCompare(left._audit_id));
  return items.slice(0, Math.max(1, limit));
}

export async function pending_action_create(
  scope: AgentKbXdbScope,
  record: Omit<PersistedAdminPendingActionRecord, "_action_id" | "_created_at"> & { _action_id?: string; _created_at?: number }
): Promise<PersistedAdminPendingActionRecord> {
  const xdb = await ensure_kb_xdb(scope);
  const created_at = record._created_at ?? Date.now();
  const _action_id = record._action_id ?? `act_${randomUUID().replace(/-/g, "")}`;
  const normalized: PersistedAdminPendingActionRecord = {
    _action_id,
    _actor_user_id: record._actor_user_id,
    _kind: record._kind,
    _kb_id: record._kb_id,
    _lang: record._lang,
    _proposal_json: record._proposal_json,
    _expires_at: record._expires_at,
    _created_at: created_at
  };
  await xdb._pending.add(
    {
      _id: normalized._action_id,
      _action_id: normalized._action_id,
      _app_id: scope._app_id,
      _env: scope._env,
      _actor_user_id: normalized._actor_user_id,
      _kind: normalized._kind,
      _kb_id: normalized._kb_id,
      _lang: normalized._lang,
      _proposal_json: normalized._proposal_json,
      _expires_at: normalized._expires_at,
      _created_at: normalized._created_at
    },
    true,
    true
  );
  return normalized;
}

export async function pending_action_get(
  scope: AgentKbXdbScope,
  action_id?: string,
  actor_user_id?: string
): Promise<PersistedAdminPendingActionRecord | undefined> {
  const xdb = await ensure_kb_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._pending.find(
      {
        _app_id: scope._app_id,
        _env: scope._env,
        ...(action_id ? { _action_id: action_id } : {}),
        ...(actor_user_id ? { _actor_user_id: actor_user_id } : {})
      },
      0,
      action_id ? 1 : 100
    ),
    "pending_action_get"
  )._data;
  const items = rows
    .map((row) => normalize_pending_record(row))
    .filter((row): row is PersistedAdminPendingActionRecord => row !== undefined);
  if (items.length === 0) return undefined;
  items.sort((left, right) => right._created_at - left._created_at || right._action_id.localeCompare(left._action_id));
  return items[0];
}

export async function pending_action_consume(
  scope: AgentKbXdbScope,
  action_id: string
): Promise<PersistedAdminPendingActionRecord | undefined> {
  const xdb = await ensure_kb_xdb(scope);
  const existing = await pending_action_get(scope, action_id);
  if (!existing) return undefined;
  await xdb._pending.delete({ _app_id: scope._app_id, _env: scope._env, _action_id: action_id });
  return existing;
}

