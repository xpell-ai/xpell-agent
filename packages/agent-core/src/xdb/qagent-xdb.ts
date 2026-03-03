import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

import type { QAgentCase, QAgentRun } from "../types/qagent.js";

type Dict = Record<string, unknown>;

export type AgentQAgentXdbScope = {
  _app_id: string;
  _env: string;
};

type QAgentXdb = {
  _runs: XDBEntity;
  _cases: XDBEntity;
};

const ENTITY_RUNS_PREFIX = "agent.qagent_runs";
const ENTITY_CASES_PREFIX = "agent.qagent_cases";

const _xdb_by_scope = new Map<string, QAgentXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<QAgentXdb>>();

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

function to_score(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.min(1, Math.max(0, value));
  }
  return 0;
}

function to_json(value: unknown): string {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return "null";
  }
}

function parse_json(value: unknown): unknown {
  if (typeof value !== "string" || !value.trim()) return undefined;
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
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
    throw new Error(`[agent-qagent:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentQAgentXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

export function qagent_runs_entity_name(scope: AgentQAgentXdbScope): string {
  return `${ENTITY_RUNS_PREFIX}::${scope_key(scope)}`;
}

export function qagent_cases_entity_name(scope: AgentQAgentXdbScope): string {
  return `${ENTITY_CASES_PREFIX}::${scope_key(scope)}`;
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

async function ensure_qagent_xdb(scope: AgentQAgentXdbScope): Promise<QAgentXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const runs = XDB.create({
      _type: "xdb-entity",
      _name: qagent_runs_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _status: { _type: "String", _required: true, _index: true },
        _kb_files_json: { _type: "String" },
        _agent_name: { _type: "String", _required: true },
        _agent_role: { _type: "String", _required: true },
        _summary: { _type: "String" },
        _meta_json: { _type: "String" },
        _totals_json: { _type: "String" }
      }
    }) as XDBEntity;

    const cases = XDB.create({
      _type: "xdb-entity",
      _name: qagent_cases_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _run_id: { _type: "String", _required: true, _index: true },
        _case_idx: { _type: "Number", _required: true, _index: true },
        _audience: { _type: "String", _required: true, _index: true },
        _intent_id: { _type: "String", _required: true, _index: true },
        _question: { _type: "String", _required: true },
        _expected_facts_json: { _type: "String" },
        _answer: { _type: "String" },
        _score: { _type: "Number", _required: true },
        _judge_notes: { _type: "String" }
      }
    }) as XDBEntity;

    await ensure_entity_loaded(runs);
    await ensure_entity_loaded(cases);

    const out = { _runs: runs, _cases: cases };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-qagent:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_run(value: unknown): QAgentRun | undefined {
  if (!is_plain_object(value)) return undefined;
  const _id = to_text(value._id);
  const _status = to_text(value._status);
  const _agent_name = to_text(value._agent_name);
  const _agent_role = to_text(value._agent_role);
  if (!_id || !_status || !_agent_name || !_agent_role) return undefined;
  const _kb_files = normalize_string_array(parse_json(value._kb_files_json));
  const _summary = to_text(value._summary);
  const _meta_json = to_text(value._meta_json);
  const _totals_json = to_text(value._totals_json);
  return {
    _id,
    _app_id: to_text(value._app_id),
    _env: to_text(value._env),
    _status: _status as QAgentRun["_status"],
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at),
    _kb_files,
    _agent_name,
    _agent_role,
    ...(_summary ? { _summary } : {}),
    ...(_meta_json ? { _meta_json } : {}),
    ...(_totals_json ? { _totals_json } : {})
  };
}

function normalize_case(value: unknown): QAgentCase | undefined {
  if (!is_plain_object(value)) return undefined;
  const _id = to_text(value._id);
  const _run_id = to_text(value._run_id);
  const _audience = to_text(value._audience);
  const _intent_id = to_text(value._intent_id);
  const _question = to_text(value._question);
  if (!_id || !_run_id || !_audience || !_intent_id || !_question) return undefined;
  return {
    _id,
    _run_id,
    _app_id: to_text(value._app_id),
    _env: to_text(value._env),
    _case_idx: typeof value._case_idx === "number" && Number.isFinite(value._case_idx) ? Math.floor(value._case_idx) : 0,
    _audience: (_audience === "admin" ? "admin" : "customer"),
    _intent_id,
    _question,
    _expected_facts: normalize_string_array(parse_json(value._expected_facts_json)),
    _answer: to_text(value._answer),
    _score: to_score(value._score),
    _judge_notes: to_text(value._judge_notes),
    _created_at: to_ts(value._created_at),
    _updated_at: to_ts(value._updated_at)
  };
}

export async function init_qagent_xdb(scope: AgentQAgentXdbScope): Promise<void> {
  await ensure_qagent_xdb(scope);
}

export async function get_qagent_run_xdb(scope: AgentQAgentXdbScope, run_id: string): Promise<QAgentRun | undefined> {
  const xdb = await ensure_qagent_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._runs.find({ _app_id: scope._app_id, _env: scope._env, _id: run_id }, 0, 1),
    "get_run"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_run(rows[0]);
}

export async function list_qagent_runs_xdb(
  scope: AgentQAgentXdbScope,
  skip = 0,
  limit = 50
): Promise<QAgentRun[]> {
  const xdb = await ensure_qagent_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._runs.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000),
    "list_runs"
  )._data;
  const out: QAgentRun[] = [];
  for (const row of rows) {
    const normalized = normalize_run(row);
    if (normalized) out.push(normalized);
  }
  out.sort((left, right) => (left._created_at !== right._created_at ? right._created_at - left._created_at : left._id.localeCompare(right._id)));
  const safe_skip = Number.isFinite(skip) && skip > 0 ? Math.floor(skip) : 0;
  const safe_limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 50;
  return out.slice(safe_skip, safe_skip + safe_limit);
}

export async function save_qagent_run_xdb(scope: AgentQAgentXdbScope, record: QAgentRun): Promise<void> {
  const xdb = await ensure_qagent_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _id: record._id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._runs.find(filter, 0, 1), "find_run")._data;
  const row = {
    _id: record._id,
    _app_id: scope._app_id,
    _env: scope._env,
    _status: record._status,
    _kb_files_json: to_json(record._kb_files),
    _agent_name: record._agent_name,
    _agent_role: record._agent_role,
    _summary: record._summary ?? "",
    _meta_json: record._meta_json ?? "",
    _totals_json: record._totals_json ?? ""
  };
  if (existing.length > 0) {
    await xdb._runs.update(filter, row, true);
    return;
  }
  await xdb._runs.add(row, true, true);
}

export async function list_qagent_cases_xdb(scope: AgentQAgentXdbScope, run_id: string): Promise<QAgentCase[]> {
  const xdb = await ensure_qagent_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._cases.find({ _app_id: scope._app_id, _env: scope._env, _run_id: run_id }, 0, 100000),
    "list_cases"
  )._data;
  const out: QAgentCase[] = [];
  for (const row of rows) {
    const normalized = normalize_case(row);
    if (normalized) out.push(normalized);
  }
  out.sort((left, right) => (left._case_idx !== right._case_idx ? left._case_idx - right._case_idx : left._id.localeCompare(right._id)));
  return out;
}

export async function get_qagent_case_xdb(scope: AgentQAgentXdbScope, case_id: string): Promise<QAgentCase | undefined> {
  const xdb = await ensure_qagent_xdb(scope);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(
    xdb._cases.find({ _app_id: scope._app_id, _env: scope._env, _id: case_id }, 0, 1),
    "get_case"
  )._data;
  if (rows.length === 0) return undefined;
  return normalize_case(rows[0]);
}

export async function save_qagent_case_xdb(scope: AgentQAgentXdbScope, record: QAgentCase): Promise<void> {
  const xdb = await ensure_qagent_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _id: record._id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._cases.find(filter, 0, 1), "find_case")._data;
  const row = {
    _id: record._id,
    _app_id: scope._app_id,
    _env: scope._env,
    _run_id: record._run_id,
    _case_idx: Math.floor(record._case_idx),
    _audience: record._audience,
    _intent_id: record._intent_id,
    _question: record._question,
    _expected_facts_json: to_json(record._expected_facts),
    _answer: record._answer,
    _score: to_score(record._score),
    _judge_notes: record._judge_notes
  };
  if (existing.length > 0) {
    await xdb._cases.update(filter, row, true);
    return;
  }
  await xdb._cases.add(row, true, true);
}
