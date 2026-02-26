import { XDB, XDBEntity, _xem, _xlog, type XResponseData } from "@xpell/node";

import type { ConversationMessage, ConversationThread } from "../types/conversations.js";

const ENTITY_THREADS_PREFIX = "agent.conv_threads";
const ENTITY_MESSAGES_PREFIX = "agent.conv_messages";

type Dict = Record<string, unknown>;

export type AgentConvXdbScope = {
  _app_id: string;
  _env: string;
};

type ConvXdb = {
  _threads: XDBEntity;
  _messages: XDBEntity;
};

const _xdb_by_scope = new Map<string, ConvXdb>();
const _xdb_creation_by_scope = new Map<string, Promise<ConvXdb>>();

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
  if (typeof raw !== "string" || !raw.trim()) return undefined;
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
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
    throw new Error(`[agent-conv:xdb] ${label} failed: ${message}`);
  }
  return res._result as T;
}

function scope_key(scope: AgentConvXdbScope): string {
  return `${scope._app_id}::${scope._env}`;
}

function threads_entity_name(scope: AgentConvXdbScope): string {
  return `${ENTITY_THREADS_PREFIX}::${scope_key(scope)}`;
}

function messages_entity_name(scope: AgentConvXdbScope): string {
  return `${ENTITY_MESSAGES_PREFIX}::${scope_key(scope)}`;
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

async function ensure_conv_xdb(scope: AgentConvXdbScope): Promise<ConvXdb> {
  const key = scope_key(scope);
  const existing = _xdb_by_scope.get(key);
  if (existing) return existing;

  const in_flight = _xdb_creation_by_scope.get(key);
  if (in_flight) return in_flight;

  const creation = (async () => {
    await ensure_xdb_ready();

    const threads = XDB.create({
      _type: "xdb-entity",
      _name: threads_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _thread_id: { _type: "String", _required: true, _index: true },
        _channel: { _type: "String", _required: true, _index: true },
        _channel_thread_id: { _type: "String", _required: true, _index: true },
        _user_id: { _type: "String", _required: true, _index: true },
        _status: { _type: "String", _required: true },
        _created_at: { _type: "Number", _required: true, _index: true },
        _updated_at: { _type: "Number", _required: true, _index: true },
        _tags: { _type: "Array" }
      }
    }) as XDBEntity;

    const messages = XDB.create({
      _type: "xdb-entity",
      _name: messages_entity_name(scope),
      _schema: {
        _app_id: { _type: "String", _required: true, _index: true },
        _env: { _type: "String", _required: true, _index: true },
        _message_id: { _type: "String", _required: true, _index: true },
        _thread_id: { _type: "String", _required: true, _index: true },
        _direction: { _type: "String", _required: true },
        _sender: { _type: "String", _required: true },
        _text: { _type: "String", _required: true },
        _ts: { _type: "Number", _required: true, _index: true },
        _channel_message_id: { _type: "String" },
        _meta_json: { _type: "String" }
      }
    }) as XDBEntity;

    await Promise.all([ensure_entity_loaded(threads), ensure_entity_loaded(messages)]);

    const out = { _threads: threads, _messages: messages };
    _xdb_by_scope.set(key, out);
    _xlog.log(`[agent-conv:xdb] entities initialized scope=${key}`);
    return out;
  })();

  _xdb_creation_by_scope.set(key, creation);
  return creation;
}

function normalize_thread(value: unknown): ConversationThread | undefined {
  if (!is_plain_object(value)) return undefined;

  const thread_id = to_text(value._thread_id || value._id);
  const channel = to_text(value._channel).toLowerCase();
  const channel_thread_id = to_text(value._channel_thread_id);
  const user_id = to_text(value._user_id);
  const status = to_text(value._status);
  if (!thread_id || !channel || !channel_thread_id || !user_id || !status) return undefined;

  const tags = Array.isArray(value._tags)
    ? value._tags.map((entry) => String(entry)).filter((entry) => entry.trim().length > 0)
    : [];

  return {
    thread_id,
    channel,
    channel_thread_id,
    user_id,
    status,
    created_at: to_ts(value._created_at),
    updated_at: to_ts(value._updated_at),
    tags
  };
}

function normalize_message(value: unknown): ConversationMessage | undefined {
  if (!is_plain_object(value)) return undefined;

  const message_id = to_text(value._message_id || value._id);
  const thread_id = to_text(value._thread_id);
  const direction = to_text(value._direction) as "in" | "out";
  const sender = to_text(value._sender) as "customer" | "agent" | "admin" | "system";
  const text = to_text(value._text);
  if (!message_id || !thread_id || !text) return undefined;
  if (direction !== "in" && direction !== "out") return undefined;
  if (sender !== "customer" && sender !== "agent" && sender !== "admin" && sender !== "system") return undefined;

  const meta = parse_json(value._meta_json);
  return {
    message_id,
    thread_id,
    direction,
    sender,
    text,
    ts: to_ts(value._ts),
    ...(to_text(value._channel_message_id) ? { channel_message_id: to_text(value._channel_message_id) } : {}),
    ...(is_plain_object(meta) ? { meta } : {})
  };
}

export async function init_conv_xdb(scope: AgentConvXdbScope): Promise<void> {
  await ensure_conv_xdb(scope);
}

export async function list_threads_xdb(scope: AgentConvXdbScope): Promise<ConversationThread[]> {
  const xdb = await ensure_conv_xdb(scope);
  const res = xdb._threads.find({ _app_id: scope._app_id, _env: scope._env }, 0, 100000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_threads")._data;

  const out: ConversationThread[] = [];
  for (const row of rows) {
    const normalized = normalize_thread(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function upsert_thread_xdb(scope: AgentConvXdbScope, thread: ConversationThread): Promise<void> {
  const xdb = await ensure_conv_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _thread_id: thread.thread_id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._threads.find(filter, 0, 1), "find_thread")._data;

  const row = {
    _id: thread.thread_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _thread_id: thread.thread_id,
    _channel: thread.channel,
    _channel_thread_id: thread.channel_thread_id,
    _user_id: thread.user_id,
    _status: thread.status,
    _created_at: thread.created_at,
    _updated_at: thread.updated_at,
    _tags: [...thread.tags]
  };

  if (existing.length > 0) {
    await xdb._threads.update(filter, row, true);
    return;
  }

  await xdb._threads.add(row, true, true);
}

export async function list_messages_xdb(scope: AgentConvXdbScope): Promise<ConversationMessage[]> {
  const xdb = await ensure_conv_xdb(scope);
  const res = xdb._messages.find({ _app_id: scope._app_id, _env: scope._env }, 0, 200000);
  const rows = ensure_xdb_result<{ _data: unknown[] }>(res, "list_messages")._data;

  const out: ConversationMessage[] = [];
  for (const row of rows) {
    const normalized = normalize_message(row);
    if (normalized) out.push(normalized);
  }
  return out;
}

export async function upsert_message_xdb(scope: AgentConvXdbScope, message: ConversationMessage): Promise<void> {
  const xdb = await ensure_conv_xdb(scope);
  const filter = { _app_id: scope._app_id, _env: scope._env, _message_id: message.message_id };
  const existing = ensure_xdb_result<{ _data: unknown[] }>(xdb._messages.find(filter, 0, 1), "find_message")._data;

  const row = {
    _id: message.message_id,
    _app_id: scope._app_id,
    _env: scope._env,
    _message_id: message.message_id,
    _thread_id: message.thread_id,
    _direction: message.direction,
    _sender: message.sender,
    _text: message.text,
    _ts: message.ts,
    _channel_message_id: message.channel_message_id ?? "",
    _meta_json: to_json(message.meta)
  };

  if (existing.length > 0) {
    await xdb._messages.update(filter, row, true);
    return;
  }

  await xdb._messages.add(row, true, true);
}
