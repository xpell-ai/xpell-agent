import { XError, XModule, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap } from "../runtime/guards.js";
import {
  init_conv_xdb,
  list_messages_xdb,
  list_threads_xdb,
  upsert_message_xdb,
  upsert_thread_xdb,
  type AgentConvXdbScope
} from "./conv-xdb.js";
import {
  MESSAGE_DIRECTION_IN,
  MESSAGE_DIRECTION_OUT,
  type ConversationDirection,
  type ConversationMessage,
  type ConversationSender,
  type ConversationThread
} from "../types/conversations.js";

export const CONVERSATIONS_MODULE_NAME = "conv";

const CONV_DEFAULT_LIST_LIMIT = 50;
const CONV_MAX_LIST_LIMIT = 500;
const CONV_DEFAULT_STATUS = "open";

type Dict = Record<string, unknown>;

type ConversationsModuleOptions = {
  _app_id?: string;
  _env?: string;
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function has_function(value: unknown): boolean {
  if (typeof value === "function") return true;
  if (Array.isArray(value)) return value.some(has_function);
  if (is_plain_object(value)) return Object.values(value).some(has_function);
  return false;
}

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_CONV_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function thread_key(channel: string, channel_thread_id: string): string {
  return `${channel}::${channel_thread_id}`;
}

function clone_thread(thread: ConversationThread): ConversationThread {
  return {
    thread_id: thread.thread_id,
    channel: thread.channel,
    channel_thread_id: thread.channel_thread_id,
    user_id: thread.user_id,
    status: thread.status,
    created_at: thread.created_at,
    updated_at: thread.updated_at,
    tags: [...thread.tags]
  };
}

function clone_message(message: ConversationMessage): ConversationMessage {
  return {
    message_id: message.message_id,
    thread_id: message.thread_id,
    direction: message.direction,
    sender: message.sender,
    text: message.text,
    ts: message.ts,
    ...(message.channel_message_id ? { channel_message_id: message.channel_message_id } : {}),
    ...(message.meta ? { meta: { ...message.meta } } : {})
  };
}

function normalize_direction(value: unknown): ConversationDirection {
  if (value === MESSAGE_DIRECTION_IN || value === MESSAGE_DIRECTION_OUT) return value;
  throw new XError("E_CONV_BAD_PARAMS", `Invalid direction: ${String(value)}`);
}

function normalize_sender(value: unknown): ConversationSender {
  const allowed: ConversationSender[] = ["customer", "agent", "admin", "system"];
  if (typeof value === "string" && allowed.includes(value as ConversationSender)) {
    return value as ConversationSender;
  }
  throw new XError("E_CONV_BAD_PARAMS", `Invalid sender: ${String(value)}`);
}

export class ConversationsModule extends XModule {
  static _name = CONVERSATIONS_MODULE_NAME;

  private _thread_seq = 0;
  private _message_seq = 0;

  private _threads_by_id = new Map<string, ConversationThread>();
  private _thread_id_by_key = new Map<string, string>();
  private _messages_by_thread_id = new Map<string, ConversationMessage[]>();
  private _xdb_scope: AgentConvXdbScope;
  private _xdb_initialized = false;

  constructor(opts: ConversationsModuleOptions = {}) {
    super({ _name: CONVERSATIONS_MODULE_NAME });
    this._xdb_scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _get_or_create_thread(xcmd: XCommandData) {
    return this.get_or_create_thread_impl(xcmd);
  }
  async _op_get_or_create_thread(xcmd: XCommandData) {
    return this.get_or_create_thread_impl(xcmd);
  }

  async _ensure_thread(xcmd: XCommandData) {
    return this.ensure_thread_impl(xcmd);
  }
  async _op_ensure_thread(xcmd: XCommandData) {
    return this.ensure_thread_impl(xcmd);
  }

  async _append_message(xcmd: XCommandData) {
    return this.append_message_impl(xcmd);
  }
  async _op_append_message(xcmd: XCommandData) {
    return this.append_message_impl(xcmd);
  }

  async _list_threads(xcmd: XCommandData) {
    return this.list_threads_impl(xcmd);
  }
  async _op_list_threads(xcmd: XCommandData) {
    return this.list_threads_impl(xcmd);
  }

  async _get_thread(xcmd: XCommandData) {
    return this.get_thread_impl(xcmd);
  }
  async _op_get_thread(xcmd: XCommandData) {
    return this.get_thread_impl(xcmd);
  }

  async _get_thread_by_channel(xcmd: XCommandData) {
    return this.get_thread_by_channel_impl(xcmd);
  }
  async _op_get_thread_by_channel(xcmd: XCommandData) {
    return this.get_thread_by_channel_impl(xcmd);
  }

  async _get_thread_by_key(xcmd: XCommandData) {
    return this.get_thread_by_key_impl(xcmd);
  }
  async _op_get_thread_by_key(xcmd: XCommandData) {
    return this.get_thread_by_key_impl(xcmd);
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_conv_xdb(this._xdb_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    return {
      ok: true,
      threads: this._threads_by_id.size
    };
  }

  private async get_or_create_thread_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel = ensure_non_empty_string(params.channel, "channel").toLowerCase();
    const channel_thread_id = ensure_non_empty_string(params.channel_thread_id, "channel_thread_id");
    const user_id = ensure_non_empty_string(params.user_id, "user_id");

    return this.ensure_thread_core(channel, channel_thread_id, user_id);
  }

  private async ensure_thread_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel_id =
      ensure_optional_string(params.channel_id) ?? ensure_non_empty_string(params.channel, "channel|channel_id");
    const thread_key_value =
      ensure_optional_string(params.thread_key) ??
      ensure_non_empty_string(params.channel_thread_id, "thread_key|channel_thread_id");
    const user_id = ensure_optional_string(params.user_id);
    const normalized_channel = channel_id.toLowerCase();
    const key = thread_key(normalized_channel, thread_key_value);

    if (!user_id) {
      const existing_thread_id = this._thread_id_by_key.get(key);
      if (!existing_thread_id) {
        throw new XError("E_CONV_BAD_PARAMS", "user_id is required to create a new thread");
      }
      return { thread: clone_thread(this.must_get_thread(existing_thread_id)), created: false };
    }

    return this.ensure_thread_core(normalized_channel, thread_key_value, user_id);
  }

  private async ensure_thread_core(channel: string, channel_thread_id: string, user_id: string) {
    const key = thread_key(channel, channel_thread_id);
    const existing_thread_id = this._thread_id_by_key.get(key);
    if (existing_thread_id) {
      const thread = this.must_get_thread(existing_thread_id);
      if (thread.user_id !== user_id) {
        throw new XError(
          "E_CONV_THREAD_USER_MISMATCH",
          `Thread already exists with different user_id. thread_id=${thread.thread_id}`
        );
      }
      return { thread: clone_thread(thread), created: false };
    }

    this._thread_seq += 1;
    const now = this.now();
    const thread: ConversationThread = {
      thread_id: `thread_${this._thread_seq.toString().padStart(6, "0")}`,
      channel,
      channel_thread_id,
      user_id,
      status: CONV_DEFAULT_STATUS,
      created_at: now,
      updated_at: now,
      tags: []
    };

    this._threads_by_id.set(thread.thread_id, thread);
    this._thread_id_by_key.set(key, thread.thread_id);
    this._messages_by_thread_id.set(thread.thread_id, []);

    try {
      await this.persist_thread(thread);
    } catch {
      this._threads_by_id.delete(thread.thread_id);
      this._thread_id_by_key.delete(key);
      this._messages_by_thread_id.delete(thread.thread_id);
      throw new XError("E_CONV_PERSIST_FAILED", "Failed to persist thread");
    }

    return { thread: clone_thread(thread), created: true };
  }

  private async append_message_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params.thread_id, "thread_id");
    const direction = normalize_direction(params.direction);
    const sender =
      params.sender === undefined || params.sender === null
        ? this.default_sender_for_direction(direction)
        : normalize_sender(params.sender);
    const text = ensure_non_empty_string(params.text, "text");
    const channel_message_id =
      ensure_optional_string(params.external_id) ?? ensure_optional_string(params.channel_message_id);
    const ts = this.ensure_optional_ts(params.ts) ?? this.now();
    const meta = this.ensure_optional_meta(params.meta);

    const thread = this.must_get_thread(thread_id);
    this._message_seq += 1;

    const message: ConversationMessage = {
      message_id: `message_${this._message_seq.toString().padStart(6, "0")}`,
      thread_id: thread.thread_id,
      direction,
      sender,
      text,
      ts,
      ...(channel_message_id ? { channel_message_id } : {}),
      ...(meta ? { meta } : {})
    };

    const list = this._messages_by_thread_id.get(thread.thread_id) ?? [];
    const previous_updated_at = thread.updated_at;
    list.push(message);
    this._messages_by_thread_id.set(thread.thread_id, list);

    thread.updated_at = message.ts;

    try {
      await this.persist_thread(thread);
      await this.persist_message(message);
    } catch {
      list.pop();
      thread.updated_at = previous_updated_at;
      throw new XError("E_CONV_PERSIST_FAILED", "Failed to persist message");
    }

    return { message: clone_message(message) };
  }

  private list_threads_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const limit = this.normalize_limit(params.limit, CONV_DEFAULT_LIST_LIMIT, CONV_MAX_LIST_LIMIT);
    const status = params.status === undefined ? undefined : ensure_non_empty_string(params.status, "status");

    const threads = Array.from(this._threads_by_id.values())
      .filter((thread) => (status ? thread.status === status : true))
      .sort((left, right) => {
        if (left.updated_at !== right.updated_at) return right.updated_at - left.updated_at;
        return left.thread_id.localeCompare(right.thread_id);
      })
      .slice(0, limit)
      .map(clone_thread);

    return { threads };
  }

  private get_thread_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params.thread_id, "thread_id");
    const limit_messages = this.normalize_optional_limit(params.limit_messages, CONV_MAX_LIST_LIMIT);

    const thread = this.must_get_thread(thread_id);
    const all_messages = this._messages_by_thread_id.get(thread_id) ?? [];

    const messages =
      limit_messages === undefined ? all_messages : all_messages.slice(Math.max(0, all_messages.length - limit_messages));

    return {
      thread: clone_thread(thread),
      messages: messages.map(clone_message)
    };
  }

  private get_thread_by_channel_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel = ensure_non_empty_string(params.channel, "channel").toLowerCase();
    const channel_thread_id = ensure_non_empty_string(params.channel_thread_id, "channel_thread_id");

    const key = thread_key(channel, channel_thread_id);
    const thread_id = this._thread_id_by_key.get(key);
    if (!thread_id) {
      return { thread: null };
    }

    const thread = this.must_get_thread(thread_id);
    return { thread: clone_thread(thread) };
  }

  private get_thread_by_key_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel_id =
      ensure_optional_string(params.channel_id) ?? ensure_non_empty_string(params.channel, "channel|channel_id");
    const thread_key_raw =
      ensure_optional_string(params.thread_key) ??
      ensure_non_empty_string(params.channel_thread_id, "thread_key|channel_thread_id");

    const key = thread_key(channel_id.toLowerCase(), thread_key_raw);
    const thread_id = this._thread_id_by_key.get(key);
    if (!thread_id) {
      return { thread: null };
    }

    const thread = this.must_get_thread(thread_id);
    return { thread: clone_thread(thread) };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_CONV_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_CONV_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private ensure_optional_meta(value: unknown): Record<string, unknown> | undefined {
    if (value === undefined || value === null) return undefined;
    if (!is_plain_object(value)) {
      throw new XError("E_CONV_BAD_PARAMS", "meta must be an object when provided");
    }
    if (has_function(value)) {
      throw new XError("E_CONV_BAD_PARAMS", "meta must be JSON-safe");
    }
    return { ...value };
  }

  private ensure_optional_ts(value: unknown): number | undefined {
    if (value === undefined || value === null) return undefined;
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new XError("E_CONV_BAD_PARAMS", "ts must be a finite number when provided");
    }
    return value;
  }

  private default_sender_for_direction(direction: ConversationDirection): ConversationSender {
    return direction === MESSAGE_DIRECTION_IN ? "customer" : "agent";
  }

  private normalize_limit(value: unknown, fallback: number, max: number): number {
    if (value === undefined || value === null) return fallback;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new XError("E_CONV_BAD_PARAMS", "limit must be a positive integer");
    }
    return Math.min(parsed, max);
  }

  private normalize_optional_limit(value: unknown, max: number): number | undefined {
    if (value === undefined || value === null) return undefined;
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new XError("E_CONV_BAD_PARAMS", "limit_messages must be a positive integer");
    }
    return Math.min(parsed, max);
  }

  private must_get_thread(thread_id: string): ConversationThread {
    const thread = this._threads_by_id.get(thread_id);
    if (!thread) {
      throw new XError("E_CONV_NOT_FOUND", `Thread not found: ${thread_id}`);
    }
    return thread;
  }

  private now(): number {
    return Date.now();
  }

  private read_thread_seq_from_id(thread_id: string): number {
    const matched = /^thread_(\d+)$/.exec(thread_id);
    if (!matched) return 0;
    const parsed = Number.parseInt(matched[1], 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private read_message_seq_from_id(message_id: string): number {
    const matched = /^message_(\d+)$/.exec(message_id);
    if (!matched) return 0;
    const parsed = Number.parseInt(matched[1], 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private async hydrate_from_xdb(): Promise<void> {
    const threads = await list_threads_xdb(this._xdb_scope);
    const messages = await list_messages_xdb(this._xdb_scope);

    this._thread_seq = 0;
    this._message_seq = 0;
    this._threads_by_id.clear();
    this._thread_id_by_key.clear();
    this._messages_by_thread_id.clear();

    for (const thread of threads) {
      const normalized = clone_thread(thread);
      this._threads_by_id.set(normalized.thread_id, normalized);
      this._thread_id_by_key.set(thread_key(normalized.channel, normalized.channel_thread_id), normalized.thread_id);
      this._messages_by_thread_id.set(normalized.thread_id, []);
      this._thread_seq = Math.max(this._thread_seq, this.read_thread_seq_from_id(normalized.thread_id));
    }

    messages
      .slice()
      .sort((left, right) => (left.ts !== right.ts ? left.ts - right.ts : left.message_id.localeCompare(right.message_id)))
      .forEach((message) => {
        const list = this._messages_by_thread_id.get(message.thread_id);
        if (!list) return;
        list.push(clone_message(message));
        this._message_seq = Math.max(this._message_seq, this.read_message_seq_from_id(message.message_id));
      });
  }

  private async persist_thread(thread: ConversationThread): Promise<void> {
    if (!this._xdb_initialized) return;
    await upsert_thread_xdb(this._xdb_scope, clone_thread(thread));
  }

  private async persist_message(message: ConversationMessage): Promise<void> {
    if (!this._xdb_initialized) return;
    await upsert_message_xdb(this._xdb_scope, clone_message(message));
  }
}

export default ConversationsModule;
