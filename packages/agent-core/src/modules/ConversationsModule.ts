import { randomUUID } from "node:crypto";

import { XError, XModule, _xlog, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, requireKernelCapOrActorRole } from "../runtime/guards.js";
import {
  delete_conv_participant_xdb,
  init_conv_participants_xdb,
  list_conv_participants_xdb,
  get_conv_participant_xdb,
  upsert_conv_participant_xdb,
  build_conv_participant_key,
  type AgentConvParticipantsXdbScope,
  type PersistedConvParticipantRecord
} from "./conv-participants-xdb.js";
import {
  delete_message_xdb,
  delete_thread_xdb,
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

type ConversationParticipantRole = "owner" | "admin" | "customer" | "system";

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
    tags: [...thread.tags],
    ...(thread.pending_action_type ? { pending_action_type: thread.pending_action_type } : {}),
    ...(thread.pending_action_id ? { pending_action_id: thread.pending_action_id } : {}),
    ...(thread.pending_action_payload_json ? { pending_action_payload_json: thread.pending_action_payload_json } : {}),
    ...(typeof thread.pending_action_created_at === "number" ? { pending_action_created_at: thread.pending_action_created_at } : {}),
    ...(typeof thread.pending_action_expires_at === "number" ? { pending_action_expires_at: thread.pending_action_expires_at } : {})
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
  private _participants_scope: AgentConvParticipantsXdbScope;
  private _participants_by_key = new Map<string, PersistedConvParticipantRecord>();
  private _xdb_initialized = false;

  constructor(opts: ConversationsModuleOptions = {}) {
    super({ _name: CONVERSATIONS_MODULE_NAME });
    this._xdb_scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
    this._participants_scope = {
      _app_id: this._xdb_scope._app_id,
      _env: this._xdb_scope._env
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

  async _list_messages(xcmd: XCommandData) {
    return this.list_messages_impl(xcmd);
  }
  async _op_list_messages(xcmd: XCommandData) {
    return this.list_messages_impl(xcmd);
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

  async _set_pending_action(xcmd: XCommandData) {
    return this.set_pending_action_impl(xcmd);
  }
  async _op_set_pending_action(xcmd: XCommandData) {
    return this.set_pending_action_impl(xcmd);
  }

  async _clear_pending_action(xcmd: XCommandData) {
    return this.clear_pending_action_impl(xcmd);
  }
  async _op_clear_pending_action(xcmd: XCommandData) {
    return this.clear_pending_action_impl(xcmd);
  }

  async _get_pending_action(xcmd: XCommandData) {
    return this.get_pending_action_impl(xcmd);
  }
  async _op_get_pending_action(xcmd: XCommandData) {
    return this.get_pending_action_impl(xcmd);
  }

  async _summary_today(xcmd: XCommandData) {
    return this.summary_today_impl(xcmd);
  }
  async _op_summary_today(xcmd: XCommandData) {
    return this.summary_today_impl(xcmd);
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _add_participant(xcmd: XCommandData) {
    return this.add_participant_impl(xcmd);
  }
  async _op_add_participant(xcmd: XCommandData) {
    return this.add_participant_impl(xcmd);
  }

  async _list_participants(xcmd: XCommandData) {
    return this.list_participants_impl(xcmd);
  }
  async _op_list_participants(xcmd: XCommandData) {
    return this.list_participants_impl(xcmd);
  }

  async _ensure_thread_participant(xcmd: XCommandData) {
    return this.ensure_thread_participant_impl(xcmd);
  }
  async _op_ensure_thread_participant(xcmd: XCommandData) {
    return this.ensure_thread_participant_impl(xcmd);
  }

  async _reset_storage(xcmd: XCommandData) {
    return this.reset_storage_impl(xcmd);
  }
  async _op_reset_storage(xcmd: XCommandData) {
    return this.reset_storage_impl(xcmd);
  }

  async _remap_user_ids(xcmd: XCommandData) {
    return this.remap_user_ids_impl(xcmd);
  }
  async _op_remap_user_ids(xcmd: XCommandData) {
    return this.remap_user_ids_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_conv_xdb(this._xdb_scope);
    await init_conv_participants_xdb(this._participants_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    return {
      ok: true,
      threads: this._threads_by_id.size,
      participants: this._participants_by_key.size
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

    const now = this.now();
    const thread: ConversationThread = {
      thread_id: randomUUID(),
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

  private async remap_user_ids_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    const params = this.ensure_params(xcmd._params);
    const raw_map = is_plain_object(params.map) ? params.map : undefined;
    if (!raw_map) {
      throw new XError("E_CONV_BAD_PARAMS", "map must be an object");
    }

    const normalized_map = new Map<string, string>();
    for (const [legacy_user_id, canonical_user_id] of Object.entries(raw_map)) {
      const from = ensure_optional_string(legacy_user_id);
      const to = ensure_optional_string(canonical_user_id);
      if (!from || !to || from === to) continue;
      normalized_map.set(from, to);
    }

    if (normalized_map.size === 0) {
      return { ok: true, updated_threads: 0 };
    }

    const touched: Array<{ thread: ConversationThread; previous_user_id: string }> = [];
    try {
      for (const thread of this._threads_by_id.values()) {
        const next_user_id = normalized_map.get(thread.user_id);
        if (!next_user_id) continue;
        touched.push({ thread, previous_user_id: thread.user_id });
        thread.user_id = next_user_id;
        await this.persist_thread(thread);
      }
    } catch {
      for (const entry of touched) {
        entry.thread.user_id = entry.previous_user_id;
      }
      throw new XError("E_CONV_PERSIST_FAILED", "Failed to remap thread user ids");
    }

    return { ok: true, updated_threads: touched.length };
  }

  private async add_participant_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params.thread_id, "thread_id");
    const user_id = ensure_non_empty_string(params.user_id, "user_id");
    const role = this.normalize_participant_role(params.role);
    const channel_id = ensure_optional_string(params.channel_id);
    const last_seen_at = this.ensure_optional_ts(params.last_seen_at);
    const participant = await this.write_participant({
      thread_id,
      user_id,
      role,
      ...(channel_id ? { channel_id } : {}),
      ...(last_seen_at !== undefined ? { last_seen_at } : {})
    });
    return { ok: true, participant: this.to_public_participant(participant) };
  }

  private list_participants_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_optional_string(params.thread_id);
    const items = Array.from(this._participants_by_key.values())
      .filter((participant) => (thread_id ? participant._thread_id === thread_id : true))
      .sort((left, right) => {
        if (left._thread_id !== right._thread_id) return left._thread_id.localeCompare(right._thread_id);
        return left._user_id.localeCompare(right._user_id);
      })
      .map((participant) => this.to_public_participant(participant));
    return { items };
  }

  private async ensure_thread_participant_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params.thread_id, "thread_id");
    const user_id = ensure_non_empty_string(params.user_id, "user_id");
    const role = this.normalize_participant_role(params.role);
    const channel_id = ensure_optional_string(params.channel_id);
    const last_seen_at = this.ensure_optional_ts(params.last_seen_at);
    const key = build_conv_participant_key(thread_id, user_id);
    const existing = this._participants_by_key.get(key) ?? (await get_conv_participant_xdb(this._participants_scope, key));
    const participant = await this.write_participant({
      thread_id,
      user_id,
      role,
      ...(channel_id ? { channel_id } : {}),
      ...(last_seen_at !== undefined ? { last_seen_at } : {}),
      ...(existing ? { created_at: existing._created_at } : {})
    });
    return {
      ok: true,
      participant: this.to_public_participant(participant),
      created: !existing
    };
  }

  private async reset_storage_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");

    const threads = await list_threads_xdb(this._xdb_scope);
    const messages = await list_messages_xdb(this._xdb_scope);
    const participants = await list_conv_participants_xdb(this._participants_scope);

    try {
      for (const message of messages) {
        await delete_message_xdb(this._xdb_scope, message.message_id);
      }
      for (const thread of threads) {
        await delete_thread_xdb(this._xdb_scope, thread.thread_id);
      }
      for (const participant of participants) {
        await delete_conv_participant_xdb(this._participants_scope, participant._key);
      }
    } catch {
      await this.hydrate_from_xdb();
      throw new XError("E_CONV_PERSIST_FAILED", "Failed to reset conversations storage");
    }

    this.clear_runtime_state();
    return {
      threads_deleted: threads.length,
      messages_deleted: messages.length,
      participants_deleted: participants.length
    };
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

    const message: ConversationMessage = {
      message_id: randomUUID(),
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

  private list_messages_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params.thread_id, "thread_id");
    const limit = this.normalize_optional_limit(params.limit, CONV_MAX_LIST_LIMIT);
    const before_ts = this.ensure_optional_ts(params.before_ts);

    const all_messages = this._messages_by_thread_id.get(thread_id) ?? [];
    const filtered = (before_ts === undefined ? all_messages : all_messages.filter((message) => message.ts < before_ts))
      .slice()
      .sort((left, right) => (left.ts !== right.ts ? left.ts - right.ts : left.message_id.localeCompare(right.message_id)));
    const messages = limit === undefined ? filtered : filtered.slice(Math.max(0, filtered.length - limit));

    return {
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

  private async set_pending_action_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params._thread_id ?? params.thread_id, "_thread_id");
    const type = ensure_non_empty_string(params._type ?? params.type, "_type");
    const id = ensure_non_empty_string(params._id ?? params.id, "_id");
    const payload = params._payload ?? params.payload;
    if (payload !== undefined && has_function(payload)) {
      throw new XError("E_CONV_BAD_PARAMS", "_payload must be JSON-safe");
    }
    const expires_at = this.ensure_optional_ts(params._expires_at ?? params.expires_at);
    const thread = this.must_get_thread(thread_id);
    const now = this.now();

    thread.pending_action_type = type;
    thread.pending_action_id = id;
    thread.pending_action_payload_json = payload === undefined ? "" : JSON.stringify(payload);
    thread.pending_action_created_at = now;
    if (expires_at !== undefined) {
      thread.pending_action_expires_at = expires_at;
    } else {
      delete thread.pending_action_expires_at;
    }
    thread.updated_at = now;
    await this.persist_thread(thread);

    return {
      ok: true,
      pending_action: this.to_public_pending_action(thread)
    };
  }

  private async clear_pending_action_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params._thread_id ?? params.thread_id, "_thread_id");
    const thread = this.must_get_thread(thread_id);
    delete thread.pending_action_type;
    delete thread.pending_action_id;
    delete thread.pending_action_payload_json;
    delete thread.pending_action_created_at;
    delete thread.pending_action_expires_at;
    thread.updated_at = this.now();
    await this.persist_thread(thread);
    return { ok: true };
  }

  private get_pending_action_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const thread_id = ensure_non_empty_string(params._thread_id ?? params.thread_id, "_thread_id");
    const thread = this.must_get_thread(thread_id);
    return {
      pending_action: this.to_public_pending_action(thread)
    };
  }

  private summary_today_impl(xcmd: XCommandData) {
    requireKernelCapOrActorRole(readCommandCtx(xcmd), "admin");
    const params = this.ensure_params(xcmd._params);
    const now = new Date();
    const start_of_day = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const end_ts = now.getTime();
    const limit = 200;
    const inbound: ConversationMessage[] = [];

    for (const messages of this._messages_by_thread_id.values()) {
      for (const message of messages) {
        if (message.direction !== MESSAGE_DIRECTION_IN) continue;
        if (message.ts < start_of_day || message.ts > end_ts) continue;
        inbound.push(clone_message(message));
      }
    }

    inbound.sort((left, right) => (left.ts !== right.ts ? right.ts - left.ts : right.message_id.localeCompare(left.message_id)));
    const bounded = inbound.slice(0, limit);

    const unique_users = new Set<string>();
    const per_thread = new Map<string, { thread_id: string; messages: number; last_ts: number }>();
    for (const message of bounded) {
      const thread = this._threads_by_id.get(message.thread_id);
      if (thread) {
        unique_users.add(thread.user_id);
      }
      const existing = per_thread.get(message.thread_id) ?? { thread_id: message.thread_id, messages: 0, last_ts: 0 };
      existing.messages += 1;
      existing.last_ts = Math.max(existing.last_ts, message.ts);
      per_thread.set(message.thread_id, existing);
    }

    const top_threads = Array.from(per_thread.values())
      .sort((left, right) => {
        if (left.messages !== right.messages) return right.messages - left.messages;
        if (left.last_ts !== right.last_ts) return right.last_ts - left.last_ts;
        return left.thread_id.localeCompare(right.thread_id);
      })
      .slice(0, 5);

    const highlights: string[] = [];
    highlights.push(`Inbound messages today: ${bounded.length}.`);
    highlights.push(`Unique users today: ${unique_users.size}.`);
    if (top_threads[0]) {
      highlights.push(`Top thread ${top_threads[0].thread_id} has ${top_threads[0].messages} messages.`);
    }

    const action_items: string[] = [];
    if (bounded.length === 0) {
      action_items.push("No inbound messages today.");
    } else if (top_threads.some((thread) => thread.messages >= 5)) {
      action_items.push("Review the busiest thread for follow-up.");
    } else {
      action_items.push("No urgent conversation spikes detected.");
    }

    const summary = {
      _date: now.toISOString().slice(0, 10),
      _total_messages: bounded.length,
      _unique_users: unique_users.size,
      _top_threads: top_threads,
      _highlights: highlights,
      _action_items: action_items
    };
    _xlog.log("[conv] summary_today", {
      date: summary._date,
      total_messages: summary._total_messages,
      unique_users: summary._unique_users
    });
    return summary;
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

  private normalize_participant_role(value: unknown): ConversationParticipantRole {
    if (value === "owner" || value === "admin" || value === "customer" || value === "system") return value;
    throw new XError("E_CONV_BAD_PARAMS", "role must be owner, admin, customer, or system");
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

  private clear_runtime_state(): void {
    this._thread_seq = 0;
    this._message_seq = 0;
    this._threads_by_id.clear();
    this._thread_id_by_key.clear();
    this._messages_by_thread_id.clear();
    this._participants_by_key.clear();
  }

  private async hydrate_from_xdb(): Promise<void> {
    const threads = await list_threads_xdb(this._xdb_scope);
    const messages = await list_messages_xdb(this._xdb_scope);

    this.clear_runtime_state();

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

    const participants = await list_conv_participants_xdb(this._participants_scope);
    for (const participant of participants) {
      if (!this._threads_by_id.has(participant._thread_id)) continue;
      this._participants_by_key.set(participant._key, {
        ...participant
      });
    }
  }

  private async persist_thread(thread: ConversationThread): Promise<void> {
    if (!this._xdb_initialized) return;
    await upsert_thread_xdb(this._xdb_scope, clone_thread(thread));
  }

  private async persist_message(message: ConversationMessage): Promise<void> {
    if (!this._xdb_initialized) return;
    await upsert_message_xdb(this._xdb_scope, clone_message(message));
  }

  private async write_participant(input: {
    thread_id: string;
    user_id: string;
    role: ConversationParticipantRole;
    channel_id?: string;
    last_seen_at?: number;
    created_at?: number;
  }): Promise<PersistedConvParticipantRecord> {
    this.must_get_thread(input.thread_id);
    const key = build_conv_participant_key(input.thread_id, input.user_id);
    const existing = this._participants_by_key.get(key);
    const now = this.now();
    const record: PersistedConvParticipantRecord = {
      _key: key,
      _thread_id: input.thread_id,
      _user_id: input.user_id,
      _role: input.role,
      ...(input.channel_id ? { _channel_id: input.channel_id } : existing?._channel_id ? { _channel_id: existing._channel_id } : {}),
      ...(input.last_seen_at !== undefined
        ? { _last_seen_at: input.last_seen_at }
        : existing?._last_seen_at !== undefined
          ? { _last_seen_at: existing._last_seen_at }
          : {}),
      _created_at: input.created_at ?? existing?._created_at ?? now,
      _updated_at: now
    };

    if (this._xdb_initialized) {
      await upsert_conv_participant_xdb(this._participants_scope, record);
    }
    this._participants_by_key.set(key, record);
    return record;
  }

  private to_public_participant(record: PersistedConvParticipantRecord) {
    return {
      thread_id: record._thread_id,
      user_id: record._user_id,
      role: record._role,
      ...(record._channel_id ? { channel_id: record._channel_id } : {}),
      ...(record._last_seen_at !== undefined ? { last_seen_at: record._last_seen_at } : {}),
      created_at: record._created_at,
      updated_at: record._updated_at
    };
  }

  private to_public_pending_action(thread: ConversationThread) {
    const type = ensure_optional_string(thread.pending_action_type);
    const id = ensure_optional_string(thread.pending_action_id);
    if (!type || !id) {
      return null;
    }
    let payload: unknown;
    if (thread.pending_action_payload_json) {
      try {
        payload = JSON.parse(thread.pending_action_payload_json);
      } catch {
        payload = undefined;
      }
    }
    return {
      type,
      id,
      ...(payload !== undefined ? { payload } : {}),
      ...(typeof thread.pending_action_created_at === "number" ? { created_at: thread.pending_action_created_at } : {}),
      ...(typeof thread.pending_action_expires_at === "number" ? { expires_at: thread.pending_action_expires_at } : {})
    };
  }
}

export default ConversationsModule;
