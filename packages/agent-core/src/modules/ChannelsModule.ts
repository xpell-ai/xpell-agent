import { XError, XModule, _x, _xem, _xlog, type XCommandData } from "@xpell/node";

import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import {
  init_channels_xdb,
  list_registrations_xdb,
  upsert_registration_xdb,
  type AgentChannelsXdbScope
} from "./channels-xdb.js";
import { USERS_MODULE_NAME } from "./UsersModule.js";
import type { ConversationMessage, ConversationThread } from "../types/conversations.js";
import { readCommandCtx, requireKernelCap } from "../runtime/guards.js";

export const CHANNELS_MODULE_NAME = "channels";

type Dict = Record<string, unknown>;

type ChannelRegistration = {
  channel: string;
  connector_module: string;
  config: Record<string, unknown>;
  created_at: number;
  updated_at: number;
};

type IdentityResolution = {
  user_id: string;
  role: string;
};

type ChannelUserRef = {
  provider: string;
  id: string;
  username?: string;
  name?: string;
};

type RouteInboundMessageInput = {
  channel_id: string;
  thread_key: string;
  user_ref?: ChannelUserRef;
  msg: {
    text?: string;
    ts?: number;
    external_id?: string;
    raw?: unknown;
  };
};

type SendMessageInput = {
  channel_id: string;
  thread_id?: string;
  thread_key?: string;
  msg: {
    text: string;
    ts?: number;
    raw?: unknown;
  };
};

type ChannelsModuleOptions = {
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
    throw new XError("E_CHANNELS_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function clone_registration(registration: ChannelRegistration): ChannelRegistration {
  return {
    channel: registration.channel,
    connector_module: registration.connector_module,
    config: { ...registration.config },
    created_at: registration.created_at,
    updated_at: registration.updated_at
  };
}

export class ChannelsModule extends XModule {
  static _name = CHANNELS_MODULE_NAME;

  private _registrations = new Map<string, ChannelRegistration>();
  private _xdb_scope: AgentChannelsXdbScope;
  private _xdb_initialized = false;

  constructor(opts: ChannelsModuleOptions = {}) {
    super({ _name: CHANNELS_MODULE_NAME });
    this._xdb_scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
  }

  async _register(xcmd: XCommandData) {
    return this.register_impl(xcmd);
  }
  async _op_register(xcmd: XCommandData) {
    return this.register_impl(xcmd);
  }

  async _configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }
  async _op_configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }

  async _list(xcmd: XCommandData) {
    return this.list_impl(xcmd);
  }
  async _op_list(xcmd: XCommandData) {
    return this.list_impl(xcmd);
  }

  async _route_inbound_message(xcmd: XCommandData) {
    return this.route_inbound_message_impl(xcmd);
  }
  async _op_route_inbound_message(xcmd: XCommandData) {
    return this.route_inbound_message_impl(xcmd);
  }

  async _send_message(xcmd: XCommandData) {
    return this.send_message_impl(xcmd);
  }
  async _op_send_message(xcmd: XCommandData) {
    return this.send_message_impl(xcmd);
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_channels_xdb(this._xdb_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    return {
      ok: true,
      channels: this._registrations.size
    };
  }

  private async register_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel = ensure_non_empty_string(params.channel, "channel").toLowerCase();
    const connector_module = ensure_non_empty_string(params.connector_module, "connector_module");

    this.assert_module_exists(connector_module);

    const now = this.now();
    const existing = this._registrations.get(channel);
    if (existing) {
      const before = clone_registration(existing);
      existing.connector_module = connector_module;
      existing.updated_at = now;
      try {
        await this.persist_registration(existing);
      } catch {
        this._registrations.set(channel, before);
        throw new XError("E_CHANNELS_PERSIST_FAILED", "Failed to persist channel registration");
      }
      return { registration: clone_registration(existing), updated: true };
    }

    const registration: ChannelRegistration = {
      channel,
      connector_module,
      config: {},
      created_at: now,
      updated_at: now
    };
    this._registrations.set(channel, registration);

    try {
      await this.persist_registration(registration);
    } catch {
      this._registrations.delete(channel);
      throw new XError("E_CHANNELS_PERSIST_FAILED", "Failed to persist channel registration");
    }

    return { registration: clone_registration(registration), updated: false };
  }

  private async configure_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));

    const params = this.ensure_params(xcmd._params);
    const channel = ensure_non_empty_string(params.channel, "channel").toLowerCase();
    const config = this.ensure_object(params.config, "config");

    const registration = this.must_get_registration(channel);
    const before = clone_registration(registration);
    registration.config = { ...config };
    registration.updated_at = this.now();

    try {
      await this.persist_registration(registration);
    } catch {
      this._registrations.set(channel, before);
      throw new XError("E_CHANNELS_PERSIST_FAILED", "Failed to persist channel configuration");
    }

    return { registration: clone_registration(registration) };
  }

  private list_impl(_xcmd: XCommandData) {
    const channels = Array.from(this._registrations.values())
      .sort((left, right) => left.channel.localeCompare(right.channel))
      .map(clone_registration);
    return { channels };
  }

  private async route_inbound_message_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const inbound = this.parse_route_inbound_input(params);

    this.must_get_registration(inbound.channel_id);

    let thread = await this.exec_conv_get_thread_by_key({
      channel_id: inbound.channel_id,
      thread_key: inbound.thread_key
    });
    let resolved: IdentityResolution | undefined;

    if (!thread) {
      if (!inbound.user_ref) {
        throw new XError("E_CHANNELS_BAD_PARAMS", "user_ref is required when creating a new thread");
      }
      if (inbound.user_ref.provider.toLowerCase() !== inbound.channel_id) {
        throw new XError("E_CHANNELS_BAD_PARAMS", "user_ref.provider must match channel_id");
      }

      const profile = this.profile_from_user_ref(inbound.user_ref);
      resolved = await this.exec_users_resolve_identity({
        channel: inbound.channel_id,
        channel_user_id: inbound.user_ref.id,
        ...(profile ? { profile } : {})
      });

      thread = await this.exec_conv_ensure_thread({
        channel_id: inbound.channel_id,
        thread_key: inbound.thread_key,
        user_id: resolved.user_id
      });
    }

    const text = ensure_optional_string(inbound.msg.text);
    if (!text) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "msg.text is required");
    }

    const appended = await this.exec_conv_append_message({
      thread_id: thread.thread_id,
      direction: "in",
      sender: "customer",
      text,
      ...(typeof inbound.msg.ts === "number" ? { ts: inbound.msg.ts } : {}),
      ...(inbound.msg.external_id ? { external_id: inbound.msg.external_id } : {}),
      meta: {
        channel_id: inbound.channel_id,
        thread_key: inbound.thread_key,
        ...(inbound.user_ref
          ? {
              provider: {
                id: inbound.user_ref.id,
                provider: inbound.user_ref.provider,
                ...(inbound.user_ref.username ? { username: inbound.user_ref.username } : {}),
                ...(inbound.user_ref.name ? { name: inbound.user_ref.name } : {})
              }
            }
          : {}),
        ...(resolved ? { user_id: resolved.user_id, user_role: resolved.role } : {}),
        ...(inbound.msg.raw !== undefined ? { raw: inbound.msg.raw } : {})
      }
    });

    const payload = {
      channel: inbound.channel_id,
      thread_id: thread.thread_id,
      message_id: appended.message_id,
      thread_key: inbound.thread_key,
      text
    };
    _xem.fire("agent.message.inbound", payload);
    if (inbound.channel_id === "telegram") {
      _xlog.log(`[telegram] routed inbound -> thread=${thread.thread_id}`);
    }

    return {
      thread_id: thread.thread_id,
      message_id: appended.message_id,
      user_id: thread.user_id,
      accepted: true
    };
  }

  private async send_message_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const outbound = this.parse_send_message_input(params);

    const registration = this.must_get_registration(outbound.channel_id);

    const thread = await this.resolve_outbound_thread(outbound);
    const thread_key = outbound.thread_key ?? thread.channel_thread_id;

    const persisted = await this.exec_conv_append_message({
      thread_id: thread.thread_id,
      direction: "out",
      sender: "agent",
      text: outbound.msg.text,
      ...(typeof outbound.msg.ts === "number" ? { ts: outbound.msg.ts } : {}),
      meta: {
        channel_id: outbound.channel_id,
        thread_key,
        ...(outbound.msg.raw !== undefined ? { raw: outbound.msg.raw } : {})
      }
    });

    this.assert_module_exists(registration.connector_module);
    const connector_result = await this.exec_connector_send({
      connector_module: registration.connector_module,
      channel: outbound.channel_id,
      channel_thread_id: thread_key,
      text: outbound.msg.text,
      ...(outbound.msg.raw !== undefined ? { meta: { raw: outbound.msg.raw } } : {}),
      config: registration.config
    });

    const channel_message_id = this.read_channel_message_id(connector_result);
    const payload = {
      channel: outbound.channel_id,
      thread_id: thread.thread_id,
      message_id: persisted.message_id,
      thread_key,
      text: outbound.msg.text,
      ...(channel_message_id ? { channel_message_id } : {})
    };
    _xem.fire("agent.message.outbound", payload);

    return {
      thread_id: thread.thread_id,
      message_id: persisted.message_id,
      accepted: true,
      ...(channel_message_id ? { channel_message_id } : {}),
      connector_result,
      delivery: connector_result
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "Expected params to be an object");
    }
    if (has_function(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private ensure_object(value: unknown, field_name: string): Record<string, unknown> {
    if (!is_plain_object(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", `${field_name} must be an object`);
    }
    if (has_function(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", `${field_name} must be JSON-safe`);
    }
    return { ...value };
  }

  private ensure_optional_number(value: unknown, field_name: string): number | undefined {
    if (value === undefined || value === null) return undefined;
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", `${field_name} must be a finite number when provided`);
    }
    return value;
  }

  private assert_json_safe(value: unknown, field_name: string): void {
    if (value === undefined || value === null) return;
    if (has_function(value)) {
      throw new XError("E_CHANNELS_BAD_PARAMS", `${field_name} must be JSON-safe`);
    }
  }

  private profile_from_user_ref(user_ref: ChannelUserRef): Record<string, unknown> | undefined {
    const out: Record<string, unknown> = {};
    if (user_ref.username) out.username = user_ref.username;
    if (user_ref.name) {
      out.name = user_ref.name;
      out.display_name = user_ref.name;
    }
    return Object.keys(out).length > 0 ? out : undefined;
  }

  private parse_route_inbound_input(params: Dict): RouteInboundMessageInput {
    const channel_id =
      ensure_optional_string(params.channel_id) ?? ensure_non_empty_string(params.channel, "channel_id|channel");
    const thread_key =
      ensure_optional_string(params.thread_key) ??
      ensure_non_empty_string(params.channel_thread_id, "thread_key|channel_thread_id");
    const msg_input = params.msg === undefined ? {} : this.ensure_object(params.msg, "msg");
    const msg_raw = Object.prototype.hasOwnProperty.call(msg_input, "raw") ? msg_input.raw : params.raw;
    this.assert_json_safe(msg_raw, "msg.raw|raw");
    const msg_text = ensure_optional_string(msg_input.text ?? params.text);
    const msg_ts = this.ensure_optional_number(msg_input.ts ?? params.ts, "msg.ts|ts");
    const msg_external_id = ensure_optional_string(msg_input.external_id ?? params.external_id ?? params.channel_message_id);

    let user_ref: ChannelUserRef | undefined;
    if (params.user_ref !== undefined && params.user_ref !== null) {
      const user_ref_raw = this.ensure_object(params.user_ref, "user_ref");
      const provider = ensure_non_empty_string(user_ref_raw.provider, "user_ref.provider").toLowerCase();
      const id = ensure_non_empty_string(user_ref_raw.id, "user_ref.id");
      const username = ensure_optional_string(user_ref_raw.username);
      const name = ensure_optional_string(user_ref_raw.name);
      user_ref = {
        provider,
        id,
        ...(username ? { username } : {}),
        ...(name ? { name } : {})
      };
    } else {
      const legacy_user_id = ensure_optional_string(params.channel_user_id);
      if (legacy_user_id) {
        const profile = params.profile === undefined ? undefined : this.ensure_object(params.profile, "profile");
        const username = profile ? ensure_optional_string(profile.username) : undefined;
        const display_name =
          (profile ? ensure_optional_string(profile.display_name) : undefined) ??
          (profile ? ensure_optional_string(profile.name) : undefined);
        user_ref = {
          provider: channel_id.toLowerCase(),
          id: legacy_user_id,
          ...(username ? { username } : {}),
          ...(display_name ? { name: display_name } : {})
        };
      }
    }

    return {
      channel_id: channel_id.toLowerCase(),
      thread_key,
      ...(user_ref ? { user_ref } : {}),
      msg: {
        ...(msg_text ? { text: msg_text } : {}),
        ...(typeof msg_ts === "number" ? { ts: msg_ts } : {}),
        ...(msg_external_id ? { external_id: msg_external_id } : {}),
        ...(msg_raw !== undefined ? { raw: msg_raw } : {})
      }
    };
  }

  private parse_send_message_input(params: Dict): SendMessageInput {
    const channel_id =
      ensure_optional_string(params.channel_id) ?? ensure_non_empty_string(params.channel, "channel_id|channel");
    const msg_input = params.msg === undefined ? {} : this.ensure_object(params.msg, "msg");
    const text = ensure_non_empty_string(msg_input.text ?? params.text, "msg.text|text");
    const ts = this.ensure_optional_number(msg_input.ts ?? params.ts, "msg.ts|ts");
    const raw = Object.prototype.hasOwnProperty.call(msg_input, "raw") ? msg_input.raw : params.raw ?? params.meta;
    this.assert_json_safe(raw, "msg.raw|raw|meta");

    const thread_id = ensure_optional_string(params.thread_id);
    const thread_key = ensure_optional_string(params.thread_key) ?? ensure_optional_string(params.channel_thread_id);
    if (!thread_id && !thread_key) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "Either thread_id or thread_key is required");
    }

    return {
      channel_id: channel_id.toLowerCase(),
      ...(thread_id ? { thread_id } : {}),
      ...(thread_key ? { thread_key } : {}),
      msg: {
        text,
        ...(typeof ts === "number" ? { ts } : {}),
        ...(raw !== undefined ? { raw } : {})
      }
    };
  }

  private async resolve_outbound_thread(input: SendMessageInput): Promise<ConversationThread> {
    if (input.thread_id) {
      const thread = await this.exec_conv_get_thread({ thread_id: input.thread_id });
      if (thread.channel !== input.channel_id) {
        throw new XError("E_CHANNELS_BAD_PARAMS", "thread_id channel does not match channel_id");
      }
      return thread;
    }

    const thread = await this.exec_conv_get_thread_by_key({
      channel_id: input.channel_id,
      thread_key: ensure_non_empty_string(input.thread_key, "thread_key")
    });
    if (!thread) {
      throw new XError("E_CHANNELS_THREAD_NOT_FOUND", `Thread not found for channel_id=${input.channel_id}`);
    }
    return thread;
  }

  private must_get_registration(channel: string): ChannelRegistration {
    const registration = this._registrations.get(channel);
    if (!registration) {
      throw new XError("E_CHANNELS_NOT_REGISTERED", `Channel not registered: ${channel}`);
    }
    return registration;
  }

  private assert_module_exists(module_name: string): void {
    try {
      _x.getModule(module_name);
    } catch {
      throw new XError("E_CHANNELS_CONNECTOR_NOT_FOUND", `Connector module not loaded: ${module_name}`);
    }
  }

  private async exec_users_resolve_identity(params: {
    channel: string;
    channel_user_id: string;
    profile?: Record<string, unknown>;
  }): Promise<IdentityResolution> {
    const out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "resolve_identity",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "users.resolve_identity returned invalid payload");
    }

    const user_id = ensure_non_empty_string(out.user_id, "users.resolve_identity.user_id");
    const role = ensure_non_empty_string(out.role, "users.resolve_identity.role");
    return { user_id, role };
  }

  private async exec_conv_ensure_thread(params: {
    channel_id: string;
    thread_key: string;
    user_id: string;
  }): Promise<ConversationThread> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "ensure_thread",
      _params: params
    });
    if (!is_plain_object(out) || !is_plain_object(out.thread)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.ensure_thread returned invalid payload");
    }
    return this.parse_thread(out.thread);
  }

  private async exec_conv_append_message(params: {
    thread_id: string;
    direction: "in" | "out";
    sender?: "customer" | "agent" | "admin" | "system";
    text: string;
    ts?: number;
    external_id?: string;
    meta?: Record<string, unknown>;
  }): Promise<ConversationMessage> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "append_message",
      _params: params
    });
    if (!is_plain_object(out) || !is_plain_object(out.message)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.append_message returned invalid payload");
    }
    return this.parse_message(out.message);
  }

  private async exec_conv_get_thread_by_key(params: {
    channel_id: string;
    thread_key: string;
  }): Promise<ConversationThread | undefined> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "get_thread_by_key",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.get_thread_by_key returned invalid payload");
    }
    if (out.thread === null || out.thread === undefined) {
      return undefined;
    }
    if (!is_plain_object(out.thread)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.get_thread_by_key.thread returned invalid payload");
    }
    return this.parse_thread(out.thread);
  }

  private async exec_conv_get_thread(params: { thread_id: string }): Promise<ConversationThread> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "get_thread",
      _params: params
    });
    if (!is_plain_object(out) || !is_plain_object(out.thread)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.get_thread returned invalid payload");
    }
    return this.parse_thread(out.thread);
  }

  private async exec_connector_send(params: {
    connector_module: string;
    channel: string;
    channel_thread_id: string;
    text: string;
    meta?: Record<string, unknown>;
    config: Record<string, unknown>;
  }): Promise<unknown> {
    try {
      return await _x.execute({
        _module: params.connector_module,
        _op: "send",
        _params: {
          channel: params.channel,
          channel_thread_id: params.channel_thread_id,
          text: params.text,
          ...(params.meta ? { meta: params.meta } : {}),
          config: params.config
        }
      });
    } catch (err) {
      throw new XError("E_CHANNELS_CONNECTOR_SEND_FAILED", "Connector send failed", { _cause: err });
    }
  }

  private read_channel_message_id(value: unknown): string | undefined {
    if (!is_plain_object(value)) return undefined;
    const direct = ensure_optional_string(value.channel_message_id);
    if (direct) return direct;
    const nested = is_plain_object(value.result) ? ensure_optional_string(value.result.channel_message_id) : undefined;
    return nested;
  }

  private parse_thread(value: Dict): ConversationThread {
    const thread: ConversationThread = {
      thread_id: ensure_non_empty_string(value.thread_id, "thread.thread_id"),
      channel: ensure_non_empty_string(value.channel, "thread.channel"),
      channel_thread_id: ensure_non_empty_string(value.channel_thread_id, "thread.channel_thread_id"),
      user_id: ensure_non_empty_string(value.user_id, "thread.user_id"),
      status: ensure_non_empty_string(value.status, "thread.status"),
      created_at: this.ensure_number(value.created_at, "thread.created_at"),
      updated_at: this.ensure_number(value.updated_at, "thread.updated_at"),
      tags: this.ensure_string_array(value.tags, "thread.tags")
    };
    return thread;
  }

  private parse_message(value: Dict): ConversationMessage {
    const direction_raw = ensure_non_empty_string(value.direction, "message.direction");
    if (direction_raw !== "in" && direction_raw !== "out") {
      throw new XError("E_CHANNELS_UPSTREAM", "Invalid message.direction");
    }
    const sender_raw = ensure_non_empty_string(value.sender, "message.sender");
    if (sender_raw !== "customer" && sender_raw !== "agent" && sender_raw !== "admin" && sender_raw !== "system") {
      throw new XError("E_CHANNELS_UPSTREAM", "Invalid message.sender");
    }
    return {
      message_id: ensure_non_empty_string(value.message_id, "message.message_id"),
      thread_id: ensure_non_empty_string(value.thread_id, "message.thread_id"),
      direction: direction_raw,
      sender: sender_raw,
      text: ensure_non_empty_string(value.text, "message.text"),
      ts: this.ensure_number(value.ts, "message.ts"),
      ...(ensure_optional_string(value.channel_message_id)
        ? { channel_message_id: ensure_optional_string(value.channel_message_id) }
        : {}),
      ...(is_plain_object(value.meta) ? { meta: { ...value.meta } } : {})
    };
  }

  private ensure_number(value: unknown, field_name: string): number {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new XError("E_CHANNELS_UPSTREAM", `Invalid ${field_name}`);
    }
    return value;
  }

  private ensure_string_array(value: unknown, field_name: string): string[] {
    if (!Array.isArray(value)) {
      throw new XError("E_CHANNELS_UPSTREAM", `Invalid ${field_name}`);
    }
    const out: string[] = [];
    for (const item of value) {
      if (typeof item !== "string") {
        throw new XError("E_CHANNELS_UPSTREAM", `Invalid ${field_name}`);
      }
      out.push(item);
    }
    return out;
  }

  private now(): number {
    return Date.now();
  }

  private async hydrate_from_xdb(): Promise<void> {
    const rows = await list_registrations_xdb(this._xdb_scope);
    this._registrations.clear();
    for (const row of rows) {
      this._registrations.set(row.channel, {
        channel: row.channel,
        connector_module: row.connector_module,
        config: { ...row.config },
        created_at: row.created_at,
        updated_at: row.updated_at
      });
    }
  }

  private async persist_registration(registration: ChannelRegistration): Promise<void> {
    if (!this._xdb_initialized) return;
    await upsert_registration_xdb(this._xdb_scope, {
      channel: registration.channel,
      connector_module: registration.connector_module,
      config: { ...registration.config },
      created_at: registration.created_at,
      updated_at: registration.updated_at
    });
  }
}

/*
Manual test steps:
1) Send a Telegram message to the bot.
2) Confirm a conversation thread/message was created in conv storage.
3) Reply from the agent and confirm outbound was persisted, then sent via Telegram.
*/
export default ChannelsModule;
