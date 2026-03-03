import { XError, XModule, _x, _xem, _xlog, type XCommandData } from "@xpell/node";

import { ADMIN_COMMANDS_MODULE_NAME } from "./AdminCommandsModule.js";
import { BROADCASTS_MODULE_NAME } from "./BroadcastsModule.js";
import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import { INTENT_REGISTRY_MODULE_NAME } from "./IntentRegistryModule.js";
import { KNOWLEDGE_MODULE_NAME } from "./KnowledgeModule.js";
import {
  build_channel_identity_key,
  get_channel_identity_xdb,
  init_channel_identities_xdb,
  list_channel_identities_xdb,
  upsert_channel_identity_xdb,
  type AgentChannelIdentitiesXdbScope,
  type PersistedChannelIdentityRecord
} from "./channel-identities-xdb.js";
import {
  init_channels_xdb,
  list_registrations_xdb,
  upsert_registration_xdb,
  type AgentChannelsXdbScope
} from "./channels-xdb.js";
import { USERS_MODULE_NAME } from "./UsersModule.js";
import { classify_intent } from "../llm/intent-classifier.js";
import type { ConversationMessage, ConversationThread } from "../types/conversations.js";
import { readCommandCtx, requireKernelCap } from "../runtime/guards.js";

export const CHANNELS_MODULE_NAME = "channels";
const DEFAULT_KB_ID = "ruta1";

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
  is_new: boolean;
};

type EnabledIntent = {
  intent_id: string;
  title: string;
  description?: string;
  skill_id?: string;
  roles_allowed: Array<"owner" | "admin" | "customer">;
  channels_allowed?: string[];
  handler: { module: string; op: string };
  params_schema?: {
    title?: string;
    fields: Array<{
      key: string;
      label: string;
      type: "string" | "number" | "boolean" | "select" | "string_list" | "json";
      options?: Array<{ label: string; value: unknown }>;
    }>;
  };
  examples?: string[];
  synonyms?: string[];
  default_params_json?: string;
  priority?: number;
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

function truncate_for_log(value: string, max_chars = 200): string {
  const trimmed = value.trim();
  if (trimmed.length <= max_chars) return trimmed;
  return `${trimmed.slice(0, max_chars)}...`;
}

export class ChannelsModule extends XModule {
  static _name = CHANNELS_MODULE_NAME;

  private _registrations = new Map<string, ChannelRegistration>();
  private _xdb_scope: AgentChannelsXdbScope;
  private _identity_scope: AgentChannelIdentitiesXdbScope;
  private _channel_identities = new Map<string, PersistedChannelIdentityRecord>();
  private _xdb_initialized = false;

  constructor(opts: ChannelsModuleOptions = {}) {
    super({ _name: CHANNELS_MODULE_NAME });
    this._xdb_scope = {
      _app_id: typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot",
      _env: typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default"
    };
    this._identity_scope = {
      _app_id: this._xdb_scope._app_id,
      _env: this._xdb_scope._env
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

  async _resolve_or_create_user_for_inbound(xcmd: XCommandData) {
    return this.resolve_or_create_user_for_inbound_impl(xcmd);
  }
  async _op_resolve_or_create_user_for_inbound(xcmd: XCommandData) {
    return this.resolve_or_create_user_for_inbound_impl(xcmd);
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
    await init_channel_identities_xdb(this._identity_scope);
    await this.hydrate_from_xdb();
    this._xdb_initialized = true;
    return {
      ok: true,
      channels: this._registrations.size,
      channel_identities: this._channel_identities.size
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

    if (inbound.user_ref) {
      if (inbound.user_ref.provider.toLowerCase() !== inbound.channel_id) {
        throw new XError("E_CHANNELS_BAD_PARAMS", "user_ref.provider must match channel_id");
      }

      resolved = await this.resolve_or_create_user_for_inbound_core({
        channel_id: inbound.channel_id,
        external_user_id: inbound.user_ref.id,
        ...(inbound.user_ref.name ? { display_name: inbound.user_ref.name } : {}),
        ...(inbound.user_ref.username ? { profile: { username: inbound.user_ref.username } } : {})
      });
    }

    if (!thread) {
      if (!resolved) {
        throw new XError("E_CHANNELS_BAD_PARAMS", "user_ref is required when creating a new thread");
      }

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

    const participant_user_id = resolved?.user_id ?? thread.user_id;
    if (participant_user_id) {
      await this.exec_conv_ensure_thread_participant({
        thread_id: thread.thread_id,
        user_id: participant_user_id,
        role: resolved?.role === "owner" || resolved?.role === "admin" || resolved?.role === "system" ? resolved.role : "customer",
        channel_id: inbound.channel_id,
        ...(typeof inbound.msg.ts === "number" ? { last_seen_at: inbound.msg.ts } : {})
      });
    }

    const appended = await this.exec_conv_append_message({
      thread_id: thread.thread_id,
      direction: "in",
      sender: resolved?.role === "owner" || resolved?.role === "admin" ? "admin" : "customer",
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

    const actor_role = resolved?.role === "owner" || resolved?.role === "admin" ? resolved.role : "customer";
    const actor_user_id = resolved?.user_id ?? thread.user_id;
    const actor_ctx = {
      actor: {
        role: actor_role,
        user_id: actor_user_id,
        channel: inbound.channel_id,
        source: `channel:${inbound.channel_id}`
      }
    };
    _xlog.log("[intent-route] inbound", {
      role: actor_role,
      channel: inbound.channel_id,
      user_id: actor_user_id,
      thread_id: thread.thread_id,
      text_preview: truncate_for_log(text)
    });

    const command_check =
      actor_role === "owner" || actor_role === "admin"
        ? await this.exec_admin_command_is_command({ _text: text })
        : { _is_command: false };

    if (command_check._is_command) {
      _xlog.log("[inbound-route]", {
        route: "command",
        actor_role: actor_role,
        trigger: "/"
      });
      const command_result = await this.exec_admin_command_handle_message({
        _text: text,
        _thread_id: thread.thread_id,
        _user_id: actor_user_id,
        _ctx: actor_ctx
      });

      const reply_text = ensure_non_empty_string(command_result._reply_text, "admin_cmd._reply_text");
      await this.exec_channels_send_message({
        channel_id: inbound.channel_id,
        thread_id: thread.thread_id,
        msg: { text: reply_text }
      });

      return {
        thread_id: thread.thread_id,
        _thread_id: thread.thread_id,
        message_id: appended.message_id,
        user_id: actor_user_id,
        accepted: true,
        _handled: "admin_cmd"
      };
    }

    const broadcast_reply_text =
      actor_role === "owner" || actor_role === "admin"
        ? await this.try_route_broadcast({
            xcmd,
            thread_id: thread.thread_id,
            user_id: actor_user_id,
            role: actor_role,
            channel_id: inbound.channel_id,
            text
          })
        : undefined;
    if (broadcast_reply_text) {
      _xlog.log("[inbound-route]", {
        route: "broadcast",
        actor_role: actor_role,
        trigger: this.detect_broadcast_trigger(text) ?? this.detect_broadcast_followup_trigger(text) ?? ""
      });
      await this.exec_channels_send_message({
        channel_id: inbound.channel_id,
        thread_id: thread.thread_id,
        msg: { text: broadcast_reply_text }
      });
      return {
        thread_id: thread.thread_id,
        _thread_id: thread.thread_id,
        message_id: appended.message_id,
        user_id: actor_user_id,
        accepted: true,
        _handled: "broadcast"
      };
    }

    const kb_followup_trigger =
      actor_role === "owner" || actor_role === "admin" ? this.detect_kb_followup_trigger(text) : undefined;
    const kb_manage_trigger =
      actor_role === "owner" || actor_role === "admin" ? this.detect_kb_manage_trigger(text) : undefined;
    const kb_trigger = kb_followup_trigger ?? kb_manage_trigger;
    const kb_reply_text = kb_trigger
      ? await this.try_route_kb_inbox({
          xcmd,
          thread_id: thread.thread_id,
          user_id: actor_user_id,
          role: actor_role,
          channel_id: inbound.channel_id,
          text
        })
      : undefined;
    if (kb_reply_text) {
      _xlog.log("[inbound-route]", {
        route: "kb_manage",
        actor_role: actor_role,
        trigger: kb_trigger ?? ""
      });
      await this.exec_channels_send_message({
        channel_id: inbound.channel_id,
        thread_id: thread.thread_id,
        msg: { text: kb_reply_text }
      });
      return {
        thread_id: thread.thread_id,
        _thread_id: thread.thread_id,
        message_id: appended.message_id,
        user_id: actor_user_id,
        accepted: true,
        _handled: "kb"
      };
    }

    _xlog.log("[inbound-route]", {
      route: "qa",
      actor_role: actor_role,
      trigger: ""
    });

    const intent_reply_text = await this.try_route_intent({
      xcmd,
      thread_id: thread.thread_id,
      user_id: actor_user_id,
      role: actor_role,
      channel_id: inbound.channel_id,
      text
    });
    if (intent_reply_text) {
      await this.exec_channels_send_message({
        channel_id: inbound.channel_id,
        thread_id: thread.thread_id,
        msg: { text: intent_reply_text }
      });
      return {
        thread_id: thread.thread_id,
        _thread_id: thread.thread_id,
        message_id: appended.message_id,
        user_id: actor_user_id,
        accepted: true,
        _handled: "intent"
      };
    }

    await this.exec_agent_handle_inbound({
      channel_id: inbound.channel_id,
      thread_id: thread.thread_id,
      text,
      user_ref: inbound.user_ref,
      _ctx: this.forward_ctx_with_actor(xcmd, actor_ctx.actor)
    });

    return {
      thread_id: thread.thread_id,
      _thread_id: thread.thread_id,
      message_id: appended.message_id,
      user_id: actor_user_id,
      accepted: true,
      _handled: "chat"
    };
  }

  private async resolve_or_create_user_for_inbound_impl(xcmd: XCommandData) {
    const params = this.ensure_params(xcmd._params);
    const channel_id =
      ensure_optional_string(params._channel_id) ?? ensure_optional_string(params.channel_id) ?? ensure_optional_string(params.channel);
    const external_user_id =
      ensure_optional_string(params._external_user_id) ??
      ensure_optional_string(params.external_user_id) ??
      ensure_optional_string(params.channel_user_id);
    if (!channel_id || !external_user_id) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "_channel_id and _external_user_id are required");
    }

    const display_name = ensure_optional_string(params._display_name) ?? ensure_optional_string(params.display_name);
    const profile = params._profile === undefined ? params.profile : params._profile;
    if (profile !== undefined && profile !== null && (!is_plain_object(profile) || has_function(profile))) {
      throw new XError("E_CHANNELS_BAD_PARAMS", "_profile must be a JSON-safe object");
    }
    const out = await this.resolve_or_create_user_for_inbound_core({
      channel_id: channel_id.toLowerCase(),
      external_user_id,
      ...(display_name ? { display_name } : {}),
      ...(is_plain_object(profile) ? { profile } : {})
    });

    return {
      _user_id: out.user_id,
      _is_new: out.is_new
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

  private parse_route_inbound_input(params: Dict): RouteInboundMessageInput {
    const channel_id =
      ensure_optional_string(params._channel) ??
      ensure_optional_string(params.channel_id) ??
      ensure_non_empty_string(params.channel, "channel_id|channel|_channel");
    const legacy_user_id =
      ensure_optional_string(params._channel_user_id) ?? ensure_optional_string(params.channel_user_id);
    const thread_key =
      ensure_optional_string(params._thread_key) ??
      ensure_optional_string(params.thread_key) ??
      ensure_optional_string(params._channel_thread_id) ??
      ensure_optional_string(params.channel_thread_id);
    const msg_input = params.msg === undefined ? {} : this.ensure_object(params.msg, "msg");
    const msg_raw =
      Object.prototype.hasOwnProperty.call(msg_input, "raw")
        ? msg_input.raw
        : Object.prototype.hasOwnProperty.call(params, "_meta")
          ? params._meta
          : params.raw;
    this.assert_json_safe(msg_raw, "msg.raw|raw|_meta");
    const msg_text = ensure_optional_string(msg_input.text ?? params._text ?? params.text);
    const msg_ts = this.ensure_optional_number(msg_input.ts ?? params.ts, "msg.ts|ts");
    const meta_obj = is_plain_object(params._meta) ? params._meta : undefined;
    const msg_external_id = ensure_optional_string(
      msg_input.external_id ??
        params._external_id ??
        params.external_id ??
        params.channel_message_id ??
        (meta_obj ? meta_obj.external_id : undefined)
    );

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
      if (legacy_user_id) {
        const profile_source = meta_obj && is_plain_object(meta_obj.profile) ? meta_obj.profile : params.profile;
        const profile = profile_source === undefined ? undefined : this.ensure_object(profile_source, "profile");
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
      thread_key: thread_key ?? ensure_non_empty_string(legacy_user_id, "thread_key|_thread_key|channel_user_id"),
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

  private async resolve_or_create_user_for_inbound_core(input: {
    channel_id: string;
    external_user_id: string;
    display_name?: string;
    profile?: Record<string, unknown>;
  }): Promise<IdentityResolution> {
    const key = build_channel_identity_key(input.channel_id, input.external_user_id);
    const existing = this._channel_identities.get(key) ?? (await get_channel_identity_xdb(this._identity_scope, key));
    if (existing) {
      const upstream = await this.exec_users_upsert_from_channel_identity({
        channel_id: input.channel_id,
        external_user_id: input.external_user_id,
        ...(input.display_name ? { display_name: input.display_name } : {}),
        ...(input.profile && is_plain_object(input.profile) && ensure_optional_string(input.profile.username)
          ? { external_username: ensure_optional_string(input.profile.username) }
          : {})
      });
      const next_record: PersistedChannelIdentityRecord = {
        ...existing,
        ...(input.profile && is_plain_object(input.profile)
          ? { _meta: { ...input.profile } }
          : existing._meta
            ? { _meta: { ...existing._meta } }
            : {}),
        _user_id: upstream.user_id,
        ...(input.display_name ? { _display_name: input.display_name } : existing._display_name ? { _display_name: existing._display_name } : {}),
        _updated_at: Date.now()
      };
      await upsert_channel_identity_xdb(this._identity_scope, next_record);
      this._channel_identities.set(key, next_record);
      return {
        user_id: upstream.user_id,
        role: upstream.role,
        is_new: false
      };
    }

    const upstream = await this.exec_users_upsert_from_channel_identity({
      channel_id: input.channel_id,
      external_user_id: input.external_user_id,
      ...(input.display_name ? { display_name: input.display_name } : {}),
      ...(input.profile && is_plain_object(input.profile) && ensure_optional_string(input.profile.username)
        ? { external_username: ensure_optional_string(input.profile.username) }
        : {})
    });

    const now = this.now();
    const record: PersistedChannelIdentityRecord = {
      _key: key,
      _channel_id: input.channel_id,
      _external_user_id: input.external_user_id,
      _user_id: upstream.user_id,
      ...(input.display_name ? { _display_name: input.display_name } : {}),
      ...(input.profile && is_plain_object(input.profile) ? { _meta: { ...input.profile } } : {}),
      _created_at: now,
      _updated_at: now
    };

    await upsert_channel_identity_xdb(this._identity_scope, record);
    this._channel_identities.set(key, record);

    return {
      user_id: upstream.user_id,
      role: upstream.role,
      is_new: true
    };
  }

  private async exec_users_upsert_from_channel_identity(params: {
    channel_id: string;
    external_user_id: string;
    external_username?: string;
    display_name?: string;
  }): Promise<IdentityResolution> {
    const out = await _x.execute({
      _module: USERS_MODULE_NAME,
      _op: "upsert_from_channel_identity",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "users.upsert_from_channel_identity returned invalid payload");
    }

    const user_id = ensure_non_empty_string(out.user_id, "users.upsert_from_channel_identity.user_id");
    const role = ensure_optional_string(out.role) ?? "customer";
    return { user_id, role, is_new: false };
  }

  private async exec_admin_command_is_command(params: { _text: string }): Promise<{ _is_command: boolean }> {
    const out = await _x.execute({
      _module: ADMIN_COMMANDS_MODULE_NAME,
      _op: "is_command",
      _params: params
    });
    if (!is_plain_object(out) || typeof out._is_command !== "boolean") {
      throw new XError("E_CHANNELS_UPSTREAM", "admin_cmd.is_command returned invalid payload");
    }
    return { _is_command: out._is_command };
  }

  private async exec_admin_command_handle_message(params: {
    _text: string;
    _thread_id: string;
    _user_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: ADMIN_COMMANDS_MODULE_NAME,
      _op: "handle_message",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "admin_cmd.handle_message returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "admin_cmd.handle_message._reply_text")
    };
  }

  private async exec_channels_send_message(params: {
    channel_id: string;
    thread_id: string;
    msg: { text: string };
  }): Promise<void> {
    const out = await _x.execute({
      _module: CHANNELS_MODULE_NAME,
      _op: "send_message",
      _params: params
    });
    if (!is_plain_object(out) || out.accepted !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "channels.send_message returned invalid payload");
    }
  }

  private async exec_agent_handle_inbound(params: {
    channel_id: string;
    thread_id: string;
    text: string;
    user_ref?: ChannelUserRef;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: "agent",
      _op: "handle_inbound",
      _params: params
    });
    if (!is_plain_object(out) || typeof out.reply_text !== "string" || out.reply_text.trim().length === 0) {
      throw new XError("E_CHANNELS_UPSTREAM", "agent.handle_inbound returned invalid payload");
    }
  }

  private async try_route_intent(input: {
    xcmd: XCommandData;
    thread_id: string;
    user_id: string;
    role: string;
    channel_id: string;
    text: string;
  }): Promise<string | undefined> {
    const ctx = readCommandCtx(input.xcmd);
    if (typeof ctx.kernel_cap !== "string" || ctx.kernel_cap.trim().length === 0) {
      _xlog.log("[intent-route] selection", {
        selected: false,
        reason: "missing_kernel_cap"
      });
      return undefined;
    }

    let enabled_intents: EnabledIntent[] = [];
    try {
      enabled_intents = await this.exec_intent_get_enabled_for_context({
        role: input.role,
        channel: input.channel_id,
        _ctx: {
          kernel_cap: ctx.kernel_cap,
          actor: {
            role: "system",
            source: "channels:intent"
          }
        }
      });
    } catch {
      _xlog.log("[intent-route] selection", {
        selected: false,
        reason: "intent_registry_unavailable"
      });
      return undefined;
    }
    if (enabled_intents.length === 0) {
      _xlog.log("[intent-route] selection", {
        selected: false,
        reason: "no_enabled_intents"
      });
      return undefined;
    }

    const thread_state = await this.exec_conv_get_thread_with_messages({
      thread_id: input.thread_id,
      limit_messages: 8
    });
    const recent_messages = thread_state.messages.map((message) => `${message.sender}: ${message.text}`);

    const forced_override = this.detect_summary_today_override(input.role, input.text);
    let classified:
      | {
          intent_id: string;
          params: Record<string, unknown>;
          confidence?: number;
        }
      | undefined;
    if (!forced_override) {
      try {
        classified = await classify_intent({
          message: input.text,
          recent_messages,
          enabled_intents: enabled_intents.map((intent) => ({
            intent_id: intent.intent_id,
            title: intent.title,
            ...(intent.description ? { description: intent.description } : {}),
            ...(intent.examples ? { examples: intent.examples } : {}),
            ...(intent.synonyms ? { synonyms: intent.synonyms } : {})
          })),
          agent_name: "XBot",
          agent_role: input.role,
          channel: input.channel_id,
          chat: async (messages) => {
            const out = await this.exec_azure_openai_chat({ messages, temperature: 0 });
            return out.text;
          }
        });
      } catch {
        _xlog.log("[intent-route] classifier", {
          intent_id: "error",
          confidence: undefined,
          text_preview: truncate_for_log(input.text)
        });
      }
    }
    if (forced_override) {
      classified = {
        intent_id: "admin.conv.summary_today",
        params: {},
        confidence: 1
      };
    }
    _xlog.log("[intent-route] classifier", {
      intent_id: classified?.intent_id ?? "none",
      confidence: classified?.confidence,
      forced: forced_override === true,
      text_preview: truncate_for_log(input.text)
    });
    if (!classified || classified.intent_id === "none") {
      _xlog.log("[intent-route] selection", {
        selected: false,
        reason: "classifier_none"
      });
      return undefined;
    }

    const selected = enabled_intents.find((intent) => intent.intent_id === classified.intent_id);
    if (!selected) {
      _xlog.log("[intent-route] selection", {
        selected: false,
        reason: "intent_not_enabled",
        intent_id: classified.intent_id
      });
      return undefined;
    }
    _xlog.log("[intent-route] selection", {
      selected: true,
      intent_id: selected.intent_id,
      handler: `${selected.handler.module}.${selected.handler.op}`,
      forced: forced_override === true
    });

    const normalized_params = this.normalize_intent_params(classified.params, selected);
    const merged_params = this.merge_default_intent_params(selected.default_params_json, normalized_params);
    let result: unknown;
    const started_at = Date.now();
    try {
      result = await this.exec_intent_handler({
        module: selected.handler.module,
        op: selected.handler.op,
        params: {
          ...merged_params,
          _ctx: this.forward_ctx_with_actor(input.xcmd, {
            role: input.role,
            user_id: input.user_id,
            source: `channel:${input.channel_id}`
          })
        }
      });
      _xlog.log("[intent-route] handler", {
        target: `${selected.handler.module}.${selected.handler.op}`,
        duration_ms: Date.now() - started_at,
        _ok: true
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      _xlog.log("[intent-route] handler", {
        target: `${selected.handler.module}.${selected.handler.op}`,
        duration_ms: Date.now() - started_at,
        _ok: false
      });
      if (selected.intent_id === "admin.conv.summary_today") {
        return `Summary failed: ${message}`;
      }
      return undefined;
    }

    if (selected.intent_id === "admin.conv.summary_today") {
      return this.format_summary_today_reply(result);
    }
    return this.read_reply_text_from_result(result);
  }

  private async try_route_broadcast(input: {
    xcmd: XCommandData;
    thread_id: string;
    user_id: string;
    role: string;
    channel_id: string;
    text: string;
  }): Promise<string | undefined> {
    if (input.role !== "admin" && input.role !== "owner") {
      return undefined;
    }

    const trimmed = input.text.trim();
    const lower = trimmed.toLowerCase();
    const ctx = this.forward_ctx_with_actor(input.xcmd, {
      role: input.role,
      user_id: input.user_id,
      source: `channel:${input.channel_id}`,
      channel: input.channel_id
    });

    const pending = await this.exec_conv_get_pending_action({
      _thread_id: input.thread_id,
      _ctx: ctx
    });
    const pending_action = pending.pending_action;
    const pending_type = pending_action ? ensure_optional_string(pending_action.type) : undefined;
    const pending_id = pending_action ? ensure_optional_string(pending_action.id) : undefined;
    const pending_expires_at =
      pending_action && typeof pending_action.expires_at === "number" && Number.isFinite(pending_action.expires_at)
        ? Math.floor(pending_action.expires_at)
        : undefined;

    if (
      lower === "confirm" ||
      lower === "apply" ||
      lower === "yes" ||
      lower === "send" ||
      lower === "send it" ||
      lower === "go ahead" ||
      lower === "ok" ||
      lower === "okay" ||
      lower === "sure" ||
      lower === "y"
    ) {
      let broadcast_id = pending_type === "broadcast" && pending_id ? pending_id : undefined;
      if (!broadcast_id) {
        const legacy_pending = await this.exec_broadcast_get_pending_draft({ _ctx: ctx });
        broadcast_id = legacy_pending._pending ? ensure_optional_string(legacy_pending._broadcast_id) : undefined;
      }
      if (!broadcast_id) return undefined;
      if (typeof pending_expires_at === "number" && pending_expires_at > 0 && pending_expires_at < Date.now()) {
        await this.exec_conv_clear_pending_action({
          _thread_id: input.thread_id,
          _ctx: ctx
        });
        return "That broadcast draft expired. Please create a new one.";
      }
      const out = await this.exec_broadcast_send({
        _id: broadcast_id,
        _ctx: ctx
      });
      await this.exec_conv_clear_pending_action({
        _thread_id: input.thread_id,
        _ctx: ctx
      });
      return `Broadcast sent ✅ sent=${out.sent_ok} failed=${out.sent_err}`;
    }

    if (
      lower === "cancel" ||
      lower === "no" ||
      lower === "n" ||
      lower === "stop" ||
      lower === "abort" ||
      lower === "nevermind" ||
      lower === "never mind"
    ) {
      let broadcast_id = pending_type === "broadcast" && pending_id ? pending_id : undefined;
      if (!broadcast_id) {
        const legacy_pending = await this.exec_broadcast_get_pending_draft({ _ctx: ctx });
        broadcast_id = legacy_pending._pending ? ensure_optional_string(legacy_pending._broadcast_id) : undefined;
      }
      if (!broadcast_id) return undefined;
      const out = await this.exec_broadcast_cancel_draft({
        _broadcast_id: broadcast_id,
        _ctx: ctx
      });
      await this.exec_conv_clear_pending_action({
        _thread_id: input.thread_id,
        _ctx: ctx
      });
      return out._reply_text || "Broadcast cancelled.";
    }

    const parsed = this.parse_broadcast_phrase(trimmed);
    if (!parsed) return undefined;
    if (!parsed.message_text) {
      return "What message should I broadcast?";
    }

    const out = await this.exec_broadcast_draft_create({
      _channel: parsed.channel,
      _audience_role: parsed.audience_role,
      _message_text: parsed.message_text,
      _ctx: ctx
    });
    await this.exec_conv_set_pending_action({
      _thread_id: input.thread_id,
      _type: "broadcast",
      _id: out.broadcast_id,
      _payload: {
        channel: parsed.channel,
        audience_role: parsed.audience_role,
        message_text: parsed.message_text,
        recipient_count: out.recipient_count
      },
      _expires_at: Date.now() + 10 * 60 * 1000,
      _ctx: ctx
    });
    return `${out.preview}\nSend it? (yes/send/confirm) or cancel.`;
  }

  private async try_route_kb_inbox(input: {
    xcmd: XCommandData;
    thread_id: string;
    user_id: string;
    role: string;
    channel_id: string;
    text: string;
  }): Promise<string | undefined> {
    if (input.role !== "admin" && input.role !== "owner") {
      return undefined;
    }
    const trimmed = input.text.trim();
    const lower = trimmed.toLowerCase();
    const kb_ctx = this.forward_ctx_with_actor(input.xcmd, {
      role: input.role,
      user_id: input.user_id,
      source: `channel:${input.channel_id}`,
      channel: input.channel_id
      });

    try {
      if (lower === "confirm" || lower === "apply" || lower === "yes") {
        const pending = await this.exec_kb_pending_get({ _ctx: kb_ctx });
        if (!pending._pending) return "No pending KB action to apply.";
        if (pending._kind === "kb.replace") {
          await this.exec_kb_replace_confirm({ _action_id: ensure_non_empty_string(pending._action_id, "_action_id"), _ctx: kb_ctx });
          return "Knowledge base replaced ✅";
        }
        if (pending._kind === "kb.delete_section") {
          await this.exec_kb_delete_section_confirm({ _action_id: ensure_non_empty_string(pending._action_id, "_action_id"), _ctx: kb_ctx });
          return "Knowledge base section deleted ✅";
        }
        if (pending._kind === "kb.append" || pending._kind === "kb.patch_section") {
          const out = await this.exec_kb_apply_legacy({ _action_id: ensure_non_empty_string(pending._action_id, "_action_id"), _ctx: kb_ctx });
          return out._reply_text;
        }
        if (pending._kind === "kb.append_to_section") {
          const out = await this.exec_kb_append_to_section({
            _dry_run: false,
            _ctx: kb_ctx
          });
          return out._message;
        }
        if (pending._kind === "kb.remove_from_section") {
          const out = await this.exec_kb_remove_from_section({
            _dry_run: false,
            _ctx: kb_ctx
          });
          return out._message;
        }
        if (pending._kind === "kb.update_price") {
          const out = await this.exec_kb_update_price({
            ...(ensure_optional_string(pending._kb_file) ? { _kb_file: ensure_optional_string(pending._kb_file) } : {}),
            _dry_run: false,
            _ctx: kb_ctx
          });
          return out._message;
        }
      }
      if (lower === "cancel") {
        const pending = await this.exec_kb_pending_get({ _ctx: kb_ctx });
        if (!pending._pending) return "No pending KB action to cancel.";
        const out = await this.exec_kb_cancel_pending({
          _action_id: pending._action_id,
          _ctx: kb_ctx
        });
        return out._reply_text;
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return `KB action failed: ${message}`;
    }

    let interpreted:
      | {
          intent: string;
          confidence: number;
          params: Record<string, unknown>;
        }
      | undefined;

    try {
      interpreted = this.detect_admin_kb_override(trimmed);
      if (!interpreted) {
        interpreted = await this.classify_admin_kb_intent(trimmed);
      }
    } catch {
      interpreted = undefined;
    }

    if (!interpreted || interpreted.confidence < 0.65 || interpreted.intent === "admin.other") {
      return undefined;
    }

    try {
      const params = interpreted.params;
      const kb_id = ensure_optional_string(params.kb_id) ?? DEFAULT_KB_ID;
      const lang = this.detect_kb_lang(ensure_optional_string(params.lang) ?? trimmed);
      if (interpreted.intent === "admin.help") {
        return "You can ask me to show, append, patch a section, replace, or delete knowledge base content.";
      }
      if (interpreted.intent === "admin.kb.show") {
        _xlog.log("[kb-route]", {
          route: "nl",
          resolved_intent: "admin.kb.show",
          called_op: "kb.show"
        });
        const section_title = ensure_optional_string(params.section_title);
        const out = await this.exec_admin_command_handle_message({
          _text: section_title ? `/kb show section ${section_title}` : "/kb show",
          _thread_id: input.thread_id,
          _user_id: input.user_id,
          _ctx: kb_ctx
        });
        return out._reply_text;
      }
      if (interpreted.intent === "admin.kb.append") {
        const section_title = ensure_optional_string(params.section_title);
        if (section_title) {
          const line_text = ensure_non_empty_string(params.line_text ?? params.content ?? params.md, "params.line_text");
          const out = await this.exec_kb_append_to_section({
            _kb_id: kb_id,
            _lang: lang,
            _section_title: section_title,
            _line_text: line_text,
            _dry_run: true,
            _ctx: kb_ctx
          });
          if (typeof out._preview === "string" && out._preview.trim().length > 0) {
            return `${out._preview}\n${out._message}`;
          }
          return out._message;
        }
        const md = ensure_non_empty_string(params.md ?? params.content, "params.md");
        await this.exec_kb_append({
          _kb_id: kb_id,
          _lang: lang,
          _md: md,
          _ctx: kb_ctx
        });
        return "Knowledge base updated ✅";
      }
      if (interpreted.intent === "admin.kb.remove") {
        const section_title = ensure_non_empty_string(params.section_title, "params.section_title");
        const match_text = ensure_non_empty_string(params.match_text, "params.match_text");
        const out = await this.exec_kb_remove_from_section({
          _kb_id: kb_id,
          _lang: lang,
          _section_title: section_title,
          _match_text: match_text,
          _dry_run: true,
          _ctx: kb_ctx
        });
        if (typeof out._preview === "string" && out._preview.trim().length > 0) {
          return `${out._preview}\n${out._message}`;
        }
        return out._message;
      }
      if (interpreted.intent === "admin.kb.update_price") {
        const item_name = ensure_non_empty_string(params.item_name, "params.item_name");
        const to_price = ensure_non_empty_string(params.to_price, "params.to_price");
        const from_price = ensure_optional_string(params.from_price);
        const out = await this.exec_kb_update_price({
          _item_name: item_name,
          ...(from_price ? { _from_price: from_price } : {}),
          _to_price: to_price,
          _kb_id: kb_id,
          _lang: lang,
          _dry_run: true,
          _ctx: kb_ctx
        });
        if (typeof out._preview === "string" && out._preview.trim().length > 0) {
          return `${out._preview}\n${out._message}`;
        }
        return out._message;
      }
      if (interpreted.intent === "admin.kb.patch_section") {
        const section_title = ensure_non_empty_string(params.section_title, "params.section_title");
        const md = ensure_non_empty_string(params.md ?? params.content, "params.md");
        await this.exec_kb_patch_section({
          _kb_id: kb_id,
          _lang: lang,
          _section_title: section_title,
          _md: md,
          _ctx: kb_ctx
        });
        return `Knowledge base section '${section_title}' updated ✅`;
      }
      if (interpreted.intent === "admin.kb.replace") {
        const content = ensure_non_empty_string(params.content, "params.content");
        const out = await this.exec_kb_replace_propose({
          _kb_id: kb_id,
          _lang: lang,
          _content: content,
          _ctx: kb_ctx
        });
        return typeof out.preview === "string"
          ? out.preview
          : `I’m about to REPLACE the knowledge base (lang=${lang}). Reply 'confirm' to proceed or 'cancel'.`;
      }
      if (interpreted.intent === "admin.kb.delete_section") {
        const section_title = ensure_non_empty_string(params.section_title, "params.section_title");
        const out = await this.exec_kb_delete_section_propose({
          _kb_id: kb_id,
          _lang: lang,
          _section_title: section_title,
          _ctx: kb_ctx
        });
        return typeof out.preview === "string"
          ? out.preview
          : `I’m about to DELETE section '${section_title}' (lang=${lang}). Reply 'confirm' to proceed or 'cancel'.`;
      }
      return undefined;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return `KB request failed: ${message}`;
    }
  }

  private detect_kb_lang(text: string): "es" | "en" {
    const normalized = text.toLowerCase();
    const spanish_markers = [" el ", " la ", " los ", " las ", " para ", " horario", " horas", " hola", "gracias", " por "];
    let score = 0;
    for (const marker of spanish_markers) {
      if (normalized.includes(marker)) score += 1;
    }
    if (/[áéíóúñ¿¡]/.test(normalized)) score += 2;
    return score >= 2 ? "es" : "en";
  }

  private detect_kb_manage_trigger(text: string): string | undefined {
    const normalized = text.trim().toLowerCase();
    if (!normalized) return undefined;
    const direct_terms = ["knowledge base", "base de conocimiento", "update kb", "kb"];
    for (const term of direct_terms) {
      if (normalized.includes(term)) return term;
    }
    if (/(^|\b)(add|append|update|replace|delete|remove|eliminar|borrar)(\b|$)/u.test(normalized)) {
      if (/\bsection\b/u.test(normalized) || /\bsección\b/u.test(normalized)) {
        return /\bsección\b/u.test(normalized) ? "sección" : "section";
      }
    }
    return undefined;
  }

  private detect_broadcast_trigger(text: string): string | undefined {
    const normalized = text.trim().toLowerCase();
    if (!normalized) return undefined;
    if (/^send message to all customers\s*:/iu.test(normalized)) return "send_message_to_all_customers";
    if (/^broadcast to telegram users\s*:/iu.test(normalized)) return "broadcast_to_telegram_users";
    if (/^broadcast to all customers\s*:/iu.test(normalized)) return "broadcast_to_all_customers";
    if (/^broadcast\s+.+\s+to\s+(?:every\s+)?telegram users$/iu.test(normalized)) return "broadcast_to_every_telegram_users";
    return undefined;
  }

  private detect_broadcast_followup_trigger(text: string): string | undefined {
    const normalized = text.trim().toLowerCase();
    if (normalized === "confirm" || normalized === "apply" || normalized === "yes") return "confirm";
    if (normalized === "cancel") return "cancel";
    return undefined;
  }

  private parse_broadcast_phrase(
    text: string
  ): { channel: "telegram" | "all"; audience_role: "customer" | "all"; message_text: string } | undefined {
    const send_all_customers = text.match(/^send message to all customers\s*:\s*(.+)$/iu);
    if (send_all_customers) {
      return {
        channel: "telegram",
        audience_role: "customer",
        message_text: send_all_customers[1].trim()
      };
    }
    const broadcast_telegram = text.match(/^broadcast to telegram users\s*:\s*(.+)$/iu);
    if (broadcast_telegram) {
      return {
        channel: "telegram",
        audience_role: "all",
        message_text: broadcast_telegram[1].trim()
      };
    }
    const broadcast_customers = text.match(/^broadcast to all customers\s*:\s*(.+)$/iu);
    if (broadcast_customers) {
      return {
        channel: "telegram",
        audience_role: "customer",
        message_text: broadcast_customers[1].trim()
      };
    }
    const broadcast_all_prefix = text.match(/^broadcast to all\s+(.+)$/iu);
    if (broadcast_all_prefix) {
      return {
        channel: "telegram",
        audience_role: "all",
        message_text: broadcast_all_prefix[1].trim()
      };
    }
    const broadcast_every_telegram = text.match(/^broadcast\s+(.+?)\s+to\s+(?:every\s+)?telegram users$/iu);
    if (broadcast_every_telegram) {
      return {
        channel: "telegram",
        audience_role: "all",
        message_text: broadcast_every_telegram[1].trim()
      };
    }
    return undefined;
  }

  private detect_kb_followup_trigger(text: string): string | undefined {
    const normalized = text.trim().toLowerCase();
    if (normalized === "confirm" || normalized === "apply" || normalized === "yes") return "confirm";
    if (normalized === "cancel") return "cancel";
    return undefined;
  }

  private detect_admin_kb_override(
    text: string
  ): { intent: string; confidence: number; params: Record<string, unknown> } | undefined {
    const trimmed = text.trim();
    const lower = trimmed.toLowerCase();
    const update_kb_match = trimmed.match(/^update (?:the )?(?:kb|knowledge base|base de conocimiento)\s*:?\s+(.+)$/i);
    if (update_kb_match) {
      const nested = this.detect_admin_kb_override(update_kb_match[1].trim());
      if (nested) return nested;
    }
    if (
      /^show (?:the )?(?:kb|knowledge base)\s+section\s+(.+)$/i.test(trimmed)
    ) {
      const section_match = trimmed.match(/^show (?:the )?(?:kb|knowledge base)\s+section\s+(.+)$/i);
      const section_title = typeof section_match?.[1] === "string" ? section_match[1].trim() : "";
      if (section_title) {
        return {
          intent: "admin.kb.show",
          confidence: 1,
          params: { section_title }
        };
      }
    }
    if (
      /^(show|display).*(\bkb\b|knowledge base)/i.test(trimmed) ||
      lower === "knowledge base" ||
      lower === "what's in the kb" ||
      lower === "what is in the kb" ||
      lower === "show the kb"
    ) {
      return { intent: "admin.kb.show", confidence: 1, params: {} };
    }
    if (/^show full (kb|knowledge base)/i.test(trimmed)) {
      return { intent: "admin.kb.show", confidence: 1, params: {} };
    }
    if (/^add to kb\s*:/i.test(trimmed)) {
      return {
        intent: "admin.kb.append",
        confidence: 1,
        params: { md: trimmed.replace(/^add to kb\s*:/i, "").trim() }
      };
    }
    const append_section_match = trimmed.match(/^add to section\s+(.+?)\s+-\s+(.+)$/i);
    if (append_section_match) {
      return {
        intent: "admin.kb.append",
        confidence: 1,
        params: {
          section_title: append_section_match[1].trim(),
          line_text: `- ${append_section_match[2].trim()}`
        }
      };
    }
    const remove_section_match = trimmed.match(/^(?:remove|delete|eliminar|borrar)\s+(.+?)\s+from\s+section\s+(.+)$/i);
    if (remove_section_match) {
      return {
        intent: "admin.kb.remove",
        confidence: 1,
        params: {
          match_text: remove_section_match[1].trim(),
          section_title: remove_section_match[2].trim()
        }
      };
    }
    const price_match = trimmed.match(/^update (?:the )?price of (.+?)(?:\s+from\s+(.+?))?\s+to\s+(.+)$/i);
    if (price_match) {
      return {
        intent: "admin.kb.update_price",
        confidence: 1,
        params: {
          item_name: price_match[1].trim(),
          ...(typeof price_match[2] === "string" && price_match[2].trim().length > 0 ? { from_price: price_match[2].trim() } : {}),
          to_price: price_match[3].trim()
        }
      };
    }
    if (/^replace kb with\s*:/i.test(trimmed)) {
      return {
        intent: "admin.kb.replace",
        confidence: 1,
        params: { content: trimmed.replace(/^replace kb with\s*:/i, "").trim() }
      };
    }
    const delete_match = trimmed.match(/^delete (?:the )?section\s+(.+)$/i);
    if (delete_match) {
      return {
        intent: "admin.kb.delete_section",
        confidence: 1,
        params: { section_title: delete_match[1].trim() }
      };
    }
    if (
      lower === "help with kb" ||
      lower === "kb help" ||
      /^(how (?:do|can) i|help)\b.*\b(update|edit|change|use)\b.*\b(kb|knowledge base|base de conocimiento)\b/i.test(trimmed)
    ) {
      return { intent: "admin.help", confidence: 1, params: {} };
    }
    return undefined;
  }

  private async classify_admin_kb_intent(
    text: string
  ): Promise<{ intent: string; confidence: number; params: Record<string, unknown> }> {
    const out = await this.exec_azure_openai_chat({
      messages: [
        {
          role: "system",
          content:
            "Classify an admin chat message into one intent. " +
            "Return strict JSON only: {\"intent\":\"admin.kb.show|admin.kb.append|admin.kb.remove|admin.kb.update_price|admin.kb.patch_section|admin.kb.replace|admin.kb.delete_section|admin.help|admin.other\",\"confidence\":0,\"params\":{}}. " +
            "For admin.kb.show with a section request, use params {\"section_title\":\"...\"}. " +
            "For admin.kb.append when adding to a section, use params {\"section_title\":\"...\",\"line_text\":\"...\"}. " +
            "For admin.kb.remove use params {\"section_title\":\"...\",\"match_text\":\"...\"}. " +
            "For admin.kb.update_price use params {\"item_name\":\"...\",\"from_price\":\"optional\",\"to_price\":\"...\"}. " +
            "Do not return prose."
        },
        {
          role: "user",
          content: JSON.stringify({
            message: text
          })
        }
      ],
      temperature: 0
    });
    const parsed = this.parse_json_object_local(out.text);
    const intent = ensure_non_empty_string(parsed.intent, "intent");
    const confidence_raw = typeof parsed.confidence === "number" && Number.isFinite(parsed.confidence) ? parsed.confidence : 0;
    const params = is_plain_object(parsed.params) && !has_function(parsed.params) ? { ...parsed.params } : {};
    return {
      intent,
      confidence: confidence_raw,
      params
    };
  }

  private parse_json_object_local(text: string): Dict {
    const trimmed = text.trim();
    const cleaned = trimmed.startsWith("```")
      ? trimmed.replace(/^```[a-zA-Z0-9_-]*\s*/, "").replace(/```$/, "").trim()
      : trimmed;
    try {
      const parsed = JSON.parse(cleaned);
      if (!is_plain_object(parsed) || has_function(parsed)) {
        throw new Error("invalid");
      }
      return parsed;
    } catch {
      throw new XError("E_CHANNELS_UPSTREAM", "Invalid admin KB classifier output");
    }
  }

  private forward_ctx_with_actor(
    xcmd: XCommandData,
    actor: { role: string; user_id: string; source: string; channel?: string }
  ): Record<string, unknown> {
    const ctx = readCommandCtx(xcmd);
    const out: Record<string, unknown> = {
      actor: {
        role: actor.role,
        user_id: actor.user_id,
        source: actor.source,
        ...(actor.channel ? { channel: actor.channel } : {})
      }
    };
    if (typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0) {
      out.kernel_cap = ctx.kernel_cap;
    }
    return out;
  }

  private async exec_broadcast_get_pending_draft(params: {
    _ctx: Record<string, unknown>;
  }): Promise<{ _pending: boolean; _broadcast_id?: string }> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "get_pending_draft",
      _params: params
    });
    if (!is_plain_object(out) || typeof out._pending !== "boolean") {
      throw new XError("E_CHANNELS_UPSTREAM", "broadcast.get_pending_draft returned invalid payload");
    }
    const broadcast_id = ensure_optional_string(out._broadcast_id);
    return {
      _pending: out._pending,
      ...(broadcast_id ? { _broadcast_id: broadcast_id } : {})
    };
  }

  private async exec_broadcast_draft_create(params: {
    _channel: string;
    _audience_role: string;
    _message_text: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ broadcast_id: string; recipient_count: number; preview: string }> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "broadcast_draft_create",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "broadcast.broadcast_draft_create returned invalid payload");
    }
    return {
      broadcast_id: ensure_non_empty_string(out.broadcast_id, "broadcast.broadcast_draft_create.broadcast_id"),
      recipient_count:
        typeof out.recipient_count === "number" && Number.isFinite(out.recipient_count)
          ? Math.max(0, Math.floor(out.recipient_count))
          : 0,
      preview: ensure_non_empty_string(out.preview, "broadcast.broadcast_draft_create.preview")
    };
  }

  private async exec_broadcast_confirm(params: {
    _broadcast_id?: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ broadcast_id: string; queued: number }> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "broadcast_confirm",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "broadcast.broadcast_confirm returned invalid payload");
    }
    return {
      broadcast_id: ensure_non_empty_string(out.broadcast_id, "broadcast.broadcast_confirm.broadcast_id"),
      queued: typeof out.queued === "number" && Number.isFinite(out.queued) ? Math.max(0, Math.floor(out.queued)) : 0
    };
  }

  private async exec_broadcast_send(params: {
    _id: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ sent_ok: number; sent_err: number; total: number }> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "send",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "broadcast.send returned invalid payload");
    }
    return {
      sent_ok: typeof out.sent_ok === "number" && Number.isFinite(out.sent_ok) ? Math.max(0, Math.floor(out.sent_ok)) : 0,
      sent_err: typeof out.sent_err === "number" && Number.isFinite(out.sent_err) ? Math.max(0, Math.floor(out.sent_err)) : 0,
      total: typeof out.total === "number" && Number.isFinite(out.total) ? Math.max(0, Math.floor(out.total)) : 0
    };
  }

  private async exec_broadcast_cancel_draft(params: {
    _broadcast_id?: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: BROADCASTS_MODULE_NAME,
      _op: "cancel_draft",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "broadcast.cancel_draft returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "broadcast.cancel_draft._reply_text")
    };
  }

  private async exec_intent_get_enabled_for_context(params: {
    role: string;
    channel: string;
    _ctx: Record<string, unknown>;
  }): Promise<EnabledIntent[]> {
    const out = await _x.execute({
      _module: INTENT_REGISTRY_MODULE_NAME,
      _op: "get_enabled_for_context",
      _params: params
    });
    if (!is_plain_object(out) || !Array.isArray(out.items)) {
      throw new XError("E_CHANNELS_UPSTREAM", "intent.get_enabled_for_context returned invalid payload");
    }
    return out.items
      .filter((entry): entry is Dict => is_plain_object(entry))
      .map((entry) => this.parse_enabled_intent(entry));
  }

  private async exec_kb_preview_state(params: {
    _ctx: Record<string, unknown>;
  }): Promise<{ _awaiting_input: boolean }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "preview",
      _params: params
    });
    if (!is_plain_object(out) || typeof out._awaiting_input !== "boolean") {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.preview returned invalid payload");
    }
    return { _awaiting_input: out._awaiting_input };
  }

  private async exec_kb_propose(params: {
    _text: string;
    _channel: string;
    _from_user_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ _preview_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "propose",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.propose returned invalid payload");
    }
    return {
      _preview_text: ensure_non_empty_string(out._preview_text, "kb.propose._preview_text")
    };
  }

  private async exec_kb_pending_get(params: {
    _ctx: Record<string, unknown>;
  }): Promise<{ _pending: boolean; _action_id?: string; _kind?: string; _kb_file?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "pending_get",
      _params: params
    });
    if (!is_plain_object(out) || typeof out._pending !== "boolean") {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.pending_get returned invalid payload");
    }
    return {
      _pending: out._pending,
      ...(ensure_optional_string(out._action_id) ? { _action_id: ensure_optional_string(out._action_id) } : {}),
      ...(ensure_optional_string(out._kind) ? { _kind: ensure_optional_string(out._kind) } : {}),
      ...(ensure_optional_string(out._kb_file) ? { _kb_file: ensure_optional_string(out._kb_file) } : {})
    };
  }

  private async exec_kb_cancel_pending(params: {
    _action_id?: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "cancel_pending",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.cancel_pending returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "kb.cancel_pending._reply_text")
    };
  }

  private async exec_kb_append(params: {
    _kb_id: string;
    _lang: string;
    _md: string;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "append",
      _params: params
    });
    if (!is_plain_object(out) || out._ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.append returned invalid payload");
    }
  }

  private async exec_kb_patch_section(params: {
    _kb_id: string;
    _lang: string;
    _section_title: string;
    _md: string;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "patch_section",
      _params: params
    });
    if (!is_plain_object(out) || out._ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.patch_section returned invalid payload");
    }
  }

  private async exec_kb_append_to_section(params: {
    _kb_id?: string;
    _lang?: string;
    _section_title?: string;
    _line_text?: string;
    _dry_run: boolean;
    _ctx: Record<string, unknown>;
  }): Promise<{ _message: string; _preview?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "append_to_section",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.append_to_section returned invalid payload");
    }
    return {
      _message: ensure_non_empty_string(out._message, "kb.append_to_section._message"),
      ...(ensure_optional_string(out._preview) ? { _preview: ensure_optional_string(out._preview) } : {})
    };
  }

  private async exec_kb_remove_from_section(params: {
    _kb_id?: string;
    _lang?: string;
    _section_title?: string;
    _match_text?: string;
    _dry_run: boolean;
    _ctx: Record<string, unknown>;
  }): Promise<{ _message: string; _preview?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "remove_from_section",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.remove_from_section returned invalid payload");
    }
    return {
      _message: ensure_non_empty_string(out._message, "kb.remove_from_section._message"),
      ...(ensure_optional_string(out._preview) ? { _preview: ensure_optional_string(out._preview) } : {})
    };
  }

  private async exec_kb_update_price(params: {
    _item_name?: string;
    _from_price?: string;
    _to_price?: string;
    _kb_id?: string;
    _lang?: string;
    _dry_run: boolean;
    _ctx: Record<string, unknown>;
  }): Promise<{ _message: string; _preview?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "update_price",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.update_price returned invalid payload");
    }
    return {
      _message: ensure_non_empty_string(out._message, "kb.update_price._message"),
      ...(ensure_optional_string(out._preview) ? { _preview: ensure_optional_string(out._preview) } : {})
    };
  }

  private async exec_kb_replace_propose(params: {
    _kb_id: string;
    _lang: string;
    _content: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ action_id?: string; preview?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "replace_propose",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.replace_propose returned invalid payload");
    }
    return {
      ...(ensure_optional_string(out.action_id) ? { action_id: ensure_optional_string(out.action_id) } : {}),
      ...(ensure_optional_string(out.preview) ? { preview: ensure_optional_string(out.preview) } : {})
    };
  }

  private async exec_kb_replace_confirm(params: {
    _action_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "replace_confirm",
      _params: params
    });
    if (!is_plain_object(out) || out._ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.replace_confirm returned invalid payload");
    }
  }

  private async exec_kb_delete_section_propose(params: {
    _kb_id: string;
    _lang: string;
    _section_title: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ action_id?: string; preview?: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "delete_section_propose",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.delete_section_propose returned invalid payload");
    }
    return {
      ...(ensure_optional_string(out.action_id) ? { action_id: ensure_optional_string(out.action_id) } : {}),
      ...(ensure_optional_string(out.preview) ? { preview: ensure_optional_string(out.preview) } : {})
    };
  }

  private async exec_kb_delete_section_confirm(params: {
    _action_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "delete_section_confirm",
      _params: params
    });
    if (!is_plain_object(out) || out._ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.delete_section_confirm returned invalid payload");
    }
  }

  private async exec_kb_apply_legacy(params: {
    _action_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ _reply_text: string }> {
    const out = await _x.execute({
      _module: KNOWLEDGE_MODULE_NAME,
      _op: "apply",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "kb.apply returned invalid payload");
    }
    return {
      _reply_text: ensure_non_empty_string(out._reply_text, "kb.apply._reply_text")
    };
  }

  private async exec_conv_get_thread_with_messages(params: {
    thread_id: string;
    limit_messages: number;
  }): Promise<{ thread: ConversationThread; messages: ConversationMessage[] }> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "get_thread",
      _params: params
    });
    if (!is_plain_object(out) || !is_plain_object(out.thread)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.get_thread returned invalid payload");
    }
    const messages = Array.isArray(out.messages)
      ? out.messages.filter((entry): entry is Dict => is_plain_object(entry)).map((entry) => this.parse_message(entry))
      : [];
    return {
      thread: this.parse_thread(out.thread),
      messages
    };
  }

  private async exec_azure_openai_chat(params: {
    messages: Array<{ role: "system" | "user"; content: string }>;
    temperature?: number;
  }): Promise<{ text: string }> {
    const out = await _x.execute({
      _module: "azure",
      _op: "openai_chat",
      _params: {
        messages: params.messages,
        ...(typeof params.temperature === "number" ? { temperature: params.temperature } : {})
      }
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "azure.openai_chat returned invalid payload");
    }
    return {
      text: ensure_non_empty_string(out.text, "azure.openai_chat.text")
    };
  }

  private async exec_intent_handler(params: {
    module: string;
    op: string;
    params: Record<string, unknown>;
  }): Promise<unknown> {
    return _x.execute({
      _module: params.module,
      _op: params.op,
      _params: params.params
    });
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

  private async exec_conv_ensure_thread_participant(params: {
    thread_id: string;
    user_id: string;
    role: "owner" | "admin" | "customer" | "system";
    channel_id?: string;
    last_seen_at?: number;
  }): Promise<void> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "ensure_thread_participant",
      _params: params
    });
    if (!is_plain_object(out) || out.ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.ensure_thread_participant returned invalid payload");
    }
  }

  private async exec_conv_set_pending_action(params: {
    _thread_id: string;
    _type: string;
    _id: string;
    _payload?: Record<string, unknown>;
    _expires_at?: number;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "set_pending_action",
      _params: params
    });
    if (!is_plain_object(out) || out.ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.set_pending_action returned invalid payload");
    }
  }

  private async exec_conv_clear_pending_action(params: {
    _thread_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<void> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "clear_pending_action",
      _params: params
    });
    if (!is_plain_object(out) || out.ok !== true) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.clear_pending_action returned invalid payload");
    }
  }

  private async exec_conv_get_pending_action(params: {
    _thread_id: string;
    _ctx: Record<string, unknown>;
  }): Promise<{ pending_action: Dict | null }> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "get_pending_action",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_CHANNELS_UPSTREAM", "conv.get_pending_action returned invalid payload");
    }
    return {
      pending_action: is_plain_object(out.pending_action) ? out.pending_action : out.pending_action === null ? null : null
    };
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
      tags: this.ensure_string_array(value.tags, "thread.tags"),
      ...(ensure_optional_string(value.pending_action_type)
        ? { pending_action_type: ensure_optional_string(value.pending_action_type) }
        : {}),
      ...(ensure_optional_string(value.pending_action_id)
        ? { pending_action_id: ensure_optional_string(value.pending_action_id) }
        : {}),
      ...(ensure_optional_string(value.pending_action_payload_json)
        ? { pending_action_payload_json: ensure_optional_string(value.pending_action_payload_json) }
        : {}),
      ...(typeof value.pending_action_created_at === "number" && Number.isFinite(value.pending_action_created_at)
        ? { pending_action_created_at: this.ensure_number(value.pending_action_created_at, "thread.pending_action_created_at") }
        : {}),
      ...(typeof value.pending_action_expires_at === "number" && Number.isFinite(value.pending_action_expires_at)
        ? { pending_action_expires_at: this.ensure_number(value.pending_action_expires_at, "thread.pending_action_expires_at") }
        : {})
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

  private parse_enabled_intent(value: Dict): EnabledIntent {
    const intent_id = ensure_non_empty_string(value.intent_id, "intent.intent_id");
    const title = ensure_non_empty_string(value.title, "intent.title");
    const handler_raw = this.ensure_object(value.handler, "intent.handler");
    const roles = this.ensure_string_array(value.roles_allowed, "intent.roles_allowed").filter(
      (entry): entry is "owner" | "admin" | "customer" => entry === "owner" || entry === "admin" || entry === "customer"
    );
    if (roles.length === 0) {
      throw new XError("E_CHANNELS_UPSTREAM", "Intent roles_allowed is empty");
    }
    const channels = Array.isArray(value.channels_allowed)
      ? value.channels_allowed.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
      : [];
    const examples = Array.isArray(value.examples)
      ? value.examples.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
      : [];
    const synonyms = Array.isArray(value.synonyms)
      ? value.synonyms.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
      : [];

    const out: EnabledIntent = {
      intent_id,
      title,
      ...(ensure_optional_string(value.description) ? { description: ensure_optional_string(value.description) } : {}),
      roles_allowed: roles,
      handler: {
        module: ensure_non_empty_string(handler_raw.module, "intent.handler.module"),
        op: ensure_non_empty_string(handler_raw.op, "intent.handler.op")
      },
      ...(channels.length > 0 ? { channels_allowed: channels } : {}),
      ...(ensure_optional_string(value.skill_id) ? { skill_id: ensure_optional_string(value.skill_id) } : {}),
      ...(examples.length > 0 ? { examples } : {}),
      ...(synonyms.length > 0 ? { synonyms } : {})
    };

    const params_schema_raw = is_plain_object(value.params_schema) ? value.params_schema : undefined;
    if (params_schema_raw && Array.isArray(params_schema_raw.fields)) {
      out.params_schema = {
        ...(ensure_optional_string(params_schema_raw.title) ? { title: ensure_optional_string(params_schema_raw.title) } : {}),
        fields: params_schema_raw.fields
          .filter((entry): entry is Dict => is_plain_object(entry))
          .map((entry) => ({
            key: ensure_non_empty_string(entry.key, "intent.params_schema.key"),
            label: ensure_non_empty_string(entry.label, "intent.params_schema.label"),
            type: this.normalize_intent_field_type(entry.type),
            ...(Array.isArray(entry.options)
              ? {
                  options: entry.options
                    .filter((option): option is Dict => is_plain_object(option))
                    .map((option) => ({
                      label: ensure_non_empty_string(option.label, "intent.params_schema.options.label"),
                      value: option.value
                    }))
                }
              : {})
          }))
      };
    }

    const default_params_json = ensure_optional_string(value.default_params_json);
    if (default_params_json) {
      out.default_params_json = default_params_json;
    }
    if (typeof value.priority === "number" && Number.isFinite(value.priority)) {
      out.priority = Math.floor(value.priority);
    }

    return out;
  }

  private normalize_intent_field_type(value: unknown): "string" | "number" | "boolean" | "select" | "string_list" | "json" {
    if (
      value === "string" ||
      value === "number" ||
      value === "boolean" ||
      value === "select" ||
      value === "string_list" ||
      value === "json"
    ) {
      return value;
    }
    throw new XError("E_CHANNELS_UPSTREAM", "Invalid intent field type");
  }

  private normalize_intent_params(value: unknown, intent: EnabledIntent): Record<string, unknown> {
    if (!intent.params_schema || intent.params_schema.fields.length === 0) {
      if (!is_plain_object(value) || has_function(value)) return {};
      return { ...value };
    }
    const input = is_plain_object(value) ? value : {};
    const out: Record<string, unknown> = {};
    for (const field of intent.params_schema.fields) {
      if (!Object.prototype.hasOwnProperty.call(input, field.key)) continue;
      const current = input[field.key];
      if (field.type === "string") {
        if (typeof current === "string") out[field.key] = current.trim();
        continue;
      }
      if (field.type === "number") {
        const parsed = typeof current === "number" ? current : Number(current);
        if (Number.isFinite(parsed)) out[field.key] = parsed;
        continue;
      }
      if (field.type === "boolean") {
        if (typeof current === "boolean") out[field.key] = current;
        else if (typeof current === "string") out[field.key] = current.trim().toLowerCase() === "true";
        continue;
      }
      if (field.type === "string_list") {
        if (Array.isArray(current)) {
          out[field.key] = current.filter((entry): entry is string => typeof entry === "string").map((entry) => entry.trim()).filter(Boolean);
        } else if (typeof current === "string") {
          out[field.key] = current.split(/\r?\n|,/g).map((entry) => entry.trim()).filter(Boolean);
        }
        continue;
      }
      if (field.type === "json") {
        if (!has_function(current)) out[field.key] = current;
        continue;
      }
      if (field.type === "select") {
        if (!has_function(current)) out[field.key] = current;
        continue;
      }
    }
    return out;
  }

  private merge_default_intent_params(default_params_json: string | undefined, params: Record<string, unknown>): Record<string, unknown> {
    if (!default_params_json) return { ...params };
    try {
      const parsed = JSON.parse(default_params_json);
      if (!is_plain_object(parsed) || has_function(parsed)) {
        return { ...params };
      }
      return {
        ...parsed,
        ...params
      };
    } catch {
      return { ...params };
    }
  }

  private read_reply_text_from_result(value: unknown): string {
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
    if (!is_plain_object(value)) {
      const serialized = JSON.stringify(value);
      return typeof serialized === "string" && serialized.trim().length > 0 ? serialized : "Done.";
    }
    const direct =
      ensure_optional_string(value._reply_text) ??
      ensure_optional_string(value.reply_text) ??
      ensure_optional_string(value.text);
    if (direct) return direct;
    const serialized = JSON.stringify(value);
    return typeof serialized === "string" && serialized.trim().length > 0 ? serialized : "Done.";
  }

  private detect_summary_today_override(role: string, text: string): boolean {
    const normalized_role = role.trim().toLowerCase();
    if (normalized_role !== "admin" && normalized_role !== "owner") return false;
    const normalized = text.trim().toLowerCase();
    if (!normalized) return false;
    const has_summary = normalized.includes("summarize") || normalized.includes("summary");
    const has_today =
      normalized.includes("today") ||
      normalized.includes("today's") ||
      normalized.includes("todays") ||
      normalized.includes("daily summary");
    return has_summary && has_today;
  }

  private format_summary_today_reply(value: unknown): string {
    if (!is_plain_object(value)) {
      return this.read_reply_text_from_result(value);
    }
    const date = ensure_optional_string(value._date) ?? "today";
    const total_messages =
      typeof value._total_messages === "number" && Number.isFinite(value._total_messages) ? Math.floor(value._total_messages) : 0;
    const unique_users =
      typeof value._unique_users === "number" && Number.isFinite(value._unique_users) ? Math.floor(value._unique_users) : 0;
    const top_threads = Array.isArray(value._top_threads) ? value._top_threads.filter((entry): entry is Dict => is_plain_object(entry)) : [];
    const highlights = Array.isArray(value._highlights) ? value._highlights.filter((entry): entry is string => typeof entry === "string") : [];
    const action_items = Array.isArray(value._action_items) ? value._action_items.filter((entry): entry is string => typeof entry === "string") : [];

    const lines = [
      `Summary for ${date}:`,
      `- Total inbound messages: ${total_messages}`,
      `- Unique users: ${unique_users}`
    ];
    if (top_threads.length > 0) {
      lines.push("- Top threads:");
      for (const thread of top_threads.slice(0, 3)) {
        const thread_id = ensure_optional_string(thread.thread_id) ?? "unknown";
        const messages = typeof thread.messages === "number" && Number.isFinite(thread.messages) ? Math.floor(thread.messages) : 0;
        lines.push(`  - ${thread_id}: ${messages} messages`);
      }
    }
    if (highlights.length > 0) {
      lines.push("- Highlights:");
      for (const item of highlights.slice(0, 3)) lines.push(`  - ${item}`);
    }
    if (action_items.length > 0) {
      lines.push("- Action items:");
      for (const item of action_items.slice(0, 3)) lines.push(`  - ${item}`);
    }
    return lines.join("\n");
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
    const identities = await list_channel_identities_xdb(this._identity_scope);
    this._registrations.clear();
    this._channel_identities.clear();
    for (const row of rows) {
      this._registrations.set(row.channel, {
        channel: row.channel,
        connector_module: row.connector_module,
        config: { ...row.config },
        created_at: row.created_at,
        updated_at: row.updated_at
      });
    }
    for (const identity of identities) {
      this._channel_identities.set(identity._key, {
        ...identity,
        ...(identity._meta ? { _meta: { ...identity._meta } } : {})
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
