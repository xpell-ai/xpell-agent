import { randomUUID } from "node:crypto";

import { XError, XModule, _x, _xlog, type XCommandData } from "@xpell/node";

import { readCommandCtx, requireKernelCap, type AgentCommandCtx } from "../runtime/guards.js";
import {
  get_broadcast_xdb,
  init_broadcasts_xdb,
  list_broadcast_deliveries_xdb,
  list_broadcasts_xdb,
  save_broadcast_delivery_xdb,
  save_broadcast_xdb,
  type AgentBroadcastsXdbScope,
  type BroadcastAudienceRole,
  type BroadcastChannel,
  type PersistedBroadcastDeliveryRecord,
  type PersistedBroadcastRecord
} from "../xdb/broadcasts-xdb.js";
import { list_channel_identities_xdb, type AgentChannelIdentitiesXdbScope } from "./channel-identities-xdb.js";
import { CONVERSATIONS_MODULE_NAME } from "./ConversationsModule.js";
import { list_threads_xdb, type AgentConvXdbScope } from "./conv-xdb.js";
import { list_users_xdb, type AgentUsersXdbScope } from "./users-xdb.js";
import { CHANNELS_MODULE_NAME } from "./ChannelsModule.js";

export const BROADCASTS_MODULE_NAME = "broadcast";

type Dict = Record<string, unknown>;

type BroadcastsModuleOptions = {
  _app_id?: string;
  _env?: string;
};

type BroadcastRecipient = {
  user_id: string;
  channel: string;
  channel_user_id: string;
  display_name: string;
  role: string;
  thread_id?: string;
};

function is_plain_object(value: unknown): value is Dict {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ensure_non_empty_string(value: unknown, field_name: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new XError("E_BROADCAST_BAD_PARAMS", `Invalid ${field_name}: expected non-empty string`);
  }
  return value.trim();
}

function ensure_optional_string(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function truncate_text(value: string, max_chars = 200): string {
  const trimmed = value.trim();
  if (trimmed.length <= max_chars) return trimmed;
  return `${trimmed.slice(0, Math.max(0, max_chars - 3))}...`;
}

function normalize_channel(value: unknown): BroadcastChannel {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (normalized === "all") return "all";
  return "telegram";
}

function normalize_audience_role(value: unknown): BroadcastAudienceRole {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return normalized === "all" ? "all" : "customer";
}

function normalize_limit(value: unknown, fallback: number, max_value: number): number {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(max_value, Math.max(1, Math.floor(parsed)));
}

export class BroadcastsModule extends XModule {
  static _name = BROADCASTS_MODULE_NAME;

  private _scope: AgentBroadcastsXdbScope;
  private _users_scope: AgentUsersXdbScope;
  private _identity_scope: AgentChannelIdentitiesXdbScope;
  private _conv_scope: AgentConvXdbScope;

  constructor(opts: BroadcastsModuleOptions = {}) {
    super({ _name: BROADCASTS_MODULE_NAME });
    const _app_id = typeof opts._app_id === "string" && opts._app_id.trim() ? opts._app_id.trim() : "xbot";
    const _env = typeof opts._env === "string" && opts._env.trim() ? opts._env.trim() : "default";
    this._scope = { _app_id, _env };
    this._users_scope = { _app_id, _env };
    this._identity_scope = { _app_id, _env };
    this._conv_scope = { _app_id, _env };
  }

  async _init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }
  async _op_init_on_boot(xcmd: XCommandData) {
    return this.init_on_boot_impl(xcmd);
  }

  async _broadcast_draft_create(xcmd: XCommandData) {
    return this.broadcast_draft_create_impl(xcmd);
  }
  async _op_broadcast_draft_create(xcmd: XCommandData) {
    return this.broadcast_draft_create_impl(xcmd);
  }

  async _broadcast_confirm(xcmd: XCommandData) {
    return this.broadcast_confirm_impl(xcmd);
  }
  async _op_broadcast_confirm(xcmd: XCommandData) {
    return this.broadcast_confirm_impl(xcmd);
  }

  async _broadcast_send_batch(xcmd: XCommandData) {
    return this.broadcast_send_batch_impl(xcmd);
  }
  async _op_broadcast_send_batch(xcmd: XCommandData) {
    return this.broadcast_send_batch_impl(xcmd);
  }

  async _preview(xcmd: XCommandData) {
    return this.preview_impl(xcmd);
  }
  async _op_preview(xcmd: XCommandData) {
    return this.preview_impl(xcmd);
  }

  async _send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }
  async _op_send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }

  async _get_pending_draft(xcmd: XCommandData) {
    return this.get_pending_draft_impl(xcmd);
  }
  async _op_get_pending_draft(xcmd: XCommandData) {
    return this.get_pending_draft_impl(xcmd);
  }

  async _cancel_draft(xcmd: XCommandData) {
    return this.cancel_draft_impl(xcmd);
  }
  async _op_cancel_draft(xcmd: XCommandData) {
    return this.cancel_draft_impl(xcmd);
  }

  async _list_recent(xcmd: XCommandData) {
    return this.list_recent_impl(xcmd);
  }
  async _op_list_recent(xcmd: XCommandData) {
    return this.list_recent_impl(xcmd);
  }

  private async init_on_boot_impl(xcmd: XCommandData) {
    requireKernelCap(readCommandCtx(xcmd));
    await init_broadcasts_xdb(this._scope);
    return { ok: true };
  }

  private async broadcast_draft_create_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const channel = normalize_channel(params._channel);
    const audience_role = normalize_audience_role(params._audience_role);
    const message_text = ensure_non_empty_string(params._message_text, "_message_text");
    const recipients = await this.resolve_recipients(channel, audience_role);
    const now = Date.now();
    const record: PersistedBroadcastRecord = {
      _id: randomUUID(),
      _app_id: this._scope._app_id,
      _env: this._scope._env,
      _status: "draft",
      _channel: channel,
      _audience_role: audience_role,
      _message_text: message_text,
      _created_at: now,
      _updated_at: now,
      _created_by_user_id: actor.user_id,
      _stats: {
        total: recipients.length,
        sent: 0,
        failed: 0
      }
    };
    await save_broadcast_xdb(this._scope, record);
    _xlog.log("[broadcast]", {
      op: "draft_create",
      broadcast_id: record._id,
      channel: record._channel,
      audience_role: record._audience_role,
      recipient_count: recipients.length,
      actor_user_id: actor.user_id
    });
    return {
      broadcast_id: record._id,
      recipient_count: recipients.length,
      preview: `Broadcast draft ${record._id}\nChannel: ${record._channel}\nAudience: ${record._audience_role}\nRecipients: ${recipients.length}\nMessage: ${truncate_text(record._message_text, 500)}\nReply 'confirm' to queue this broadcast.`
    };
  }

  private async broadcast_confirm_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const requested_id = ensure_optional_string(params._broadcast_id);
    const broadcast = requested_id
      ? await this.must_get_broadcast(requested_id)
      : await this.must_get_latest_draft_for_actor(actor.user_id);

    if (broadcast._status !== "draft") {
      const deliveries = await list_broadcast_deliveries_xdb(this._scope, broadcast._id);
      const queued = deliveries.filter((entry) => entry._status === "queued").length;
      return {
        broadcast_id: broadcast._id,
        queued,
        already_queued: true
      };
    }

    const recipients = await this.resolve_recipients(broadcast._channel, broadcast._audience_role);
    for (const recipient of recipients) {
      const delivery: PersistedBroadcastDeliveryRecord = {
        _id: randomUUID(),
        _broadcast_id: broadcast._id,
        _user_id: recipient.user_id,
        _channel: recipient.channel,
        _status: "queued",
        _created_at: Date.now(),
        _updated_at: Date.now(),
        _thread_id: recipient.thread_id
      };
      await save_broadcast_delivery_xdb(this._scope, delivery);
    }

    broadcast._status = "queued";
    broadcast._stats = {
      total: recipients.length,
      sent: 0,
      failed: 0
    };
    await save_broadcast_xdb(this._scope, broadcast);
    _xlog.log("[broadcast]", {
      op: "confirm",
      broadcast_id: broadcast._id,
      queued: recipients.length,
      actor_user_id: actor.user_id
    });
    return {
      broadcast_id: broadcast._id,
      queued: recipients.length
    };
  }

  private async broadcast_send_batch_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const broadcast_id = ensure_non_empty_string(params._broadcast_id, "_broadcast_id");
    const limit = normalize_limit(params._limit, 25, 100);
    const broadcast = await this.must_get_broadcast(broadcast_id);
    const all_deliveries = await list_broadcast_deliveries_xdb(this._scope, broadcast_id);
    const queued = all_deliveries.filter((entry) => entry._status === "queued").slice(0, limit);

    if (queued.length === 0) {
      const stats = this.compute_stats(all_deliveries);
      broadcast._status = "done";
      broadcast._stats = stats;
      await save_broadcast_xdb(this._scope, broadcast);
      return {
        broadcast_id,
        sent: 0,
        failed: 0,
        remaining: 0
      };
    }

    broadcast._status = "sending";
    await save_broadcast_xdb(this._scope, broadcast);

    let sent = 0;
    let failed = 0;
    for (const delivery of queued) {
      try {
        const thread_id = ensure_non_empty_string(delivery._thread_id, "delivery._thread_id");
        await _x.execute({
          _module: CHANNELS_MODULE_NAME,
          _op: "send_message",
          _params: {
            channel_id: delivery._channel,
            thread_id,
            msg: {
              text: broadcast._message_text
            }
          }
        });
        delivery._status = "sent";
        delete delivery._error;
        await save_broadcast_delivery_xdb(this._scope, delivery);
        sent += 1;
      } catch (err) {
        delivery._status = "failed";
        delivery._error = truncate_text(err instanceof Error ? err.message : String(err), 300);
        await save_broadcast_delivery_xdb(this._scope, delivery);
        failed += 1;
      }
    }

    const current_deliveries = await list_broadcast_deliveries_xdb(this._scope, broadcast_id);
    const stats = this.compute_stats(current_deliveries);
    const remaining = current_deliveries.filter((entry) => entry._status === "queued").length;
    broadcast._stats = stats;
    broadcast._status = remaining === 0 ? "done" : "queued";
    await save_broadcast_xdb(this._scope, broadcast);
    _xlog.log("[broadcast]", {
      op: "send_batch",
      broadcast_id,
      sent,
      failed,
      remaining
    });
    return {
      broadcast_id,
      sent,
      failed,
      remaining
    };
  }

  private async preview_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_privileged_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const audience = this.read_audience(params);
    const limit = normalize_limit(params._limit, 25, 500);
    const recipients = await this.resolve_recipients_for_audience(audience.roles, audience.channel, limit);
    return {
      audience: {
        roles: [...audience.roles],
        channel: audience.channel
      },
      recipients: recipients.map((recipient) => ({
        user_id: recipient.user_id,
        role: recipient.role,
        display_name: recipient.display_name,
        channel: recipient.channel,
        channel_user_id: recipient.channel_user_id
      }))
    };
  }

  private async send_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_privileged_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const requested_id = ensure_optional_string(params._id);
    if (requested_id) {
      const broadcast = await this.must_get_broadcast(requested_id);
      if (broadcast._status === "draft") {
        await this.broadcast_confirm_impl({
          ...xcmd,
          _params: {
            _broadcast_id: requested_id,
            _ctx: {
              ...(typeof ctx.kernel_cap === "string" && ctx.kernel_cap.trim().length > 0 ? { kernel_cap: ctx.kernel_cap } : {}),
              actor: {
                role: actor.role,
                ...(actor.user_id ? { user_id: actor.user_id } : {})
              }
            }
          }
        } as XCommandData);
      }

      const deliveries = await list_broadcast_deliveries_xdb(this._scope, requested_id);
      const queued = deliveries.filter((entry) => entry._status === "queued");
      const results: Array<{ user_id: string; channel_user_id: string; ok: boolean; error?: string }> = [];
      let sent_ok = 0;
      let sent_err = 0;

      for (const delivery of queued) {
        let channel_user_id = "";
        try {
          const recipient = await this.resolve_delivery_recipient(delivery);
          channel_user_id = recipient.channel_user_id;
          const thread_id =
            delivery._thread_id ??
            (await this.exec_conv_ensure_thread({
              channel_id: recipient.channel,
              thread_key: recipient.channel_user_id,
              user_id: recipient.user_id
            })).thread_id;
          await _x.execute({
            _module: CHANNELS_MODULE_NAME,
            _op: "send_message",
            _params: {
              channel_id: recipient.channel,
              thread_id,
              msg: { text: broadcast._message_text }
            }
          });
          delivery._status = "sent";
          delete delivery._error;
          delivery._thread_id = thread_id;
          await save_broadcast_delivery_xdb(this._scope, delivery);
          sent_ok += 1;
          results.push({
            user_id: recipient.user_id,
            channel_user_id: recipient.channel_user_id,
            ok: true
          });
          _xlog.log("[broadcast]", {
            op: "send_recipient",
            broadcast_id: requested_id,
            user_id: recipient.user_id,
            channel: recipient.channel,
            channel_user_id: recipient.channel_user_id,
            ok: true
          });
        } catch (err) {
          const message = truncate_text(err instanceof Error ? err.message : String(err), 300);
          delivery._status = "failed";
          delivery._error = message;
          await save_broadcast_delivery_xdb(this._scope, delivery);
          sent_err += 1;
          results.push({
            user_id: delivery._user_id,
            channel_user_id,
            ok: false,
            error: message
          });
          _xlog.log("[broadcast]", {
            op: "send_recipient",
            broadcast_id: requested_id,
            user_id: delivery._user_id,
            channel: delivery._channel,
            channel_user_id,
            ok: false,
            error: message
          });
        }
      }

      const current_deliveries = await list_broadcast_deliveries_xdb(this._scope, requested_id);
      const stats = this.compute_stats(current_deliveries);
      const remaining = current_deliveries.filter((entry) => entry._status === "queued").length;
      broadcast._stats = stats;
      broadcast._status = remaining === 0 ? "done" : "queued";
      await save_broadcast_xdb(this._scope, broadcast);

      return {
        total: current_deliveries.length,
        sent_ok,
        sent_err,
        remaining,
        results
      };
    }

    const message_raw = params._message;
    if (!is_plain_object(message_raw)) {
      throw new XError("E_BROADCAST_BAD_PARAMS", "_message must be an object");
    }
    const text =
      ensure_optional_string(message_raw.text) ??
      ensure_optional_string((message_raw as Dict)._text) ??
      ensure_non_empty_string(params._text, "_message.text|_text");
    const dry_run = params._dry_run === true;
    const audience = this.read_audience(params);
    const limit = normalize_limit(params._limit, 25, 500);
    const recipients = await this.resolve_recipients_for_audience(audience.roles, audience.channel, limit);

    if (dry_run) {
      return {
        audience: {
          roles: [...audience.roles],
          channel: audience.channel
        },
        recipients: recipients.map((recipient) => ({
          user_id: recipient.user_id,
          role: recipient.role,
          display_name: recipient.display_name,
          channel: recipient.channel,
          channel_user_id: recipient.channel_user_id
        })),
        note: "dry_run"
      };
    }

    const results: Array<{ user_id: string; channel_user_id: string; ok: boolean; error?: string }> = [];
    let sent_ok = 0;
    let sent_err = 0;

    for (const recipient of recipients) {
      try {
        const thread_id =
          recipient.thread_id ??
          (await this.exec_conv_ensure_thread({
            channel_id: recipient.channel,
            thread_key: recipient.channel_user_id,
            user_id: recipient.user_id
          })).thread_id;
        await _x.execute({
          _module: CHANNELS_MODULE_NAME,
          _op: "send_message",
          _params: {
            channel_id: recipient.channel,
            thread_id,
            msg: { text }
          }
        });
        results.push({
          user_id: recipient.user_id,
          channel_user_id: recipient.channel_user_id,
          ok: true
        });
        sent_ok += 1;
        _xlog.log("[broadcast]", {
          op: "send_recipient",
          user_id: recipient.user_id,
          channel: recipient.channel,
          channel_user_id: recipient.channel_user_id,
          ok: true
        });
      } catch (err) {
        const message = truncate_text(err instanceof Error ? err.message : String(err), 300);
        results.push({
          user_id: recipient.user_id,
          channel_user_id: recipient.channel_user_id,
          ok: false,
          error: message
        });
        sent_err += 1;
        _xlog.log("[broadcast]", {
          op: "send_recipient",
          user_id: recipient.user_id,
          channel: recipient.channel,
          channel_user_id: recipient.channel_user_id,
          ok: false,
          error: message
        });
      }
    }

    return {
      total: recipients.length,
      sent_ok,
      sent_err,
      results
    };
  }

  private async get_pending_draft_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const pending = await this.find_latest_draft_for_actor(actor.user_id);
    if (!pending) {
      return { _pending: false };
    }
    return {
      _pending: true,
      _broadcast_id: pending._id,
      _channel: pending._channel,
      _audience_role: pending._audience_role
    };
  }

  private async cancel_draft_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    const actor = this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const requested_id = ensure_optional_string(params._broadcast_id);
    const draft = requested_id
      ? await this.must_get_broadcast(requested_id)
      : await this.find_latest_draft_for_actor(actor.user_id);
    if (!draft || draft._status !== "draft") {
      return { _reply_text: "No pending broadcast draft." };
    }
    draft._status = "failed";
    await save_broadcast_xdb(this._scope, draft);
    return { _reply_text: `Broadcast draft ${draft._id} canceled.` };
  }

  private async list_recent_impl(xcmd: XCommandData) {
    const ctx = readCommandCtx(xcmd);
    this.require_admin_actor(ctx);
    await init_broadcasts_xdb(this._scope);
    const params = this.ensure_params(xcmd._params);
    const limit = normalize_limit(params._limit, 5, 50);
    const items = await list_broadcasts_xdb(this._scope, 0, limit);
    return {
      items
    };
  }

  private ensure_params(value: unknown): Dict {
    if (value === undefined || value === null) return {};
    if (!is_plain_object(value)) {
      throw new XError("E_BROADCAST_BAD_PARAMS", "Expected params to be an object");
    }
    if (Object.values(value).some((entry) => typeof entry === "function")) {
      throw new XError("E_BROADCAST_BAD_PARAMS", "params must be JSON-safe");
    }
    return value;
  }

  private require_admin_actor(ctx: AgentCommandCtx): { user_id: string; role: "admin" | "owner" } {
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role.trim().toLowerCase() : "";
    const user_id = typeof ctx.actor?.user_id === "string" ? ctx.actor.user_id.trim() : "";
    if ((role !== "admin" && role !== "owner") || !user_id) {
      throw new XError("E_AGENT_FORBIDDEN", "Admin actor required");
    }
    return {
      user_id,
      role: role as "admin" | "owner"
    };
  }

  private require_privileged_actor(ctx: AgentCommandCtx): { user_id?: string; role: "admin" | "owner" | "system" } {
    const role = typeof ctx.actor?.role === "string" ? ctx.actor.role.trim().toLowerCase() : "";
    const user_id = typeof ctx.actor?.user_id === "string" ? ctx.actor.user_id.trim() : "";
    if (role !== "admin" && role !== "owner" && role !== "system") {
      throw new XError("E_AGENT_FORBIDDEN", "Admin actor required");
    }
    return {
      ...(user_id ? { user_id } : {}),
      role: role as "admin" | "owner" | "system"
    };
  }

  private read_audience(params: Dict): { roles: string[]; channel: string } {
    const raw = params._audience;
    let roles = ["customer"];
    let channel = "telegram";
    if (is_plain_object(raw)) {
      const raw_roles = Array.isArray(raw.roles) ? raw.roles : undefined;
      if (raw_roles) {
        const next_roles = raw_roles
          .filter((entry): entry is string => typeof entry === "string")
          .map((entry) => entry.trim().toLowerCase())
          .filter((entry) => entry === "customer" || entry === "admin" || entry === "owner");
        if (next_roles.length > 0) {
          roles = Array.from(new Set(next_roles));
        }
      }
      const raw_channel = ensure_optional_string(raw.channel);
      if (raw_channel) {
        channel = raw_channel.toLowerCase();
      }
    }
    return { roles, channel };
  }

  private async must_get_broadcast(broadcast_id: string): Promise<PersistedBroadcastRecord> {
    const record = await get_broadcast_xdb(this._scope, broadcast_id);
    if (!record) {
      throw new XError("E_BROADCAST_NOT_FOUND", `Broadcast not found: ${broadcast_id}`);
    }
    return record;
  }

  private async must_get_latest_draft_for_actor(user_id: string): Promise<PersistedBroadcastRecord> {
    const draft = await this.find_latest_draft_for_actor(user_id);
    if (!draft) {
      throw new XError("E_BROADCAST_NOT_FOUND", "No pending broadcast draft.");
    }
    return draft;
  }

  private async find_latest_draft_for_actor(user_id: string): Promise<PersistedBroadcastRecord | undefined> {
    const items = await list_broadcasts_xdb(this._scope, 0, 100);
    return items.find((item) => item._created_by_user_id === user_id && item._status === "draft");
  }

  private compute_stats(deliveries: PersistedBroadcastDeliveryRecord[]): PersistedBroadcastRecord["_stats"] {
    let sent = 0;
    let failed = 0;
    for (const delivery of deliveries) {
      if (delivery._status === "sent") sent += 1;
      else if (delivery._status === "failed") failed += 1;
    }
    return {
      total: deliveries.length,
      sent,
      failed
    };
  }

  private async exec_conv_ensure_thread(params: {
    channel_id: string;
    thread_key: string;
    user_id: string;
  }): Promise<{ thread_id: string }> {
    const out = await _x.execute({
      _module: CONVERSATIONS_MODULE_NAME,
      _op: "ensure_thread",
      _params: params
    });
    if (!is_plain_object(out)) {
      throw new XError("E_BROADCAST_UPSTREAM", "conv.ensure_thread returned invalid payload");
    }
    return {
      thread_id: ensure_non_empty_string(out.thread_id, "conv.ensure_thread.thread_id")
    };
  }

  private async resolve_delivery_recipient(
    delivery: PersistedBroadcastDeliveryRecord
  ): Promise<{ user_id: string; channel: string; channel_user_id: string }> {
    const identities = await list_channel_identities_xdb(this._identity_scope);
    const match = identities.find(
      (identity) => identity._user_id === delivery._user_id && identity._channel_id === delivery._channel
    );
    if (!match) {
      throw new XError("E_BROADCAST_RECIPIENT_NOT_FOUND", `Identity not found for user ${delivery._user_id}`);
    }
    return {
      user_id: delivery._user_id,
      channel: delivery._channel,
      channel_user_id: match._external_user_id
    };
  }

  private async resolve_recipients_for_audience(
    roles: string[],
    channel: string,
    limit?: number
  ): Promise<BroadcastRecipient[]> {
    const [users, identities, threads] = await Promise.all([
      list_users_xdb(this._users_scope),
      list_channel_identities_xdb(this._identity_scope),
      list_threads_xdb(this._conv_scope)
    ]);

    const allowed_roles = new Set(roles);
    const best_thread_by_user_channel = new Map<string, { thread_id: string; updated_at: number }>();
    for (const thread of threads) {
      const key = `${thread.user_id}::${thread.channel}`;
      const current = best_thread_by_user_channel.get(key);
      if (!current || thread.updated_at > current.updated_at) {
        best_thread_by_user_channel.set(key, { thread_id: thread.thread_id, updated_at: thread.updated_at });
      }
    }

    const user_by_id = new Map(users.map((user) => [user._id, user] as const));
    const recipients: BroadcastRecipient[] = [];
    for (const identity of identities) {
      if (channel !== "all" && identity._channel_id !== channel) continue;
      const user = user_by_id.get(identity._user_id);
      if (!user) continue;
      if (!allowed_roles.has(user._role)) continue;
      const thread_ref = best_thread_by_user_channel.get(`${user._id}::${identity._channel_id}`);
      recipients.push({
        user_id: user._id,
        role: user._role,
        display_name: user._display_name,
        channel: identity._channel_id,
        channel_user_id: identity._external_user_id,
        ...(thread_ref ? { thread_id: thread_ref.thread_id } : {})
      });
    }

    recipients.sort((left, right) => {
      if (left.channel !== right.channel) return left.channel.localeCompare(right.channel);
      if (left.role !== right.role) return left.role.localeCompare(right.role);
      if (left.display_name !== right.display_name) return left.display_name.localeCompare(right.display_name);
      if (left.user_id !== right.user_id) return left.user_id.localeCompare(right.user_id);
      return left.channel_user_id.localeCompare(right.channel_user_id);
    });

    return typeof limit === "number" ? recipients.slice(0, limit) : recipients;
  }

  private async resolve_recipients(
    channel: BroadcastChannel,
    audience_role: BroadcastAudienceRole
  ): Promise<BroadcastRecipient[]> {
    const roles = audience_role === "all" ? ["customer", "admin", "owner"] : ["customer"];
    return this.resolve_recipients_for_audience(roles, channel);
  }
}

export default BroadcastsModule;
