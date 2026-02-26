import { XError, XModule, _xlog, type XCommandData } from "@xpell/node";

import {
  clamp_poll_timeout,
  telegram_delete_webhook,
  telegram_get_me,
  telegram_get_updates,
  telegram_send_message,
  telegram_set_webhook_stub
} from "./telegram/api.js";
import { normalize_admin_chat_ids, normalize_telegram_update } from "./telegram/normalize.js";
import type { NormalizedTelegramInbound, TelegramParseMode } from "./telegram/types.js";
import { TELEGRAM_SETTINGS_DEFAULTS, normalize_telegram_skill_settings, type TelegramMode, type TelegramSkillSettings } from "./settings.js";

type Dict = Record<string, unknown>;

type SkillLogLevel = "debug" | "info" | "warn" | "error";

type SkillContext = {
  execute(module: string, op: string, params?: any, meta?: any): Promise<any>;
  log(level: SkillLogLevel, msg: string, meta?: any): void;
  skill: { id: string; version: string };
};

type TelegramConfig = {
  bot_token?: string;
  mode: TelegramMode;
  polling: {
    timeout_sec: number;
  };
  webhook: {
    url?: string;
    secret_token?: string;
  };
  admins: {
    chat_ids: string[];
  };
};

type TelegramConfigureParams = {
  bot_token?: string;
  mode?: TelegramMode;
  polling?: { timeout_sec?: number };
  webhook?: { url?: string; secret_token?: string };
  admins?: { chat_ids?: Array<string | number> };
};

const POLL_MAX_CONSECUTIVE_ERRORS = 8;
const TG_SECRET_HEADER = "x-telegram-bot-api-secret-token";
const TG_LOG_PREFIX = "[agent-core][telegram]";

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
    throw new XError("E_TG_BAD_PARAMS", `Invalid ${field_name}`);
  }
  return value.trim();
}

function ensure_params(value: unknown): Dict {
  if (value === undefined || value === null) return {};
  if (!is_plain_object(value)) throw new XError("E_TG_BAD_PARAMS", "params must be an object");
  if (has_function(value)) throw new XError("E_TG_BAD_PARAMS", "params must be JSON-safe");
  return value;
}

function ensure_mode(value: unknown, fallback: TelegramMode): TelegramMode {
  if (value === undefined || value === null) return fallback;
  if (value === "polling" || value === "webhook") return value;
  throw new XError("E_TG_BAD_PARAMS", "mode must be polling or webhook");
}

function parse_chat_id(value: unknown, field_name = "chat_id"): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  throw new XError("E_TG_BAD_PARAMS", `Invalid ${field_name}`);
}

function ensure_parse_mode(value: unknown): TelegramParseMode | undefined {
  if (value === undefined || value === null) return undefined;
  if (value === "HTML" || value === "MarkdownV2") return value;
  throw new XError("E_TG_BAD_PARAMS", "parse_mode must be HTML or MarkdownV2");
}

function ensure_optional_object(value: unknown, field_name: string): Dict | undefined {
  if (value === undefined || value === null) return undefined;
  if (!is_plain_object(value)) throw new XError("E_TG_BAD_PARAMS", `${field_name} must be an object`);
  if (has_function(value)) throw new XError("E_TG_BAD_PARAMS", `${field_name} must be JSON-safe`);
  return value;
}

function read_header_value(headers: Dict | undefined, header_name_lower: string): string | undefined {
  if (!headers) return undefined;
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() !== header_name_lower) continue;
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return undefined;
}

function format_error(err: unknown): string {
  if (err && typeof err === "object" && typeof (err as any).toXData === "function") {
    const xdata = (err as any).toXData();
    if (typeof xdata?.message === "string") return xdata.message;
  }
  if (err instanceof Error) return err.message;
  return String(err);
}

function message_preview(text: string): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= 64) return normalized;
  return `${normalized.slice(0, 64)}...`;
}

function is_start_command(text: string): boolean {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) return false;
  const [command] = trimmed.split(/\s+/g);
  const normalized = command.toLowerCase();
  return normalized === "/start" || normalized.startsWith("/start@");
}

export class TelegramConnectorModule extends XModule {
  static _name = "telegram";

  private _ctx: SkillContext;
  private _config: TelegramConfig;
  private _running = false;
  private _last_update_id?: number;
  private _poll_task?: Promise<void>;
  private _consecutive_errors = 0;

  constructor(ctx: SkillContext) {
    super({ _name: "telegram" });
    this._ctx = ctx;
    this._config = {
      mode: TELEGRAM_SETTINGS_DEFAULTS.mode,
      polling: {
        timeout_sec: TELEGRAM_SETTINGS_DEFAULTS.polling.timeout_sec
      },
      webhook: {
        ...(TELEGRAM_SETTINGS_DEFAULTS.webhook.url ? { url: TELEGRAM_SETTINGS_DEFAULTS.webhook.url } : {}),
        ...(TELEGRAM_SETTINGS_DEFAULTS.webhook.secret_token
          ? { secret_token: TELEGRAM_SETTINGS_DEFAULTS.webhook.secret_token }
          : {})
      },
      admins: {
        chat_ids: [...TELEGRAM_SETTINGS_DEFAULTS.admin_chat_ids]
      }
    };
  }

  async _configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }
  async _op_configure(xcmd: XCommandData) {
    return this.configure_impl(xcmd);
  }

  async _start(xcmd: XCommandData) {
    return this.start_impl(xcmd);
  }
  async _op_start(xcmd: XCommandData) {
    return this.start_impl(xcmd);
  }

  async _stop(xcmd: XCommandData) {
    return this.stop_impl(xcmd);
  }
  async _op_stop(xcmd: XCommandData) {
    return this.stop_impl(xcmd);
  }

  async _send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }
  async _op_send(xcmd: XCommandData) {
    return this.send_impl(xcmd);
  }

  async _status(_xcmd: XCommandData) {
    return this.status_impl();
  }
  async _op_status(xcmd: XCommandData) {
    return this._status(xcmd);
  }

  async _set_webhook(xcmd: XCommandData) {
    return this.set_webhook_impl(xcmd);
  }
  async _op_set_webhook(xcmd: XCommandData) {
    return this.set_webhook_impl(xcmd);
  }

  async _handle_webhook_update(xcmd: XCommandData) {
    return this.handle_webhook_update_impl(xcmd);
  }
  async _op_handle_webhook_update(xcmd: XCommandData) {
    return this.handle_webhook_update_impl(xcmd);
  }

  async _reload_settings(xcmd: XCommandData) {
    return this.reload_settings_impl(xcmd);
  }
  async _op_reload_settings(xcmd: XCommandData) {
    return this.reload_settings_impl(xcmd);
  }

  async _verify_token(xcmd: XCommandData) {
    return this.verify_token_impl(xcmd);
  }
  async _op_verify_token(xcmd: XCommandData) {
    return this.verify_token_impl(xcmd);
  }

  private configure_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params) as TelegramConfigureParams;

    if (params.bot_token !== undefined) {
      if (typeof params.bot_token !== "string") {
        throw new XError("E_TG_BAD_PARAMS", "bot_token must be a string when provided");
      }
      const token = params.bot_token.trim();
      this._config.bot_token = token.length > 0 ? token : undefined;
    }

    this._config.mode = ensure_mode(params.mode, this._config.mode);

    const polling_obj = ensure_optional_object(params.polling, "polling");
    if (polling_obj && polling_obj.timeout_sec !== undefined) {
      this._config.polling.timeout_sec = clamp_poll_timeout(polling_obj.timeout_sec);
    }

    const webhook_obj = ensure_optional_object(params.webhook, "webhook");
    if (webhook_obj) {
      const url =
        webhook_obj.url === undefined
          ? this._config.webhook.url
          : typeof webhook_obj.url === "string"
            ? webhook_obj.url.trim()
            : ensure_non_empty_string(webhook_obj.url, "webhook.url");
      const secret_token =
        webhook_obj.secret_token === undefined
          ? this._config.webhook.secret_token
          : typeof webhook_obj.secret_token === "string"
            ? webhook_obj.secret_token.trim()
            : ensure_non_empty_string(webhook_obj.secret_token, "webhook.secret_token");

      this._config.webhook = {
        ...(typeof url === "string" && url.length > 0 ? { url } : {}),
        ...(typeof secret_token === "string" && secret_token.length > 0 ? { secret_token } : {})
      };
    }

    const admins_obj = ensure_optional_object(params.admins, "admins");
    if (admins_obj) {
      this._config.admins.chat_ids = normalize_admin_chat_ids(admins_obj.chat_ids);
    }

    _xlog.log(`${TG_LOG_PREFIX} configure`, {
      running: this._running,
      mode: this._config.mode,
      has_bot_token: Boolean(this._config.bot_token),
      timeout_sec: this._config.polling.timeout_sec,
      admins_count: this._config.admins.chat_ids.length
    });

    return {
      ok: true,
      running: this._running,
      mode: this._config.mode,
      has_bot_token: Boolean(this._config.bot_token),
      polling: {
        timeout_sec: this._config.polling.timeout_sec
      },
      admins_count: this._config.admins.chat_ids.length,
      has_webhook_secret: Boolean(this._config.webhook.secret_token)
    };
  }

  private async reload_settings_impl(_xcmd: XCommandData) {
    const was_running = this._running;
    const settings = await this.read_settings_from_kernel();
    this.apply_skill_settings(settings);

    if (was_running) {
      if (!this._config.bot_token) {
        this._running = false;
      } else if (this._config.mode === "polling") {
        this._running = true;
        this.start_poll_task();
      } else {
        this._running = false;
      }
    }

    _xlog.log(`${TG_LOG_PREFIX} reload_settings`, {
      was_running,
      running: this._running,
      mode: this._config.mode,
      has_bot_token: Boolean(this._config.bot_token),
      timeout_sec: this._config.polling.timeout_sec,
      admins_count: this._config.admins.chat_ids.length
    });

    return {
      ok: true,
      running: this._running,
      mode: this._config.mode,
      timeout_sec: this._config.polling.timeout_sec,
      admins_count: this._config.admins.chat_ids.length,
      has_bot_token: Boolean(this._config.bot_token)
    };
  }

  private async verify_token_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params);
    const raw_token = typeof params.bot_token === "string" ? params.bot_token.trim() : "";
    const has_input_token = raw_token.length > 0;
    const token = has_input_token ? raw_token : this.require_bot_token();

    try {
      const bot = await telegram_get_me({ bot_token: token });
      _xlog.log(`${TG_LOG_PREFIX} verify_token success`, {
        source: has_input_token ? "input" : "configured",
        bot_id: bot.id,
        bot_username: typeof bot.username === "string" ? bot.username : "",
        is_bot: bot.is_bot === true
      });
      return {
        ok: true,
        valid: true,
        source: has_input_token ? "input" : "configured",
        bot: {
          id: bot.id,
          username: bot.username ?? "",
          first_name: bot.first_name ?? "",
          is_bot: bot.is_bot === true
        }
      };
    } catch (err) {
      const error = format_error(err);
      _xlog.log(`${TG_LOG_PREFIX} verify_token failed`, {
        source: has_input_token ? "input" : "configured",
        error
      });
      return {
        ok: true,
        valid: false,
        source: has_input_token ? "input" : "configured",
        error
      };
    }
  }

  private async start_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params);
    const mode = ensure_mode(params.mode, this._config.mode);

    this.require_bot_token();
    this._config.mode = mode;
    _xlog.log(`${TG_LOG_PREFIX} start requested`, {
      mode,
      running: this._running,
      timeout_sec: this._config.polling.timeout_sec
    });

    if (mode === "webhook") {
      this._running = false;
      return {
        ok: false,
        mode,
        running: false,
        reason: "webhook_not_implemented_in_alpha"
      };
    }

    if (this._running) {
      _xlog.log(`${TG_LOG_PREFIX} start skipped already running`, {
        mode,
        running: true
      });
      return {
        ok: true,
        mode,
        running: true,
        already_running: true
      };
    }

    // Telegram rejects getUpdates while a webhook is active.
    // Clear webhook before polling start to keep runtime deterministic.
    try {
      await telegram_delete_webhook({
        bot_token: this.require_bot_token(),
        drop_pending_updates: false
      });
      _xlog.log(`${TG_LOG_PREFIX} deleteWebhook before polling start success`, {
        drop_pending_updates: false
      });
    } catch (err) {
      this._ctx.log("warn", "telegram deleteWebhook before polling start failed", {
        error: format_error(err)
      });
      _xlog.log(`${TG_LOG_PREFIX} deleteWebhook before polling start failed`, {
        error: format_error(err)
      });
    }

    this._running = true;
    this._consecutive_errors = 0;
    this.start_poll_task();
    _xlog.log(`${TG_LOG_PREFIX} polling started`, {
      mode,
      timeout_sec: this._config.polling.timeout_sec
    });

    return {
      ok: true,
      mode,
      running: true,
      timeout_sec: this._config.polling.timeout_sec
    };
  }

  private stop_impl(_xcmd: XCommandData) {
    this._running = false;
    _xlog.log(`${TG_LOG_PREFIX} polling stopped`, {
      mode: this._config.mode
    });
    return { ok: true, running: false, mode: this._config.mode };
  }

  private async send_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params);

    const text = ensure_non_empty_string(params.text, "text");
    const chat_id = parse_chat_id(params.chat_id ?? params.channel_thread_id, "chat_id|channel_thread_id");
    const meta = ensure_optional_object(params.meta, "meta");

    const parse_mode = ensure_parse_mode(params.parse_mode ?? meta?.parse_mode);

    const result = await telegram_send_message({
      bot_token: this.require_bot_token(),
      chat_id,
      text,
      ...(parse_mode ? { parse_mode } : {})
    });

    return {
      accepted: true,
      chat_id,
      message_id: result.message_id,
      channel_message_id: String(result.message_id)
    };
  }

  private status_impl() {
    return {
      running: this._running,
      mode: this._config.mode,
      ...(typeof this._last_update_id === "number" ? { last_update_id: this._last_update_id } : {}),
      admins_count: this._config.admins.chat_ids.length,
      timeout_sec: this._config.polling.timeout_sec
    };
  }

  private async set_webhook_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params);
    const url = ensure_non_empty_string(params.url, "url");
    const secret_token = params.secret_token === undefined ? undefined : ensure_non_empty_string(params.secret_token, "secret_token");

    this._config.webhook = {
      url,
      ...(secret_token ? { secret_token } : {})
    };

    const result = await telegram_set_webhook_stub({
      bot_token: this.require_bot_token(),
      url,
      ...(secret_token ? { secret_token } : {})
    });

    return {
      ...result,
      mode: this._config.mode
    };
  }

  private async handle_webhook_update_impl(xcmd: XCommandData) {
    const params = ensure_params(xcmd._params);
    const update = ensure_optional_object(params.update, "update");
    if (!update) {
      throw new XError("E_TG_BAD_PARAMS", "update is required");
    }

    const headers = ensure_optional_object(params.headers, "headers");
    const expected_secret = this._config.webhook.secret_token;
    if (expected_secret) {
      const actual_secret = read_header_value(headers, TG_SECRET_HEADER);
      if (!actual_secret || actual_secret !== expected_secret) {
        throw new XError("E_TG_FORBIDDEN", "Webhook secret token validation failed");
      }
    }

    await this.handle_update(update, "webhook");
    return { ok: true };
  }

  private start_poll_task(): void {
    if (this._poll_task) return;

    this._poll_task = this.poll_loop()
      .catch((err) => {
        this._ctx.log("error", "telegram polling loop crashed", { error: format_error(err) });
      })
      .finally(() => {
        this._poll_task = undefined;
      });
  }

  private async poll_loop(): Promise<void> {
    while (this._running && this._config.mode === "polling") {
      const should_continue = await this.poll_once();
      if (!should_continue) break;
    }
  }

  private async poll_once(): Promise<boolean> {
    try {
      const updates = await telegram_get_updates({
        bot_token: this.require_bot_token(),
        timeout_sec: this._config.polling.timeout_sec,
        ...(typeof this._last_update_id === "number" ? { offset: this._last_update_id + 1 } : {})
      });

      this._consecutive_errors = 0;
      if (updates.length > 0) {
        _xlog.log(`${TG_LOG_PREFIX} poll batch`, {
          updates: updates.length,
          timeout_sec: this._config.polling.timeout_sec,
          ...(typeof this._last_update_id === "number" ? { last_update_id: this._last_update_id } : {})
        });
      }

      for (const update of updates) {
        if (typeof (update as any)?.update_id === "number") {
          this._last_update_id = (update as any).update_id;
        }

        try {
          await this.handle_update(update, "polling");
        } catch (err) {
          this._ctx.log("warn", "telegram update handling failed", { error: format_error(err) });
        }
      }

      return this._running;
    } catch (err) {
      this._consecutive_errors += 1;
      this._ctx.log("warn", "telegram polling request failed", {
        error: format_error(err),
        consecutive_errors: this._consecutive_errors
      });

      if (this._consecutive_errors >= POLL_MAX_CONSECUTIVE_ERRORS) {
        this._running = false;
        this._ctx.log("error", "telegram polling stopped after max consecutive errors", {
          max_errors: POLL_MAX_CONSECUTIVE_ERRORS
        });
        return false;
      }

      return this._running;
    }
  }

  private async handle_update(update_input: unknown, source: "polling" | "webhook"): Promise<void> {
    const normalized = normalize_telegram_update(update_input);
    if (!normalized) return;

    if (normalized.update_id > (this._last_update_id ?? -1)) {
      this._last_update_id = normalized.update_id;
    }

    const is_admin = this._config.admins.chat_ids.includes(normalized.chat_id);
    _xlog.log(`${TG_LOG_PREFIX} inbound message`, {
      source,
      update_id: normalized.update_id,
      chat_id: normalized.chat_id,
      from_id: normalized.from_id,
      role: is_admin ? "admin" : "customer",
      text_preview: message_preview(normalized.text)
    });
    if (is_admin) {
      await this.handle_admin_message(normalized, source);
      return;
    }

    await this.handle_customer_message(normalized, source);
  }

  private async handle_admin_message(inbound: NormalizedTelegramInbound, source: "polling" | "webhook"): Promise<void> {
    const trimmed = inbound.text.trim();
    if (is_start_command(trimmed)) {
      await this.send_direct_message(
        inbound.chat_id,
        [
          "Welcome, admin.",
          "Available commands:",
          "/status",
          "/customers [N]",
          "/say <chat_id> <text>"
        ].join("\n")
      );
      return;
    }
    if (!trimmed.startsWith("/")) {
      await this.handle_customer_message(inbound, source);
      return;
    }

    const [raw_command, ...args] = trimmed.split(/\s+/g);
    const command = raw_command.toLowerCase();

    if (command === "/status") {
      await this.reply_admin_status(inbound.chat_id);
      return;
    }

    if (command === "/customers") {
      const limit = this.parse_limit(args[0], 10, 50);
      await this.reply_admin_customers(inbound.chat_id, limit);
      return;
    }

    if (command === "/say") {
      await this.reply_admin_say(inbound.chat_id, args);
      return;
    }

    await this.send_direct_message(
      inbound.chat_id,
      [
        "Unknown admin command.",
        "Available:",
        "/status",
        "/customers [N]",
        "/say <chat_id> <text>"
      ].join("\n")
    );
  }

  private async reply_admin_status(chat_id: string): Promise<void> {
    try {
      const status = await this._ctx.execute("agent", "status", {});
      await this.send_direct_message(chat_id, `Agent status:\n${JSON.stringify(status, null, 2)}`);
      return;
    } catch {
      const fallback = this.status_impl();
      await this.send_direct_message(chat_id, `Agent status unavailable. Telegram status:\n${JSON.stringify(fallback, null, 2)}`);
    }
  }

  private async reply_admin_customers(chat_id: string, limit: number): Promise<void> {
    try {
      const out = await this._ctx.execute("conv", "list_threads", { limit });
      const threads = Array.isArray((out as any)?.threads) ? ((out as any).threads as Array<Record<string, unknown>>) : undefined;
      if (!threads) {
        await this.send_direct_message(chat_id, "customers not_available");
        return;
      }

      if (threads.length === 0) {
        await this.send_direct_message(chat_id, "No customer threads yet.");
        return;
      }

      const lines = threads.slice(0, limit).map((thread) => {
        const thread_id = typeof thread.thread_id === "string" ? thread.thread_id : "n/a";
        const channel = typeof thread.channel === "string" ? thread.channel : "n/a";
        const channel_thread_id = typeof thread.channel_thread_id === "string" ? thread.channel_thread_id : "n/a";
        return `${thread_id} | ${channel}:${channel_thread_id}`;
      });

      await this.send_direct_message(chat_id, [`Threads (${lines.length}):`, ...lines].join("\n"));
    } catch {
      await this.send_direct_message(chat_id, "customers not_available");
    }
  }

  private async reply_admin_say(chat_id: string, args: string[]): Promise<void> {
    const target_chat_id = args[0];
    const text = args.slice(1).join(" ").trim();

    if (!target_chat_id || !text) {
      await this.send_direct_message(chat_id, "Usage: /say <chat_id> <text>");
      return;
    }

    try {
      await this._ctx.execute("channels", "send_message", {
        channel: "telegram",
        channel_thread_id: target_chat_id,
        text
      });
      await this.send_direct_message(chat_id, `Sent to ${target_chat_id} via channels.send_message`);
      return;
    } catch (err) {
      this._ctx.log("warn", "channels.send_message failed for /say, using direct telegram send", {
        target_chat_id,
        error: format_error(err)
      });
    }

    await this.send_direct_message(target_chat_id, text);
    await this.send_direct_message(chat_id, `Sent to ${target_chat_id} via telegram.send (outbound storage skipped)`);
  }

  private async handle_customer_message(inbound: NormalizedTelegramInbound, source: "polling" | "webhook"): Promise<void> {
    if (is_start_command(inbound.text)) {
      await this.send_direct_message(
        inbound.chat_id,
        "Welcome to XBot Alpha. Send any message and we will route it to the agent."
      );
      return;
    }

    const profile_name = [inbound.profile.first_name, inbound.profile.last_name]
      .filter((part): part is string => typeof part === "string" && part.trim().length > 0)
      .join(" ")
      .trim();

    const routed = await this._ctx.execute("channels", "route_inbound_message", {
      channel_id: "telegram",
      thread_key: inbound.chat_id,
      user_ref: {
        provider: "telegram",
        id: inbound.from_id,
        ...(inbound.profile.username ? { username: inbound.profile.username } : {}),
        ...(profile_name ? { name: profile_name } : {})
      },
      msg: {
        text: inbound.text,
        external_id: inbound.message_id,
        raw: {
          ...inbound.raw,
          source
        }
      }
    });

    const thread_id =
      is_plain_object(routed) && typeof routed.thread_id === "string" && routed.thread_id.trim().length > 0
        ? routed.thread_id.trim()
        : "";
    if (!thread_id) {
      throw new XError("E_TG_UPSTREAM", "channels.route_inbound_message did not return thread_id");
    }

    await this._ctx.execute("agent", "handle_inbound", {
      channel_id: "telegram",
      thread_id,
      text: inbound.text,
      user_ref: {
        provider: "telegram",
        id: inbound.from_id,
        ...(inbound.profile.username ? { username: inbound.profile.username } : {}),
        ...(profile_name ? { name: profile_name } : {})
      }
    });
  }

  private async send_direct_message(chat_id: string, text: string, parse_mode?: TelegramParseMode): Promise<void> {
    await telegram_send_message({
      bot_token: this.require_bot_token(),
      chat_id,
      text,
      ...(parse_mode ? { parse_mode } : {})
    });
  }

  private parse_limit(raw: string | undefined, fallback: number, max: number): number {
    if (!raw) return fallback;
    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.min(parsed, max);
  }

  private require_bot_token(): string {
    if (!this._config.bot_token) {
      throw new XError("E_TG_BAD_CONFIG", "bot_token is not configured. call telegram.configure first");
    }
    return this._config.bot_token;
  }

  private apply_skill_settings(settings: TelegramSkillSettings): void {
    this._config.bot_token = settings.bot_token.trim().length > 0 ? settings.bot_token.trim() : undefined;
    this._config.mode = settings.mode;
    this._config.polling.timeout_sec = clamp_poll_timeout(settings.polling.timeout_sec);
    this._config.webhook = {
      ...(settings.webhook.url.trim().length > 0 ? { url: settings.webhook.url.trim() } : {}),
      ...(settings.webhook.secret_token.trim().length > 0
        ? { secret_token: settings.webhook.secret_token.trim() }
        : {})
    };
    this._config.admins.chat_ids = normalize_admin_chat_ids(settings.admin_chat_ids);
  }

  private async read_settings_from_kernel(): Promise<TelegramSkillSettings> {
    const out = await this._ctx.execute("settings", "get_skill", {
      skill_id: this._ctx.skill.id,
      include_schema: false,
      include_masked: false
    });

    const result = is_plain_object(out) && is_plain_object(out.result) ? out.result : {};
    const settings = Object.prototype.hasOwnProperty.call(result, "settings") ? result.settings : TELEGRAM_SETTINGS_DEFAULTS;
    return normalize_telegram_skill_settings(settings);
  }
}

export default TelegramConnectorModule;
